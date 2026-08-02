const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const { syncMarketSubscriptions } = require("./marketSync");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");

const MARKET_TYPES = ["session", "bookmaker", "tied-match", "match-odd",
  "winner-market", "TOSS", "super-over", "goals"];
let running = false;
const state = { running: false, lastStartedAt: null, lastCompletedAt: null,
  lastError: null, lastResult: null };

function marketRows(response, eventsById) {
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  return rows.map((item) => {
    const event = eventsById.get(String(item?.eventId));
    const bookmaker = String(item?.type || "").toLowerCase() === "bookmaker";
    const zeroCommission = bookmaker && ["bookmaker 0%comm", "0%comm"]
      .includes(String(item?.name || "").toLowerCase());
    return {
      marketId: String(item?.id || "").trim(), eventId: Number(item?.eventId),
      sportId: Number(item?.sportId), marketName: zeroCommission ? "Bookmaker" : String(item?.name || "").trim(),
      marketType: String(item?.type || "").toLowerCase(),
      matchName: event?.eventName || null, openDate: event?.openDate || null,
      inPlay: Boolean(event?.inPlay), gameOver: Boolean(item?.gameOver), isActive: item?.isActive !== false,
      betDelay: bookmaker ? 0 : 3, minBet: 100, maxBet: bookmaker ? 25000 : 1,
      seriesId: event?.seriesId ?? null,
    };
  }).filter((item) => item.marketId && Number.isInteger(item.eventId)
    && Number.isInteger(item.sportId) && item.marketName
    && (item.marketType === "session" || (item.isActive && !item.gameOver)));
}

function oddsType(marketId) {
  const id = String(marketId).toUpperCase();
  if (id.includes("F2")) return "F2"; if (id.includes("OE")) return "OE";
  if (id.includes("F3")) return "F3"; if (id.includes("BB")) return "BB";
  if (id.includes("CC")) return "CC"; return "UNKNOWN";
}

async function upsertFancies(fancies) {
  if (!fancies.length) return { inserted: 0, updated: 0, fancyIds: [] };
  const connection = await getSourcePool().getConnection(); let inserted = 0; let updated = 0;
  try {
    await connection.beginTransaction();
    const ids = fancies.map((fancy) => fancy.marketId);
    const [existingRows] = await connection.query(
      `SELECT fancyid FROM t_matchfancy WHERE fancyid IN (${ids.map(() => "?").join(",")})`, ids,
    );
    const existing = new Set(existingRows.map((row) => String(row.fancyid)));
    for (const fancy of fancies) {
      const active = fancy.isActive && !fancy.gameOver;
      if (existing.has(fancy.marketId)) {
        await connection.execute(
          `UPDATE t_matchfancy SET name=?, oddstype=?, eventid=?, isactive=?, isshow=?, is_show=?,
             matchname=?, sportid=?, updatedon=NOW() WHERE fancyid=?`,
          [fancy.marketName, oddsType(fancy.marketId), fancy.eventId, active, true, true,
            fancy.matchName, fancy.sportId, fancy.marketId],
        );
        updated += 1; continue;
      }
      // Match the Java service: inactive/game-over sessions can deactivate an existing fancy,
      // but must not create a new historical row.
      if (!active) continue;
      await connection.execute(
        `INSERT INTO t_matchfancy
          (fancyid,name,oddstype,status,maxliabilityper_market,betdelay,minbet,maxbet,eventid,
           issuspendedbyadmin,isactive,mtype,isshow,is_show,suspendedby,createdon,matchname,
           sportid,provider,isbettable,isplay,maxliabilityperbet)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [fancy.marketId, fancy.marketName, oddsType(fancy.marketId), "OPEN", 100000, 0, 100,
          100000, fancy.eventId, false, active,
          "player", true, true, "", new Date(),
          fancy.matchName, fancy.sportId, "RS", 1, fancy.inPlay, 100000],
      );
      inserted += 1;
    }
    // Migrate only IDs positively identified as provider session markets in this response.
    await connection.query(`DELETE FROM t_market WHERE marketid IN (${ids.map(() => "?").join(",")})`, ids);
    await connection.commit(); return { inserted, updated, fancyIds: ids };
  } catch (error) { await connection.rollback(); throw error; }
  finally { connection.release(); }
}

async function upsertMarkets(markets) {
  if (!markets.length) return { inserted: 0, updated: 0, marketIds: [] };
  const connection = await getSourcePool().getConnection(); let inserted = 0; let updated = 0;
  try {
    await connection.beginTransaction();
    const ids = markets.map((market) => market.marketId);
    const [existingRows] = await connection.query(
      `SELECT marketid FROM t_market WHERE marketid IN (${ids.map(() => "?").join(",")})`, ids,
    );
    const existing = new Set(existingRows.map((row) => String(row.marketid)));
    for (const market of markets) {
      if (existing.has(market.marketId)) {
        await connection.execute(
          `UPDATE t_market SET marketname=?, matchname=?, opendate=?, sportid=?, eventid=?, seriesid=?,
             inplay=IF(inplay=1,1,?), updatedon=NOW() WHERE marketid=?`,
          [market.marketName, market.matchName, market.openDate, market.sportId, market.eventId,
            market.seriesId, market.inPlay, market.marketId],
        );
        updated += 1; continue;
      }
      await connection.execute(
        `INSERT INTO t_market
          (marketid,sportid,eventid,marketname,matchname,status,isactive,createdon,updatedon,opendate,
           minbet,maxbet,betdelay,inplay,minbetrate,maxbetrate,is_redis_updated,display_message,
           issuspended,is_rolled_back,maximum_profit,maximumprofit,betlock,seriesid)
         VALUES (?,?,?,?,?,?,?,NOW(),NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [market.marketId, market.sportId, market.eventId, market.marketName, market.matchName,
          true, true, market.openDate, market.minBet, market.maxBet, market.betDelay, market.inPlay,
          1, 500, false, "", false, false, 0, 1000000, false, market.seriesId],
      );
      inserted += 1;
    }
    await connection.commit(); return { inserted, updated, marketIds: ids };
  } catch (error) { await connection.rollback(); throw error; }
  finally { connection.release(); }
}

async function marketsMissingRunners(marketIds) {
  if (!marketIds.length) return [];
  const [rows] = await getSourcePool().query(
    `SELECT DISTINCT marketid FROM t_selectionid WHERE marketid IN (${marketIds.map(() => "?").join(",")})`,
    marketIds,
  );
  const present = new Set(rows.map((row) => String(row.marketid)));
  return marketIds.filter((marketId) => !present.has(String(marketId)));
}

async function fetchAndStoreRunners(marketIds) {
  const missing = await marketsMissingRunners(marketIds);
  const responses = await Promise.allSettled(missing.map(async (marketId) => ({
    marketId, response: await provider.runners(marketId),
  })));
  const runners = responses.flatMap((result) => {
    if (result.status !== "fulfilled") return [];
    const { marketId, response } = result.value;
    const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
    return rows.map((runner) => ({ marketId,
      selectionId: Number(runner?.runnerId ?? runner?.selectionId),
      runnerName: String(runner?.name ?? runner?.nation ?? "").trim(),
    })).filter((runner) => Number.isInteger(runner.selectionId) && runner.runnerName);
  });
  if (runners.length) {
    const connection = await getSourcePool().getConnection();
    try {
      await connection.beginTransaction();
      for (const runner of runners) {
        const [existing] = await connection.execute(
          "SELECT id FROM t_selectionid WHERE marketid=? AND selectionid=? LIMIT 1",
          [runner.marketId, runner.selectionId],
        );
        if (existing.length) {
          await connection.execute("UPDATE t_selectionid SET runner_name=? WHERE id=?",
            [runner.runnerName, existing[0].id]);
        } else {
          await connection.execute(
            "INSERT INTO t_selectionid (createdon,marketid,runner_name,selectionid,is_redis_updated) VALUES (NOW(),?,?,?,?)",
            [runner.marketId, runner.runnerName, runner.selectionId, false],
          );
        }
      }
      await connection.commit();
    } catch (error) { await connection.rollback(); throw error; }
    finally { connection.release(); }
  }
  return { requestedMarkets: missing.length, storedRunners: runners.length,
    failedMarkets: responses.filter((result) => result.status === "rejected").length };
}

async function syncMarketDiscovery(events) {
  if (running) return { skipped: true, reason: "already-running" };
  running = true; state.running = true; state.lastStartedAt = new Date().toISOString(); state.lastError = null;
  try {
    const eventsById = new Map(events.map((event) => [String(event.eventId), event]));
    const eventIds = [...eventsById.keys()].map(Number); const batchSize = 50; const discovered = [];
    for (let index = 0; index < eventIds.length; index += batchSize) {
      const response = await provider.markets({ eids: eventIds.slice(index, index + batchSize), type: MARKET_TYPES });
      discovered.push(...marketRows(response, eventsById));
    }
    const unique = [...new Map(discovered.map((market) => [market.marketId, market])).values()];
    const fancies = unique.filter((market) => market.marketType === "session");
    const regularMarkets = unique.filter((market) => market.marketType !== "session");
    const persisted = await upsertMarkets(regularMarkets);
    const fancyPersisted = await upsertFancies(fancies);
    const runnerResult = await fetchAndStoreRunners(persisted.marketIds);
    const subscriptionResult = await syncMarketSubscriptions();
    const subscription = { total: subscriptionResult.total, requested: subscriptionResult.requested,
      newlySubscribed: subscriptionResult.newlySubscribed,
      providerSkipped: subscriptionResult.providerSkipped,
      active: Array.isArray(subscriptionResult.activeMarketIds) ? subscriptionResult.activeMarketIds.length : 0 };
    const activeFancies = fancies.filter((fancy) => fancy.isActive && !fancy.gameOver).length;
    const result = { skipped: false, events: eventIds.length, markets: regularMarkets.length,
      fancies: activeFancies, sessionRecords: fancies.length,
      inserted: persisted.inserted, updated: persisted.updated,
      fancyInserted: fancyPersisted.inserted, fancyUpdated: fancyPersisted.updated,
      runners: runnerResult, subscription };
    state.lastResult = result; state.lastCompletedAt = new Date().toISOString();
    logger.info("[MarketDiscovery] completed", result); return result;
  } catch (error) {
    state.lastError = error.message; state.lastCompletedAt = new Date().toISOString();
    logger.error("[MarketDiscovery] failed", { error: error.message }); throw error;
  } finally { running = false; state.running = false; }
}

async function fetchActiveEventsForMarketDiscovery() {
  const sportIds = String(process.env.SPORT_IDS || "1,2,4").split(",")
    .map((value) => Number(value.trim())).filter(Number.isFinite);
  if (!sportIds.length) return [];
  const [rows] = await getSourcePool().query(
    `SELECT eventid,eventname,sportid,seriesid,open_date,in_play
       FROM t_event
      WHERE isactive=? AND sportid IN (${sportIds.map(() => "?").join(",")})
      ORDER BY eventid ASC`,
    [true, ...sportIds],
  );
  return rows.map((row) => ({
    eventId: Number(row.eventid), eventName: String(row.eventname || "").trim(),
    sportId: Number(row.sportid), seriesId: Number(row.seriesid),
    openDate: row.open_date, inPlay: Boolean(row.in_play),
  })).filter((event) => Number.isInteger(event.eventId) && event.eventName);
}

async function syncStoredEventMarkets() {
  const events = await fetchActiveEventsForMarketDiscovery();
  return syncMarketDiscovery(events);
}

function startMarketDiscoverySync() {
  const { expression } = cronConfig.marketDiscovery;
  const task = cron.schedule(expression, () => void syncStoredEventMarkets().catch(() => {}));
  logger.info("[MarketDiscovery] scheduled", { expression });
  return task;
}

function getMarketDiscoveryStatus() { return { ...state }; }

module.exports = { MARKET_TYPES, marketRows, oddsType, upsertMarkets, upsertFancies, fetchAndStoreRunners,
  fetchActiveEventsForMarketDiscovery, syncMarketDiscovery, syncStoredEventMarkets,
  startMarketDiscoverySync, getMarketDiscoveryStatus };
