const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const { syncMarketSubscriptions } = require("./marketSync");
const { unsubscribeEventMarkets } = require("../services/marketSubscriptionService");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const redisStore = require("../config/redis");
const { publishEventSnapshot } = require("../services/frontendSocketService");

const FANCY_MARKET_TYPES = new Set([
  "session", "odd-even", "cricket-casino", "ball-by-ball", "other-market", "meter",
]);
const REGULAR_MARKET_TYPES = ["bookmaker", "tied-match", "match-odd", "winner-market",
  "TOSS", "super-over", "goals", "line-market", "completed-match"];
const FANCY_MARKET_REQUESTS = ["session", "odd-even", "cricket-casino", "ball-by-ball"]
  .map((type) => [type]);
const MARKET_TYPES = [...FANCY_MARKET_TYPES, ...REGULAR_MARKET_TYPES];
const DISCOVERABLE_MARKET_TYPES = new Set(MARKET_TYPES.map((type) => String(type).toLowerCase()));
let running = false;
const state = { running: false, lastStartedAt: null, lastCompletedAt: null,
  lastError: null, lastResult: null };

function marketRows(response, eventsById) {
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  return rows.map((item) => {
    const event = eventsById.get(String(item?.eventId));
    const marketId = String(item?.id || "").trim();
    const inferredBookmaker2 = /-BM2$/i.test(marketId);
    const marketType = String(item?.type || (inferredBookmaker2 ? "bookmaker" : "unknown")).toLowerCase();
    const bookmaker = marketType === "bookmaker";
    const fancy = FANCY_MARKET_TYPES.has(marketType);
    const zeroCommission = bookmaker && ["bookmaker 0%comm", "0%comm"]
      .includes(String(item?.name || "").toLowerCase());
    const providedName = String(item?.name || "").trim();
    const marketName = zeroCommission ? "Bookmaker"
      : providedName || (inferredBookmaker2 ? "Bookmaker2" : `Market ${marketId}`);
    return {
      marketId, eventId: Number(item?.eventId),
      sportId: Number(item?.sportId), marketName,
      marketType,
      matchName: event?.eventName || null, openDate: event?.openDate || null,
      inPlay: Boolean(event?.inPlay), gameOver: Boolean(item?.gameOver), isActive: item?.isActive !== false,
      betDelay: bookmaker || fancy ? 0 : 3, minBet: 100,
      maxBet: fancy ? 100000 : bookmaker ? 25000 : 1,
      seriesId: event?.seriesId ?? null,
    };
  }).filter((item) => redisStore.validMarketIdentifier(item.marketId) && Number.isInteger(item.eventId)
    && Number.isInteger(item.sportId) && item.marketName
    && (DISCOVERABLE_MARKET_TYPES.has(item.marketType) || (item.isActive && !item.gameOver)));
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
  if (!markets.length) return { inserted: 0, updated: 0, marketIds: [], deactivatedMarketIds: [] };
  const connection = await getSourcePool().getConnection(); let inserted = 0; let updated = 0;
  const deactivatedMarketIds = [];
  try {
    await connection.beginTransaction();
    const ids = markets.map((market) => market.marketId);
    const [existingRows] = await connection.query(
      `SELECT marketid,isactive FROM t_market WHERE marketid IN (${ids.map(() => "?").join(",")})`, ids,
    );
    const existing = new Map(existingRows.map((row) => [String(row.marketid), Number(row.isactive) === 1]));
    for (const market of markets) {
      const active = market.isActive && !market.gameOver;
      if (existing.has(market.marketId)) {
        await connection.execute(
          `UPDATE t_market SET marketname=?, matchname=?, opendate=?, sportid=?, eventid=?, seriesid=?,
             inplay=IF(inplay=1,1,?), isactive=?, updatedon=NOW() WHERE marketid=?`,
          [market.marketName, market.matchName, market.openDate, market.sportId, market.eventId,
            market.seriesId, market.inPlay, active, market.marketId],
        );
        if (existing.get(market.marketId) && !active) deactivatedMarketIds.push(market.marketId);
        updated += 1; continue;
      }
      if (!active) continue;
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
    await connection.commit(); return { inserted, updated,
      marketIds: markets.filter((market) => market.isActive && !market.gameOver).map((market) => market.marketId),
      deactivatedMarketIds };
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

async function regularMarketsWithRunners(markets) {
  const ids = [...new Set((markets || []).map((market) => String(market.marketId)).filter(Boolean))];
  if (!ids.length) return [];
  const [rows] = await getSourcePool().query(
    `SELECT marketid,selectionid,runner_name FROM t_selectionid
      WHERE marketid IN (${ids.map(() => "?").join(",")}) ORDER BY id ASC`, ids,
  );
  const byMarket = new Map();
  for (const row of rows) {
    const marketId = String(row.marketid);
    if (!byMarket.has(marketId)) byMarket.set(marketId, []);
    byMarket.get(marketId).push({ selectionId: row.selectionid, runnerName: row.runner_name });
  }
  return markets.map((market) => ({ ...market, runners: byMarket.get(String(market.marketId)) || [] }));
}

async function seedTossMarkets(markets) {
  const tossMarkets = (markets || []).filter((market) =>
    String(market.marketName || "").trim().toUpperCase() === "TOSS"
      && market.isActive && !market.gameOver);
  const results = await Promise.allSettled(tossMarkets.map(async (market) => {
    const response = await provider.runners(market.marketId);
    const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
    if (!rows.length) return false;
    return redisStore.writeTick({ eid: market.eventId, mid: market.marketId, s: true,
      r: rows.map((runner) => ({ rid: runner.runnerId ?? runner.selectionId,
        na: runner.name ?? runner.nation, back: runner.back, lay: runner.lay,
        b1: runner.b1, l1: runner.l1, bs1: runner.br1, ls1: runner.lr1, sb: runner.sb })) });
  }));
  return { requested: tossMarkets.length,
    seeded: results.filter((result) => result.status === "fulfilled" && result.value).length,
    failed: results.filter((result) => result.status === "rejected").length };
}

async function syncMarketDiscovery(events) {
  if (running) return { skipped: true, reason: "already-running" };
  running = true; state.running = true; state.lastStartedAt = new Date().toISOString(); state.lastError = null;
  try {
    const eventsById = new Map(events.map((event) => [String(event.eventId), event]));
    const eventIds = [...eventsById.keys()].map(Number); const batchSize = 50; const discovered = [];
    for (let index = 0; index < eventIds.length; index += batchSize) {
      const eids = eventIds.slice(index, index + batchSize);
      // The vendor omits some advanced markets (notably odd-even) when every type is
      // requested together. Fetch each fancy family independently, matching its API behavior.
      // Omitting `type` returns undocumented families such as other-market/F3,
      // meter/MT, line-market and BM2. Keep the explicit fancy calls as a fallback
      // because the vendor has previously omitted odd-even from combined responses.
      const requests = [null, REGULAR_MARKET_TYPES, ...FANCY_MARKET_REQUESTS];
      const responses = await Promise.all(requests.map((type) =>
        provider.markets(type === null ? { eids } : { eids, type })));
      for (const response of responses) discovered.push(...marketRows(response, eventsById));
    }
    const unique = [...new Map(discovered.map((market) => [market.marketId, market])).values()];
    const fancies = unique.filter((market) => FANCY_MARKET_TYPES.has(market.marketType));
    const regularMarkets = unique.filter((market) => !FANCY_MARKET_TYPES.has(market.marketType));
    const persisted = await upsertMarkets(regularMarkets);
    redisStore.invalidateMarkets(persisted.deactivatedMarketIds);
    const fancyPersisted = await upsertFancies(fancies);
    const redisDefinitions = await redisStore.reconcileFancyDefinitions(fancies);
    const runnerResult = await fetchAndStoreRunners(persisted.marketIds);
    const regularDefinitions = await redisStore.reconcileRegularDefinitions(
      await regularMarketsWithRunners(regularMarkets),
    );
    if (persisted.deactivatedMarketIds.length) {
      await unsubscribeEventMarkets(persisted.deactivatedMarketIds);
    }
    const changedEventIds = [...new Set([
      ...(redisDefinitions.changedEventIds || []),
      ...(regularDefinitions.changedEventIds || []),
    ])];
    await Promise.allSettled(changedEventIds.map((eventId) => publishEventSnapshot(eventId)));
    const tossDefinitions = await seedTossMarkets(regularMarkets);
    const subscriptionResult = await syncMarketSubscriptions();
    const subscription = { total: subscriptionResult.total, requested: subscriptionResult.requested,
      newlySubscribed: subscriptionResult.newlySubscribed,
      providerSkipped: subscriptionResult.providerSkipped,
      active: Array.isArray(subscriptionResult.activeMarketIds) ? subscriptionResult.activeMarketIds.length : 0 };
    const activeFancies = fancies.filter((fancy) => fancy.isActive && !fancy.gameOver).length;
    const result = { skipped: false, events: eventIds.length, markets: regularMarkets.length,
      fancies: activeFancies, sessionRecords: fancies.length,
      inserted: persisted.inserted, updated: persisted.updated,
      deactivated: persisted.deactivatedMarketIds.length,
      fancyInserted: fancyPersisted.inserted, fancyUpdated: fancyPersisted.updated,
      redisDefinitions, regularDefinitions, tossDefinitions, runners: runnerResult, subscription };
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

module.exports = { MARKET_TYPES, REGULAR_MARKET_TYPES, FANCY_MARKET_TYPES, FANCY_MARKET_REQUESTS,
  marketRows, oddsType, upsertMarkets, upsertFancies, fetchAndStoreRunners,
  regularMarketsWithRunners, seedTossMarkets,
  fetchActiveEventsForMarketDiscovery, syncMarketDiscovery, syncStoredEventMarkets,
  startMarketDiscoverySync, getMarketDiscoveryStatus };
