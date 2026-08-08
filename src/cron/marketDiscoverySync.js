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
  "session", "khado", "odd-even", "cricket-casino", "ball-by-ball", "other-market", "meter",
]);
const REGULAR_MARKET_TYPES = ["bookmaker", "tied-match", "match-odd", "winner-market",
  "TOSS", "super-over", "goals", "line-market", "completed-match"];
const FANCY_MARKET_REQUESTS = ["session", "khado", "odd-even", "cricket-casino", "ball-by-ball"]
  .map((type) => [type]);
const MARKET_TYPES = [...FANCY_MARKET_TYPES, ...REGULAR_MARKET_TYPES];
const DISCOVERABLE_MARKET_TYPES = new Set(MARKET_TYPES.map((type) => String(type).toLowerCase()));
let running = false;
const state = { running: false, lastStartedAt: null, lastCompletedAt: null,
  lastError: null, lastResult: null };
const DB_WRITE_BATCH_SIZE = Math.max(50, Number(process.env.MARKET_DB_WRITE_BATCH_SIZE || 500));

function chunks(items, size = DB_WRITE_BATCH_SIZE) {
  const result = [];
  for (let index = 0; index < items.length; index += size) result.push(items.slice(index, index + size));
  return result;
}

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
    const displayMessage = item?.inPlayFilter == null
      ? null : String(item.inPlayFilter).trim().slice(0, 255) || null;
    const marketName = zeroCommission ? "Bookmaker"
      : providedName || (inferredBookmaker2 ? "Bookmaker2" : `Market ${marketId}`);
    return {
      marketId, eventId: Number(item?.eventId),
      sportId: Number(item?.sportId), marketName,
      marketType,
      matchName: event?.eventName || null, openDate: event?.openDate || null,
      inPlay: Boolean(event?.inPlay), gameOver: Boolean(item?.gameOver), isActive: item?.isActive !== false,
      betDelay: marketType === "line-market" ? 5 : bookmaker || fancy ? 0 : 3, minBet: 100,
      maxBet: fancy ? 100000 : bookmaker ? 25000 : 1,
      displayMessage,
      seriesId: event?.seriesId ?? null,
    };
  }).filter((item) => redisStore.validMarketIdentifier(item.marketId) && Number.isInteger(item.eventId)
    && Number.isInteger(item.sportId) && item.marketName
    && (DISCOVERABLE_MARKET_TYPES.has(item.marketType) || (item.isActive && !item.gameOver)));
}

// The vendor can return contradictory states for the same market between the
// unfiltered request and a typed request. Treat the responses as a union: a
// market remains active when any response says it is active. It is deactivated
// only when every response containing that ID agrees that it is inactive.
function mergeDiscoveredMarkets(markets) {
  const merged = new Map();
  for (const market of markets || []) {
    const current = merged.get(market.marketId);
    if (!current) {
      merged.set(market.marketId, market);
      continue;
    }
    const currentActive = current.isActive && !current.gameOver;
    const incomingActive = market.isActive && !market.gameOver;
    const preferIncomingMetadata = current.marketType === "unknown" && market.marketType !== "unknown";
    const metadata = preferIncomingMetadata ? market : current;
    merged.set(market.marketId, {
      ...metadata,
      isActive: currentActive || incomingActive,
      gameOver: !currentActive && !incomingActive && (current.gameOver || market.gameOver),
    });
  }
  return [...merged.values()];
}

function oddsType(marketId) {
  const id = String(marketId).toUpperCase();
  if (id.includes("F2")) return "F2"; if (id.includes("OE")) return "OE";
  if (id.includes("KD")) return "KD";
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
    const writable = fancies.filter((fancy) => existing.has(fancy.marketId)
      || (fancy.isActive && !fancy.gameOver));
    inserted = writable.filter((fancy) => !existing.has(fancy.marketId)).length;
    updated = writable.length - inserted;
    for (const batch of chunks(writable)) {
      const values = batch.map((fancy) => [
        fancy.marketId, fancy.marketName, oddsType(fancy.marketId), "OPEN", 100000, 0, 100,
        100000, fancy.eventId, false, fancy.isActive && !fancy.gameOver,
        fancy.marketType, true, true, "", fancy.displayMessage, new Date(),
        fancy.matchName, fancy.sportId, "RS", 1, fancy.inPlay, 100000,
      ]);
      await connection.query(
        `INSERT INTO t_matchfancy
          (fancyid,name,oddstype,status,maxliabilityper_market,betdelay,minbet,maxbet,eventid,
           issuspendedbyadmin,isactive,mtype,isshow,is_show,suspendedby,remarks,createdon,matchname,
           sportid,provider,isbettable,isplay,maxliabilityperbet) VALUES ?
         ON DUPLICATE KEY UPDATE name=VALUES(name),oddstype=VALUES(oddstype),eventid=VALUES(eventid),
           isactive=VALUES(isactive),isshow=VALUES(isshow),is_show=VALUES(is_show),
           matchname=VALUES(matchname),sportid=VALUES(sportid),mtype=VALUES(mtype),
           remarks=VALUES(remarks),updatedon=NOW()`,
        [values],
      );
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
    const writable = markets.filter((market) => existing.has(market.marketId)
      || (market.isActive && !market.gameOver));
    inserted = writable.filter((market) => !existing.has(market.marketId)).length;
    updated = writable.length - inserted;
    for (const market of writable) {
      if (existing.get(market.marketId) && (!market.isActive || market.gameOver)) {
        deactivatedMarketIds.push(market.marketId);
      }
    }
    for (const batch of chunks(writable)) {
      const values = batch.map((market) => [
        market.marketId, market.sportId, market.eventId, market.marketName, market.matchName,
        true, market.isActive && !market.gameOver, new Date(), new Date(), market.openDate,
        market.minBet, market.maxBet, market.betDelay, market.inPlay, 1, 500, false,
        market.displayMessage, false, false, 0, 1000000, false, market.seriesId,
      ]);
      await connection.query(
        `INSERT INTO t_market
          (marketid,sportid,eventid,marketname,matchname,status,isactive,createdon,updatedon,opendate,
           minbet,maxbet,betdelay,inplay,minbetrate,maxbetrate,is_redis_updated,display_message,
           issuspended,is_rolled_back,maximum_profit,maximumprofit,betlock,seriesid) VALUES ?
         ON DUPLICATE KEY UPDATE marketname=VALUES(marketname),matchname=VALUES(matchname),
           opendate=VALUES(opendate),sportid=VALUES(sportid),eventid=VALUES(eventid),
           seriesid=VALUES(seriesid),inplay=IF(inplay=1,1,VALUES(inplay)),
           isactive=VALUES(isactive),betdelay=VALUES(betdelay),
           display_message=VALUES(display_message),updatedon=NOW()`,
        [values],
      );
    }
    await connection.commit(); return { inserted, updated,
      marketIds: markets.filter((market) => market.isActive && !market.gameOver).map((market) => market.marketId),
      deactivatedMarketIds };
  } catch (error) { await connection.rollback(); throw error; }
  finally { connection.release(); }
}

function bookmaker2BaseMarketId(marketId) {
  const normalized = String(marketId || "").trim();
  return /-BM2$/i.test(normalized) ? normalized.replace(/-BM2$/i, "") : null;
}

function runnerSourceMarketId(marketId) {
  return bookmaker2BaseMarketId(marketId) || String(marketId);
}

function runnerLookupMarketIds(marketIds) {
  return [...new Set((marketIds || []).flatMap((marketId) => {
    const normalized = String(marketId);
    const baseMarketId = bookmaker2BaseMarketId(normalized);
    return baseMarketId ? [normalized, baseMarketId] : [normalized];
  }))];
}

function enforceBookmaker2Eligibility(markets) {
  const byId = new Map((markets || []).map((market) => [String(market.marketId), market]));
  return (markets || []).map((market) => {
    const baseMarketId = bookmaker2BaseMarketId(market.marketId);
    if (!baseMarketId) return market;
    const baseMarket = byId.get(baseMarketId);
    // Only the Match Odds clone belongs in the Bookmaker2 section. When the
    // vendor also exposes BM2 clones for Tied/Completed/etc., retain them as
    // explicit inactive records so reconciliation removes old Redis rows.
    if (baseMarket && baseMarket.marketType !== "match-odd") {
      return { ...market, isActive: false };
    }
    return market;
  });
}

async function marketsMissingRunners(marketIds) {
  if (!marketIds.length) return [];
  const lookupIds = runnerLookupMarketIds(marketIds);
  const [rows] = await getSourcePool().query(
    `SELECT DISTINCT marketid FROM t_selectionid WHERE marketid IN (${lookupIds.map(() => "?").join(",")})`,
    lookupIds,
  );
  const present = new Set(rows.map((row) => String(row.marketid)));
  return marketIds.filter((marketId) => {
    const normalized = String(marketId);
    const baseMarketId = bookmaker2BaseMarketId(normalized);
    return !present.has(normalized) && !(baseMarketId && present.has(baseMarketId));
  });
}

async function fetchAndStoreRunners(marketIds) {
  const missing = await marketsMissingRunners(marketIds);
  const responses = await Promise.allSettled(missing.map(async (marketId) => ({
    marketId, response: await provider.runners(runnerSourceMarketId(marketId)),
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
  const lookupIds = runnerLookupMarketIds(ids);
  const [rows] = await getSourcePool().query(
    `SELECT marketid,selectionid,runner_name FROM t_selectionid
      WHERE marketid IN (${lookupIds.map(() => "?").join(",")}) ORDER BY id ASC`, lookupIds,
  );
  const byMarket = new Map();
  for (const row of rows) {
    const marketId = String(row.marketid);
    if (!byMarket.has(marketId)) byMarket.set(marketId, []);
    byMarket.get(marketId).push({ selectionId: row.selectionid, runnerName: row.runner_name });
  }
  return markets.map((market) => {
    const marketId = String(market.marketId);
    const exactRunners = byMarket.get(marketId) || [];
    const baseMarketId = bookmaker2BaseMarketId(marketId);
    return { ...market, runners: exactRunners.length
      ? exactRunners : baseMarketId ? byMarket.get(baseMarketId) || [] : [] };
  });
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

function inactiveLineMarkets(markets) {
  return (markets || []).filter((market) => market.marketType === "line-market"
    && (!market.isActive || market.gameOver));
}

async function reconcileInactiveLineMarkets(markets) {
  const inactive = inactiveLineMarkets(markets);
  if (!inactive.length) return { checked: 0, deactivated: 0, removed: 0 };
  const persisted = await upsertMarkets(inactive);
  redisStore.invalidateMarkets([...inactive.map((market) => market.marketId),
    ...persisted.deactivatedMarketIds]);
  const redisDefinitions = await redisStore.reconcileRegularDefinitions(inactive);
  if (persisted.deactivatedMarketIds.length) {
    await unsubscribeEventMarkets(persisted.deactivatedMarketIds);
  }
  await Promise.allSettled((redisDefinitions.changedEventIds || [])
    .map((eventId) => publishEventSnapshot(eventId)));
  return { checked: inactive.length, deactivated: persisted.deactivatedMarketIds.length,
    removed: redisDefinitions.removed };
}

async function syncMarketDiscovery(events) {
  if (running) return { skipped: true, reason: "already-running" };
  running = true; state.running = true; state.lastStartedAt = new Date().toISOString(); state.lastError = null;
  try {
    const eventsById = new Map(events.map((event) => [String(event.eventId), event]));
    const eventIds = [...eventsById.keys()].map(Number); const batchSize = 50; const discovered = [];
    const eventBatches = chunks(eventIds, batchSize);

    // First pass: one unfiltered request per event batch. Inactive line markets are
    // reconciled immediately instead of waiting for every fancy family and thousands
    // of database upserts in the full discovery pass.
    const primaryResponses = await Promise.all(eventBatches.map((eids) => provider.markets({ eids })));
    const primaryRows = primaryResponses.flatMap((response) => marketRows(response, eventsById));
    discovered.push(...primaryRows);
    const fastLineReconciliation = await reconcileInactiveLineMarkets(primaryRows);

    // Second pass: fetch typed fallbacks needed for families the vendor can omit from
    // the unfiltered response. These no longer delay line-market deactivation.
    const typedResponses = await Promise.all(eventBatches.map(async (eids) => {
      // The vendor omits some advanced markets (notably odd-even) when every type is
      // requested together. Fetch each fancy family independently, matching its API behavior.
      // Omitting `type` returns undocumented families such as other-market/F3,
      // meter/MT, line-market and BM2. Keep the explicit fancy calls as a fallback
      // because the vendor has previously omitted odd-even from combined responses.
      const requests = [REGULAR_MARKET_TYPES, ...FANCY_MARKET_REQUESTS];
      return Promise.all(requests.map((type) => provider.markets({ eids, type })));
    }));
    for (const responses of typedResponses) {
      for (const response of responses) discovered.push(...marketRows(response, eventsById));
    }
    const unique = enforceBookmaker2Eligibility(mergeDiscoveredMarkets(discovered));
    const fancies = unique.filter((market) => FANCY_MARKET_TYPES.has(market.marketType));
    const regularMarkets = unique.filter((market) => !FANCY_MARKET_TYPES.has(market.marketType));
    const persisted = await upsertMarkets(regularMarkets);
    redisStore.invalidateMarkets([...persisted.marketIds, ...persisted.deactivatedMarketIds]);
    const fancyPersisted = await upsertFancies(fancies);
    redisStore.invalidateMarkets(fancyPersisted.fancyIds);
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
      fastLineReconciliation, redisDefinitions, regularDefinitions,
      tossDefinitions, runners: runnerResult, subscription };
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
  marketRows, mergeDiscoveredMarkets, oddsType, upsertMarkets, upsertFancies, fetchAndStoreRunners,
  bookmaker2BaseMarketId, runnerSourceMarketId, runnerLookupMarketIds, enforceBookmaker2Eligibility,
  regularMarketsWithRunners, seedTossMarkets, inactiveLineMarkets,
  fetchActiveEventsForMarketDiscovery, syncMarketDiscovery, syncStoredEventMarkets,
  startMarketDiscoverySync, getMarketDiscoveryStatus };
