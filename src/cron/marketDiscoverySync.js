const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const { unsubscribeEventMarkets } = require("../services/marketSubscriptionService");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const redisStore = require("../config/redis");
const { publishEventSnapshot } = require("../services/frontendSocketService");
const { integer, csvIntegers } = require("../config/env");
const { eventInWindow, eventWindowSql } = require("../utils/eventWindow");
const { setBounded } = require("../utils/boundedMap");
const { retryDeadlock } = require("../utils/dbRetry");
const { handleSocketGameOver } = require("./resultSync");
const lifecycle = require("../services/eventLifecyclePolicy");
const {
  FANCY_MARKET_TYPES,
  REGULAR_MARKET_TYPES,
  FANCY_MARKET_REQUESTS,
  MARKET_TYPES,
  DISCOVERABLE_MARKET_TYPES,
} = require("../config/marketTypes");
let running = false;
let liveCleanupRunning = false;
const queuedDiscoveryLanes = new Set();
const runnerMisses = new Map();
const tossSeeded = new Map();
const missingLineMarketPasses = new Map();
const discoveryFingerprints = new Map();
const lastFullDiscoveryAt = new Map();
const lastPrimaryDiscoveryAt = new Map();
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};
const DB_WRITE_BATCH_SIZE = integer("MARKET_DB_WRITE_BATCH_SIZE", 500, { min: 50, max: 5000 });
const DISCOVERY_CACHE_LIMIT = integer("MARKET_DISCOVERY_CACHE_LIMIT", 50000, { min: 1000 });
const MISSING_LINE_MARKET_PASSES = integer("MARKET_MISSING_LINE_PASSES", 2, { min: 1, max: 20 });
const DISCOVERY_CONCURRENCY = integer("MARKET_DISCOVERY_CONCURRENCY", 4, { min: 1, max: 16 });

async function settleWithConcurrency(items, mapper, concurrency = DISCOVERY_CONCURRENCY) {
  const results = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      try {
        results[index] = { status: "fulfilled", value: await mapper(items[index], index) };
      } catch (reason) {
        results[index] = { status: "rejected", reason };
      }
    }
  }
  const workerCount = Math.min(Math.max(1, concurrency), items.length);
  await Promise.all(Array.from({ length: workerCount }, worker));
  return results;
}

function discoveryEventBatchSize(lane, sportId) {
  // The provider caps large market responses. A single cricket event can contain
  // more than a thousand current and historical markets, so batching cricket
  // events can silently truncate both future and active snapshots and leave
  // valid markets hidden until the event enters the active discovery window.
  if (lane === "active" || Number(sportId) === cricketSportId()) return 1;
  return integer("MARKET_DISCOVERY_EVENT_BATCH_SIZE", 10, { min: 1, max: 100 });
}

function cricketSportId() {
  return integer("CRICKET_SPORT_ID", 4, { min: 1 });
}

function prioritizedDiscoveryEvents(events, lane = "active", now = Date.now()) {
  const cricketId = cricketSportId();
  const nonCricketIntervalMs = integer("NON_CRICKET_DISCOVERY_MS", 30000, { min: 15000 });
  return (events || [])
    .filter((event) => {
      const sportId = Number(event.sportId);
      if (lane !== "active" || sportId === cricketId) return true;
      return now - (lastPrimaryDiscoveryAt.get(`${lane}:${sportId}`) || 0) >= nonCricketIntervalMs;
    })
    .sort((left, right) => {
      const leftPriority = Number(left.sportId) === cricketId ? 0 : 1;
      const rightPriority = Number(right.sportId) === cricketId ? 0 : 1;
      return leftPriority - rightPriority || Number(left.eventId) - Number(right.eventId);
    });
}

function discoveryEventBatches(events, lane) {
  const bySport = new Map();
  for (const event of events || []) {
    const sportId = Number(event.sportId);
    if (!bySport.has(sportId)) bySport.set(sportId, []);
    bySport.get(sportId).push(Number(event.eventId));
  }
  return [...bySport.entries()].flatMap(([sportId, eventIds]) =>
    chunks(eventIds, discoveryEventBatchSize(lane, sportId)).map((eids) => ({ eids, sportId })),
  );
}

function typedDiscoveryRequests(sportId) {
  if (Number(sportId) === cricketSportId()) return [REGULAR_MARKET_TYPES, ...FANCY_MARKET_REQUESTS];
  return [["match-odd", "winner-market", "goals"]];
}

function discoveryPriority(sportId, lane = "active") {
  if (Number(sportId) === cricketSportId()) return 3;
  return lane === "active" ? 6 : 8;
}

function nextDiscoveryLane(lanes) {
  if (lanes.has("future")) return "future";
  if (lanes.has("active")) return "active";
  return null;
}

function queueDiscoveryLane(lane) {
  queuedDiscoveryLanes.add(lane === "future" ? "future" : "active");
}

function drainQueuedDiscovery() {
  if (running || liveCleanupRunning) return;
  const lane = nextDiscoveryLane(queuedDiscoveryLanes);
  if (!lane) return;
  queuedDiscoveryLanes.delete(lane);
  setImmediate(() => void requestStoredEventMarkets(lane).catch(() => {}));
}

function chunks(items, size = DB_WRITE_BATCH_SIZE) {
  const result = [];
  for (let index = 0; index < items.length; index += size) result.push(items.slice(index, index + size));
  return result;
}

function marketFingerprint(market) {
  return JSON.stringify([
    market.eventId,
    market.sportId,
    market.marketName,
    market.marketType,
    market.matchName,
    market.openDate,
    market.inPlay,
    market.gameOver,
    market.isActive,
    market.betDelay,
    market.displayMessage,
    market.seriesId,
  ]);
}

function inferredMarketType(marketId, providedType) {
  const explicit = String(providedType || "")
    .trim()
    .toLowerCase();
  if (explicit) return explicit === "bookmaker2" ? "bookmaker" : explicit;
  const id = String(marketId || "")
    .trim()
    .toUpperCase();
  if (id.endsWith("-BM2")) return "bookmaker";
  if (id.endsWith("-F2")) return "session";
  if (id.endsWith("-OE")) return "odd-even";
  if (id.endsWith("-KD")) return "khado";
  if (id.endsWith("-F3")) return "other-market";
  if (id.endsWith("-BB")) return "ball-by-ball";
  if (id.endsWith("-CC")) return "cricket-casino";
  if (id.endsWith("-MT")) return "meter";
  return "unknown";
}

function fallbackMarketName(marketType, marketId) {
  const names = {
    session: "Fancy2",
    "odd-even": "OddEven",
    khado: "Khado",
    "other-market": "OtherMarket",
    "ball-by-ball": "BallByBall",
    "cricket-casino": "CricketCasino",
    meter: "Meter",
  };
  return names[marketType] || `Market ${marketId}`;
}

function marketRows(response, eventsById) {
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  return rows
    .map((item) => {
      const event = eventsById.get(String(item?.eventId));
      const marketId = String(item?.id || "").trim();
      const inferredBookmaker2 = /-BM2$/i.test(marketId);
      const marketType = inferredMarketType(marketId, item?.type);
      const bookmaker = marketType === "bookmaker";
      const fancy = FANCY_MARKET_TYPES.has(marketType);
      const zeroCommission =
        bookmaker && ["bookmaker 0%comm", "0%comm"].includes(String(item?.name || "").toLowerCase());
      const providedName = String(item?.name || "").trim();
      const ballLine = item?.ballLine == null ? null : String(item.ballLine).trim();
      const numberedBallName =
        marketType === "ball-by-ball" && ballLine && !providedName.startsWith(`${ballLine} `)
          ? `${ballLine} ${providedName}`.trim()
          : providedName;
      const displayMessage =
        item?.inPlayFilter == null ? null : String(item.inPlayFilter).trim().slice(0, 255) || null;
      const marketName = zeroCommission
        ? "Bookmaker"
        : numberedBallName || (inferredBookmaker2 ? "Bookmaker2" : fallbackMarketName(marketType, marketId));
      return {
        marketId,
        eventId: Number(item?.eventId),
        sportId: Number(item?.sportId),
        marketName,
        marketType,
        matchName: event?.eventName || null,
        openDate: event?.openDate || null,
        inPlay: Boolean(event?.inPlay),
        gameOver: Boolean(item?.gameOver),
        isActive: item?.isActive !== false,
        betDelay: marketType === "line-market" ? 5 : bookmaker || fancy ? 0 : 3,
        minBet: 100,
        maxBet: fancy ? 100000 : bookmaker ? 25000 : 1,
        displayMessage,
        seriesId: event?.seriesId ?? null,
      };
    })
    .filter(
      (item) =>
        redisStore.validMarketIdentifier(item.marketId) &&
        Number.isInteger(item.eventId) &&
        Number.isInteger(item.sportId) &&
        item.marketName &&
        (DISCOVERABLE_MARKET_TYPES.has(item.marketType) || (item.isActive && !item.gameOver)),
    );
}

function isMarketSnapshotResponse(response) {
  return Boolean(
    Array.isArray(response) || (response && response.status !== false && Array.isArray(response.data)),
  );
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
  if (id.includes("F2")) return "F2";
  if (id.includes("OE")) return "OE";
  if (id.includes("KD")) return "KD";
  if (id.includes("F3")) return "F3";
  if (id.includes("BB")) return "BB";
  if (id.includes("CC")) return "CC";
  if (!id.includes("-") || id.startsWith("1.")) return "LINE";
  return "UNKNOWN";
}

function storedInFancyTable(market) {
  return FANCY_MARKET_TYPES.has(market?.marketType) || market?.marketType === "line-market";
}

function primaryMarketLifecycle(markets) {
  const byEvent = new Map();
  for (const market of markets || []) {
    const name = String(market?.marketName || "").trim().toLowerCase();
    const eventTerminalPrimary =
      market?.marketType === "match-odd" ||
      (market?.marketType === "bookmaker" && name.includes("bookmaker"));
    if (!eventTerminalPrimary) continue;
    if (!byEvent.has(market.eventId)) byEvent.set(market.eventId, []);
    byEvent.get(market.eventId).push(market);
  }
  return [...byEvent.entries()].map(([eventId, primaryMarkets]) => {
    const hasActivePrimary = primaryMarkets.some((market) => market.isActive && !market.gameOver);
    const terminal = primaryMarkets.find((market) => market.gameOver);
    return {
      eventId: Number(eventId),
      active: hasActivePrimary,
      terminalMarketId: hasActivePrimary ? null : terminal?.marketId || null,
    };
  });
}

function terminalPrimaryMarketIds(markets) {
  return primaryMarketLifecycle(markets)
    .map((state) => state.terminalMarketId)
    .filter(Boolean);
}

async function upsertFancies(fancies) {
  if (!fancies.length) return { inserted: 0, updated: 0, fancyIds: [], deactivatedFancyIds: [] };
  fancies = [...fancies].sort((left, right) => String(left.marketId).localeCompare(String(right.marketId)));
  const connection = await getSourcePool().getConnection();
  let inserted = 0;
  let updated = 0;
  try {
    await connection.beginTransaction();
    const ids = fancies.map((fancy) => fancy.marketId);
    const [existingRows] = await connection.query(
      `SELECT fancyid,isactive FROM t_matchfancy WHERE fancyid IN (${ids.map(() => "?").join(",")})`,
      ids,
    );
    const existing = new Map(existingRows.map((row) => [String(row.fancyid), Number(row.isactive) === 1]));
    const writable = fancies.filter(
      (fancy) =>
        existing.has(fancy.marketId) ||
        (fancy.isActive && !fancy.gameOver) ||
        fancy.marketType === "line-market",
    );
    inserted = writable.filter((fancy) => !existing.has(fancy.marketId)).length;
    updated = writable.length - inserted;
    for (const batch of chunks(writable)) {
      const values = batch.map((fancy) => {
        const active = fancy.isActive && !fancy.gameOver;
        return [
          fancy.marketId,
          fancy.marketName,
          oddsType(fancy.marketId),
          active ? "OPEN" : "SUSPENDED",
          fancy.maxBet,
          fancy.betDelay,
          fancy.minBet,
          fancy.maxBet,
          fancy.eventId,
          false,
          active,
          fancy.marketType,
          active,
          active,
          "",
          fancy.displayMessage,
          new Date(),
          fancy.matchName,
          fancy.sportId,
          "RS",
          1,
          fancy.inPlay,
          fancy.maxBet,
        ];
      });
      await connection.query(
        `INSERT INTO t_matchfancy
          (fancyid,name,oddstype,status,maxliabilityper_market,betdelay,minbet,maxbet,eventid,
           issuspendedbyadmin,isactive,mtype,isshow,is_show,suspendedby,remarks,createdon,matchname,
           sportid,provider,isbettable,isplay,maxliabilityperbet) VALUES ?
         ON DUPLICATE KEY UPDATE name=VALUES(name),oddstype=VALUES(oddstype),eventid=VALUES(eventid),
           status=VALUES(status),isactive=VALUES(isactive),isshow=VALUES(isshow),is_show=VALUES(is_show),
           matchname=VALUES(matchname),sportid=VALUES(sportid),mtype=VALUES(mtype),
           betdelay=VALUES(betdelay),minbet=VALUES(minbet),maxbet=VALUES(maxbet),
           maxliabilityper_market=VALUES(maxliabilityper_market),
           maxliabilityperbet=VALUES(maxliabilityperbet),remarks=VALUES(remarks),updatedon=NOW()`,
        [values],
      );
    }
    // Migrate only IDs positively identified as provider session markets in this response.
    await connection.query(`DELETE FROM t_market WHERE marketid IN (${ids.map(() => "?").join(",")})`, ids);
    await connection.commit();
    return {
      inserted,
      updated,
      fancyIds: ids,
      deactivatedFancyIds: writable
        .filter(
          (fancy) =>
            (!fancy.isActive || fancy.gameOver) &&
            (existing.get(fancy.marketId) ||
              (!existing.has(fancy.marketId) && fancy.marketType === "line-market")),
        )
        .map((fancy) => fancy.marketId),
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

async function upsertMarkets(markets) {
  if (!markets.length) return { inserted: 0, updated: 0, marketIds: [], deactivatedMarketIds: [] };
  markets = [...markets].sort((left, right) => String(left.marketId).localeCompare(String(right.marketId)));
  const connection = await getSourcePool().getConnection();
  let inserted = 0;
  let updated = 0;
  const deactivatedMarketIds = [];
  try {
    await connection.beginTransaction();
    const ids = markets.map((market) => market.marketId);
    const [existingRows] = await connection.query(
      `SELECT marketid,isactive FROM t_market WHERE marketid IN (${ids.map(() => "?").join(",")})`,
      ids,
    );
    const existing = new Map(existingRows.map((row) => [String(row.marketid), Number(row.isactive) === 1]));
    const writable = markets.filter(
      (market) => existing.has(market.marketId) || (market.isActive && !market.gameOver),
    );
    inserted = writable.filter((market) => !existing.has(market.marketId)).length;
    updated = writable.length - inserted;
    for (const market of writable) {
      if (existing.get(market.marketId) && (!market.isActive || market.gameOver)) {
        deactivatedMarketIds.push(market.marketId);
      }
    }
    for (const batch of chunks(writable)) {
      const values = batch.map((market) => [
        market.marketId,
        market.sportId,
        market.eventId,
        market.marketName,
        market.matchName,
        true,
        market.isActive && !market.gameOver,
        new Date(),
        new Date(),
        market.openDate,
        market.minBet,
        market.maxBet,
        market.betDelay,
        market.inPlay,
        1,
        500,
        false,
        market.displayMessage,
        false,
        false,
        0,
        1000000,
        false,
        market.seriesId,
      ]);
      await connection.query(
        `INSERT INTO t_market
          (marketid,sportid,eventid,marketname,matchname,status,isactive,createdon,updatedon,opendate,
           minbet,maxbet,betdelay,inplay,minbetrate,maxbetrate,is_redis_updated,display_message,
           issuspended,is_rolled_back,maximum_profit,maximumprofit,betlock,seriesid) VALUES ?
         ON DUPLICATE KEY UPDATE marketname=VALUES(marketname),matchname=VALUES(matchname),
           opendate=VALUES(opendate),sportid=VALUES(sportid),eventid=VALUES(eventid),
           seriesid=VALUES(seriesid),inplay=IF(inplay=1,1,VALUES(inplay)),
           status=VALUES(status),isactive=VALUES(isactive),betdelay=VALUES(betdelay),
           display_message=VALUES(display_message),updatedon=NOW()`,
        [values],
      );
    }
    await connection.commit();
    return {
      inserted,
      updated,
      marketIds: markets
        .filter((market) => market.isActive && !market.gameOver)
        .map((market) => market.marketId),
      deactivatedMarketIds,
    };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

function bookmaker2BaseMarketId(marketId) {
  const normalized = String(marketId || "").trim();
  return /-BM2$/i.test(normalized) ? normalized.replace(/-BM2$/i, "") : null;
}

function runnerSourceMarketId(marketId) {
  return String(marketId);
}

function runnerLookupMarketIds(marketIds) {
  return [...new Set((marketIds || []).map(String))];
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
  const now = Date.now();
  for (const [marketId, expiresAt] of runnerMisses) if (expiresAt <= now) runnerMisses.delete(marketId);
  return marketIds.filter((marketId) => {
    const normalized = String(marketId);
    return !runnerMisses.has(normalized) && !present.has(normalized);
  });
}

async function fetchAndStoreRunners(marketIds) {
  const missing = await marketsMissingRunners(marketIds);
  const marketsBySource = new Map();
  for (const marketId of missing) {
    const sourceMarketId = runnerSourceMarketId(marketId);
    if (!marketsBySource.has(sourceMarketId)) marketsBySource.set(sourceMarketId, []);
    marketsBySource.get(sourceMarketId).push(marketId);
  }
  const responses = await settleWithConcurrency(
    [...marketsBySource.entries()],
    async ([sourceMarketId, targetMarketIds]) => ({
      sourceMarketId,
      targetMarketIds,
      response: await provider.runners(sourceMarketId),
    }),
  );
  const runners = responses.flatMap((result) => {
    if (result.status !== "fulfilled") return [];
    const { targetMarketIds, response } = result.value;
    const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
    return targetMarketIds.flatMap((marketId) =>
      rows
        .map((runner) => ({
          marketId,
          selectionId: Number(runner?.runnerId ?? runner?.selectionId),
          runnerName: String(runner?.name ?? runner?.nation ?? "").trim(),
        }))
        .filter((runner) => Number.isInteger(runner.selectionId) && runner.runnerName),
    );
  });
  const missTtlMs = integer("RUNNER_MISS_CACHE_MS", 300000, { min: 10000 });
  for (const result of responses) {
    if (result.status !== "fulfilled") continue;
    const { targetMarketIds, response } = result.value;
    const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
    for (const marketId of targetMarketIds) {
      if (!rows.length) runnerMisses.set(String(marketId), Date.now() + missTtlMs);
      else runnerMisses.delete(String(marketId));
    }
  }
  if (runners.length) {
    const connection = await getSourcePool().getConnection();
    try {
      await connection.beginTransaction();
      for (const batch of chunks(runners)) {
        const values = batch.map((runner) => [
          new Date(),
          runner.marketId,
          runner.runnerName,
          runner.selectionId,
          false,
        ]);
        await connection.query(
          `INSERT INTO t_selectionid
             (createdon,marketid,runner_name,selectionid,is_redis_updated) VALUES ?
           ON DUPLICATE KEY UPDATE runner_name=VALUES(runner_name)`,
          [values],
        );
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }
  return {
    requestedMarkets: missing.length,
    storedRunners: runners.length,
    storedMarketIds: [...new Set(runners.map((runner) => String(runner.marketId)))],
    failedMarkets: responses.filter((result) => result.status === "rejected").length,
  };
}

async function regularMarketsWithRunners(markets) {
  const ids = [...new Set((markets || []).map((market) => String(market.marketId)).filter(Boolean))];
  if (!ids.length) return [];
  const lookupIds = runnerLookupMarketIds(ids);
  const [rows] = await getSourcePool().query(
    `SELECT marketid,selectionid,runner_name FROM t_selectionid
      WHERE marketid IN (${lookupIds.map(() => "?").join(",")}) ORDER BY id ASC`,
    lookupIds,
  );
  const byMarket = new Map();
  for (const row of rows) {
    const marketId = String(row.marketid);
    if (!byMarket.has(marketId)) byMarket.set(marketId, []);
    byMarket.get(marketId).push({ selectionId: row.selectionid, runnerName: row.runner_name });
  }
  return markets.map((market) => {
    const marketId = String(market.marketId);
    return {
      ...market,
      runners: byMarket.get(marketId) || [],
    };
  });
}

async function seedTossMarkets(markets) {
  const tossMarkets = (markets || []).filter(
    (market) =>
      String(market.marketName || "")
        .trim()
        .toUpperCase() === "TOSS" &&
      market.isActive &&
      !market.gameOver &&
      !tossSeeded.has(String(market.marketId)),
  );
  const results = await Promise.allSettled(
    tossMarkets.map(async (market) => {
      const response = await provider.runners(market.marketId);
      const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
      if (!rows.length) return false;
      const written = await redisStore.writeTick({
        eid: market.eventId,
        mid: market.marketId,
        s: true,
        r: rows.map((runner) => ({
          rid: runner.runnerId ?? runner.selectionId,
          na: runner.name ?? runner.nation,
          back: runner.back,
          lay: runner.lay,
          b1: runner.b1,
          l1: runner.l1,
          bs1: runner.br1,
          ls1: runner.lr1,
          sb: runner.sb,
        })),
      });
      if (written) setBounded(tossSeeded, String(market.marketId), true, DISCOVERY_CACHE_LIMIT);
      return written;
    }),
  );
  return {
    requested: tossMarkets.length,
    seeded: results.filter((result) => result.status === "fulfilled" && result.value).length,
    failed: results.filter((result) => result.status === "rejected").length,
  };
}

function inactiveLineMarkets(markets) {
  return (markets || []).filter(
    (market) => market.marketType === "line-market" && (!market.isActive || market.gameOver),
  );
}

async function reconcileInactiveLineMarkets(markets) {
  const inactive = inactiveLineMarkets(markets);
  if (!inactive.length) return { checked: 0, deactivated: 0, removed: 0 };
  const persisted = await upsertFancies(inactive);
  redisStore.invalidateMarkets([...inactive.map((market) => market.marketId), ...persisted.fancyIds]);
  const redisDefinitions = await redisStore.reconcileRegularDefinitions(inactive);
  const deactivatedMarketIds = persisted.deactivatedFancyIds;
  if (deactivatedMarketIds.length) {
    await unsubscribeEventMarkets(deactivatedMarketIds);
  }
  await Promise.allSettled(
    (redisDefinitions.changedEventIds || []).map((eventId) => publishEventSnapshot(eventId)),
  );
  return {
    checked: inactive.length,
    deactivated: deactivatedMarketIds.length,
    removed: redisDefinitions.removed,
  };
}

function missingLineMarketIds(storedMarkets, vendorMarkets, missCounts = missingLineMarketPasses) {
  const present = new Set((vendorMarkets || []).map((market) => String(market.marketId)));
  const storedIds = new Set((storedMarkets || []).map((market) => String(market.marketId)));
  const deactivated = [];

  for (const marketId of present) missCounts.delete(marketId);
  for (const market of storedMarkets || []) {
    const marketId = String(market.marketId);
    if (present.has(marketId)) continue;
    const misses = (missCounts.get(marketId) || 0) + 1;
    missCounts.set(marketId, misses);
    if (misses >= MISSING_LINE_MARKET_PASSES) deactivated.push(marketId);
  }
  // Do not retain counters for events/markets outside the current reconciliation scope.
  for (const marketId of missCounts.keys()) {
    if (!storedIds.has(marketId) && !present.has(marketId)) missCounts.delete(marketId);
  }
  return deactivated;
}

async function reconcileMissingLineMarkets(eventIds, vendorMarkets) {
  if (!eventIds.length) return { checked: 0, missing: 0, deactivated: 0, removed: 0 };
  const [rows] = await getSourcePool().query(
    `SELECT f.fancyid AS marketId, COALESCE(f.sportid,e.sportid) AS sportId,
            f.eventid AS eventId, f.name AS marketName,
            COALESCE(f.matchname,e.eventname) AS matchName, e.open_date AS openDate,
            f.isplay AS inPlay, f.betdelay AS betDelay, f.minbet AS minBet,
            f.maxbet AS maxBet, f.remarks AS displayMessage, e.seriesid AS seriesId
       FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
      WHERE f.isactive=? AND f.eventid IN (${eventIds.map(() => "?").join(",")})
        AND f.mtype=?`,
    [true, ...eventIds, "line-market"],
  );
  const stored = rows.map((row) => ({
    ...row,
    marketId: String(row.marketId),
    eventId: Number(row.eventId),
    sportId: Number(row.sportId),
    marketType: "line-market",
    inPlay: Boolean(row.inPlay),
    isActive: true,
    gameOver: false,
  }));
  const ids = new Set(missingLineMarketIds(stored, vendorMarkets));
  const inactive = stored
    .filter((market) => ids.has(market.marketId))
    .map((market) => ({ ...market, isActive: false }));
  if (!inactive.length) {
    return {
      checked: stored.length,
      missing: [...missingLineMarketPasses.values()].filter(Boolean).length,
      deactivated: 0,
      removed: 0,
    };
  }
  const result = await reconcileInactiveLineMarkets(inactive);
  for (const market of inactive) {
    missingLineMarketPasses.delete(market.marketId);
    discoveryFingerprints.delete(market.marketId);
  }
  return { checked: stored.length, missing: inactive.length, ...result };
}

async function syncMarketDiscovery(events, lane = "active") {
  if (liveCleanupRunning || running) {
    queueDiscoveryLane(lane);
    return {
      skipped: true,
      reason: liveCleanupRunning ? "live-cleanup-running" : "already-running",
      rerunQueued: true,
      queuedLane: lane,
    };
  }
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const selectedEvents = prioritizedDiscoveryEvents(events, lane);
    const eventsById = new Map(selectedEvents.map((event) => [String(event.eventId), event]));
    const eventIds = [...eventsById.keys()].map(Number);
    // Cricket events use isolated snapshots in both lanes to avoid the provider's
    // response cap. Other future sports retain configurable batching.
    const discovered = [];
    const eventBatches = discoveryEventBatches(selectedEvents, lane);

    // First pass: one unfiltered request per event batch. Inactive line markets are
    // reconciled immediately instead of waiting for every fancy family and thousands
    // of database upserts in the full discovery pass.
    const primarySettled = await settleWithConcurrency(
      eventBatches,
      async ({ eids, sportId }) => {
        const response = await provider.markets({ eids }, { priority: discoveryPriority(sportId, lane) });
        return { valid: isMarketSnapshotResponse(response), rows: marketRows(response, eventsById) };
      },
    );
    const primaryRows = primarySettled.flatMap((result) =>
      result.status === "fulfilled" ? result.value.rows : [],
    );
    discovered.push(...primaryRows);
    // Commit the useful primary snapshot immediately. Typed-family discovery and runner
    // hydration are independent stages and must not prevent core markets from appearing.
    const primaryUnique = enforceBookmaker2Eligibility(mergeDiscoveredMarkets(primaryRows));
    const primaryStoredRegular = primaryUnique.filter((market) => !storedInFancyTable(market));
    const primaryStoredFancies = primaryUnique.filter(storedInFancyTable);
    // Both writers can touch t_market (fancy persistence removes migrated rows), so
    // serialize them to avoid cross-transaction lock waits during busy discovery runs.
    const primaryPersisted = await upsertMarkets(primaryStoredRegular);
    const primaryFancyPersisted = await upsertFancies(primaryStoredFancies);
    // An authoritative snapshot with no active primary market and a completed
    // Match Odds/Bookmaker record is event-terminal. Historical completed
    // primaries cannot close an event while another primary remains active.
    const terminalMarketIds = [];
    for (const primaryState of primaryMarketLifecycle(primaryRows)) {
      if (primaryState.active) {
        lifecycle.observe({
          eventId: primaryState.eventId,
          source: "market-discovery",
          terminal: false,
        });
        lifecycle.clearConfirmed(primaryState.eventId, "active-primary-market");
        continue;
      }
      if (!primaryState.terminalMarketId) continue;
      const decision = lifecycle.observe({
        eventId: primaryState.eventId,
        source: "market-discovery",
        terminal: true,
        evidence: { marketId: primaryState.terminalMarketId },
      });
      if (decision.execute) terminalMarketIds.push(primaryState.terminalMarketId);
    }
    const terminalCleanup = await handleSocketGameOver(terminalMarketIds);
    redisStore.invalidateMarkets([
      ...primaryPersisted.marketIds,
      ...primaryPersisted.deactivatedMarketIds,
      ...primaryFancyPersisted.fancyIds,
    ]);
    const fastLineReconciliation = await reconcileInactiveLineMarkets(primaryRows);
    // Reconcile only batches with a successful, structurally valid response. A timeout,
    // error response, or malformed body must never be interpreted as an empty market list.
    const validPrimaryEventIds = eventBatches.flatMap(({ eids }, index) =>
      primarySettled[index]?.status === "fulfilled" && primarySettled[index].value.valid ? eids : [],
    );
    const validPrimaryEventIdSet = new Set(validPrimaryEventIds.map(Number));
    const missingLineReconciliation = await reconcileMissingLineMarkets(
      validPrimaryEventIds,
      primaryRows.filter((market) => validPrimaryEventIdSet.has(market.eventId)),
    );

    // Second pass: fetch typed fallbacks needed for families the vendor can omit from
    // the unfiltered response. These no longer delay line-market deactivation.
    // Typed fallback discovery fans out into several /v1/markets calls per event.
    // Live prices arrive over the socket, so repeating this metadata-only fan-out
    // every five seconds adds no pricing freshness and can exhaust the vendor cap.
    const cricketFullIntervalMs = integer("MARKET_FULL_DISCOVERY_MS", 60000, { min: 10000 });
    const otherFullIntervalMs = integer("NON_CRICKET_FULL_DISCOVERY_MS", 300000, { min: 60000 });
    const typedQueueLimit = integer("MARKET_TYPED_DISCOVERY_QUEUE_LIMIT", 200, { min: 1 });
    const providerQueue = provider.providerLimiter.counts();
    const typedWork =
      Number(providerQueue.QUEUED || 0) < typedQueueLimit
        ? eventBatches.flatMap(({ eids, sportId }) => {
            const intervalMs = sportId === cricketSportId() ? cricketFullIntervalMs : otherFullIntervalMs;
            const key = `${lane}:${sportId}`;
            if (Date.now() - (lastFullDiscoveryAt.get(key) || 0) < intervalMs) return [];
            return typedDiscoveryRequests(sportId).map((type) => ({ eids, type, sportId }));
          })
        : [];
    const fullDiscovery = typedWork.length > 0;
    const typedSettled = fullDiscovery
      ? await settleWithConcurrency(
          typedWork,
          async ({ eids, type, sportId }) => {
            const response = await provider.markets(
              { eids, type },
              { priority: discoveryPriority(sportId, lane) },
            );
            return marketRows(response, eventsById);
          },
        )
      : [];
    for (const result of typedSettled) {
      if (result.status === "fulfilled") discovered.push(...result.value);
    }
    const unique = enforceBookmaker2Eligibility(mergeDiscoveredMarkets(discovered));
    const fancies = unique.filter((market) => FANCY_MARKET_TYPES.has(market.marketType));
    const regularMarkets = unique.filter((market) => !FANCY_MARKET_TYPES.has(market.marketType));
    const changed = unique.filter(
      (market) => discoveryFingerprints.get(market.marketId) !== marketFingerprint(market),
    );
    const changedIds = new Set(changed.map((market) => market.marketId));
    const changedMarkets = unique.filter((market) => changedIds.has(market.marketId));
    const persisted = await upsertMarkets(changedMarkets.filter((market) => !storedInFancyTable(market)));
    redisStore.invalidateMarkets([...persisted.marketIds, ...persisted.deactivatedMarketIds]);
    const fancyPersisted = await upsertFancies(changedMarkets.filter(storedInFancyTable));
    redisStore.invalidateMarkets(fancyPersisted.fancyIds);
    // Reconcile every discovered fancy against Redis. Restricting this to changed
    // fingerprints cannot restore a definition after external eviction or expiry.
    // The Redis reconciler only writes when the resulting payload actually differs.
    const redisDefinitions = await redisStore.reconcileFancyDefinitions(fancies);
    const activeRegularIds = regularMarkets
      .filter((market) => market.isActive && !market.gameOver)
      .map((market) => market.marketId);
    const runnerResult = await fetchAndStoreRunners(activeRegularIds);
    const redisRegularIds = new Set([...changedIds, ...(runnerResult.storedMarketIds || [])]);
    const redisRegularMarkets = regularMarkets.filter((market) => redisRegularIds.has(market.marketId));
    const regularDefinitions = await redisStore.reconcileRegularDefinitions(
      await regularMarketsWithRunners(redisRegularMarkets),
    );
    if (persisted.deactivatedMarketIds.length) {
      await unsubscribeEventMarkets(persisted.deactivatedMarketIds);
    }
    const changedEventIds = [
      ...new Set([
        ...(redisDefinitions.changedEventIds || []),
        ...(regularDefinitions.changedEventIds || []),
      ]),
    ];
    await Promise.allSettled(changedEventIds.map((eventId) => publishEventSnapshot(eventId)));
    const tossDefinitions = await seedTossMarkets(regularMarkets);
    const activeFancies = fancies.filter((fancy) => fancy.isActive && !fancy.gameOver).length;
    for (const market of unique)
      setBounded(discoveryFingerprints, market.marketId, marketFingerprint(market), DISCOVERY_CACHE_LIMIT);
    const completedAt = Date.now();
    for (const event of selectedEvents) lastPrimaryDiscoveryAt.set(`${lane}:${event.sportId}`, completedAt);
    if (fullDiscovery) {
      for (const { sportId } of typedWork) lastFullDiscoveryAt.set(`${lane}:${sportId}`, completedAt);
    }
    const result = {
      skipped: false,
      lane,
      events: eventIds.length,
      markets: regularMarkets.length,
      fancies: activeFancies,
      sessionRecords: fancies.length,
      fullDiscovery,
      primaryFailedBatches: primarySettled.filter((item) => item.status === "rejected").length,
      typedFailedRequests: typedSettled.filter((item) => item.status === "rejected").length,
      changedDefinitions: changed.length,
      inserted: primaryPersisted.inserted + persisted.inserted,
      updated: persisted.updated,
      deactivated: persisted.deactivatedMarketIds.length,
      fancyInserted: primaryFancyPersisted.inserted + fancyPersisted.inserted,
      fancyUpdated: fancyPersisted.updated,
      fastLineReconciliation,
      missingLineReconciliation,
      redisDefinitions,
      regularDefinitions,
      tossDefinitions,
      runners: runnerResult,
      subscription: { delegatedToMarketSync: true },
      terminalCleanup,
    };
    state.lastResult = result;
    state.lastCompletedAt = new Date().toISOString();
    logger.info("[MarketDiscovery] completed", result);
    return result;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[MarketDiscovery] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
    drainQueuedDiscovery();
  }
}

async function fetchActiveEventsForMarketDiscovery(lane = "active") {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  if (!sportIds.length) return [];
  const cachedBySport = await Promise.all(sportIds.map((sportId) => redisStore.getEvents(sportId)));
  if (cachedBySport.every((events) => events !== null)) {
    return cachedBySport
      .flat()
      .filter((event) => !event.gameOver && eventInWindow(event, lane))
      .sort((left, right) => left.eventId - right.eventId);
  }
  const [rows] = await getSourcePool().query(
    `SELECT eventid,eventname,sportid,seriesid,open_date,in_play
       FROM t_event
      WHERE isactive=? AND sportid IN (${sportIds.map(() => "?").join(",")})
        AND ${eventWindowSql("t_event", lane)}
      ORDER BY eventid ASC`,
    [true, ...sportIds],
  );
  return rows
    .map((row) => ({
      eventId: Number(row.eventid),
      eventName: String(row.eventname || "").trim(),
      sportId: Number(row.sportid),
      seriesId: Number(row.seriesid),
      openDate: row.open_date,
      inPlay: Boolean(row.in_play),
    }))
    .filter((event) => Number.isInteger(event.eventId) && event.eventName);
}

async function fetchLiveEventsForMarketCleanup() {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  if (!sportIds.length) return [];
  const cachedBySport = await Promise.all(sportIds.map((sportId) => redisStore.getEvents(sportId)));
  if (cachedBySport.every((events) => events !== null)) {
    return cachedBySport
      .flat()
      .filter((event) => event.inPlay === true && event.gameOver !== true)
      .map((event) => ({ ...event, inPlay: true }));
  }
  const settled = await Promise.allSettled(sportIds.map((si) => provider.events({ si, today: 1 })));
  return settled
    .filter((result) => result.status === "fulfilled")
    .map((result) => result.value)
    .flatMap((response) =>
      Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [],
    )
    .filter((event) => event?.inPlay === true && event?.gameOver !== true)
    .map((event) => ({
      eventId: Number(event?.eventId ?? event?.id),
      eventName: String(event?.eventName ?? event?.name ?? "").trim(),
      sportId: Number(event?.sportId),
      seriesId: Number(event?.competitionId ?? event?.seriesId),
      openDate: event?.startTime ?? event?.openDate ?? null,
      inPlay: true,
    }))
    .filter((event) => Number.isInteger(event.eventId) && event.eventName);
}

async function syncLiveMarketCleanup() {
  if (liveCleanupRunning || running) {
    return { skipped: true, reason: liveCleanupRunning ? "already-running" : "market-discovery-running" };
  }
  liveCleanupRunning = true;
  try {
    const events = await fetchLiveEventsForMarketCleanup();
    if (!events.length) return { events: 0, inactive: 0, removed: 0 };
    const eventsById = new Map(events.map((event) => [String(event.eventId), event]));
    // Cleanup must also use complete per-event snapshots. A truncated batch could
    // otherwise be mistaken for missing markets and deactivate valid definitions.
    const eventBatchSize = 1;
    const eventBatches = chunks(
      events.map((event) => event.eventId),
      eventBatchSize,
    );
    const settled = await settleWithConcurrency(eventBatches, (eids) => provider.markets({ eids }));
    const responses = settled.map((result) => (result.status === "fulfilled" ? result.value : null));
    const vendorMarkets = responses.flatMap((response) => marketRows(response, eventsById));
    const validEventIds = eventBatches.flatMap((eids, index) =>
      isMarketSnapshotResponse(responses[index]) ? eids : [],
    );
    const missingLines = await reconcileMissingLineMarkets(validEventIds, vendorMarkets);
    const inactive = vendorMarkets.filter((market) => !market.isActive || market.gameOver);
    if (!inactive.length) {
      return { events: events.length, inactive: 0, removed: missingLines.removed, missingLines };
    }
    const unique = [...new Map(inactive.map((market) => [market.marketId, market])).values()];
    const fancies = unique.filter(storedInFancyTable);
    const regular = unique.filter((market) => !storedInFancyTable(market));
    const regularResult = await retryDeadlock(() => upsertMarkets(regular));
    const fancyResult = await retryDeadlock(() => upsertFancies(fancies));
    const marketIdsByEvent = new Map();
    for (const market of unique) {
      const eventId = String(market.eventId);
      if (!marketIdsByEvent.has(eventId)) marketIdsByEvent.set(eventId, []);
      marketIdsByEvent.get(eventId).push(market.marketId);
    }
    const removals = await Promise.all(
      [...marketIdsByEvent.entries()].map(([eventId, marketIds]) => redisStore.removeMarkets(eventId, marketIds)),
    );
    const removedCount = removals.reduce((total, removedIds) => total + removedIds.size, 0);
    const marketIds = unique.map((market) => market.marketId);
    redisStore.invalidateMarkets(marketIds);
    await unsubscribeEventMarkets(marketIds);
    const changedEventIds = [...new Set(unique.map((market) => String(market.eventId)))];
    await Promise.allSettled(changedEventIds.map((eventId) => publishEventSnapshot(eventId)));
    return {
      events: events.length,
      inactive: unique.length,
      deactivated: regularResult.updated + fancyResult.updated,
      removed: removedCount + missingLines.removed,
      missingLines,
    };
  } finally {
    liveCleanupRunning = false;
    drainQueuedDiscovery();
  }
}

async function syncStoredEventMarkets(lane = "active") {
  const events = await fetchActiveEventsForMarketDiscovery(lane);
  return syncMarketDiscovery(events, lane);
}

async function requestStoredEventMarkets(lane = "active") {
  if (running || liveCleanupRunning) {
    queueDiscoveryLane(lane);
    return { skipped: true, reason: "discovery-busy", rerunQueued: true, queuedLane: lane };
  }
  return syncStoredEventMarkets(lane);
}

function startMarketDiscoverySync() {
  const { expression } = cronConfig.marketDiscovery;
  const discoveryTask = cron.schedule(expression, () => {
    void requestStoredEventMarkets("active").catch(() => {});
  });
  const futureTask = cron.schedule(cronConfig.futureMarketDiscovery.expression, () => {
    void requestStoredEventMarkets("future").catch(() => {});
  });
  const cleanupTask = cron.schedule(cronConfig.liveMarketCleanup.expression, () => {
    void syncLiveMarketCleanup().catch((error) =>
      logger.error("[LiveMarketCleanup] failed", { error: error.message }),
    );
  });
  logger.info("[MarketDiscovery] scheduled", { expression });
  logger.info("[LiveMarketCleanup] scheduled", { expression: cronConfig.liveMarketCleanup.expression });
  logger.info("[MarketDiscovery] future lane scheduled", {
    expression: cronConfig.futureMarketDiscovery.expression,
  });
  setImmediate(() => void requestStoredEventMarkets("active").catch(() => {}));
  return {
    stop: () => {
      discoveryTask.stop();
      futureTask.stop();
      cleanupTask.stop();
    },
  };
}

function getMarketDiscoveryStatus() {
  return { ...state };
}

module.exports = {
  MARKET_TYPES,
  REGULAR_MARKET_TYPES,
  FANCY_MARKET_TYPES,
  FANCY_MARKET_REQUESTS,
  marketRows,
  isMarketSnapshotResponse,
  inferredMarketType,
  fallbackMarketName,
  mergeDiscoveredMarkets,
  oddsType,
  storedInFancyTable,
  terminalPrimaryMarketIds,
  primaryMarketLifecycle,
  upsertMarkets,
  upsertFancies,
  fetchAndStoreRunners,
  bookmaker2BaseMarketId,
  runnerSourceMarketId,
  runnerLookupMarketIds,
  enforceBookmaker2Eligibility,
  regularMarketsWithRunners,
  seedTossMarkets,
  inactiveLineMarkets,
  missingLineMarketIds,
  discoveryEventBatchSize,
  prioritizedDiscoveryEvents,
  discoveryEventBatches,
  typedDiscoveryRequests,
  discoveryPriority,
  nextDiscoveryLane,
  reconcileMissingLineMarkets,
  fetchActiveEventsForMarketDiscovery,
  fetchLiveEventsForMarketCleanup,
  syncLiveMarketCleanup,
  syncMarketDiscovery,
  syncStoredEventMarkets,
  startMarketDiscoverySync,
  getMarketDiscoveryStatus,
};
