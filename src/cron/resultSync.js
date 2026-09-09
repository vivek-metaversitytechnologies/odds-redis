const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const redis = require("../config/redis");
const subscriptions = require("../services/marketSubscriptionService");
const frontendSocket = require("../services/frontendSocketService");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const { eventWindowSql } = require("../utils/eventWindow");
const { retryDeadlock } = require("../utils/dbRetry");
const lifecycle = require("../services/eventLifecyclePolicy");
const pendingResults = require("../services/pendingResultQueue");

let running = false;
let headroomStopped = false;
let headroomCooldownUntil = 0;
const headroomState = { running: false, lastCompletedAt: null, lastError: null, skippedReason: null, lastResult: null };
let exceptionalTableAvailable;
const candidateCursors = { market: null, fancy: null };
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};

function responseRows(response) {
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  return rows
    .map((item) => ({
      marketId: String(item?.marketId || "").trim(),
      marketType: String(item?.marketType || "")
        .trim()
        .toLowerCase(),
      result: item?.result == null ? null : String(item.result).trim(),
      isTie: item?.isTie === true,
      isAbandoned: item?.isAbandoned === true || String(item?.result || "").toLowerCase() === "abandoned",
    }))
    .filter((item) => item.marketId && item.result != null);
}

function fancyResultValue(marketId, result, marketType) {
  const id = String(marketId).toUpperCase();
  if (id.includes("-OE") || id.includes("-F3")) return String(result).toLowerCase() === "back" ? 1 : 0;
  if (
    id.includes("-F2") ||
    id.includes("-BB") ||
    id.includes("-CC") ||
    id.includes("-KD") ||
    id.includes("-MT") ||
    ["line-market", "khado", "meter"].includes(String(marketType).toLowerCase())
  ) {
    const value = Number.parseInt(result, 10);
    return Number.isInteger(value) ? value : null;
  }
  return null;
}

function rejectedResultReason(market, result, isFancy) {
  if (isFancy) {
    return fancyResultValue(market.marketid, result.result, market.mtype) == null
      ? "unsupported-fancy-result"
      : "fancy-result-not-persisted";
  }
  return Number.isInteger(Number(result.result))
    ? "winner-selection-metadata-missing"
    : "non-numeric-market-winner";
}

function rejectedResultDetail(market, result, isFancy) {
  return {
    marketId: result.marketId,
    family: isFancy ? "fancy" : "regular",
    marketName: market.marketname || null,
    vendorMarketType: result.marketType || null,
    dbMarketType: isFancy ? market.mtype || market.oddstype || null : market.marketname || null,
    result: result.result,
    reason: rejectedResultReason(market, result, isFancy),
  };
}

function isEventTerminalMarketName(name) {
  const normalized = String(name || "").trim().toLowerCase();
  return normalized === "match odds" || normalized.includes("bookmaker");
}

function interleaveResultCandidates(markets = [], fancies = []) {
  const combined = [];
  const length = Math.max(markets.length, fancies.length);
  for (let index = 0; index < length; index += 1) {
    if (markets[index]) combined.push(markets[index]);
    if (fancies[index]) combined.push(fancies[index]);
  }
  return combined;
}

async function settleWithConcurrency(items, mapper, concurrency) {
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

function advanceCandidateCursors(requested, marketCandidates, fancyCandidates) {
  const marketIds = new Set(marketCandidates.map((candidate) => candidate));
  const fancyIds = new Set(fancyCandidates.map((candidate) => candidate));
  const requestedMarkets = requested.filter((candidate) => marketIds.has(candidate));
  const requestedFancies = requested.filter((candidate) => fancyIds.has(candidate));
  if (!marketCandidates.length) candidateCursors.market = null;
  else if (requestedMarkets.length) candidateCursors.market = Number(requestedMarkets.at(-1).candidateid);
  if (!fancyCandidates.length) candidateCursors.fancy = null;
  else if (requestedFancies.length) candidateCursors.fancy = Number(requestedFancies.at(-1).candidateid);
  return { ...candidateCursors };
}

async function hasExceptionalTable(connection) {
  if (exceptionalTableAvailable != null) return exceptionalTableAvailable;
  const [rows] = await connection.query(
    `SELECT COUNT(*) AS total FROM information_schema.tables
     WHERE table_schema=DATABASE() AND table_name='t_matchabondendtie'`,
  );
  exceptionalTableAvailable = Number(rows[0]?.total) > 0;
  return exceptionalTableAvailable;
}

async function loadCandidates() {
  const sportIds = String(process.env.SPORT_IDS || "1,2,4")
    .split(",")
    .map((value) => Number(value.trim()))
    .filter(Number.isFinite);
  const placeholders = sportIds.map(() => "?").join(",");
  const limit = Math.max(1, Number(process.env.RESULT_MARKET_LIMIT || 2000));
  const [markets] = await getSourcePool().query(
    `SELECT m.id AS candidateid, m.marketid, m.marketname, m.eventid, m.matchname, m.sportid
     FROM t_market m LEFT JOIN t_event e ON e.eventid=m.eventid
     WHERE m.isactive=?
       AND m.sportid IN (${placeholders})
       AND ${eventWindowSql("e", "active")}
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ORDER BY CASE WHEN ? IS NULL OR m.id < ? THEN 0 ELSE 1 END, m.id DESC
     LIMIT ?`,
    [true, ...sportIds, candidateCursors.market, candidateCursors.market, limit],
  );
  const [fancies] = await getSourcePool().query(
    `SELECT f.id AS candidateid, f.fancyid AS marketid, f.name AS marketname, f.oddstype, f.mtype,
            f.eventid, COALESCE(f.matchname,e.eventname) AS matchname,
            COALESCE(f.sportid,e.sportid) AS sportid
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE UPPER(f.status)=?
       AND COALESCE(f.sportid,e.sportid) IN (${placeholders})
       AND ${eventWindowSql("e", "active")}
       AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
     ORDER BY CASE WHEN ? IS NULL OR f.id < ? THEN 0 ELSE 1 END, f.id DESC
     LIMIT ?`,
    ["OPEN", ...sportIds, candidateCursors.fancy, candidateCursors.fancy, limit],
  );
  return { markets, fancies, cursorsUsed: { ...candidateCursors } };
}

async function handleSocketGameOver(marketIds) {
  const ids = [...new Set((marketIds || []).map(String).map((id) => id.trim()).filter(redis.validMarketIdentifier))]
    .sort((left, right) => left.localeCompare(right));
  if (!ids.length) return { markets: 0, events: 0, removed: 0 };
  const placeholders = ids.map(() => "?").join(",");
  const cleanup = await retryDeadlock(async () => {
    const connection = await getSourcePool().getConnection();
    try {
      await connection.beginTransaction();
      const [markets] = await connection.query(
        `SELECT marketid,eventid,marketname FROM t_market WHERE marketid IN (${placeholders})`,
        ids,
      );
      const [fancies] = await connection.query(
        `SELECT fancyid AS marketid,eventid,name AS marketname FROM t_matchfancy
         WHERE fancyid IN (${placeholders})`,
        ids,
      );
      const candidateTerminalEventIds = [...new Set([...markets, ...fancies]
        .filter((market) => isEventTerminalMarketName(market.marketname))
        .map((market) => Number(market.eventid))
        .filter(Number.isInteger))].sort((left, right) => left - right);
      const terminalEventIds = candidateTerminalEventIds.filter((eventId) =>
        lifecycle.observe({
          eventId,
          source: "socket-primary-game-over",
          terminal: true,
          evidence: {
            marketIds: [...markets, ...fancies]
              .filter((market) => Number(market.eventid) === eventId)
              .map((market) => String(market.marketid)),
          },
        }).execute,
      );
      await pendingResults.enqueue(markets.map((market) => market.marketid));
      await connection.query(
        `UPDATE t_market SET isactive=?,status=?,issubscribed=?,updatedon=NOW()
         WHERE marketid IN (${placeholders})`,
        [false, false, false, ...ids],
      );
      await connection.query(
        `UPDATE t_matchfancy SET isactive=?,isshow=?,is_show=?,issubscribed=?,updatedon=NOW()
         WHERE fancyid IN (${placeholders})`,
        [false, false, false, false, ...ids],
      );
      let terminalEvents = [];
      let eventMarkets = [];
      let eventFancies = [];
      if (terminalEventIds.length) {
        const eventPlaceholders = terminalEventIds.map(() => "?").join(",");
        [terminalEvents] = await connection.query(
          `SELECT eventid,sportid FROM t_event WHERE eventid IN (${eventPlaceholders})`,
          terminalEventIds,
        );
        [eventMarkets] = await connection.query(
          `SELECT marketid,eventid,marketname FROM t_market WHERE eventid IN (${eventPlaceholders})`,
          terminalEventIds,
        );
        [eventFancies] = await connection.query(
          `SELECT fancyid AS marketid,eventid,name AS marketname FROM t_matchfancy
           WHERE eventid IN (${eventPlaceholders})`,
          terminalEventIds,
        );
        await pendingResults.enqueue(eventMarkets.map((market) => market.marketid));
        await connection.query(
          `UPDATE t_event SET isactive=?,status=?,in_play=?,updatedon=NOW()
           WHERE eventid IN (${eventPlaceholders})`,
          [false, false, false, ...terminalEventIds],
        );
        await connection.query(
          `UPDATE t_market SET isactive=?,status=?,issubscribed=?,updatedon=NOW()
           WHERE eventid IN (${eventPlaceholders})`,
          [false, false, false, ...terminalEventIds],
        );
        await connection.query(
          `UPDATE t_matchfancy SET isactive=?,isshow=?,is_show=?,issubscribed=?,updatedon=NOW()
           WHERE eventid IN (${eventPlaceholders})`,
          [false, false, false, false, ...terminalEventIds],
        );
      }
      await connection.commit();
      const closedRows = [...new Map([...markets, ...fancies, ...eventMarkets, ...eventFancies]
        .map((market) => [String(market.marketid), market])).values()];
      return { closedRows, terminalEvents };
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  });

  const { closedRows, terminalEvents } = cleanup;
  const terminalEventIds = new Set(terminalEvents.map((event) => String(event.eventid)));
  const marketIdsByEvent = new Map();
  for (const row of closedRows) {
    const eventId = String(row.eventid);
    if (!marketIdsByEvent.has(eventId)) marketIdsByEvent.set(eventId, []);
    marketIdsByEvent.get(eventId).push(String(row.marketid));
  }
  const removals = await Promise.allSettled(
    [...marketIdsByEvent].map(([eventId, idsForEvent]) =>
      terminalEventIds.has(eventId) ? redis.removeEvent(eventId).then(() => new Set(idsForEvent)) : redis.removeMarkets(eventId, idsForEvent),
    ),
  );
  const removed = removals.reduce(
    (total, result) => total + (result.status === "fulfilled" ? result.value.size : 0),
    0,
  );
  await redis.removeEventsFromMetadata(terminalEvents);
  await subscriptions.unsubscribeResultMarkets(closedRows.map((market) => market.marketid));
  await Promise.allSettled([...marketIdsByEvent.keys()].map((eventId) => {
    if (terminalEventIds.has(eventId)) return frontendSocket.publishEventRemoved(eventId, "primary-market-game-over");
    return frontendSocket.publishEventSnapshot(eventId);
  }));
  logger.info("[ResultSync] socket game-over cleanup completed", {
    markets: closedRows.length,
    events: marketIdsByEvent.size,
    removed,
    removedEvents: terminalEvents.length,
  });
  return { markets: closedRows.length, events: marketIdsByEvent.size, removed, removedEvents: terminalEvents.length };
}

async function persistExceptional(connection, market, result) {
  const label = result.isAbandoned ? "Abandoned" : "Tie";
  if (await hasExceptionalTable(connection)) {
    await connection.execute(
      `INSERT INTO t_matchabondendtie
       (date,marketid,marketname,matchid,matchname,result,sportid,sportname,status,declared_by)
       SELECT ?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS
         (SELECT 1 FROM t_matchabondendtie WHERE marketid=? LIMIT 1)`,
      [
        new Date().toISOString(),
        market.marketid,
        market.marketname,
        market.eventid,
        market.matchname,
        label,
        market.sportid,
        String(market.sportid) === "4" ? "Cricket" : null,
        true,
        "API",
        market.marketid,
      ],
    );
  } else {
    logger.warn("[ResultSync] exceptional result table is absent; result remains pending", {
      marketId: market.marketid,
      result: label,
    });
    return false;
  }
  await connection.execute(
    "UPDATE t_market SET isactive=?, status=?, issubscribed=?, updatedon=NOW() WHERE marketid=?",
    [false, false, false, market.marketid],
  );
  return true;
}

async function persistMarketResult(connection, market, result) {
  if (result.isAbandoned || result.isTie) return persistExceptional(connection, market, result);
  const selectionId = Number(result.result);
  if (!Number.isInteger(selectionId)) {
    logger.warn("[ResultSync] ignoring non-numeric market winner", {
      marketId: market.marketid,
      result: result.result,
    });
    return false;
  }
  const [selections] = await connection.execute(
    "SELECT runner_name FROM t_selectionid WHERE marketid=? AND selectionid=? LIMIT 1",
    [market.marketid, selectionId],
  );
  if (!selections.length) {
    logger.warn("[ResultSync] winner selection metadata is missing", {
      marketId: market.marketid,
      selectionId,
    });
    return false;
  }
  await connection.execute(
    `INSERT INTO t_matchresult
      (date,isresult,ismysqlupdated,marketid,marketname,markettype,matchid,matchname,result,
       resultstatus,resultstatuscron,selectionid,selectionname,sportid,status,type,declared_by)
     SELECT NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS
       (SELECT 1 FROM t_matchresult WHERE marketid=? AND selectionid=? LIMIT 1)`,
    [
      false,
      false,
      market.marketid,
      market.marketname,
      market.marketname,
      market.eventid,
      market.matchname,
      selectionId,
      "OPEN",
      false,
      selectionId,
      selections[0].runner_name,
      market.sportid,
      true,
      market.marketname,
      "API",
      market.marketid,
      selectionId,
    ],
  );
  return true;
}

async function persistFancyResult(connection, fancy, result) {
  if (result.isAbandoned) {
    await connection.execute(
      "UPDATE t_matchfancy SET isactive=?, isshow=?, is_show=?, issubscribed=?, updatedon=NOW() WHERE fancyid=?",
      [false, false, false, false, fancy.marketid],
    );
    return true;
  }
  const value = fancyResultValue(fancy.marketid, result.result, fancy.mtype);
  if (value == null) {
    logger.warn("[ResultSync] invalid fancy result", { marketId: fancy.marketid, result: result.result });
    return false;
  }
  await connection.execute(
    `INSERT INTO t_fancyresult
      (createdon,fancyid,fancyname,fancytype,isprofitlossclear,isresult,matchid,matchname,
       result,resultdeclareby,sportid,sportname,resultstatuscron,resultstatus)
     SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS
       (SELECT 1 FROM t_fancyresult WHERE fancyid=? LIMIT 1)`,
    [
      new Date().toISOString(),
      fancy.marketid,
      fancy.marketname,
      fancy.oddstype,
      false,
      false,
      fancy.eventid,
      fancy.matchname,
      value,
      "API",
      fancy.sportid,
      String(fancy.sportid) === "4" ? "CRICKET" : null,
      false,
      "OPEN",
      fancy.marketid,
    ],
  );
  await connection.execute(
    "UPDATE t_matchfancy SET isshow=?, is_show=?, issubscribed=?, updatedon=NOW() WHERE fancyid=?",
    [false, false, false, fancy.marketid],
  );
  return true;
}

async function applyResults(results, candidates) {
  const regularById = new Map(candidates.markets.map((market) => [String(market.marketid), market]));
  const fancyById = new Map(candidates.fancies.map((market) => [String(market.marketid), market]));
  const persistenceConcurrency = Math.max(
    1,
    Math.min(16, Number(process.env.RESULT_PERSIST_CONCURRENCY || 4)),
  );
  const rejectionDetailLimit = Math.max(
    0,
    Math.min(500, Number(process.env.RESULT_REJECTED_DETAIL_LIMIT || 100)),
  );
  const matched = [];
  let regularSettled = 0;
  let fancySettled = 0;
  let unmatchedResults = 0;
  let persistenceFailures = 0;
  let rejectedResults = 0;
  for (const result of results) {
    const market = regularById.get(result.marketId) || fancyById.get(result.marketId);
    if (!market) {
      unmatchedResults += 1;
      continue;
    }
    matched.push({ result, market, isFancy: fancyById.has(result.marketId) });
  }

  const dbStartedAt = Date.now();
  const persisted = await settleWithConcurrency(matched, async ({ result, market, isFancy }) => {
    const connection = await getSourcePool().getConnection();
    let transactionStarted = false;
    try {
      await connection.beginTransaction();
      transactionStarted = true;
      const saved = isFancy
        ? await persistFancyResult(connection, market, result)
        : await persistMarketResult(connection, market, result);
      if (!saved) {
        await connection.rollback();
        transactionStarted = false;
        return { status: "rejected", result, market, isFancy };
      }
      await connection.commit();
      transactionStarted = false;
      return { status: "settled", result, market, isFancy };
    } catch (error) {
      if (transactionStarted) await connection.rollback().catch(() => {});
      throw Object.assign(error, { resultMarketId: result.marketId });
    } finally {
      connection.release();
    }
  }, persistenceConcurrency);

  const dbWriteMs = Date.now() - dbStartedAt;
  const settledRows = [];
  const rejectedResultDetails = [];
  const rejectedResultReasons = {};
  for (const outcome of persisted) {
    if (outcome.status === "rejected") {
      persistenceFailures += 1;
      logger.error("[ResultSync] result persistence failed", {
        marketId: outcome.reason?.resultMarketId,
        error: outcome.reason?.message || String(outcome.reason),
      });
      continue;
    }
    if (outcome.value.status === "rejected") {
      rejectedResults += 1;
      if (!outcome.value.isFancy && Number.isInteger(Number(outcome.value.result.result)) &&
          !outcome.value.result.isTie && !outcome.value.result.isAbandoned) {
        await require("../services/resultRunnerRepair").enqueue(outcome.value.market.marketid, outcome.value.result.result)
          .catch((error) => logger.warn("[ResultSync] runner repair enqueue failed", { error: error.message }));
      }
      const detail = rejectedResultDetail(
        outcome.value.market,
        outcome.value.result,
        outcome.value.isFancy,
      );
      rejectedResultReasons[detail.reason] = (rejectedResultReasons[detail.reason] || 0) + 1;
      if (rejectedResultDetails.length < rejectionDetailLimit) {
        rejectedResultDetails.push(detail);
      }
      continue;
    }
    settledRows.push(outcome.value);
    if (outcome.value.isFancy) fancySettled += 1;
    else regularSettled += 1;
  }

  const marketsByEvent = new Map();
  for (const { result, market } of settledRows) {
    const eventId = String(market.eventid);
    if (!marketsByEvent.has(eventId)) marketsByEvent.set(eventId, []);
    marketsByEvent.get(eventId).push(result.marketId);
  }
  const redisStartedAt = Date.now();
  const eventIds = [...marketsByEvent.keys()];
  const cleanupResults = await settleWithConcurrency(
    [...marketsByEvent],
    ([eventId, marketIds]) => redis.removeMarkets(eventId, marketIds),
    persistenceConcurrency,
  );
  cleanupResults.forEach((outcome, index) => {
    if (outcome.status === "rejected") {
      logger.error("[ResultSync] Redis cleanup failed", {
        eventId: eventIds[index],
        error: outcome.reason?.message || String(outcome.reason),
      });
    }
  });
  const redisCleanupMs = Date.now() - redisStartedAt;
  const publishStartedAt = Date.now();
  const publishResults = await settleWithConcurrency(
    eventIds,
    (eventId) => frontendSocket.publishEventSnapshot(eventId),
    persistenceConcurrency,
  );
  publishResults.forEach((outcome, index) => {
    if (outcome.status === "rejected") logger.error("[ResultSync] frontend snapshot publish failed", {
      eventId: eventIds[index],
      error: outcome.reason?.message || String(outcome.reason),
    });
  });
  const snapshotPublishMs = Date.now() - publishStartedAt;
  const settled = settledRows.map(({ result }) => result.marketId);
  const unsubscribeStartedAt = Date.now();
  if (settled.length) await subscriptions.unsubscribeResultMarkets(settled);
  const unsubscribeMs = Date.now() - unsubscribeStartedAt;
  return {
    settled,
    persistenceConcurrency,
    dbWriteMs,
    redisCleanupMs,
    snapshotPublishMs,
    unsubscribeMs,
    regularSettled,
    fancySettled,
    unmatchedResults,
    persistenceFailures,
    rejectedResults,
    rejectedResultReasons,
    rejectedResultDetails,
  };
}

async function syncResults() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const startedAt = Date.now();
    const candidatesStartedAt = Date.now();
    const candidates = await loadCandidates();
    const pending = await pendingResults.load();
    const activeRegular = candidates.markets;
    const activeRegularIds = new Set(activeRegular.map((row) => String(row.marketid)));
    const queued = pending.rows.filter((row) => !activeRegularIds.has(String(row.marketid)));
    candidates.markets = interleaveResultCandidates(queued, await pendingResults.excludeReviewed(activeRegular));
    const candidateLoadMs = Date.now() - candidatesStartedAt;
    // A bounded run must not let a large regular-market backlog consume every
    // provider call before the first fancy is reached. Alternate both queues so
    // each market family receives result-polling capacity on every run.
    const all = interleaveResultCandidates(candidates.markets, candidates.fancies);
    const configuredBatchSize = Number(process.env.RESULT_BATCH_SIZE || 100);
    const batchSize = Number.isFinite(configuredBatchSize)
      ? Math.min(100, Math.max(1, Math.floor(configuredBatchSize))) : 100;
    const maxCalls = Math.max(1, Number(process.env.RESULT_MAX_CALLS_PER_RUN || 100));
    const batches = [];
    for (let index = 0; index < all.length && batches.length < maxCalls; index += batchSize) {
      batches.push(all.slice(index, index + batchSize).map((market) => market.marketid));
    }
    const requested = all.slice(0, batches.length * batchSize);
    const fancyObjects = new Set(candidates.fancies);
    const nextCursors = advanceCandidateCursors(
      requested,
      activeRegular,
      candidates.fancies,
    );
    const requestConcurrency = Math.max(
      1,
      Math.min(20, Number(process.env.RESULT_REQUEST_CONCURRENCY || 4)),
    );
    const vendorStartedAt = Date.now();
    const responses = await settleWithConcurrency(
      batches,
      (mids) => provider.results({ mids }),
      requestConcurrency,
    );
    const vendorDurationMs = Date.now() - vendorStartedAt;
    const results = responses.flatMap((response) =>
      response.status === "fulfilled" ? responseRows(response.value) : [],
    );
    const persistenceStartedAt = Date.now();
    const applied = await applyResults(results, candidates);
    await pendingResults.remove(applied.settled);
    const settledIds = new Set(applied.settled);
    const pendingIds = new Set(pending.rows.map((row) => String(row.marketid)));
    await pendingResults.defer(requested
      .map((row) => String(row.marketid))
      .filter((id) => pendingIds.has(id) && !settledIds.has(id)));
    const persistenceDurationMs = Date.now() - persistenceStartedAt;
    const output = {
      skipped: false,
      candidates: all.length,
      pendingResults: { depth: pending.depth, eligible: pending.rows.length, recoveredRows: pending.recovered, reviewCount: pending.reviewCount },
      regular: candidates.markets.length,
      fancies: candidates.fancies.length,
      calls: batches.length,
      requestConcurrency,
      requestedRegular: requested.filter((market) => !fancyObjects.has(market)).length,
      requestedFancies: requested.filter((market) => fancyObjects.has(market)).length,
      failedCalls: responses.filter((response) => response.status === "rejected").length,
      results: results.length,
      settled: applied.settled.length,
      persistenceConcurrency: applied.persistenceConcurrency,
      dbWriteMs: applied.dbWriteMs,
      redisCleanupMs: applied.redisCleanupMs,
      snapshotPublishMs: applied.snapshotPublishMs,
      unsubscribeMs: applied.unsubscribeMs,
      regularSettled: applied.regularSettled,
      fancySettled: applied.fancySettled,
      unmatchedResults: applied.unmatchedResults,
      settledMarketIds: applied.settled,
      persistenceFailures: applied.persistenceFailures,
      rejectedResults: applied.rejectedResults,
      rejectedResultReasons: applied.rejectedResultReasons,
      rejectedResultDetails: applied.rejectedResultDetails,
      cursorsUsed: candidates.cursorsUsed,
      nextCursors,
      candidateLoadMs,
      vendorDurationMs,
      persistenceDurationMs,
      durationMs: Date.now() - startedAt,
    };
    state.lastResult = output;
    if (applied.persistenceFailures) {
      state.lastError = `${applied.persistenceFailures} result persistence operation(s) failed`;
    }
    state.lastCompletedAt = new Date().toISOString();
    logger.info("[ResultSync] completed", output);
    return output;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[ResultSync] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
  }
}

function startResultSync() {
  headroomStopped = false;
  const { expression } = cronConfig.result;
  const task = cron.schedule(expression, () => void syncResults().catch(() => {}));
  const timer = setInterval(() => void syncHeadroomResults(), 10000);
  timer.unref?.();
  const stop = task.stop.bind(task);
  task.stop = () => { headroomStopped = true; clearInterval(timer); return stop(); };
  logger.info("[ResultSync] scheduled", { expression });
  return task;
}

function getResultSyncStatus() {
  return { ...state, headroom: { ...headroomState } };
}

function availableHeadroom() {
  return require("../services/resultHeadroom").headroomReason({
    rate: provider.getProviderRateLimitStatus(), counts: provider.providerLimiter.counts(),
    health: require("../services/healthSupervisor").getHealthStatus(),
  });
}

async function syncHeadroomResults() {
  if (Date.now() < headroomCooldownUntil) { headroomState.skippedReason = "failure-cooldown"; return; }
  if (headroomStopped || running) { headroomState.skippedReason = "worker-busy-or-stopped"; return; }
  const reason = availableHeadroom();
  if (reason) { headroomState.skippedReason = reason; return; }
  running = true;
  headroomState.running = true;
  headroomState.lastError = null;
  headroomState.skippedReason = null;
  let requested = [];
  try {
    const started = Date.now();
    const runnerRepair = await require("../services/resultRunnerRepair").repairOne();
    headroomState.runnerRepair = runnerRepair;
    if (headroomStopped || availableHeadroom()) { headroomState.skippedReason = "yield-after-runner-repair"; return; }
    const pending = await pendingResults.load({ adaptive: true });
    const markets = pending.rows.slice(0, 100);
    headroomState.lastResult = { requested: 0, recoveredRows: pending.recovered, queueDepth: pending.depth, reviewCount: pending.reviewCount };
    const nextReason = availableHeadroom();
    if (headroomStopped || nextReason || !markets.length) {
      headroomState.skippedReason = nextReason || (headroomStopped ? "stopped" : "no-due-markets");
      return;
    }
    requested = markets.map((row) => String(row.marketid));
    const response = await provider.results({ mids: requested }, { priority: 9, source: "result-headroom", retries: 0 });
    const applied = await applyResults(responseRows(response), { markets, fancies: [] });
    if (applied.persistenceFailures) headroomCooldownUntil = Date.now() + 60000;
    await pendingResults.remove(applied.settled);
    await pendingResults.defer(requested.filter((id) => !applied.settled.includes(id)));
    headroomState.lastResult = { ...headroomState.lastResult, requested: requested.length, settled: applied.settled.length,
      rejected: applied.rejectedResults, persistenceFailures: applied.persistenceFailures, durationMs: Date.now() - started };
    logger.info("[ResultHeadroom] completed", headroomState.lastResult);
  } catch (error) {
    headroomState.lastError = error.message;
    headroomCooldownUntil = Date.now() + 60000;
    await pendingResults.defer(requested).catch(() => {});
    logger.warn("[ResultHeadroom] failed", { error: error.message });
  } finally {
    headroomState.lastCompletedAt = new Date().toISOString();
    headroomState.running = false;
    running = false;
  }
}

async function reconcileReviewed(ids) {
  if (!Array.isArray(ids) || !ids.length || ids.length > 20 || ids.some((id) => typeof id !== "string" || !redis.validMarketIdentifier(id))) {
    throw Object.assign(new Error("Select between 1 and 20 valid market IDs"), { status: 400 });
  }
  if (running) throw Object.assign(new Error("Result reconciliation is already running; try again after it finishes"), { status: 409 });
  running = true;
  try {
    const c = await redis.getRedisClient();
    const unique = [...new Set(ids)];
    const reviewed = await c.hmGet("Pending-Regular-Results:review", unique);
    if (reviewed.some((value) => value == null)) throw Object.assign(new Error("One or more markets are no longer in review"), { status: 409 });
    const [markets] = await getSourcePool().query(
      `SELECT marketid,marketname,eventid,matchname,sportid FROM t_market WHERE marketid IN (${unique.map(() => "?").join(",")})`, unique,
    );
    const response = await provider.results({ mids: unique });
    const applied = await applyResults(responseRows(response), { markets, fancies: [] });
    if (applied.settled.length) await c.hDel("Pending-Regular-Results:review", applied.settled);
    return { requested: unique.length, settled: applied.settled, remaining: unique.filter((id) => !applied.settled.includes(id)), rejected: applied.rejectedResultDetails, persistenceFailures: applied.persistenceFailures };
  } finally { running = false; }
}

module.exports = {
  reconcileReviewed,
  responseRows,
  fancyResultValue,
  rejectedResultReason,
  rejectedResultDetail,
  interleaveResultCandidates,
  settleWithConcurrency,
  advanceCandidateCursors,
  isEventTerminalMarketName,
  loadCandidates,
  handleSocketGameOver,
  applyResults,
  syncResults,
  startResultSync,
  getResultSyncStatus,
  persistMarketResult,
  persistFancyResult,
  persistExceptional,
  __testing__: {
    candidateCursors,
    resetCandidateCursors() {
      candidateCursors.market = null;
      candidateCursors.fancy = null;
    },
  },
};
