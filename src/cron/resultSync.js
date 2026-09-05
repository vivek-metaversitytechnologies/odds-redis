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

let running = false;
let exceptionalTableAvailable;
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
    String(marketType).toLowerCase() === "line-market"
  ) {
    const value = Number.parseInt(result, 10);
    return Number.isInteger(value) ? value : null;
  }
  return null;
}

function isEventTerminalMarketName(name) {
  const normalized = String(name || "").trim().toLowerCase();
  return normalized === "match odds" || normalized.includes("bookmaker");
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
    `SELECT m.marketid, m.marketname, m.eventid, m.matchname, m.sportid
     FROM t_market m LEFT JOIN t_event e ON e.eventid=m.eventid
     WHERE (m.isactive=? OR (m.isactive=? AND m.status=?
       AND m.updatedon >= DATE_SUB(NOW(), INTERVAL 48 HOUR)))
       AND m.sportid IN (${placeholders})
       AND ${eventWindowSql("e", "active")}
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ORDER BY m.id DESC LIMIT ?`,
    [true, false, false, ...sportIds, limit],
  );
  const [fancies] = await getSourcePool().query(
    `SELECT f.fancyid AS marketid, f.name AS marketname, f.oddstype, f.mtype,
            f.eventid, COALESCE(f.matchname,e.eventname) AS matchname,
            COALESCE(f.sportid,e.sportid) AS sportid
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE ((f.isactive=? AND COALESCE(UPPER(f.status),'') NOT IN ('SUSPENDED','CLOSED'))
       OR (f.isactive=? AND UPPER(f.status) IN ('SUSPENDED','CLOSED')
         AND f.updatedon >= DATE_SUB(NOW(), INTERVAL 48 HOUR)))
       AND COALESCE(f.sportid,e.sportid) IN (${placeholders})
       AND ${eventWindowSql("e", "active")}
       AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
     ORDER BY f.id DESC LIMIT ?`,
    [true, false, ...sportIds, limit],
  );
  return { markets, fancies };
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
    logger.warn("[ResultSync] exceptional result table is absent; market deactivated without result row", {
      marketId: market.marketid,
      result: label,
    });
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
  const settled = [];
  let persistenceFailures = 0;
  let rejectedResults = 0;
  const changedEventIds = new Set();
  for (const result of results) {
    const market = regularById.get(result.marketId) || fancyById.get(result.marketId);
    if (!market) continue;
    const connection = await getSourcePool().getConnection();
    let saved = false;
    try {
      await connection.beginTransaction();
      saved = fancyById.has(result.marketId)
        ? await persistFancyResult(connection, market, result)
        : await persistMarketResult(connection, market, result);
      if (!saved) {
        await connection.rollback();
        rejectedResults += 1;
        continue;
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      persistenceFailures += 1;
      logger.error("[ResultSync] result persistence failed", {
        marketId: result.marketId,
        error: error.message,
      });
    } finally {
      connection.release();
    }
    if (!saved) continue;
    try {
      await redis.removeMarket(market.eventid, result.marketId);
    } catch (error) {
      logger.error("[ResultSync] Redis cleanup failed", { marketId: result.marketId, error: error.message });
    }
    changedEventIds.add(String(market.eventid));
    settled.push(result.marketId);
  }
  for (const eventId of changedEventIds) {
    try {
      await frontendSocket.publishEventSnapshot(eventId);
    } catch (error) {
      logger.error("[ResultSync] frontend snapshot publish failed", { eventId, error: error.message });
    }
  }
  if (settled.length) await subscriptions.unsubscribeResultMarkets(settled);
  return { settled, persistenceFailures, rejectedResults };
}

async function syncResults() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const candidates = await loadCandidates();
    const all = [...candidates.markets, ...candidates.fancies];
    const batchSize = Math.max(1, Number(process.env.RESULT_BATCH_SIZE || 100));
    const maxCalls = Math.max(1, Number(process.env.RESULT_MAX_CALLS_PER_RUN || 100));
    const batches = [];
    for (let index = 0; index < all.length && batches.length < maxCalls; index += batchSize) {
      batches.push(all.slice(index, index + batchSize).map((market) => market.marketid));
    }
    const responses = await Promise.allSettled(batches.map((mids) => provider.results({ mids })));
    const results = responses.flatMap((response) =>
      response.status === "fulfilled" ? responseRows(response.value) : [],
    );
    const applied = await applyResults(results, candidates);
    const output = {
      skipped: false,
      candidates: all.length,
      regular: candidates.markets.length,
      fancies: candidates.fancies.length,
      calls: batches.length,
      failedCalls: responses.filter((response) => response.status === "rejected").length,
      results: results.length,
      settled: applied.settled.length,
      settledMarketIds: applied.settled,
      persistenceFailures: applied.persistenceFailures,
      rejectedResults: applied.rejectedResults,
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
  const { expression } = cronConfig.result;
  const task = cron.schedule(expression, () => void syncResults().catch(() => {}));
  logger.info("[ResultSync] scheduled", { expression });
  return task;
}

function getResultSyncStatus() {
  return { ...state };
}

module.exports = {
  responseRows,
  fancyResultValue,
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
};
