// Vendor `rt` (recall): the result declared for a market was withdrawn and a new one may follow.
// When this database already holds a result for the market, an OPEN row in
// t_marketid_for_rollback asks the settlement backend to roll it back. Nothing here deletes or
// rewrites a stored result.
//
// Recalls are seen on socket ticks and in results API rows. Settled markets are unsubscribed, so
// a periodic recheck asks the results endpoint about recently settled markets as well.
const { getSourcePool } = require("../config/sourceDb");
const { querySourceOnly } = require("../config/mirrorDb");
const provider = require("./providerApi");
const logger = require("../utils/logger");
const marketTrace = require("../utils/marketTrace");
const { integer } = require("../config/env");

const ROLLBACK_STATUS = "OPEN";
const recentlyChecked = new Map();
const inFlight = new Set();
let recheckTimer = null;
let recheckRunning = false;
const state = {
  received: 0,
  recorded: 0,
  alreadyOpen: 0,
  noResult: 0,
  failed: 0,
  lastRecordedAt: null,
  lastError: null,
  recheck: { lastStartedAt: null, lastCompletedAt: null, lastChecked: 0, lastRecalled: 0, lastError: null },
};

function isRecalled(item) {
  return item?.rt === true;
}

// Checked in this order; the first table holding a result decides `markettype`.
async function findStoredResult(connection, marketId) {
  const [fancy] = await connection.query(
    "SELECT fancytype AS markettype FROM t_fancyresult WHERE fancyid=? LIMIT 1",
    [marketId],
  );
  if (fancy.length) return { table: "t_fancyresult", markettype: fancy[0].markettype };
  const [regular] = await connection.query("SELECT markettype FROM t_matchresult WHERE marketid=? LIMIT 1", [
    marketId,
  ]);
  if (regular.length) return { table: "t_matchresult", markettype: regular[0].markettype };
  // Abandoned and tied markets (regular or fancy); this table has no type column.
  const [exceptional] = await connection
    .query("SELECT marketname AS markettype FROM t_matchabondendtie WHERE marketid=? LIMIT 1", [marketId])
    .catch((error) => (error.code === "ER_NO_SUCH_TABLE" ? [[]] : Promise.reject(error)));
  if (exceptional.length) return { table: "t_matchabondendtie", markettype: exceptional[0].markettype };
  return null;
}

async function recordOne(connection, marketId, source) {
  const stored = await findStoredResult(connection, marketId);
  if (!stored) {
    state.noResult += 1;
    return "no-result";
  }
  // The mirror schema has no t_marketid_for_rollback, so this row stays on the source database.
  // One OPEN row per market; once the backend moves it on, a later recall adds a new one.
  const [insert] = await querySourceOnly(
    connection,
    `INSERT INTO t_marketid_for_rollback (marketid,markettype,status)
     SELECT ?,?,? WHERE NOT EXISTS
       (SELECT 1 FROM t_marketid_for_rollback WHERE marketid=? AND status=? LIMIT 1)`,
    [marketId, stored.markettype, ROLLBACK_STATUS, marketId, ROLLBACK_STATUS],
  );
  if (!insert.affectedRows) {
    state.alreadyOpen += 1;
    return "already-open";
  }
  state.recorded += 1;
  state.lastRecordedAt = new Date().toISOString();
  logger.warn("[ResultRecall] result recalled; rollback requested", {
    marketId,
    table: stored.table,
    markettype: stored.markettype,
    source,
  });
  marketTrace.trace("result", { marketId, outcome: "recalled", table: stored.table, source });
  return "recorded";
}

// Accepts vendor items (`{ mid, rt }` ticks or `{ marketId, rt }` result rows) and records a
// rollback for each recalled market that has a stored result. Socket ticks repeat, so a market is
// looked up at most once per cooldown.
async function recordRecalls(items, { source = "socket", force = false } = {}) {
  const now = Date.now();
  const cooldownMs = integer("RESULT_RECALL_COOLDOWN_MS", 60000, { min: 0 });
  for (const [marketId, checkedAt] of recentlyChecked) {
    if (now - checkedAt >= cooldownMs) recentlyChecked.delete(marketId);
  }
  const ids = [
    ...new Set(
      (items || [])
        .filter(isRecalled)
        .map((item) => String(item.mid ?? item.marketId ?? "").trim())
        .filter(Boolean),
    ),
  ].filter((id) => !inFlight.has(id) && (force || !recentlyChecked.has(id)));
  if (!ids.length) return { checked: 0, recorded: [] };
  state.received += ids.length;
  ids.forEach((id) => {
    inFlight.add(id);
    recentlyChecked.set(id, now);
  });
  const recorded = [];
  let connection;
  try {
    connection = await getSourcePool().getConnection();
    for (const marketId of ids) {
      try {
        if ((await recordOne(connection, marketId, source)) === "recorded") recorded.push(marketId);
      } catch (error) {
        state.failed += 1;
        state.lastError = error.message;
        // Forget the cooldown so the next recall tick retries this market.
        recentlyChecked.delete(marketId);
        logger.error("[ResultRecall] rollback request failed", { marketId, source, error: error.message });
      }
    }
  } finally {
    connection?.release();
    ids.forEach((id) => inFlight.delete(id));
  }
  return { checked: ids.length, recorded };
}

async function recentlySettledMarketIds(connection) {
  const hours = integer("RESULT_RECALL_RECHECK_HOURS", 24, { min: 1, max: 168 });
  // Bounds every scan to the newest rows by primary key; the result tables have no date index.
  const scanRows = integer("RESULT_RECALL_RECHECK_SCAN_ROWS", 5000, { min: 100, max: 50000 });
  // t_fancyresult.createdon and t_matchabondendtie.date hold ISO-8601 UTC text.
  const isoCutoff = new Date(Date.now() - hours * 3600000).toISOString();
  const [fancy] = await connection.query(
    `SELECT fancyid AS marketid FROM t_fancyresult
     WHERE id > (SELECT COALESCE(MAX(id),0) FROM t_fancyresult) - ? AND createdon >= ?`,
    [scanRows, isoCutoff],
  );
  const [regular] = await connection.query(
    `SELECT marketid FROM t_matchresult
     WHERE id > (SELECT COALESCE(MAX(id),0) FROM t_matchresult) - ? AND date >= NOW() - INTERVAL ? HOUR`,
    [scanRows, hours],
  );
  const [exceptional] = await connection
    .query(
      `SELECT marketid FROM t_matchabondendtie
       WHERE id > (SELECT COALESCE(MAX(id),0) FROM t_matchabondendtie) - ? AND date >= ?`,
      [scanRows, isoCutoff],
    )
    .catch((error) => (error.code === "ER_NO_SUCH_TABLE" ? [[]] : Promise.reject(error)));
  return [
    ...new Set([...fancy, ...regular, ...exceptional].map((row) => String(row.marketid)).filter(Boolean)),
  ];
}

function responseItems(response) {
  if (Array.isArray(response)) return response;
  if (Array.isArray(response?.data)) return response.data;
  return [];
}

async function recheckRecentResults() {
  if (recheckRunning) return { skipped: true, reason: "already-running" };
  recheckRunning = true;
  state.recheck.lastStartedAt = new Date().toISOString();
  try {
    const connection = await getSourcePool().getConnection();
    let ids;
    try {
      ids = await recentlySettledMarketIds(connection);
    } finally {
      connection.release();
    }
    let recalled = [];
    if (ids.length) {
      const response = await provider.results(
        { mids: ids },
        { priority: 9, source: "result-recall-recheck", retries: 0 },
      );
      const outcome = await recordRecalls(responseItems(response), { source: "recheck", force: true });
      recalled = outcome.recorded;
    }
    Object.assign(state.recheck, {
      lastCompletedAt: new Date().toISOString(),
      lastChecked: ids.length,
      lastRecalled: recalled.length,
      lastError: null,
    });
    return { checked: ids.length, recalled };
  } catch (error) {
    state.recheck.lastError = error.message;
    logger.error("[ResultRecall] recheck failed", { error: error.message });
    return { error: error.message };
  } finally {
    recheckRunning = false;
  }
}

function startResultRecallRecheck() {
  const intervalMs = integer("RESULT_RECALL_RECHECK_MS", 300000, { min: 0 });
  if (!intervalMs || recheckTimer) return null;
  recheckTimer = setInterval(() => void recheckRecentResults(), intervalMs);
  recheckTimer.unref?.();
  logger.info("[ResultRecall] recheck scheduled", { intervalMs });
  return recheckTimer;
}

function stopResultRecallRecheck() {
  clearInterval(recheckTimer);
  recheckTimer = null;
}

function getResultRecallStatus() {
  return { ...state, recheck: { ...state.recheck, running: recheckRunning } };
}

module.exports = {
  isRecalled,
  recordRecalls,
  recheckRecentResults,
  startResultRecallRecheck,
  stopResultRecallRecheck,
  getResultRecallStatus,
};
