const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const redis = require("../config/redis");
const subscriptions = require("../services/marketSubscriptionService");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");

let running = false;
let exceptionalTableAvailable;
const state = { running: false, lastStartedAt: null, lastCompletedAt: null,
  lastError: null, lastResult: null };

function responseRows(response) {
  const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
  return rows.map((item) => ({
    marketId: String(item?.marketId || "").trim(),
    marketType: String(item?.marketType || "").trim().toLowerCase(),
    result: item?.result == null ? null : String(item.result).trim(),
    isTie: item?.isTie === true,
    isAbandoned: item?.isAbandoned === true || String(item?.result || "").toLowerCase() === "abandoned",
  })).filter((item) => item.marketId && item.result != null);
}

function fancyResultValue(marketId, result) {
  const id = String(marketId).toUpperCase();
  if (id.includes("-OE") || id.includes("-F3")) return String(result).toLowerCase() === "back" ? 1 : 0;
  if (id.includes("-F2") || id.includes("-BB") || id.includes("-CC")) {
    const value = Number.parseInt(result, 10);
    return Number.isInteger(value) ? value : null;
  }
  return null;
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
  const sportIds = String(process.env.SPORT_IDS || "1,2,4").split(",")
    .map((value) => Number(value.trim())).filter(Number.isFinite);
  const placeholders = sportIds.map(() => "?").join(",");
  const limit = Math.max(1, Number(process.env.RESULT_MARKET_LIMIT || 2000));
  const [markets] = await getSourcePool().query(
    `SELECT m.marketid, m.marketname, m.eventid, m.matchname, m.sportid
     FROM t_market m
     WHERE m.isactive=? AND m.sportid IN (${placeholders})
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ORDER BY m.id DESC LIMIT ?`, [true, ...sportIds, limit],
  );
  const [fancies] = await getSourcePool().query(
    `SELECT f.fancyid AS marketid, f.name AS marketname, f.oddstype,
            f.eventid, COALESCE(f.matchname,e.eventname) AS matchname,
            COALESCE(f.sportid,e.sportid) AS sportid
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE f.isactive=? AND COALESCE(f.sportid,e.sportid) IN (${placeholders})
       AND COALESCE(UPPER(f.status),'') NOT IN ('SUSPENDED','CLOSED')
       AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
     ORDER BY f.id DESC LIMIT ?`, [true, ...sportIds, limit],
  );
  return { markets, fancies };
}

async function persistExceptional(connection, market, result) {
  const label = result.isAbandoned ? "Abandoned" : "Tie";
  if (await hasExceptionalTable(connection)) {
    await connection.execute(
      `INSERT INTO t_matchabondendtie
       (date,marketid,marketname,matchid,matchname,result,sportid,sportname,status,declared_by)
       SELECT ?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS
         (SELECT 1 FROM t_matchabondendtie WHERE marketid=? LIMIT 1)`,
      [new Date().toISOString(), market.marketid, market.marketname, market.eventid,
        market.matchname, label, market.sportid, String(market.sportid) === "4" ? "Cricket" : null,
        true, "API", market.marketid],
    );
  } else {
    logger.warn("[ResultSync] exceptional result table is absent; market deactivated without result row", {
      marketId: market.marketid, result: label,
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
      marketId: market.marketid, result: result.result,
    });
    return false;
  }
  const [selections] = await connection.execute(
    "SELECT runner_name FROM t_selectionid WHERE marketid=? AND selectionid=? LIMIT 1",
    [market.marketid, selectionId],
  );
  if (!selections.length) {
    logger.warn("[ResultSync] winner selection metadata is missing", {
      marketId: market.marketid, selectionId,
    });
    return false;
  }
  await connection.execute(
    `INSERT INTO t_matchresult
      (date,isresult,ismysqlupdated,marketid,marketname,markettype,matchid,matchname,result,
       resultstatus,resultstatuscron,selectionid,selectionname,sportid,sportname,status,type,declared_by)
     SELECT NOW(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS
       (SELECT 1 FROM t_matchresult WHERE marketid=? AND selectionid=? LIMIT 1)`,
    [false, false, market.marketid, market.marketname, market.marketname, market.eventid,
      market.matchname, selectionId, "OPEN", false, selectionId, selections[0].runner_name,
      market.sportid, String(market.sportid) === "4" ? "Cricket" : null, true,
      market.marketname, "API", market.marketid, selectionId],
  );
  return true;
}

async function persistFancyResult(connection, fancy, result) {
  if (result.isAbandoned) {
    await connection.execute(
      "UPDATE t_matchfancy SET status=?, isactive=?, isshow=?, is_show=?, issubscribed=?, updatedon=NOW() WHERE fancyid=?",
      ["SUSPENDED", false, false, false, false, fancy.marketid],
    );
    return true;
  }
  const value = fancyResultValue(fancy.marketid, result.result);
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
    [new Date().toISOString(), fancy.marketid, fancy.marketname, fancy.oddstype,
      false, false, fancy.eventid, fancy.matchname, value, "API", fancy.sportid,
      String(fancy.sportid) === "4" ? "CRICKET" : null, false, "OPEN", fancy.marketid],
  );
  await connection.execute(
    "UPDATE t_matchfancy SET result=?, isshow=?, is_show=?, issubscribed=?, updatedon=NOW() WHERE fancyid=?",
    [result.result, false, false, false, fancy.marketid],
  );
  return true;
}

async function applyResults(results, candidates) {
  const regularById = new Map(candidates.markets.map((market) => [String(market.marketid), market]));
  const fancyById = new Map(candidates.fancies.map((market) => [String(market.marketid), market]));
  const settled = [];
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
      if (!saved) { await connection.rollback(); continue; }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      logger.error("[ResultSync] result persistence failed", { marketId: result.marketId, error: error.message });
    } finally { connection.release(); }
    if (!saved) continue;
    try { await redis.removeMarket(market.eventid, result.marketId); }
    catch (error) {
      logger.error("[ResultSync] Redis cleanup failed", { marketId: result.marketId, error: error.message });
    }
    settled.push(result.marketId);
  }
  if (settled.length) await subscriptions.unsubscribeResultMarkets(settled);
  return settled;
}

async function syncResults() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true; state.running = true; state.lastStartedAt = new Date().toISOString(); state.lastError = null;
  try {
    const candidates = await loadCandidates();
    const all = [...candidates.markets, ...candidates.fancies];
    const batchSize = Math.max(1, Number(process.env.RESULT_BATCH_SIZE || 100));
    const maxCalls = Math.max(1, Number(process.env.RESULT_MAX_CALLS_PER_RUN || 100));
    const results = []; let calls = 0;
    for (let index = 0; index < all.length && calls < maxCalls; index += batchSize) {
      const mids = all.slice(index, index + batchSize).map((market) => market.marketid);
      const response = await provider.results({ mids }); calls += 1;
      results.push(...responseRows(response));
    }
    const settled = await applyResults(results, candidates);
    const output = { skipped: false, candidates: all.length, regular: candidates.markets.length,
      fancies: candidates.fancies.length, calls, results: results.length,
      settled: settled.length, settledMarketIds: settled };
    state.lastResult = output; state.lastCompletedAt = new Date().toISOString();
    logger.info("[ResultSync] completed", output); return output;
  } catch (error) {
    state.lastError = error.message; state.lastCompletedAt = new Date().toISOString();
    logger.error("[ResultSync] failed", { error: error.message }); throw error;
  } finally { running = false; state.running = false; }
}

function startResultSync() {
  const { expression } = cronConfig.result;
  const task = cron.schedule(expression, () => void syncResults().catch(() => {}));
  logger.info("[ResultSync] scheduled", { expression }); return task;
}

function getResultSyncStatus() { return { ...state }; }

module.exports = { responseRows, fancyResultValue, loadCandidates, applyResults, syncResults,
  startResultSync, getResultSyncStatus, persistMarketResult, persistFancyResult,
  persistExceptional };
