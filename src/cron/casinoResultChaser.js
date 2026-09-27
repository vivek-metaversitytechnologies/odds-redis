const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const logger = require("../utils/logger");
const { boolean, integer } = require("../config/env");
const resultSync = require("./resultSync");

// The vendor computes a cricket-casino result only when it is asked for that market in a small
// results request; poller-sized batches (hundreds of ids) return results that already exist but
// never create one. Measured live: a 450-id request missed the result twice, a single-id request
// created it at once, and so did a 25-id request. Without this, casino results waited 45-75 minutes
// for some other small request. This asks for every unsettled casino market of in-play cricket
// events in small batches.
const CHASE_BATCH_SIZE = 50;

let timer;
let stopped = true;
// Set only by stop(); lets a run in progress end early without blocking direct chaseOnce() calls.
let halted = false;
let running = null;
const state = {
  enabled: false,
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastRun: null,
  totals: { runs: 0, requests: 0, failedRequests: 0, results: 0, settled: 0 },
};

function config() {
  return {
    enabled: boolean("CASINO_RESULT_CHASE_ENABLED", true),
    intervalMs: integer("CASINO_RESULT_CHASE_INTERVAL_MS", 20000, { min: 5000, max: 600000 }),
  };
}

async function unsettledCasinoMarkets() {
  const exceptional = await resultSync.hasExceptionalTable(getSourcePool());
  const [rows] = await getSourcePool().query(
    `SELECT f.id AS candidateid, f.fancyid AS marketid, f.name AS marketname, f.oddstype, f.mtype,
            f.eventid, COALESCE(f.matchname,e.eventname) AS matchname,
            COALESCE(f.sportid,e.sportid) AS sportid
       FROM t_matchfancy f JOIN t_event e ON e.eventid=f.eventid
      WHERE f.mtype='cricket-casino' AND e.isactive=1 AND e.in_play=1
        AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
        ${exceptional ? "AND NOT EXISTS (SELECT 1 FROM t_matchabondendtie x WHERE x.marketid=f.fancyid)" : ""}
      ORDER BY f.eventid, f.id
      LIMIT 500`,
  );
  return rows;
}

async function chaseOnce() {
  const startedAt = Date.now();
  state.running = true;
  state.lastStartedAt = new Date(startedAt).toISOString();
  state.lastError = null;
  try {
    const fancies = await unsettledCasinoMarkets();
    const run = { markets: fancies.length, requests: 0, failedRequests: 0, results: 0, settled: 0 };
    const results = [];
    for (let index = 0; index < fancies.length && !halted; index += CHASE_BATCH_SIZE) {
      const mids = fancies.slice(index, index + CHASE_BATCH_SIZE).map((row) => String(row.marketid));
      run.requests += 1;
      try {
        const response = await provider.results({ mids }, { priority: 2, source: "casino-result-chase", retries: 0 });
        const wanted = new Set(mids);
        results.push(...resultSync.responseRows(response).filter((row) => wanted.has(row.marketId)));
      } catch (error) {
        run.failedRequests += 1;
        state.lastError = error.message;
      }
    }
    run.results = results.length;
    if (results.length) {
      const applied = await resultSync.applyResults(results, { markets: [], fancies });
      run.settled = applied.settled.length;
      logger.info("[CasinoResultChase] results settled", {
        markets: fancies.length,
        results: results.length,
        settled: applied.settled,
      });
    }
    run.durationMs = Date.now() - startedAt;
    state.lastRun = run;
    for (const key of ["requests", "failedRequests", "results", "settled"]) state.totals[key] += run[key];
    return run;
  } catch (error) {
    state.lastError = error.message;
    logger.error("[CasinoResultChase] run failed", { error: error.message });
    throw error;
  } finally {
    state.totals.runs += 1;
    state.running = false;
    state.lastCompletedAt = new Date().toISOString();
  }
}

// Runs are chained, never overlapped.
function schedule(delayMs) {
  if (stopped) return;
  timer = setTimeout(() => {
    const startedAt = Date.now();
    running = chaseOnce()
      .catch(() => {})
      .finally(() => {
        running = null;
        schedule(Math.max(0, config().intervalMs - (Date.now() - startedAt)));
      });
  }, delayMs);
  timer.unref?.();
}

function startCasinoResultChase() {
  const cfg = config();
  state.enabled = cfg.enabled;
  if (!cfg.enabled) {
    logger.info("[CasinoResultChase] disabled");
    return;
  }
  if (!stopped) return;
  stopped = false;
  halted = false;
  logger.info("[CasinoResultChase] started", { intervalMs: cfg.intervalMs, batchSize: CHASE_BATCH_SIZE });
  schedule(cfg.intervalMs);
}

async function stopCasinoResultChase() {
  stopped = true;
  halted = true;
  if (timer) clearTimeout(timer);
  timer = undefined;
  if (running) await running;
}

function getCasinoResultChaseStatus() {
  return { ...state, totals: { ...state.totals }, config: { ...config(), batchSize: CHASE_BATCH_SIZE } };
}

module.exports = {
  CHASE_BATCH_SIZE,
  chaseOnce,
  startCasinoResultChase,
  stopCasinoResultChase,
  getCasinoResultChaseStatus,
};
