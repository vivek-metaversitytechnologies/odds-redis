const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const logger = require("../utils/logger");
const { boolean, integer } = require("../config/env");
const closedFancies = require("../services/closedFancyTracker");
const resultSync = require("./resultSync");

// Asks the results API only for fancies the socket just closed (s=false), every few seconds, instead
// of waiting for the scheduled poller to reach them in its sweep (median ~40 s, up to 3 min when a
// sweep missed them). Settled markets leave the list; markets still unsettled after
// CLOSED_FANCY_CHASE_MAX_AGE_MS are left to the scheduled backlog sweep.
let timer;
let stopped = true;
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
    enabled: boolean("CLOSED_FANCY_CHASE_ENABLED", true),
    intervalMs: integer("CLOSED_FANCY_CHASE_INTERVAL_MS", 15000, { min: 5000, max: 600000 }),
    maxAgeMs: integer("CLOSED_FANCY_CHASE_MAX_AGE_MS", 3600000, { min: 60000 }),
  };
}

async function unsettledRows(marketIds) {
  if (!marketIds.length) return [];
  const exceptional = await resultSync.hasExceptionalTable(getSourcePool());
  const [rows] = await getSourcePool().query(
    `SELECT f.id AS candidateid, f.fancyid AS marketid, f.name AS marketname, f.oddstype, f.mtype,
            f.eventid, COALESCE(f.matchname,e.eventname) AS matchname,
            COALESCE(f.sportid,e.sportid) AS sportid
       FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
      WHERE f.fancyid IN (${marketIds.map(() => "?").join(",")})
        AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
        ${exceptional ? "AND NOT EXISTS (SELECT 1 FROM t_matchabondendtie x WHERE x.marketid=f.fancyid)" : ""}`,
    marketIds,
  );
  return rows;
}

async function chaseOnce() {
  const cfg = config();
  const startedAt = Date.now();
  state.running = true;
  state.lastStartedAt = new Date(startedAt).toISOString();
  state.lastError = null;
  try {
    const tracked = closedFancies.pending(cfg.maxAgeMs, startedAt).map((entry) => entry.marketId);
    const fancies = await unsettledRows(tracked);
    // Anything no longer unsettled (settled by the socket or the poller) stops being chased.
    const unsettled = new Set(fancies.map((row) => String(row.marketid)));
    closedFancies.remove(tracked.filter((id) => !unsettled.has(id)));
    const run = { tracked: tracked.length, unsettled: fancies.length, requests: 0, failedRequests: 0, results: 0, settled: 0 };
    const results = [];
    for (const mids of provider.resultIdBatches([...unsettled])) {
      if (halted) break;
      run.requests += 1;
      try {
        const response = await provider.results({ mids }, { priority: 2, source: "closed-fancy-chase", retries: 0 });
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
      closedFancies.remove(applied.settled);
    }
    run.durationMs = Date.now() - startedAt;
    state.lastRun = run;
    for (const key of ["requests", "failedRequests", "results", "settled"]) state.totals[key] += run[key];
    return run;
  } catch (error) {
    state.lastError = error.message;
    logger.error("[ClosedFancyChase] run failed", { error: error.message });
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

function startClosedFancyChase() {
  const cfg = config();
  state.enabled = cfg.enabled;
  if (!cfg.enabled) {
    logger.info("[ClosedFancyChase] disabled");
    return;
  }
  if (!stopped) return;
  stopped = false;
  halted = false;
  logger.info("[ClosedFancyChase] started", { intervalMs: cfg.intervalMs, maxAgeMs: cfg.maxAgeMs });
  schedule(cfg.intervalMs);
}

async function stopClosedFancyChase() {
  stopped = true;
  halted = true;
  if (timer) clearTimeout(timer);
  timer = undefined;
  if (running) await running;
}

function getClosedFancyChaseStatus() {
  return { ...state, totals: { ...state.totals }, tracked: closedFancies.size(), config: config() };
}

module.exports = { chaseOnce, startClosedFancyChase, stopClosedFancyChase, getClosedFancyChaseStatus };
