// Closes fancies left OPEN on finished events. Once an event has been over for
// STALE_FANCY_CLOSE_AFTER_MINUTES, every fancy of it that is still active and OPEN is
// marked isactive=false, status=CLOSED. t_event.updatedon marks when the event ended:
// event sync leaves it untouched while a finished event stays finished.
//
// A multi-day match (a Test between days) stays isactive=1 overnight, so it is never picked.
// An event closed only because its primary markets were suspended still has a primary market
// with status=1, and event sync may reopen it; such an event is skipped as well.
//
// Both guards stop at the carry-over window: once open_date is older than
// EVENT_CARRY_OVER_MAX_DAYS, event sync can no longer reopen the event. A primary market whose
// result never arrived then keeps status=1 forever, and an ended event can keep a stale
// in_play=1 for weeks, so neither is checked for such an event.
const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const { integer } = require("../config/env");
const { retryDeadlock } = require("../utils/dbRetry");
const { PRIMARY_MARKET_SQL } = require("../utils/primaryMarket");
const { carryOverMaxDays } = require("../services/eventLifecyclePolicy");

let running = false;
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};

function closeAfterMinutes() {
  return integer("STALE_FANCY_CLOSE_AFTER_MINUTES", 120, { min: 1, max: 10080 });
}

async function staleEventIds(pool) {
  const [rows] = await pool.query(
    `SELECT DISTINCT f.eventid
     FROM t_matchfancy f JOIN t_event e ON e.eventid=f.eventid
     WHERE f.isactive=? AND UPPER(f.status)=?
       AND e.isactive=? AND e.status=?
       AND e.updatedon <= DATE_SUB(NOW(), INTERVAL ? MINUTE)
       AND (
         e.open_date < DATE_SUB(NOW(), INTERVAL ? DAY)
         OR (
           COALESCE(e.in_play,0)=0
           AND NOT EXISTS (
             SELECT 1 FROM t_market pm
              WHERE pm.eventid=e.eventid AND pm.status=? AND ${PRIMARY_MARKET_SQL}
           )
         )
       )
     ORDER BY f.eventid LIMIT ?`,
    [
      true,
      "OPEN",
      false,
      false,
      closeAfterMinutes(),
      carryOverMaxDays(),
      true,
      integer("STALE_FANCY_CLOSE_EVENT_LIMIT", 500, { min: 1 }),
    ],
  );
  return rows.map((row) => Number(row.eventid)).filter(Number.isInteger);
}

async function closeStaleFancies() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const pool = getSourcePool();
    const eventIds = await staleEventIds(pool);
    let fancies = 0;
    for (const eventId of eventIds) {
      // Keyed on eventid, so the statement replays as-is on the mirror database.
      const [result] = await retryDeadlock(() =>
        pool.query(
          `UPDATE t_matchfancy SET isactive=?,status=?,updatedon=NOW()
           WHERE eventid=? AND isactive=? AND UPPER(status)=?`,
          [false, "CLOSED", eventId, true, "OPEN"],
        ),
      );
      fancies += Number(result?.affectedRows || 0);
    }
    const result = { skipped: false, events: eventIds.length, fancies, eventIds: eventIds.slice(0, 100) };
    state.lastResult = result;
    state.lastCompletedAt = new Date().toISOString();
    logger[fancies ? "info" : "debug"]("[StaleFancyClose] completed", result);
    return result;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[StaleFancyClose] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
  }
}

function startStaleFancyClose() {
  const { expression } = cronConfig.staleFancyClose;
  const task = cron.schedule(expression, () => void closeStaleFancies().catch(() => {}));
  logger.info("[StaleFancyClose] scheduled", { expression, closeAfterMinutes: closeAfterMinutes() });
  return task;
}

function getStaleFancyCloseStatus() {
  return { ...state };
}

module.exports = {
  closeStaleFancies,
  startStaleFancyClose,
  getStaleFancyCloseStatus,
};
