// Copies fancy deactivation from the source to the mirror. Another application deactivates
// fancies on the source directly (isactive=0, often status=CLOSE); those writes bypass the
// outbox, so the mirror keeps them active. Every run reads the fancies the mirror holds as
// active, looks them up on the source, and queues isactive=0 through the outbox for each one
// the source has deactivated.
//
// Only that direction is copied: this service never sets t_matchfancy.isactive back to 1, so a
// queued deactivation cannot overtake a newer write. Status is copied only when it is terminal
// (CLOSE/CLOSED); the socket flips OPEN/SUSPENDED itself and a copied value could be stale.
const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const mirrorDb = require("../config/mirrorDb");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const { integer } = require("../config/env");

const TERMINAL_STATUSES = new Set(["CLOSE", "CLOSED"]);
const LOOKUP_BATCH_SIZE = 500;

let running = false;
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};

function chunks(values, size) {
  const batches = [];
  for (let offset = 0; offset < values.length; offset += size) batches.push(values.slice(offset, offset + size));
  return batches;
}

async function deactivatedOnSource(fancyIds) {
  const deactivated = [];
  for (const batch of chunks(fancyIds, LOOKUP_BATCH_SIZE)) {
    const [rows] = await getSourcePool().query(
      `SELECT fancyid,status FROM t_matchfancy WHERE fancyid IN (${batch.map(() => "?").join(",")}) AND isactive=?`,
      [...batch, false],
    );
    deactivated.push(...rows);
  }
  return deactivated;
}

async function reconcileMirrorFancies() {
  if (!mirrorDb.mirrorEnabled()) return { skipped: true, reason: "mirror-disabled" };
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const limit = integer("MIRROR_FANCY_RECONCILE_LIMIT", 5000, { min: 1 });
    const [mirrorRows] = await mirrorDb.getMirrorPool().query(
      "SELECT fancyid FROM t_matchfancy WHERE isactive=? ORDER BY fancyid",
      [true],
    );
    const mirrorActive = mirrorRows.map((row) => String(row.fancyid));
    const rows = (await deactivatedOnSource(mirrorActive)).slice(0, limit);
    // One statement per copied status (null: isactive only), keyed on fancyid for the mirror.
    const byStatus = new Map();
    for (const row of rows) {
      const status = String(row.status || "").toUpperCase();
      const key = TERMINAL_STATUSES.has(status) ? row.status : null;
      if (!byStatus.has(key)) byStatus.set(key, []);
      byStatus.get(key).push(String(row.fancyid));
    }
    for (const [status, fancyIds] of byStatus) {
      for (const batch of chunks(fancyIds, LOOKUP_BATCH_SIZE)) {
        mirrorDb.queueForMirror(
          status === null
            ? "UPDATE t_matchfancy SET isactive=? WHERE fancyid IN (?) AND isactive=?"
            : "UPDATE t_matchfancy SET isactive=?,status=? WHERE fancyid IN (?) AND isactive=?",
          status === null ? [false, batch, true] : [false, status, batch, true],
        );
      }
    }
    const result = {
      skipped: false,
      mirrorActive: mirrorActive.length,
      queued: rows.length,
      byStatus: Object.fromEntries([...byStatus].map(([status, ids]) => [status ?? "unchanged", ids.length])),
      fancyIds: rows.slice(0, 100).map((row) => String(row.fancyid)),
    };
    state.lastResult = result;
    state.lastCompletedAt = new Date().toISOString();
    logger[rows.length ? "warn" : "debug"]("[MirrorFancyReconcile] completed", result);
    return result;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[MirrorFancyReconcile] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
  }
}

function startMirrorFancyReconcile() {
  const { expression } = cronConfig.mirrorFancyReconcile;
  const task = cron.schedule(expression, () => void reconcileMirrorFancies().catch(() => {}));
  logger.info("[MirrorFancyReconcile] scheduled", { expression });
  return task;
}

function getMirrorFancyReconcileStatus() {
  return { ...state };
}

module.exports = {
  reconcileMirrorFancies,
  startMirrorFancyReconcile,
  getMirrorFancyReconcileStatus,
};
