const logger = require("../utils/logger");
const { boolean, integer } = require("../config/env");
const { setBounded } = require("../utils/boundedMap");

const observations = new Map();
const confirmedEvents = new Set();

function requiredConfirmations() {
  return integer("EVENT_TERMINAL_CONFIRMATIONS", 2, { min: 1, max: 10 });
}

function dryRun() {
  return boolean("EVENT_TERMINAL_DRY_RUN", false);
}

function confirmationWindowMs() {
  return integer("EVENT_TERMINAL_CONFIRMATION_WINDOW_MS", 120000, { min: 1000, max: 3600000 });
}

function observationKey(eventId, source) {
  return `${Number(eventId)}:${String(source || "unknown")}`;
}

function observe({ eventId, source, terminal, evidence = null }) {
  const id = Number(eventId);
  if (!Number.isInteger(id) || id <= 0) {
    return { valid: false, confirmed: false, execute: false, count: 0 };
  }
  const key = observationKey(id, source);
  if (!terminal) {
    const previous = observations.get(key);
    observations.delete(key);
    if (previous) {
      logger.info("[EventLifecycle] terminal observation cleared", { eventId: id, source });
    }
    return { valid: true, confirmed: false, execute: false, count: 0 };
  }
  const previous = observations.get(key);
  const now = Date.now();
  const previousAt = Date.parse(previous?.observedAt || "");
  const consecutive = Number.isFinite(previousAt) && now - previousAt <= confirmationWindowMs();
  const count = (consecutive ? previous.count : 0) + 1;
  const required = requiredConfirmations();
  const confirmed = count >= required;
  setBounded(observations, key, { count, evidence, observedAt: new Date().toISOString() }, 10000);
  if (confirmed) confirmedEvents.add(id);
  const execute = confirmed && !dryRun();
  logger[confirmed ? "warn" : "info"]("[EventLifecycle] terminal observation", {
    eventId: id,
    source,
    count,
    required,
    confirmed,
    dryRun: dryRun(),
    execute,
    evidence,
  });
  return { valid: true, confirmed, execute, count, required, dryRun: dryRun() };
}

function clearConfirmed(eventId, reason = "active-authoritative-state") {
  const id = Number(eventId);
  if (!confirmedEvents.delete(id)) return false;
  logger.info("[EventLifecycle] confirmed terminal state cleared", { eventId: id, reason });
  return true;
}

function isConfirmed(eventId) {
  return confirmedEvents.has(Number(eventId));
}

function getStatus() {
  return {
    dryRun: dryRun(),
    requiredConfirmations: requiredConfirmations(),
    confirmationWindowMs: confirmationWindowMs(),
    pendingObservations: observations.size,
    confirmedEvents: confirmedEvents.size,
    confirmedEventIds: [...confirmedEvents].slice(0, 100),
  };
}

function resetForTests() {
  observations.clear();
  confirmedEvents.clear();
}

module.exports = {
  observe,
  clearConfirmed,
  isConfirmed,
  getStatus,
  requiredConfirmations,
  dryRun,
  confirmationWindowMs,
  resetForTests,
};
