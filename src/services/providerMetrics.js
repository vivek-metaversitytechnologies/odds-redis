const { integer, boolean } = require("../config/env");
const logger = require("../utils/logger");

const KEY_PREFIX = "VendorMetrics:minute:";
let pending = new Map();
let flushTimer;
let flushPromise;

function enabled() {
  return boolean("PROVIDER_METRICS_ENABLED", true);
}

function minuteId(timestamp) {
  return new Date(timestamp).toISOString().slice(0, 16).replace(/[-:T]/g, "");
}

function metricKey(timestamp) {
  return `${KEY_PREFIX}${minuteId(timestamp)}`;
}

function field(method, route, outcome) {
  return `${method}|${route}|${outcome}`;
}

function sourceField(source, method, route, outcome) {
  return `SOURCE|${source || "unclassified"}|${method}|${route}|${outcome}`;
}

function incrementField(timestamp, name, amount = 1) {
  if (!enabled()) return;
  const key = metricKey(timestamp);
  if (!pending.has(key)) pending.set(key, new Map());
  const fields = pending.get(key);
  fields.set(name, (fields.get(name) || 0) + amount);
  scheduleFlush();
}

function increment(timestamp, method, route, outcome, amount = 1) {
  incrementField(timestamp, field(method, route, outcome), amount);
}

function recordAttempt(timestamp, method, route, source) {
  increment(timestamp, method, route, "attempts");
  incrementField(timestamp, sourceField(source, method, route, "attempts"));
}

function recordOutcome(timestamp, method, route, outcome, durationMs, source) {
  increment(timestamp, method, route, outcome);
  increment(timestamp, method, route, "durationMs", Math.max(0, Math.round(durationMs || 0)));
  incrementField(timestamp, sourceField(source, method, route, outcome));
  incrementField(
    timestamp,
    sourceField(source, method, route, "durationMs"),
    Math.max(0, Math.round(durationMs || 0)),
  );
}

function mergePending(snapshot) {
  for (const [key, fields] of snapshot) {
    if (!pending.has(key)) pending.set(key, new Map());
    const target = pending.get(key);
    for (const [name, value] of fields) target.set(name, (target.get(name) || 0) + value);
  }
}

async function flush() {
  if (flushPromise) return flushPromise;
  if (!pending.size || !enabled()) return undefined;
  const snapshot = pending;
  pending = new Map();
  flushPromise = (async () => {
    // Lazy loading avoids the existing redis -> providerApi dependency cycle.
    const { getRedisClient } = require("../config/redis");
    const client = await getRedisClient();
    const retentionSeconds = integer("PROVIDER_METRICS_RETENTION_DAYS", 14, { min: 1, max: 365 }) * 86400;
    const transaction = client.multi();
    for (const [key, fields] of snapshot) {
      for (const [name, value] of fields) transaction.hIncrBy(key, name, value);
      transaction.expire(key, retentionSeconds);
    }
    await transaction.exec();
  })()
    .catch((error) => {
      mergePending(snapshot);
      logger.warn("[ProviderMetrics] Redis flush failed", { error: error.message });
    })
    .finally(() => {
      flushPromise = null;
      if (pending.size) scheduleFlush();
    });
  return flushPromise;
}

function scheduleFlush() {
  if (flushTimer || flushPromise || !enabled()) return;
  const delay = integer("PROVIDER_METRICS_FLUSH_MS", 5000, { min: 1000, max: 60000 });
  flushTimer = setTimeout(() => {
    flushTimer = undefined;
    void flush();
  }, delay);
  flushTimer.unref?.();
}

function parseBucket(id, values) {
  const endpoints = {};
  const sources = {};
  for (const [name, rawValue] of Object.entries(values || {})) {
    const parts = name.split("|");
    if (parts[0] === "SOURCE") {
      const [, source, method, route, outcome] = parts;
      const endpoint = `${method} ${route}`;
      if (!sources[source]) sources[source] = {};
      if (!sources[source][endpoint]) sources[source][endpoint] = {};
      sources[source][endpoint][outcome] = Number(rawValue) || 0;
      continue;
    }
    const [method, route, outcome] = parts;
    const endpoint = `${method} ${route}`;
    if (!endpoints[endpoint]) endpoints[endpoint] = {};
    endpoints[endpoint][outcome] = Number(rawValue) || 0;
  }
  return { minute: id, endpoints, sources };
}

async function history(minutes = 60) {
  const limit = Math.min(1440, Math.max(1, Number(minutes) || 60));
  await flush();
  const { getRedisReadClient } = require("../config/redis");
  const client = await getRedisReadClient();
  const now = Date.now();
  const ids = Array.from({ length: limit }, (_, index) => minuteId(now - (limit - index - 1) * 60000));
  const rows = await Promise.all(ids.map((id) => client.hGetAll(`${KEY_PREFIX}${id}`)));
  const buckets = ids.map((id, index) => parseBucket(id, rows[index]));
  const totals = {};
  const sourceTotals = {};
  for (const bucket of buckets) {
    for (const [endpoint, outcomes] of Object.entries(bucket.endpoints)) {
      if (!totals[endpoint]) totals[endpoint] = {};
      for (const [outcome, value] of Object.entries(outcomes)) {
        totals[endpoint][outcome] = (totals[endpoint][outcome] || 0) + value;
      }
    }
    for (const [source, endpoints] of Object.entries(bucket.sources)) {
      if (!sourceTotals[source]) sourceTotals[source] = {};
      for (const [endpoint, outcomes] of Object.entries(endpoints)) {
        if (!sourceTotals[source][endpoint]) sourceTotals[source][endpoint] = {};
        for (const [outcome, value] of Object.entries(outcomes)) {
          sourceTotals[source][endpoint][outcome] =
            (sourceTotals[source][endpoint][outcome] || 0) + value;
        }
      }
    }
  }
  return { minutes: limit, totals, sourceTotals, buckets };
}

async function stop() {
  if (flushTimer) clearTimeout(flushTimer);
  flushTimer = undefined;
  await flush();
}

module.exports = { recordAttempt, recordOutcome, flush, history, stop, minuteId, parseBucket };
