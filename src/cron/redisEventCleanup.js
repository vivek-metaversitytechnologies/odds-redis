const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const redisStore = require("../config/redis");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const { csvIntegers, integer } = require("../config/env");

let running = false;
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};

function eventIdFromKey(key, prefixes) {
  const value = String(key || "");
  for (const prefix of prefixes) {
    if (!value.startsWith(prefix)) continue;
    const eventId = value.slice(prefix.length);
    return /^\d+$/.test(eventId) && Number(eventId) > 0 ? eventId : null;
  }
  return null;
}

async function activeEventIds() {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  const cachedBySport = await Promise.all(sportIds.map((sportId) => redisStore.getEvents(sportId)));
  if (cachedBySport.length && cachedBySport.every((events) => events !== null)) {
    return new Set(
      cachedBySport
        .flat()
        .filter((event) => !event.gameOver)
        .map((event) => String(event.eventId))
        .filter((id) => /^\d+$/.test(id)),
    );
  }
  const [rows] = await getSourcePool().query(
    "SELECT eventid FROM t_event WHERE isactive = ? AND eventid IS NOT NULL",
    [true],
  );
  return new Set(rows.map((row) => String(row.eventid)).filter((id) => /^\d+$/.test(id)));
}

async function redisEventIds() {
  const client = await redisStore.getRedisClient();
  if (!client?.isOpen) throw new Error("Redis is not connected");
  const dataPrefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  const scorePrefix = process.env.REDIS_SCORE_KEY_PREFIX || "Score-Rs:";
  const prefixes = [dataPrefix, scorePrefix];
  const count = integer("REDIS_EVENT_CLEANUP_SCAN_COUNT", 500, { min: 10, max: 10000 });
  const ids = new Set();
  let keysScanned = 0;
  for (const prefix of prefixes) {
    for await (const keys of client.scanIterator({ MATCH: `${prefix}*`, COUNT: count })) {
      const batch = Array.isArray(keys) ? keys : [keys];
      keysScanned += batch.length;
      for (const key of batch) {
        const eventId = eventIdFromKey(key, prefixes);
        if (eventId) ids.add(eventId);
      }
    }
  }
  return { ids, keysScanned };
}

async function reconcileRedisEvents() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    // Resolve the authoritative active set before scanning or deleting Redis. If the
    // database query fails, the job exits without mutating Redis.
    const active = await activeEventIds();
    const scanned = await redisEventIds();
    const stale = [...scanned.ids].filter((eventId) => !active.has(eventId));
    const results = await Promise.allSettled(stale.map((eventId) => redisStore.removeEvent(eventId)));
    const removedEventIds = stale.filter((_, index) => results[index].status === "fulfilled");
    const failed = results.length - removedEventIds.length;
    const result = {
      skipped: false,
      activeEvents: active.size,
      redisEvents: scanned.ids.size,
      keysScanned: scanned.keysScanned,
      staleEvents: stale.length,
      removed: removedEventIds.length,
      failed,
      removedEventIds,
    };
    state.lastResult = result;
    state.lastCompletedAt = new Date().toISOString();
    logger.info("[RedisEventCleanup] completed", result);
    return result;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[RedisEventCleanup] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
  }
}

function startRedisEventCleanup() {
  const { expression, runOnStart } = cronConfig.redisEventCleanup;
  const task = cron.schedule(expression, () => void reconcileRedisEvents().catch(() => {}));
  logger.info("[RedisEventCleanup] scheduled", { expression });
  if (runOnStart) setImmediate(() => void reconcileRedisEvents().catch(() => {}));
  return task;
}

function getRedisEventCleanupStatus() {
  return { ...state };
}

module.exports = {
  eventIdFromKey,
  activeEventIds,
  redisEventIds,
  reconcileRedisEvents,
  startRedisEventCleanup,
  getRedisEventCleanupStatus,
};
