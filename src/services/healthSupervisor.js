const v8 = require("node:v8");
const resourceMonitor = require("./resourceMonitor");
const redis = require("../config/redis");
const { getSourcePool } = require("../config/sourceDb");
const websocket = require("./websocketService");
const { providerLimiter, getProviderRateLimitStatus } = require("./providerApi");
const { getCompetitionSyncStatus } = require("../cron/competitionSync");
const { getEventSyncStatus } = require("../cron/eventSync");
const { getMarketDiscoveryStatus } = require("../cron/marketDiscoverySync");
const { getMarketSyncStatus } = require("../cron/marketSync");
const { getResultSyncStatus } = require("../cron/resultSync");
const { getRedisEventCleanupStatus } = require("../cron/redisEventCleanup");
const logger = require("../utils/logger");
const { integer } = require("../config/env");

const severityRank = { healthy: 0, degraded: 1, critical: 2 };
const incidents = [];
const previousChecks = new Map();
const recoveryAt = new Map();
let timer;
let running = false;
let startedAt;
let snapshot = { status: "starting", checkedAt: null, checks: {}, incidents: [] };

function boundedTimeout(promise, timeoutMs, label) {
  let timeout;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timeout = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      timeout.unref?.();
    }),
  ]).finally(() => clearTimeout(timeout));
}

function check(status, message, details = {}) {
  return { status, message, ...details };
}

function pipelineCheck(name, state, now) {
  const maxRunMs = integer("HEALTH_PIPELINE_STUCK_MS", 120000, { min: 30000 });
  if (state?.running && state.lastStartedAt) {
    const runningMs = now - Date.parse(state.lastStartedAt);
    if (runningMs > maxRunMs) return check("critical", `${name} pipeline appears stuck`, { runningMs });
  }
  if (state?.lastError) return check("degraded", `${name} pipeline last run failed`, { error: state.lastError });
  return check("healthy", `${name} pipeline operational`, {
    running: Boolean(state?.running),
    lastCompletedAt: state?.lastCompletedAt || null,
  });
}

async function infrastructureChecks() {
  const timeoutMs = integer("HEALTH_DEPENDENCY_TIMEOUT_MS", 5000, { min: 500 });
  const checks = {};
  const redisStarted = Date.now();
  try {
    await boundedTimeout(
      (async () => {
        const client = await redis.getRedisClient();
        await client.ping();
      })(),
      timeoutMs,
      "Redis health check",
    );
    const latencyMs = Date.now() - redisStarted;
    checks.redis = check(latencyMs > 1000 ? "degraded" : "healthy", "Redis reachable", { latencyMs });
  } catch (error) {
    checks.redis = check("critical", "Redis unavailable", { error: error.message });
  }
  const databaseStarted = Date.now();
  try {
    await boundedTimeout(getSourcePool().query("SELECT 1"), timeoutMs, "Database health check");
    const latencyMs = Date.now() - databaseStarted;
    checks.database = check(latencyMs > 1000 ? "degraded" : "healthy", "Database reachable", {
      latencyMs,
    });
  } catch (error) {
    checks.database = check("critical", "Database unavailable", { error: error.message });
  }
  return checks;
}

function runtimeChecks(now) {
  const checks = {};
  const memory = process.memoryUsage();
  const heapLimit = v8.getHeapStatistics().heap_size_limit;
  const heapRatio = resourceMonitor.currentHeapRatio();
  checks.memory = check(resourceMonitor.memoryStatus(heapRatio), "Process memory sampled", {
    heapUsedMb: Number((memory.heapUsed / 1048576).toFixed(1)),
    heapLimitMb: Number((heapLimit / 1048576).toFixed(1)),
    heapPercent: Number((heapRatio * 100).toFixed(1)),
    rssMb: Number((memory.rss / 1048576).toFixed(1)),
  });
  const p95Ms = resourceMonitor.currentEventLoopP95Ms();
  checks.eventLoop = check(resourceMonitor.eventLoopStatus(p95Ms), "Event-loop delay sampled", {
    p95Ms: Number(p95Ms.toFixed(1)),
  });
  const socket = websocket.getSocketStatus();
  const startupGraceMs = integer("HEALTH_STARTUP_GRACE_MS", 30000, { min: 5000 });
  const silenceMs = integer("HEALTH_SOCKET_SILENCE_MS", 120000, { min: 30000 });
  const lastTickAgeMs = socket.lastTickAt ? now - Date.parse(socket.lastTickAt) : null;
  if (!socket.connected && now - startedAt > startupGraceMs) {
    checks.socket = check("critical", "Provider socket disconnected", {
      lastConnectError: socket.lastConnectError,
      lastDisconnectedAt: socket.lastDisconnectedAt,
    });
  } else if (
    socket.connected &&
    socket.subscribedCount > 0 &&
    (lastTickAgeMs == null ? now - startedAt > silenceMs : lastTickAgeMs > silenceMs)
  ) {
    checks.socket = check("degraded", "Provider socket connected but tick stream is silent", {
      subscribedCount: socket.subscribedCount,
      lastTickAgeMs,
    });
  } else {
    checks.socket = check("healthy", "Provider socket operational", {
      connected: socket.connected,
      subscribedCount: socket.subscribedCount,
      lastTickAgeMs,
    });
  }
  const queue = providerLimiter.counts();
  const queued = Number(queue.QUEUED || 0);
  checks.providerQueue = check(
    queued >= 1000 ? "critical" : queued >= 200 ? "degraded" : "healthy",
    "Provider request queue sampled",
    { ...queue, rateLimit: getProviderRateLimitStatus() },
  );
  return checks;
}

function pipelineChecks(now) {
  return {
    competition: pipelineCheck("competition", getCompetitionSyncStatus(), now),
    events: pipelineCheck("events", getEventSyncStatus(), now),
    discovery: pipelineCheck("discovery", getMarketDiscoveryStatus(), now),
    subscriptions: pipelineCheck("subscriptions", getMarketSyncStatus(), now),
    results: pipelineCheck("results", getResultSyncStatus(), now),
    redisEventCleanup: pipelineCheck("redisEventCleanup", getRedisEventCleanupStatus(), now),
  };
}

function recordTransitions(checks, now) {
  for (const [name, value] of Object.entries(checks)) {
    const previous = previousChecks.get(name);
    if (previous === value.status) continue;
    previousChecks.set(name, value.status);
    if (value.status === "healthy" && previous && previous !== "healthy") {
      incidents.unshift({ at: new Date(now).toISOString(), check: name, status: "recovered", message: value.message });
      logger.info("[HealthSupervisor] recovered", { check: name, ...value });
    } else if (value.status !== "healthy") {
      incidents.unshift({ at: new Date(now).toISOString(), check: name, ...value });
      logger[value.status === "critical" ? "error" : "warn"]("[HealthSupervisor] incident", {
        check: name,
        ...value,
      });
    }
  }
  incidents.splice(100);
}

function attemptSafeRecovery(checks, now) {
  const cooldownMs = integer("HEALTH_RECOVERY_COOLDOWN_MS", 300000, { min: 60000 });
  if (!["degraded", "critical"].includes(checks.socket?.status)) return;
  if (now - (recoveryAt.get("socket") || 0) < cooldownMs) return;
  recoveryAt.set("socket", now);
  websocket.reconnectSocket();
  incidents.unshift({
    at: new Date(now).toISOString(),
    check: "socket",
    status: "recovery_attempted",
    message: "Provider socket reconnect requested",
  });
}

async function runHealthCheck() {
  if (running) return snapshot;
  running = true;
  const now = Date.now();
  try {
    const checks = {
      ...(await infrastructureChecks()),
      ...runtimeChecks(now),
      ...pipelineChecks(now),
    };
    recordTransitions(checks, now);
    attemptSafeRecovery(checks, now);
    const status = Object.values(checks).reduce(
      (worst, item) => (severityRank[item.status] > severityRank[worst] ? item.status : worst),
      "healthy",
    );
    snapshot = { status, checkedAt: new Date(now).toISOString(), checks, incidents: incidents.slice(0, 25) };
    return snapshot;
  } finally {
    running = false;
  }
}

function startHealthSupervisor() {
  if (timer) return;
  startedAt = Date.now();
  resourceMonitor.start();
  const intervalMs = integer("HEALTH_CHECK_INTERVAL_MS", 15000, { min: 5000 });
  timer = setInterval(() => void runHealthCheck().catch((error) => logger.error("[HealthSupervisor] check failed", { error: error.message })), intervalMs);
  timer.unref?.();
  void runHealthCheck().catch((error) => logger.error("[HealthSupervisor] initial check failed", { error: error.message }));
}

function stopHealthSupervisor() {
  if (timer) clearInterval(timer);
  timer = undefined;
  resourceMonitor.stop();
}

function getHealthStatus() {
  return snapshot;
}

module.exports = { startHealthSupervisor, stopHealthSupervisor, runHealthCheck, getHealthStatus, pipelineCheck };
