const { monitorEventLoopDelay } = require("node:perf_hooks");
const v8 = require("node:v8");
const { integer } = require("../config/env");

// Single shared event-loop-delay histogram. healthSupervisor reports it on the
// /health endpoint; marketSync reads the same live signal to decide whether
// there is headroom to admit another not-yet-live future event onto the
// provider socket. One histogram, reset on its own timer, so both callers see
// a consistent rolling window instead of resetting each other's samples.
const loopDelay = monitorEventLoopDelay({ resolution: 20 });
let started = false;
let resetTimer;

function start() {
  if (started) return;
  started = true;
  loopDelay.enable();
  const resetMs = integer("HEALTH_CHECK_INTERVAL_MS", 15000, { min: 5000 });
  resetTimer = setInterval(() => loopDelay.reset(), resetMs);
  resetTimer.unref?.();
}

function stop() {
  if (!started) return;
  started = false;
  clearInterval(resetTimer);
  loopDelay.disable();
}

function currentEventLoopP95Ms() {
  start();
  return Number(loopDelay.percentile(95) / 1e6);
}

function currentHeapRatio() {
  const memory = process.memoryUsage();
  const heapLimit = v8.getHeapStatistics().heap_size_limit;
  return heapLimit ? memory.heapUsed / heapLimit : 0;
}

function eventLoopStatus(p95Ms) {
  if (p95Ms >= 1000) return "critical";
  if (p95Ms >= 250) return "degraded";
  return "healthy";
}

function memoryStatus(heapRatio) {
  if (heapRatio >= 0.9) return "critical";
  if (heapRatio >= 0.75) return "degraded";
  return "healthy";
}

// Gate for admitting another batch of not-yet-live future events onto the
// provider socket. Live/in-play events are never gated by this — only
// speculative early admission of future events yields to real load.
function isHealthyForAdmission() {
  return (
    eventLoopStatus(currentEventLoopP95Ms()) === "healthy" && memoryStatus(currentHeapRatio()) === "healthy"
  );
}

module.exports = {
  start,
  stop,
  currentEventLoopP95Ms,
  currentHeapRatio,
  eventLoopStatus,
  memoryStatus,
  isHealthyForAdmission,
};
