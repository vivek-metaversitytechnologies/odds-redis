const test = require("node:test");
const assert = require("node:assert/strict");
const { headroomReason } = require("../src/services/resultHeadroom");

function sample() {
  const now = Date.now();
  return { now, counts: {}, health: { checkedAt: new Date(now).toISOString(),
    checks: Object.fromEntries(["database", "redis", "eventLoop", "memory", "socket"].map((key) => [key, { status: "healthy" }])) },
  rate: { effectiveRequestsPerMinute: 2400, safeWindowCap: 800, requests: {
    last60Seconds: { attempts: 250, failed: 0, aborted: 0 }, last20Seconds: { attempts: 80 },
  } } };
}

test("result headroom admits healthy idle capacity", () => {
  assert.equal(headroomReason(sample()), null);
});

test("result headroom yields to traffic, rate windows, failures and resource pressure", () => {
  for (const [change, reason] of [
    [(x) => { x.counts.QUEUED = 1; }, "vendor-busy"],
    [(x) => { x.rate.requests.last60Seconds.attempts = 300; }, "minute-budget"],
    [(x) => { x.rate.requests.last20Seconds.attempts = 400; }, "rolling-window-budget"],
    [(x) => { x.rate.requests.last60Seconds.aborted = 1; }, "recent-vendor-errors"],
    [(x) => { x.health.checks.database.status = "critical"; }, "resources-busy"],
    [(x) => { x.health.checkedAt = new Date(x.now - 61000).toISOString(); }, "health-stale"],
    [(x) => { x.rate.blockedUntil = new Date(x.now + 1000).toISOString(); }, "vendor-cooldown"],
    [(x) => { x.rate.effectiveRequestsPerMinute = 300; }, "minute-budget"],
  ]) {
    const value = sample(); change(value); assert.equal(headroomReason(value), reason);
  }
});
