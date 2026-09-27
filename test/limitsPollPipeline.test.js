const test = require("node:test");
const assert = require("node:assert/strict");

process.env.MARKET_LIMITS_LOG_TO_FILE = "false";
process.env.SPORT_IDS = "4";

const provider = require("../src/services/providerApi");
const redisStore = require("../src/config/redis");
const dashboard = require("../src/services/dashboardService");
const marketSettings = require("../src/services/marketSettingsService");
const websocket = require("../src/services/websocketService");
const pipelines = require("../src/services/limitsPollPipeline");

const ENV_KEYS = [
  "LIMITS_TEST_POLL_INTERVAL_MS",
  "LIMITS_TEST_POLL_BATCH_SIZE",
  "LIMITS_TEST_POLL_MAX_REQUESTS_PER_MINUTE",
  "LIMITS_TEST_POLL_CONCURRENCY",
];

function entry(mid, minBet, maxBet) {
  return { marketId: mid, minBet, maxBet };
}

function setup(t, { payloads, settings = {}, budget = { usedLastMinute: 0, perMinute: 800 } }) {
  pipelines.__testing__.resetListedEvents();
  marketSettings.__testing__.reset();
  t.after(() => ENV_KEYS.forEach((key) => delete process.env[key]));
  t.mock.method(dashboard, "activeMatchesFromRedis", async () =>
    Object.keys(payloads).map((eventId) => ({ matchId: Number(eventId) })),
  );
  t.mock.method(redisStore, "getFrontendEventPayloads", async (eventIds) => {
    return new Map(eventIds.map((id) => [id, payloads[id]]));
  });
  t.mock.method(provider, "providerBudget", () => budget);
  const calls = [];
  t.mock.method(provider, "marketSettings", async (mids, options) => {
    calls.push({ mids, options });
    return { data: mids.filter((mid) => settings[mid]).map((mid) => ({ mid, ...settings[mid] })) };
  });
  const applied = [];
  t.mock.method(websocket, "applyMarketSettings", async (item) => applied.push(item));
  return { calls, applied };
}

function pipeline(groups = ["Odds", "Bookmaker"], env = {}) {
  Object.assign(process.env, {
    LIMITS_TEST_POLL_INTERVAL_MS: "30000",
    LIMITS_TEST_POLL_BATCH_SIZE: "50",
    LIMITS_TEST_POLL_MAX_REQUESTS_PER_MINUTE: "60",
    ...env,
  });
  return pipelines.createPipeline({
    name: "test",
    groups,
    defaults: { intervalMs: 30000, batchSize: 50, concurrency: 1, maxRequestsPerMinute: 60 },
  });
}

test("polls only its groups and writes only limits that changed", async (t) => {
  const { calls, applied } = setup(t, {
    payloads: {
      101: {
        Odds: [entry("1.1", 100, 1), entry("1.1", 100, 1), entry("1.2", 50, 25000)],
        Bookmaker: [entry("1.1-BM2", 50, 25000)],
        Fancy2: [entry("4.1-F2", 1, 1)],
      },
    },
    settings: {
      "1.1": { ms: 50, mas: 50000, eid: 101 },
      "1.2": { ms: 50, mas: 25000 },
      "1.1-BM2": { ms: 50, mas: 25000 },
    },
  });

  const run = await pipeline().runOnce();

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].mids, ["1.1", "1.1-BM2", "1.2"]);
  assert.equal(calls[0].options.retries, 0);
  assert.equal(calls[0].options.priority, 8);
  assert.equal(calls[0].options.source, "limits-test");
  assert.deepEqual(applied, [{ eid: 101, mid: "1.1", settings: { ms: 50, mas: 50000 } }]);
  assert.equal(run.changed, 1);
  assert.equal(run.applied, 1);
  assert.equal(run.targets, 3);
});

test("rotates through markets when they exceed one run's request budget", async (t) => {
  const odds = ["1.1", "1.2", "1.3", "1.4", "1.5"].map((mid) => entry(mid, 50, 25000));
  const settings = Object.fromEntries(odds.map((item) => [item.marketId, { ms: 50, mas: 25000 }]));
  const { calls } = setup(t, { payloads: { 7: { Odds: odds } }, settings });
  // 2 requests/min over a 30s interval allows one request of two ids per run.
  const poller = pipeline(["Odds"], {
    LIMITS_TEST_POLL_BATCH_SIZE: "2",
    LIMITS_TEST_POLL_MAX_REQUESTS_PER_MINUTE: "2",
  });

  const first = await poller.runOnce();
  await poller.runOnce();
  await poller.runOnce();

  assert.deepEqual(
    calls.map((call) => call.mids),
    [
      ["1.1", "1.2"],
      ["1.3", "1.4"],
      ["1.5", "1.1"],
    ],
  );
  assert.equal(first.fullCycleMs, 90000);
});

test("yields when overall provider traffic is near the cap", async (t) => {
  const { calls } = setup(t, {
    payloads: { 7: { Odds: [entry("1.1", 1, 1)] } },
    budget: { usedLastMinute: 700, perMinute: 800 },
  });

  const run = await pipeline().runOnce();

  assert.equal(run.skipped, "provider-headroom");
  assert.equal(calls.length, 0);
});

test("markets the provider omits are not requested again until the backoff expires", async (t) => {
  const { calls } = setup(t, {
    payloads: { 7: { Fancy2: [entry("4.1-F2", 1, 1), entry("4.2-F2", 1, 1)] } },
    settings: { "4.2-F2": { ms: 100, mas: 1000 } },
  });
  const poller = pipeline(["Fancy2"]);

  const first = await poller.runOnce();
  const second = await poller.runOnce();

  assert.equal(first.omitted, 1);
  assert.deepEqual(calls[1].mids, ["4.2-F2"]);
  assert.equal(second.unsupported, 1);
});

test("a room update that lands during the request wins over the HTTP response", async (t) => {
  const { applied } = setup(t, { payloads: { 7: { Odds: [entry("1.1", 100, 1)] } } });
  t.mock.method(provider, "marketSettings", async () => {
    marketSettings.noteRoomUpdate("1.1");
    return { data: [{ mid: "1.1", ms: 50, mas: 25000 }] };
  });

  const run = await pipeline().runOnce();

  assert.equal(run.skippedForRoomUpdate, 1);
  assert.equal(applied.length, 0);
});

test("a different provider min stake alone never causes a write", async (t) => {
  const { applied } = setup(t, {
    payloads: { 7: { Odds: [entry("1.1", 100, 25000)] } },
    settings: { "1.1": { ms: 50, mas: 25000 } },
  });

  const run = await pipeline(["Odds"]).runOnce();

  assert.equal(run.changed, 0);
  assert.equal(applied.length, 0);
});

test("a failed request is counted and does not stop the other batches", async (t) => {
  const { applied } = setup(t, {
    payloads: { 7: { Odds: [entry("1.1", 100, 1), entry("1.2", 100, 1)] } },
  });
  t.mock.method(provider, "marketSettings", async (mids) => {
    if (mids.includes("1.1")) throw new Error("timeout");
    return { data: [{ mid: "1.2", ms: 50, mas: 25000 }] };
  });

  const run = await pipeline(["Odds"], { LIMITS_TEST_POLL_BATCH_SIZE: "1" }).runOnce();

  assert.equal(run.requests, 2);
  assert.equal(run.failedRequests, 1);
  assert.deepEqual(
    applied.map((item) => item.mid),
    ["1.2"],
  );
});

test("batch size never exceeds the settings API's 50-market response cap", async (t) => {
  const odds = Array.from({ length: 120 }, (_, index) => entry(`1.${1000 + index}`, 50, 25000));
  const { calls } = setup(t, { payloads: { 7: { Odds: odds } } });

  await pipeline(["Odds"], { LIMITS_TEST_POLL_BATCH_SIZE: "500" }).runOnce();

  assert.deepEqual(
    calls.map((call) => call.mids.length),
    [50, 50, 20],
  );
});
