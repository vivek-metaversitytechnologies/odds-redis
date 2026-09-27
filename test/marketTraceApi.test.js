const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");
const request = require("supertest");

process.env.INTERNAL_API_KEY = "trace-test-internal-key";
process.env.ADMIN_PANEL_PASSWORD = "trace-test-panel-password";
process.env.MARKET_TRACE_API_KEY = "trace-test-read-key";
process.env.MARKET_TRACE_LOG_DIR = path.join(__dirname, "fixtures", "market-trace");
const { createApp } = require("../src/app");

const TRACE_KEY = ["X-Market-Trace-Key", "trace-test-read-key"];

test("market trace requires the trace key or admin credentials", async () => {
  const app = createApp();
  await request(app).get("/api/market-trace").expect(401);
  await request(app).get("/api/market-trace").set("X-Market-Trace-Key", "wrong").expect(401);
  await request(app).get("/api/market-trace").set(...TRACE_KEY).expect(200);
  await request(app).get("/api/market-trace").set("X-Internal-API-Key", process.env.INTERNAL_API_KEY).expect(200);
});

test("the trace key opens nothing but the trace reports", async () => {
  const app = createApp();
  await request(app).get("/api/logs").set(...TRACE_KEY).expect(401);
  await request(app).get("/api/source/overview").set(...TRACE_KEY).expect(401);
  await request(app).post("/api/source/subscribe").set(...TRACE_KEY).send({ marketIds: ["1.1"] }).expect(401);
});

test("anomalies, event, market, recent and files views return the analysed trace", async () => {
  const app = createApp();
  const anomalies = await request(app).get("/api/market-trace?view=anomalies").set(...TRACE_KEY).expect(200);
  assert.deepEqual(Object.keys(anomalies.body.data.issues).sort(), [
    "blocked-while-live",
    "discovery-active-after-terminal",
    "ticks-before-db-row",
  ]);
  assert.equal(anomalies.body.data.latencySeconds.gameOverToResult.p50, 6);
  // 4.01-BB only ever appears blocked right at the start of the window: finished before tracing.
  assert.equal(anomalies.body.data.preTraceMarkets, 1);

  const event = await request(app).get("/api/market-trace?view=event&id=9001").set(...TRACE_KEY).expect(200);
  assert.deepEqual(
    event.body.data.markets.map((market) => market.marketId).sort(),
    ["4.01-BB", "4.17-BB", "4.18-BB"],
  );

  const market = await request(app).get("/api/market-trace?view=market&id=4.17-BB&limit=3").set(...TRACE_KEY).expect(200);
  assert.deepEqual(
    market.body.data.records.map((record) => record.stage),
    ["visibility", "discovery.row", "result"],
  );

  const recent = await request(app).get("/api/market-trace?view=recent&kind=LINE").set(...TRACE_KEY).expect(200);
  assert.deepEqual(recent.body.data.records.map((record) => record.marketId), ["1.777"]);

  const files = await request(app).get("/api/market-trace?view=files").set(...TRACE_KEY).expect(200);
  assert.equal(files.body.data[0].name, "market-trace-2026-09-27.jsonl");
});

test("invalid views and missing ids are rejected", async () => {
  const app = createApp();
  await request(app).get("/api/market-trace?view=market").set(...TRACE_KEY).expect(400);
  await request(app).get("/api/market-trace?view=nope").set(...TRACE_KEY).expect(400);
});
