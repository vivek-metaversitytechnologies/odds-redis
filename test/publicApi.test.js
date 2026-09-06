const assert = require("node:assert/strict");
const test = require("node:test");
const request = require("supertest");
const { createPublicApiApp } = require("../src/publicApiApp");
const { publicApiPort } = require("../src/publicApiServer");
const redis = require("../src/config/redis");

test("public API exposes only the split service routes", async () => {
  const app = createPublicApiApp();
  await request(app).get("/betfair_api/fancy/not-an-id").expect(400);
  await request(app).get("/betfair_api/fancy/score/not-an-id").expect(400);
  await request(app).get("/betfair_api/active_match/not-an-id").expect(400);
  const missing = await request(app).get("/api/socket/status").expect(404);
  assert.equal(missing.body.message, "Route not found");
  await request(app).get("/admin/").expect(404);
});

test("public API rejects an invalid listen port", () => {
  const original = process.env.PUBLIC_API_PORT;
  process.env.PUBLIC_API_PORT = "invalid";
  assert.throws(() => publicApiPort(), /PUBLIC_API_PORT/);
  if (original === undefined) delete process.env.PUBLIC_API_PORT;
  else process.env.PUBLIC_API_PORT = original;
});

test("active-match response exposes its Redis and application timings", async (t) => {
  const originalGetEvents = redis.getEvents;
  const originalGetEventSnapshots = redis.getEventSnapshots;
  const originalGetActiveMatches = redis.getActiveMatches;
  t.after(() => {
    redis.getEvents = originalGetEvents;
    redis.getEventSnapshots = originalGetEventSnapshots;
    redis.getActiveMatches = originalGetActiveMatches;
  });
  redis.getActiveMatches = async () => null;
  redis.getEvents = async () => [];
  redis.getEventSnapshots = async () => new Map();

  const response = await request(createPublicApiApp()).get("/betfair_api/active_match/4").expect(200);
  assert.match(response.headers["server-timing"], /redis-events;dur=/);
  assert.match(response.headers["server-timing"], /redis-events-command;dur=/);
  assert.match(response.headers["server-timing"], /redis-snapshots;dur=/);
  assert.match(response.headers["server-timing"], /redis-snapshots-command;dur=/);
  assert.match(response.headers["server-timing"], /snapshots-parse;dur=/);
  assert.match(response.headers["server-timing"], /transform;dur=/);
  assert.match(response.headers["server-timing"], /controller;dur=/);
});
