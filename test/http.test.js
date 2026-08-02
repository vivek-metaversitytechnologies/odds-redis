const assert = require("node:assert/strict");
const test = require("node:test");
const request = require("supertest");
const { createApp } = require("../src/app");

test("Helmet applies security headers to API responses", async () => {
  const response = await request(createApp()).get("/missing-route").expect(404);
  assert.equal(response.headers["x-content-type-options"], "nosniff");
  assert.equal(response.headers["x-frame-options"], "SAMEORIGIN");
  assert.equal(response.body.message, "Route not found");
});

test("manual subscription requires at least one market ID", async () => {
  const response = await request(createApp()).post("/api/source/subscribe")
    .send({ marketIds: [] }).expect(400);
  assert.equal(response.body.message, "Provide between 1 and 100 market IDs");
});

test("legacy event write registration is not exposed", async () => {
  const response = await request(createApp()).post("/api/events/subscribe-all").send({ events: [] }).expect(404);
  assert.equal(response.body.message, "Route not found");
});

test("event unsubscribe requires a valid event ID", async () => {
  const response = await request(createApp()).post("/api/source/events/not-an-id/unsubscribe").expect(400);
  assert.equal(response.body.message, "A positive numeric event ID is required");
});

test("frontend snapshot endpoint validates the event ID", async () => {
  const response = await request(createApp()).get("/betfair_api/fancy/not-an-id").expect(400);
  assert.equal(response.body.message, "A positive numeric event ID is required");
});

test("active-match endpoint validates the sport ID", async () => {
  const response = await request(createApp()).get("/betfair_api/active_match/not-an-id").expect(400);
  assert.equal(response.body.status, false);
  assert.deepEqual(response.body.data, []);
});
