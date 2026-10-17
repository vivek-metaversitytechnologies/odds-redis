const assert = require("node:assert/strict");
const test = require("node:test");
const request = require("supertest");
const { createApp } = require("../src/app");

process.env.INTERNAL_API_KEY = "vendor-explorer-test-key";

const explore = (body) =>
  request(createApp())
    .post("/api/provider/explorer")
    .set("X-Internal-API-Key", process.env.INTERNAL_API_KEY)
    .send(body);

test("explorer requires admin access", async () => {
  await request(createApp()).post("/api/provider/explorer").send({ endpoint: "sports" }).expect(401);
});

test("explorer lists only read-only vendor endpoints", async () => {
  const response = await request(createApp())
    .get("/api/provider/explorer")
    .set("X-Internal-API-Key", process.env.INTERNAL_API_KEY)
    .expect(200);
  const paths = Object.values(response.body.data).map((endpoint) => endpoint.path);
  assert.ok(paths.includes("/v1/markets/results"));
  assert.ok(!paths.some((path) => /subscribe/.test(path)));
});

test("explorer rejects unknown endpoints and missing path params", async () => {
  assert.equal(
    (await explore({ endpoint: "subscribe" }).expect(400)).body.message,
    "Unknown vendor endpoint",
  );
  await explore({ endpoint: "constructor" }).expect(400);
  assert.equal(
    (await explore({ endpoint: "runners", params: {} }).expect(400)).body.message,
    "marketId is required",
  );
});

test("explorer forwards query, path params and body to the vendor", async (t) => {
  const calls = [];
  t.mock.method(global, "fetch", async (url, init) => {
    calls.push({ url: String(url), method: init.method, body: init.body });
    return new Response(JSON.stringify({ data: [{ id: 1 }] }), { status: 200 });
  });

  const events = await explore({ endpoint: "events", query: { si: 4, today: 1 } }).expect(200);
  assert.equal(events.body.data.ok, true);
  assert.deepEqual(events.body.data.body, { data: [{ id: 1 }] });
  assert.match(calls[0].url, /\/v1\/events\?si=4&today=1$/);

  await explore({ endpoint: "runners", params: { marketId: "1.23" } }).expect(200);
  assert.match(calls[1].url, /\/v1\/markets\/1\.23\/runners$/);

  await explore({ endpoint: "results", body: { mids: ["1.23"] } }).expect(200);
  assert.equal(calls[2].method, "POST");
  assert.deepEqual(JSON.parse(calls[2].body), { mids: ["1.23"] });
});

test("explorer returns the vendor's error body instead of failing", async (t) => {
  let attempts = 0;
  t.mock.method(global, "fetch", async () => {
    attempts += 1;
    return new Response(JSON.stringify({ message: "bad market" }), { status: 400 });
  });
  const response = await explore({ endpoint: "markets", body: { eids: [] } }).expect(200);
  assert.equal(attempts, 1);
  assert.equal(response.body.data.ok, false);
  assert.equal(response.body.data.status, 400);
  assert.deepEqual(response.body.data.body, { message: "bad market" });
});
