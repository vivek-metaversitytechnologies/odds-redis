const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const mirrorDb = require("../src/config/mirrorDb");
const provider = require("../src/services/providerApi");

// Source runner rows by market id; discovery takes getSourcePool at load time, so the fake is
// installed before it is required.
const sourceRunners = new Map();
const sourceQueries = [];
db.getSourcePool = () => ({
  query: async (sql, ids) => {
    sourceQueries.push(sql);
    const rows = ids.flatMap((id) => sourceRunners.get(id) || []);
    if (sql.includes("SELECT DISTINCT marketid")) return [[...new Set(rows.map((row) => row.marketid))].map((marketid) => ({ marketid }))];
    return [rows];
  },
});
const { fetchAndStoreRunners } = require("../src/cron/marketDiscoverySync");

function mockMirror(t, mirrorMarkets) {
  const mirrorQueries = [];
  const queued = [];
  t.mock.method(mirrorDb, "mirrorEnabled", () => true);
  t.mock.method(mirrorDb, "getMirrorPool", () => ({
    query: async (sql, ids) => {
      mirrorQueries.push(ids);
      return [ids.filter((id) => mirrorMarkets.has(id)).map((marketid) => ({ marketid }))];
    },
  }));
  t.mock.method(mirrorDb, "queueForMirror", (sql, values) => queued.push({ sql, values }));
  t.mock.method(provider, "runners", async () => assert.fail("source already holds the runners"));
  return { mirrorQueries, queued };
}

test("source runners missing from the mirror are queued once per cooldown", async (t) => {
  sourceRunners.set("1.501", [
    { createdon: "2026-10-13 16:21:11.135000", marketid: "1.501", runner_name: "Over", selectionid: 1, is_redis_updated: 0 },
    { createdon: "2026-10-13 16:21:11.135000", marketid: "1.501", runner_name: "Under", selectionid: 2, is_redis_updated: 0 },
  ]);
  sourceRunners.set("1.502", [
    { createdon: "2026-10-13 16:21:11.135000", marketid: "1.502", runner_name: "Home", selectionid: 3, is_redis_updated: 1 },
  ]);
  const { mirrorQueries, queued } = mockMirror(t, new Set(["1.502"]));

  const result = await fetchAndStoreRunners(["1.501", "1.502"]);
  assert.equal(result.mirrorRepairedMarkets, 1);
  assert.equal(queued.length, 1);
  assert.match(queued[0].sql, /INSERT INTO t_selectionid/);
  assert.deepEqual(queued[0].values, [[
    ["2026-10-13 16:21:11.135000", "1.501", "Over", 1, 0],
    ["2026-10-13 16:21:11.135000", "1.501", "Under", 2, 0],
  ]]);

  // 1.502 is confirmed in the mirror and 1.501 is cooling down: neither is checked again.
  const again = await fetchAndStoreRunners(["1.501", "1.502"]);
  assert.equal(again.mirrorRepairedMarkets, 0);
  assert.equal(queued.length, 1);
  assert.equal(mirrorQueries.length, 1);
});

test("a mirror failure leaves runner discovery intact", async (t) => {
  sourceRunners.set("1.503", [
    { createdon: "2026-10-14 10:00:00.000000", marketid: "1.503", runner_name: "Away", selectionid: 4, is_redis_updated: 0 },
  ]);
  mockMirror(t, new Set());
  t.mock.method(mirrorDb, "getMirrorPool", () => ({
    query: async () => { throw new Error("connect ETIMEDOUT"); },
  }));

  const result = await fetchAndStoreRunners(["1.503"]);
  assert.equal(result.mirrorRepairedMarkets, 0);
  assert.equal(result.requestedMarkets, 0);
});

test("nothing is checked while the mirror is disabled", async (t) => {
  sourceRunners.set("1.504", [
    { createdon: "2026-10-14 10:00:00.000000", marketid: "1.504", runner_name: "Draw", selectionid: 5, is_redis_updated: 0 },
  ]);
  const { mirrorQueries, queued } = mockMirror(t, new Set());
  t.mock.method(mirrorDb, "mirrorEnabled", () => false);
  sourceQueries.length = 0;

  const result = await fetchAndStoreRunners(["1.504"]);
  assert.equal(result.mirrorRepairedMarkets, 0);
  assert.equal(mirrorQueries.length, 0);
  assert.equal(queued.length, 0);
  assert.equal(sourceQueries.length, 1);
});
