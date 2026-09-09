const test = require("node:test");
const assert = require("node:assert/strict");
const redis = require("../src/config/redis");
const provider = require("../src/services/providerApi");
const db = require("../src/config/sourceDb");
const repair = require("../src/services/resultRunnerRepair");

test("winner repair fetches the missing selection even when other runners exist", async (t) => {
  const calls = [];
  const member = JSON.stringify(["1.123", 42]);
  let removed = false;
  t.mock.method(redis, "getRedisClient", async () => ({
    zRangeByScore: async () => [member],
    zAdd: async (_key, values) => { assert.ok(values[0].score > Date.now() + 299000); },
    hExists: async () => false,
    zRem: async () => { removed = true; },
  }));
  t.mock.method(db, "getSourcePool", () => ({ query: async (sql, params) => {
    calls.push({ sql, params }); return [[]];
  } }));
  t.mock.method(provider, "runners", async (id, options) => {
    assert.equal(id, "1.123"); assert.equal(options.retries, 0);
    return { data: [{ runnerId: 7, name: "Other" }, { runnerId: 42, name: "Winner" }] };
  });
  const result = await repair.repairOne();
  assert.equal(result.repaired, true);
  assert.deepEqual(calls[1].params, ["1.123", "Winner", 42]);
  assert.equal(removed, true);
});

test("markets in manual review do not generate metadata requests", async (t) => {
  t.mock.method(redis, "getRedisClient", async () => ({
    zRangeByScore: async () => [JSON.stringify(["1.123", 42])],
    zAdd: async () => {}, hExists: async () => true, zRem: async () => {},
  }));
  t.mock.method(provider, "runners", () => { throw new Error("Unexpected vendor request"); });
  assert.equal((await repair.repairOne()).skipped, "manual-review");
});
