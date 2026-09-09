const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const redis = require("../src/config/redis");

test("recovery rescans old checkpoints and enqueues SQL-normalized inactive BIT rows", async (t) => {
  const enqueued = [];
  let checkpoint;
  t.mock.method(redis, "getRedisClient", async () => ({
    isOpen: true,
    get: async () => JSON.stringify({ cursor: 9999999, nextRecoveryAt: Date.now() + 3600000 }),
    eval: async (_script, options) => enqueued.push(...options.arguments.slice(1)),
    set: async (_key, value) => { checkpoint = JSON.parse(value); },
  }));
  t.mock.method(db, "getSourcePool", () => ({ query: async (sql, params) => {
    if (sql.includes("information_schema")) return [[]];
    if (sql.startsWith("SELECT id,")) {
      assert.match(sql, /isactive\+0 AS isactive/);
      assert.equal(params[0], 0);
      return [[{ id: 6488811, marketid: "1.262087180", isactive: 0 }, { id: 6488812, marketid: "1.active", isactive: 1 }]];
    }
    assert.deepEqual(params, ["1.262087180"]);
    return [[{ marketid: "1.262087180" }]];
  } }));
  const queue = require("../src/services/pendingResultQueue");
  assert.equal(await queue.__testing__.recover(true), 2);
  assert.deepEqual(enqueued, ["1.262087180"]);
  assert.equal(checkpoint.version, 2);
  assert.equal(checkpoint.completedPasses, 1);
});
