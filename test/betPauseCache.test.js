const test = require("node:test");
const assert = require("node:assert/strict");
const service = require("../src/services/betPauseCacheService");

test("market updates delete the corresponding bet-pause cache key", async (t) => {
  const deleted = [];
  service.__testing__.setClient({
    isOpen: true,
    del: async (key) => deleted.push(key),
  });
  t.after(() => service.__testing__.reset());

  assert.equal(
    await service.deleteMarketBetPause({ eid: 36059770, mid: "1.262311910" }),
    true,
  );
  assert.deepEqual(deleted, ["kalyanexch_com_redis36059770_1.262311910_bp"]);
});

test("invalid market updates do not issue cache deletions", async (t) => {
  let calls = 0;
  service.__testing__.setClient({
    isOpen: true,
    del: async () => {
      calls += 1;
    },
  });
  t.after(() => service.__testing__.reset());

  assert.equal(await service.deleteMarketBetPause({ eid: null, mid: "1.2" }), false);
  assert.equal(await service.deleteMarketBetPause({ eid: 123, mid: "bad market" }), false);
  assert.equal(calls, 0);
});
