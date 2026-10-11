const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const redis = require("../src/config/redis");
const provider = require("../src/services/providerApi");
const pending = require("../src/services/pendingResultQueue");
const subscriptions = require("../src/services/marketSubscriptionService");
const frontend = require("../src/services/frontendSocketService");

test("the daily review recheck settles markets with a late vendor result and drops settled entries", async (t) => {
  // Review list on 2026-10-11: the rescheduled TOSS now has a vendor result, one entry was settled
  // by hand, and one market never gets a result.
  const review = new Set(["4.108499223403-BM", "4.273870138312-BM", "1.dead"]);
  const inserted = [];
  let requested;
  t.mock.method(pending, "reviewIds", async () => [...review]);
  t.mock.method(redis, "getRedisClient", async () => ({
    isOpen: true,
    hDel: async (_key, ids) => ids.forEach((id) => review.delete(id)),
  }));
  t.mock.method(redis, "removeMarkets", async () => {});
  t.mock.method(frontend, "publishEventSnapshot", async () => {});
  t.mock.method(subscriptions, "unsubscribeResultMarkets", async () => {});
  t.mock.method(provider, "results", async (body, options) => {
    requested = body.mids;
    assert.equal(options.source, "review-recheck");
    return { data: [{ marketId: "4.108499223403-BM", marketType: "bookmaker", result: "361960", isTie: false, isAbandoned: false }] };
  });
  t.mock.method(db, "getSourcePool", () => ({
    query: async (sql, ids) => {
      if (sql.includes("information_schema")) return [[{ total: 1 }]];
      if (sql.includes("NOT EXISTS (SELECT 1 FROM t_matchresult")) {
        assert.match(sql, /t_matchabondendtie/);
        return [ids.filter((id) => id !== "4.273870138312-BM").map((marketid) => ({ marketid }))];
      }
      return [ids.map((marketid) => ({ marketid, marketname: "TOSS", eventid: 36151023, matchname: "India Legends v South Africa Legends", sportid: 4 }))];
    },
    getConnection: async () => ({
      beginTransaction: async () => {},
      commit: async () => {},
      rollback: async () => {},
      release: () => {},
      execute: async (sql, params) => {
        if (sql.startsWith("SELECT runner_name")) return [[{ runner_name: "SA-L" }]];
        inserted.push(params);
        return [{}];
      },
    }),
  }));
  const { recheckReviewed } = require("../src/cron/resultSync");

  const output = await recheckReviewed();
  assert.deepEqual(requested.sort(), ["1.dead", "4.108499223403-BM"]);
  assert.equal(output.reviewed, 3);
  assert.equal(output.dropped, 1);
  assert.deepEqual(output.settledMarketIds, ["4.108499223403-BM"]);
  assert.equal(inserted.length, 1);
  assert.equal(inserted[0][2], "4.108499223403-BM");
  assert.deepEqual([...review], ["1.dead"]);
});
