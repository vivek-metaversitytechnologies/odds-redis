const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");

// The discovery module takes getSourcePool at load time, so the stub is installed first.
let connection;
db.getSourcePool = () => ({ getConnection: async () => connection });
const { upsertFancies } = require("../src/cron/marketDiscoverySync");
const subscriptions = require("../src/services/marketSubscriptionService");

// Production shape: discovery never writes t_matchfancy.isactive, so a closed market keeps
// reading back as active on every later upsert.
function stickyActiveDatabase(ids) {
  connection = {
    query: async (sql) =>
      sql.startsWith("SELECT") ? [ids.map((fancyid) => ({ fancyid, name: "Runs Line", isactive: 1, status: "OPEN" }))] : [[]],
    execute: async () => [[]],
    beginTransaction: async () => {},
    commit: async () => {},
    rollback: async () => {},
    release: () => {},
  };
}

const market = (marketId, marketType, isActive) => ({
  marketId,
  eventId: 1,
  marketName: "Runs Line",
  marketType,
  isActive,
  gameOver: false,
});

test("a closed fancy is deactivated once, not on every discovery cycle", async () => {
  stickyActiveDatabase(["1.901", "4.901-BB"]);
  const closed = [market("1.901", "line-market", false), market("4.901-BB", "ball-by-ball", false)];

  const first = await upsertFancies(closed);
  const second = await upsertFancies(closed);
  const third = await upsertFancies(closed);

  assert.deepEqual(first.deactivatedFancyIds.sort(), ["1.901", "4.901-BB"]);
  assert.deepEqual(second.deactivatedFancyIds, []);
  assert.deepEqual(third.deactivatedFancyIds, []);
});

test("a market that reopens can be deactivated again", async () => {
  stickyActiveDatabase(["1.902"]);

  assert.deepEqual((await upsertFancies([market("1.902", "line-market", false)])).deactivatedFancyIds, ["1.902"]);
  assert.deepEqual((await upsertFancies([market("1.902", "line-market", true)])).deactivatedFancyIds, []);
  assert.deepEqual((await upsertFancies([market("1.902", "line-market", false)])).deactivatedFancyIds, ["1.902"]);
});

test("a closed market is kept out of subscription until it reopens", async () => {
  stickyActiveDatabase(["1.903"]);

  await upsertFancies([market("1.903", "line-market", false)]);
  assert.equal(subscriptions.isMarketSuppressed("1.903"), true);
  assert.deepEqual(await subscriptions.subscribeMarkets(["1.903"]), { subscribed: [], skipped: [] });

  await upsertFancies([market("1.903", "line-market", true)]);
  assert.equal(subscriptions.isMarketSuppressed("1.903"), false);
});

test("event sync's eligibility restore does not reopen a closed market", async () => {
  stickyActiveDatabase(["1.904"]);
  await upsertFancies([market("1.904", "line-market", false)]);
  subscriptions.restoreMarketEligibility(["1.904"]);
  assert.equal(subscriptions.isMarketSuppressed("1.904"), true);
});
