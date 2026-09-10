const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const redis = require("../src/config/redis");
const provider = require("../src/services/providerApi");
const subscriptions = require("../src/services/marketSubscriptionService");
const websocket = require("../src/services/websocketService");
const frontend = require("../src/services/frontendSocketService");

test("line discovery retries failed reconciliation, subscribes unchanged markets and retires explicit inactive rows", async (t) => {
  let writes = 0;
  const connection = {
    query: async (sql) => {
      if (sql.includes("INSERT")) writes += 1;
      return [[]];
    },
    beginTransaction: async () => {}, commit: async () => {},
    rollback: async () => {}, release: () => {},
  };
  t.mock.method(db, "getSourcePool", () => ({
    getConnection: async () => connection,
    query: async () => [[{ marketid: "1.900", selectionid: 1, runner_name: "Runs" }]],
  }));
  t.mock.method(redis, "getDiscoveryEvents", async (sport) => sport === 4
    ? [{ eventId: 900, sportId: 4, eventName: "Test match", inPlay: true }] : []);
  t.mock.method(redis, "invalidateMarkets", () => {});
  let fail = true;
  const definitions = [];
  t.mock.method(redis, "reconcileRegularDefinitions", async (rows) => {
    if (fail) { fail = false; throw new Error("temporary Redis failure"); }
    definitions.push(rows);
    return { changedEventIds: [900] };
  });
  const subscribed = [];
  const retired = [];
  t.mock.method(subscriptions, "subscribeMarkets", async (ids) => { subscribed.push(ids); });
  t.mock.method(subscriptions, "unsubscribeEventMarkets", async (ids) => { retired.push(ids); });
  t.mock.method(subscriptions, "isMarketSuppressed", () => false);
  t.mock.method(websocket, "getSubscribedMarketIds", () => []);
  t.mock.method(frontend, "publishEventSnapshot", async () => {});
  let rows = [{ id: "1.900", eventId: 900, sportId: 4, type: "line-market", name: "Total runs", isActive: true }];
  let release;
  let entered;
  const started = new Promise((resolve) => { entered = resolve; });
  const gate = new Promise((resolve) => { release = resolve; });
  t.mock.method(provider, "markets", async (args) => {
    assert.deepEqual(args, { eids: [900], type: ["line-market"] });
    entered();
    await gate;
    return { data: rows };
  });
  // Load after mocks because discovery captures database/subscription functions.
  const { syncActiveLineMarketDiscovery: sync } = require("../src/cron/marketDiscoverySync");
  const first = sync();
  await started;
  assert.equal((await sync()).reason, "already-running");
  release();
  assert.equal((await first).failedRequests, 1);
  assert.equal((await sync()).changed, 1);
  assert.equal(writes, 2, "failed reconciliation must retry persistence");
  assert.equal((await sync()).changed, 0);
  assert.equal(subscribed.length, 2, "unchanged unsubscribed markets are retried");
  rows = [];
  await sync();
  assert.equal(retired.length, 0, "omission must not retire a market");
  rows = [{ id: "1.900", eventId: 900, sportId: 4, type: "line-market", isActive: false }];
  await sync();
  assert.deepEqual(retired, [["1.900"]]);
  assert.equal(definitions.at(-1)[0].isActive, false);
});
