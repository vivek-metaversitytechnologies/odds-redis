const test = require("node:test");
const assert = require("node:assert/strict");

process.env.LINE_MARKET_PRICE_REFRESH_MS = "500";
process.env.LINE_MARKET_EVENT_BATCH_SIZE = "1";

const db = require("../src/config/sourceDb");
const redis = require("../src/config/redis");
const provider = require("../src/services/providerApi");
const subscriptions = require("../src/services/marketSubscriptionService");
const websocket = require("../src/services/websocketService");
const frontend = require("../src/services/frontendSocketService");

// marketDiscoverySync captures these functions when it loads, so install stable delegates first.
const state = {
  log: [],
  rows: {},
  runners: async () => ({ data: [{ runnerId: 1, name: "Runs" }] }),
  subscribe: async () => {},
};
const connection = {
  query: async () => [[]],
  beginTransaction: async () => {},
  commit: async () => {},
  rollback: async () => {},
  release: () => {},
};
db.getSourcePool = () => ({
  getConnection: async () => connection,
  query: async () => [
    ["1.900", "1.901", "1.910", "1.920"].map((marketid) => ({
      marketid,
      selectionid: 1,
      runner_name: "Runs",
    })),
  ],
});
redis.getDiscoveryEvents = async (sport) =>
  sport === 4
    ? [900, 901].map((eventId) => ({ eventId, sportId: 4, eventName: `Match ${eventId}`, inPlay: true }))
    : [];
redis.invalidateMarkets = () => {};
redis.writeTick = async () => true;
redis.reconcileRegularDefinitions = async (rows) => {
  state.log.push(`reconcile:${rows.map((row) => `${row.marketId}${row.isActive ? "+" : "-"}`).join(",")}`);
  return {
    changedEventIds: [...new Set(rows.map((row) => row.eventId))],
    removed: rows.filter((row) => !row.isActive).length,
  };
};
subscriptions.subscribeMarkets = async (ids) => {
  state.log.push(`subscribe:${ids.join(",")}`);
  await state.subscribe(ids);
};
subscriptions.unsubscribeEventMarkets = async (ids) => {
  state.log.push(`unsubscribe:${ids.join(",")}`);
};
subscriptions.isMarketSuppressed = () => false;
websocket.getSubscribedMarketIds = () => [];
frontend.publishEventSnapshot = async (eventId) => {
  state.log.push(`publish:${eventId}`);
};
provider.markets = async (args) => ({ data: args.eids.flatMap((eventId) => state.rows[eventId] || []) });
provider.runners = async (marketId) => state.runners(marketId);

const {
  syncActiveLineMarketDiscovery: sync,
  refreshLineMarketPrices,
} = require("../src/cron/marketDiscoverySync");

const line = (id, eventId, isActive = true) => ({
  id,
  eventId,
  sportId: 4,
  type: "line-market",
  name: `Line ${id}`,
  isActive,
});
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("a closed line market is removed and published while a slow price refresh is still running", async () => {
  state.rows = { 900: [line("1.900", 900), line("1.901", 900)], 901: [] };
  await sync();
  await sleep(600);

  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const priced = [];
  state.runners = async (marketId) => {
    priced.push(marketId);
    await gate;
    return { data: [{ runnerId: 1, name: "Runs" }] };
  };
  let refreshDone = false;
  const refresh = refreshLineMarketPrices().then((value) => {
    refreshDone = true;
    return value;
  });
  await sleep(20);
  assert.deepEqual(
    priced.sort(),
    ["1.900", "1.901"],
    "both markets are being refreshed and are stuck on the vendor",
  );

  state.rows = { 900: [line("1.900", 900, false), line("1.901", 900)], 901: [] };
  state.log = [];
  const cycle = await sync();

  assert.equal(refreshDone, false, "the discovery cycle must not wait for the price refresh");
  assert.equal(cycle.retired, 1);
  const removal = state.log.indexOf("reconcile:1.900-");
  assert.ok(removal >= 0, "the closed market is reconciled out of Redis");
  assert.ok(state.log.indexOf("publish:900") > removal, "and published to frontends");
  assert.ok(
    state.log.indexOf("unsubscribe:1.900") > state.log.indexOf("publish:900"),
    "the vendor unsubscribe comes after publishing",
  );

  release();
  await refresh;
  await sleep(600);
  priced.length = 0;
  state.runners = async (marketId) => {
    priced.push(marketId);
    return { data: [{ runnerId: 1, name: "Runs" }] };
  };
  await refreshLineMarketPrices();
  assert.deepEqual(priced, ["1.901"], "a closed market is no longer price-refreshed");
});

test("removals in a later batch do not wait for slow work on an earlier batch", async () => {
  state.rows = { 900: [line("1.910", 900)], 901: [line("1.920", 901, false)] };
  state.log = [];
  let release;
  let entered;
  const subscribing = new Promise((resolve) => {
    entered = resolve;
  });
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  state.subscribe = async () => {
    entered();
    await gate;
  };
  const cycle = sync();
  await subscribing;

  assert.ok(state.log.includes("reconcile:1.920-"), "event 901's closed market is already removed");
  assert.ok(state.log.includes("publish:901"), "and published while event 900 is still subscribing");
  assert.ok(
    !state.log.some((entry) => entry.startsWith("unsubscribe:")),
    "unsubscribe has not been sent yet",
  );

  release();
  assert.equal((await cycle).failedRequests, 0);
  state.subscribe = async () => {};
});
