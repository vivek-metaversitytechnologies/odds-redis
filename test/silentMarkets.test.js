// Never connect to the real provider: subscribing markets starts the provider socket.
process.env.PROVIDER_SOCKET_URL = "";

const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

sourceDb.getSourcePool = () => ({ query: async () => [[]] });
const redisStore = require("../src/config/redis");
const provider = require("../src/services/providerApi");
const websocket = require("../src/services/websocketService");

const LIVE = 36241438;
const PRE = 36241439;
const ODDS = "1.264600001";
const FANCY = "4.264600002-F2";
const PRE_ODDS = "1.264600003";

function fakeRedis() {
  const store = new Map();
  const sets = new Map();
  const hashes = new Map();
  const set = (key) => sets.get(key) || sets.set(key, new Set()).get(key);
  const hash = (key) => hashes.get(key) || hashes.set(key, new Map()).get(key);
  return {
    isOpen: true,
    get: async (key) => store.get(key) ?? null,
    set: async (key, value) => store.set(key, value),
    sAdd: async (key, values) => [].concat(values).forEach((value) => set(key).add(String(value))),
    sRem: async (key, values) => [].concat(values).forEach((value) => set(key).delete(String(value))),
    sMembers: async (key) => [...set(key)],
    hSet: async (key, values) => Object.entries(values).forEach(([field, value]) => hash(key).set(field, String(value))),
    hGetAll: async (key) => Object.fromEntries(hash(key)),
    hDel: async (key, fields) => [].concat(fields).forEach((field) => hash(key).delete(String(field))),
    expire: async () => 1,
    multi() {
      const ops = [];
      const builder = {
        set(key, value) {
          ops.push(() => store.set(key, value));
          return builder;
        },
        hSet(key, field, value) {
          ops.push(() => hash(key).set(String(field), String(value)));
          return builder;
        },
        hDel(key, field) {
          ops.push(() => hash(key).delete(String(field)));
          return builder;
        },
        expire() {
          return builder;
        },
        exec: async () => ops.forEach((op) => op()),
      };
      return builder;
    },
  };
}

const event = (eventId, inPlay) => ({
  eventId,
  eventName: `Event ${eventId}`,
  sportId: 4,
  seriesId: 9,
  openDate: "2026-10-29 15:30:00",
  inPlay,
  gameOver: false,
});

function setup(t) {
  const testing = redisStore.__testing__;
  testing.reset();
  testing.setRedisClient(fakeRedis());
  t.mock.method(provider, "runners", async () => ({ data: [] }));
  const market = (marketid, eventid, marketname, mtype) => [
    marketid,
    { marketid, eventid, marketname, mtype, isactive: true, status: "OPEN" },
  ];
  testing.primeMarketCache([
    market(ODDS, LIVE, "Match Odds", "match-odd"),
    market(FANCY, LIVE, "10 Over Runs", "session"),
    market(PRE_ODDS, PRE, "Match Odds", "match-odd"),
  ]);
  testing.primeEventMetadata([event(LIVE, true), event(PRE, false)]);
  const published = [];
  websocket.setTickPublisher((eventId, payload) => published.push({ eventId, payload }));
  t.after(async () => {
    websocket.unsubscribeMarkets([ODDS, FANCY, PRE_ODDS]);
    websocket.setTickPublisher(null);
    await websocket.stopSocket();
    testing.reset();
    delete process.env.TICK_SILENCE_SUSPEND_MS;
  });
  return published;
}

const runner = (rid) => ({ rid, s: "ACTIVE", b1: 1.5, l1: 1.52, b: 1.5, l: 1.52, bs: 100, ls: 100 });
const odds = (eid, mid) => ({ eid, mid, s: "OPEN", r: [runner(1), runner(2)] });
const fancy = () => ({ eid: LIVE, mid: FANCY, s: "OPEN", sb: "ACTIVE", r: [{ ...runner(1), sb: "ACTIVE" }] });

async function tickAll() {
  await redisStore.writeTicks([odds(LIVE, ODDS)]);
  await redisStore.writeTicks([fancy()]);
  await redisStore.writeTicks([odds(PRE, PRE_ODDS)]);
}

async function snapshot(eventId) {
  return redisStore.getEventSnapshot(eventId);
}

test("an in-play Odds market silent past 10s is suspended once and pushed to frontends", async (t) => {
  const published = setup(t);
  websocket.subscribeMarkets([ODDS, FANCY, PRE_ODDS]);
  await tickAll();
  const now = Date.now();

  assert.deepEqual(await websocket.sweepSilentMarkets(now + 5000), []);

  const updates = await websocket.sweepSilentMarkets(now + 11000);
  assert.deepEqual(updates.map((update) => [update.eventId, update.marketIds]), [[String(LIVE), [ODDS]]]);
  assert.equal(published.length, 1);
  const live = await snapshot(LIVE);
  const market = live.Odds.find((entry) => entry.marketId === ODDS);
  assert.equal(market.status, "SUSPENDED");
  assert.deepEqual(market.runners.map((r) => r.status), ["SUSPENDED", "SUSPENDED"]);
  // Fancies are outside the default groups, and pre-match markets may legitimately go quiet.
  assert.notEqual(live.Fancy2.find((entry) => entry.mid === FANCY).gstatus, "SUSPENDED");
  assert.equal((await snapshot(PRE)).Odds[0].status, "OPEN");

  // A dead market is handled once per silence, not on every sweep.
  assert.deepEqual(await websocket.sweepSilentMarkets(now + 20000), []);
  assert.equal(published.length, 1);
});

test("the next tick reopens a silence-suspended market", async (t) => {
  setup(t);
  websocket.subscribeMarkets([ODDS]);
  await redisStore.writeTicks([odds(LIVE, ODDS)]);
  await websocket.sweepSilentMarkets(Date.now() + 11000);
  const reopened = await redisStore.writeTicks([odds(LIVE, ODDS)]);
  assert.equal(reopened.changed, true);
  assert.equal(reopened.payload.Odds.find((entry) => entry.marketId === ODDS).status, "OPEN");
});

test("a pre-match market is checked again once its event goes in play", async (t) => {
  setup(t);
  websocket.subscribeMarkets([PRE_ODDS]);
  await redisStore.writeTicks([odds(PRE, PRE_ODDS)]);
  const now = Date.now();
  assert.deepEqual(await websocket.sweepSilentMarkets(now + 11000), []);
  redisStore.__testing__.primeEventMetadata([event(PRE, true)]);
  const updates = await websocket.sweepSilentMarkets(now + 12000);
  assert.deepEqual(updates.map((update) => update.marketIds), [[PRE_ODDS]]);
});

test("a market that never ticked is timed from its subscription, and 0 disables the sweep", async (t) => {
  setup(t);
  websocket.subscribeMarkets([ODDS]);
  const now = Date.now();
  // No tick at all, and no payload entry yet: nothing to suspend.
  assert.deepEqual(await websocket.sweepSilentMarkets(now + 11000), []);

  process.env.TICK_SILENCE_SUSPEND_MS = "0";
  await redisStore.writeTicks([odds(LIVE, ODDS)]);
  assert.deepEqual(await websocket.sweepSilentMarkets(now + 60000), []);
});
