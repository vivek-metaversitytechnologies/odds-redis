const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

sourceDb.getSourcePool = () => ({ query: async () => [[]] });
const redisStore = require("../src/config/redis");

const EVENT = 36111199;
const MID = "1.262893095";

function fakeRedis() {
  const store = new Map();
  const sets = new Map();
  const hashes = new Map();
  const set = (key) => sets.get(key) || sets.set(key, new Set()).get(key);
  const hash = (key) => hashes.get(key) || hashes.set(key, new Map()).get(key);
  return {
    isOpen: true,
    hashes,
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
          ops.push([key, value]);
          return builder;
        },
        exec: async () => ops.forEach(([key, value]) => store.set(key, value)),
      };
      return builder;
    },
  };
}

function setup(t) {
  const testing = redisStore.__testing__;
  testing.reset();
  const redis = fakeRedis();
  testing.setRedisClient(redis);
  testing.primeMarketCache([
    [
      MID,
      {
        marketid: MID,
        eventid: EVENT,
        marketname: "1st Innings 10 Overs Line",
        mtype: "line-market",
        isactive: true,
        status: "OPEN",
      },
    ],
  ]);
  t.after(() => {
    testing.reset();
    delete process.env.LINE_MARKET_SUSPENDED_HIDE_MS;
  });
  return redis;
}

const socket = (s) => ({ eid: EVENT, mid: MID, s, go: false, r: [{ rid: 15316, s: "ACTIVE", b1: 52, l1: 51 }] });
// The HTTP runner-price refresh carries prices but no status at all.
const refresh = () => ({ eid: EVENT, mid: MID, r: [{ rid: 15316, na: "Total Runs", b1: 52, l1: 51 }] });
const entry = (result) => result.payload?.LineMarket?.find((item) => item.marketId === MID);

test("a price refresh without status keeps the socket's SUSPENDED status", async (t) => {
  setup(t);
  process.env.LINE_MARKET_SUSPENDED_HIDE_MS = "600000";
  await redisStore.writeTicks([socket("OPEN")]);
  assert.equal(entry(await redisStore.writeTicks([socket("SUSPENDED")])).status, "SUSPENDED");

  const refreshed = entry(await redisStore.writeTicks([refresh()]));
  assert.equal(refreshed.status, "SUSPENDED");
  assert.equal(refreshed.runners[0].status, "SUSPENDED");
});

test("a brief suspension stays visible and an open tick resets the timer", async (t) => {
  const redis = setup(t);
  process.env.LINE_MARKET_SUSPENDED_HIDE_MS = "600000";
  await redisStore.writeTicks([socket("OPEN")]);
  assert.ok(entry(await redisStore.writeTicks([socket("SUSPENDED")])));
  assert.ok(redis.hashes.get(`Suspended-LineMarket-Rs:${EVENT}`).has(MID));
  assert.equal(entry(await redisStore.writeTicks([socket("OPEN")])).status, "OPEN");
  assert.equal(redis.hashes.get(`Suspended-LineMarket-Rs:${EVENT}`).has(MID), false);
});

test("a suspension outlasting the threshold hides the line until the socket reopens it", async (t) => {
  const redis = setup(t);
  process.env.LINE_MARKET_SUSPENDED_HIDE_MS = "30000";
  await redisStore.writeTicks([socket("OPEN")]);
  await redisStore.writeTicks([socket("SUSPENDED")]);
  // Backdate the start of the suspension past the threshold.
  redis.hashes.get(`Suspended-LineMarket-Rs:${EVENT}`).set(MID, String(Date.now() - 31000));

  assert.equal(entry(await redisStore.writeTicks([socket("SUSPENDED")])), undefined);
  assert.equal(entry(await redisStore.writeTicks([refresh()])), undefined);
  assert.equal(entry(await redisStore.writeTicks([socket("OPEN")])).status, "OPEN");
});
