const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

sourceDb.getSourcePool = () => ({ query: async () => [[]] });
const redisStore = require("../src/config/redis");
const provider = require("../src/services/providerApi");

const EVENT = 36241438;
const MID = "1.264500001";

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
  testing.setRedisClient(fakeRedis());
  t.mock.method(provider, "runners", async () => ({ data: [] }));
  testing.primeMarketCache([
    [MID, { marketid: MID, eventid: EVENT, marketname: "Match Odds", mtype: "match-odd", isactive: true, status: "OPEN" }],
  ]);
  t.after(() => testing.reset());
}

const tick = (t, back) => ({ eid: EVENT, mid: MID, s: "OPEN", t, r: [{ rid: 1, b1: back, l1: back + 0.02 }] });
const matchOdds = (result) => result.payload?.Odds?.find((item) => item.marketId === MID);

test("a market carries the vendor's tick time and when the tick was received", async (t) => {
  setup(t);
  const result = await redisStore.writeTicks([tick(1793268000000, 1.5)], {
    receivedAtMs: Date.parse("2026-10-29T10:00:00.250Z"),
  });
  const entry = matchOdds(result);
  assert.equal(entry.providerTs, "2026-10-29T10:00:00.000Z");
  assert.equal(entry.receivedAt, "2026-10-29T10:00:00.250Z");
  // The frontend view (snapshot API and socket tick) keeps both fields.
  assert.equal(redisStore.frontendEventPayload(result.payload).Odds[0].providerTs, "2026-10-29T10:00:00.000Z");
});

test("vendor tick times in seconds or ISO text are normalised, and a missing one is null", async (t) => {
  setup(t);
  assert.equal(matchOdds(await redisStore.writeTicks([tick(1793268000, 1.5)])).providerTs, "2026-10-29T10:00:00.000Z");
  assert.equal(
    matchOdds(await redisStore.writeTicks([tick("2026-10-29T10:00:05Z", 1.6)])).providerTs,
    "2026-10-29T10:00:05.000Z",
  );
  assert.equal(matchOdds(await redisStore.writeTicks([tick(undefined, 1.7)])).providerTs, null);
});

test("a tick that changes nothing is not a change and keeps the last change's times", async (t) => {
  setup(t);
  await redisStore.writeTicks([tick(1793268000000, 1.5)], { receivedAtMs: Date.parse("2026-10-29T10:00:00Z") });
  const repeat = await redisStore.writeTicks([tick(1793268001000, 1.5)], {
    receivedAtMs: Date.parse("2026-10-29T10:00:01Z"),
  });
  assert.equal(repeat.changed, false);
  assert.equal(matchOdds(repeat).receivedAt, "2026-10-29T10:00:00.000Z");

  const moved = await redisStore.writeTicks([tick(1793268002000, 1.6)], {
    receivedAtMs: Date.parse("2026-10-29T10:00:02Z"),
  });
  assert.equal(moved.changed, true);
  assert.equal(matchOdds(moved).providerTs, "2026-10-29T10:00:02.000Z");
  assert.equal(matchOdds(moved).receivedAt, "2026-10-29T10:00:02.000Z");
});
