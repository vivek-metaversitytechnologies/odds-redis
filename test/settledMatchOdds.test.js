const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

sourceDb.getSourcePool = () => ({ query: async () => [[]] });
const redisStore = require("../src/config/redis");
const provider = require("../src/services/providerApi");

const EVENT = 36228850;
const MID = "1.264385430";

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

const tick = (s, runnerStatuses) => ({
  eid: EVENT,
  mid: MID,
  ...(s === undefined ? {} : { s }),
  r: [60551673, 39309985].map((rid, index) => ({
    rid,
    b1: 0,
    l1: 0,
    ...(runnerStatuses ? { s: runnerStatuses[index] } : {}),
  })),
});
const matchOdds = (result) => result.payload?.Odds?.find((item) => item.marketId === MID);
const statuses = (entry) => [entry.status, ...entry.runners.map((runner) => runner.status)];

test("a tick without status does not reopen a settled Match Odds", async (t) => {
  setup(t);
  await redisStore.writeTicks([tick("CLOSED", ["WINNER", "LOSER"])]);
  // The vendor's resubscribe snapshot after a restart carries prices but no status.
  const after = matchOdds(await redisStore.writeTicks([tick(undefined)]));
  assert.deepEqual(statuses(after), ["CLOSED", "WINNER", "LOSER"]);
});

test("a tick that states a status reopens a settled Match Odds", async (t) => {
  setup(t);
  await redisStore.writeTicks([tick("CLOSED", ["WINNER", "LOSER"])]);
  const after = matchOdds(await redisStore.writeTicks([tick("OPEN", ["ACTIVE", "ACTIVE"])]));
  assert.equal(after.status, "OPEN");
});

test("an unsettled Match Odds takes a status-less tick as before", async (t) => {
  setup(t);
  await redisStore.writeTicks([tick("CLOSED", ["CLOSED", "CLOSED"])]);
  const after = matchOdds(await redisStore.writeTicks([tick(undefined)]));
  assert.notEqual(after.status, "CLOSED");
});
