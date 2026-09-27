const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

sourceDb.getSourcePool = () => ({ query: async () => [[]] });
const redisStore = require("../src/config/redis");

const EVENT = 36111199;
// Provider timestamps are epoch milliseconds.
const BASE = Date.UTC(2026, 8, 27, 8, 0, 0);
const at = (offsetMs) => BASE + offsetMs;
const MID = "4.444417773481-BB";

function fakeRedis() {
  const store = new Map();
  const sets = new Map();
  const hashes = new Map();
  const set = (key) => sets.get(key) || sets.set(key, new Set()).get(key);
  const hash = (key) => hashes.get(key) || hashes.set(key, new Map()).get(key);
  return {
    isOpen: true,
    sets,
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
        fancyid: MID,
        eventid: EVENT,
        marketname: "8.1 Ball Run SA",
        mtype: "ball-by-ball",
        status: "OPEN",
        isactive: true,
      },
    ],
  ]);
  t.after(() => testing.reset());
  return redis;
}

const tick = (s, t, extra = {}) => ({ eid: EVENT, mid: MID, s, t, r: [{ rid: 1, na: "Runs", s: "ACTIVE", b1: 1 }], ...extra });
const visible = (result) => Boolean(result.payload?.BallByBall?.some((entry) => (entry.marketId ?? entry.mid) === MID));

test("a closed ball reopened by a newer live tick becomes visible again", async (t) => {
  setup(t);
  assert.equal(visible(await redisStore.writeTicks([tick(true, at(1000))])), true);
  assert.equal(visible(await redisStore.writeTicks([tick(false, at(2000))])), false);
  // A late tick from before the closure must not resurrect it.
  assert.equal(visible(await redisStore.writeTicks([tick(true, at(1500))])), false);
  assert.equal(visible(await redisStore.writeTicks([tick(true, at(3000))])), true);
});

test("legacy terminal members without a closure time reopen only on ticks after the upgrade", async (t) => {
  const redis = setup(t);
  await redis.sAdd(`Terminal-BallByBall-Rs:${EVENT}`, [MID]);
  const now = Date.now();

  // Stamps the legacy member "closed now"; this tick is older, so it stays hidden.
  assert.equal(visible(await redisStore.writeTicks([tick(true, now - 60000)])), false);
  assert.ok(Number(redis.hashes.get(`Terminal-BallByBall-At-Rs:${EVENT}`).get(MID)) >= now);
  assert.equal(visible(await redisStore.writeTicks([tick(true, now + 60000)])), true);
});

test("game-over, recalled and abandoned ticks never reopen a ball", async (t) => {
  setup(t);
  await redisStore.writeTicks([tick(false, at(1000))]);
  for (const extra of [{ go: true }, { rt: true }, { res: "abandoned" }]) {
    assert.equal(visible(await redisStore.writeTicks([tick(true, at(5000), extra)])), false);
  }
});

test("a stale discovery closure older than the reopen does not close the ball again", async (t) => {
  const redis = setup(t);
  await redisStore.writeTicks([tick(false, at(1000))]);
  await redisStore.writeTicks([tick(true, at(3000))]);

  await redisStore.reconcileFancyDefinitions([
    {
      eventId: EVENT,
      marketId: MID,
      marketName: "8.1 Ball Run SA",
      marketType: "ball-by-ball",
      isActive: false,
      gameOver: false,
      recalled: true,
      providerTimestamp: new Date(at(2000)).toISOString(),
    },
  ]);
  assert.equal(redis.sets.get(`Terminal-BallByBall-Rs:${EVENT}`).has(MID), false);

  await redisStore.reconcileFancyDefinitions([
    {
      eventId: EVENT,
      marketId: MID,
      marketName: "8.1 Ball Run SA",
      marketType: "ball-by-ball",
      isActive: false,
      gameOver: true,
      recalled: false,
      providerTimestamp: new Date(at(4000)).toISOString(),
    },
  ]);
  assert.equal(redis.sets.get(`Terminal-BallByBall-Rs:${EVENT}`).has(MID), true);
});
