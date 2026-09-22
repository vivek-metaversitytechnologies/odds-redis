const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

let currentPool;
sourceDb.getSourcePool = () => currentPool;
const redisStore = require("../src/config/redis");

const testing = redisStore.__testing__;

function fakeRedis() {
  const store = new Map();
  const sets = new Map();
  return {
    isOpen: true,
    async get(key) {
      return store.has(key) ? store.get(key) : null;
    },
    async set(key, value) {
      store.set(key, value);
      return "OK";
    },
    async sAdd(key, values) {
      const members = sets.get(key) || new Set();
      for (const value of values) members.add(String(value));
      sets.set(key, members);
      return values.length;
    },
    async sMembers(key) {
      return [...(sets.get(key) || [])];
    },
    async expire() {
      return 1;
    },
    multi() {
      const ops = [];
      const builder = {
        set(key, value) {
          ops.push([key, value]);
          return builder;
        },
        async exec() {
          for (const [key, value] of ops) store.set(key, value);
          return ops.map(() => "OK");
        },
      };
      return builder;
    },
  };
}

function setup(t, { marketRow } = {}) {
  testing.reset();
  testing.setRedisClient(fakeRedis());
  const queries = [];
  currentPool = {
    query: async (sql, params) => {
      queries.push({ sql, params });
      return [{ affectedRows: 1 }];
    },
  };
  testing.primeMarketCache([
    [
      "4.1-F2",
      marketRow ?? {
        marketid: "4.1-F2",
        fancyid: "4.1-F2",
        eventid: 9001,
        marketname: "Session Runs",
        mtype: "session",
        status: "OPEN",
        isactive: true,
        matchname: "Alpha v Beta",
      },
    ],
  ]);
  t.after(() => testing.reset());
  return queries;
}

const tick = (sb) => ({ eid: 9001, mid: "4.1-F2", s: true, r: [{ rid: 1, na: "6 over", b: 30, l: 32, sb }] });

test("vendor ticks do not update t_matchfancy.status", async (t) => {
  const queries = setup(t);
  const result = await redisStore.writeTicks([tick("S")]);

  assert.equal(result.accepted.length, 1);
  assert.equal(queries.length, 0);
});

test("a ball-by-ball inactive socket tick blocks later socket and API re-adds", async (t) => {
  testing.reset();
  testing.setRedisClient(fakeRedis());
  testing.primeMarketCache([["4.17-BB", {
    marketid: "4.17-BB", fancyid: "4.17-BB", eventid: 9001, marketname: "17.1 Ball Run",
    mtype: "ball-by-ball", status: "OPEN", isactive: true,
  }]]);
  t.after(() => testing.reset());

  const inactive = await redisStore.writeTicks([{ eid: 9001, mid: "4.17-BB", s: false, r: [] }]);
  assert.equal(inactive.ballByBallTransitions[0].action, "redis.remove");
  const active = await redisStore.writeTicks([{ eid: 9001, mid: "4.17-BB", s: true, r: [] }]);
  assert.equal(active.ballByBallTransitions[0].action, "redis.blocked");
  assert.equal(active.payload.BallByBall.some((entry) => entry.mid === "4.17-BB"), false);
  const api = await redisStore.reconcileFancyDefinitions([{
    eventId: 9001, marketId: "4.17-BB", marketName: "17.1 Ball Run", marketType: "ball-by-ball",
    isActive: true, gameOver: false,
  }]);
  assert.equal(api.added, 0);
});

test("line-market socket ticks expose Redis transitions for diagnostic logging", async (t) => {
  testing.reset();
  testing.setRedisClient(fakeRedis());
  testing.primeMarketCache([["1.17", {
    marketid: "1.17", eventid: 9001, marketname: "17 Over Runs Line", mtype: "line-market", isactive: true,
  }]]);
  t.after(() => testing.reset());

  const inactive = await redisStore.writeTicks([{ eid: 9001, mid: "1.17", s: false, r: [] }]);
  assert.equal(inactive.lineMarketTransitions[0].action, "redis.remove");
  assert.equal(inactive.lineMarketTransitions[0].reason, "socket-inactive");
  const active = await redisStore.writeTicks([{ eid: 9001, mid: "1.17", s: true, r: [] }]);
  assert.equal(active.lineMarketTransitions[0].action, "redis.upsert");
});
