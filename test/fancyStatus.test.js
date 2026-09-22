const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

let currentPool;
sourceDb.getSourcePool = () => currentPool;
const redisStore = require("../src/config/redis");

const testing = redisStore.__testing__;

function fakeRedis() {
  const store = new Map();
  return {
    isOpen: true,
    async get(key) {
      return store.has(key) ? store.get(key) : null;
    },
    async set(key, value) {
      store.set(key, value);
      return "OK";
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
