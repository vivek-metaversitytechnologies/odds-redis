const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

// redis.js destructures getSourcePool when it loads, so install one stable delegate first.
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

function setup(t, { marketRow, queryImpl } = {}) {
  testing.reset();
  testing.setRedisClient(fakeRedis());
  const queries = [];
  currentPool = {
    query: async (sql, params) => {
      queries.push({ sql, params });
      if (queryImpl) return queryImpl(sql, params);
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

async function send(item) {
  const result = await redisStore.writeTicks([item]);
  await redisStore.queueFancyStatusWrites(result.fancyStatusUpdates);
  return result;
}

test("a suspended fancy tick sets t_matchfancy.status to SUSPENDED once", async (t) => {
  const queries = setup(t);
  const first = await send(tick("S"));
  assert.deepEqual(first.fancyStatusUpdates, [{ marketId: "4.1-F2", eventId: "9001", status: "SUSPENDED" }]);
  assert.equal(queries.length, 1);
  assert.match(queries[0].sql, /UPDATE t_matchfancy SET status=\?/);
  assert.deepEqual(queries[0].params, ["SUSPENDED", "4.1-F2", "9001"]);

  await send(tick("S"));
  assert.equal(queries.length, 1, "repeated suspended ticks must not hit MySQL");
});

test("a resumed fancy goes back to OPEN, and an already-open one is not written", async (t) => {
  const queries = setup(t);
  await send(tick(""));
  assert.equal(queries.length, 0, "an open fancy that was never suspended needs no write");

  await send(tick("S"));
  await send(tick(""));
  assert.deepEqual(
    queries.map((query) => query.params[0]),
    ["SUSPENDED", "OPEN"],
  );
});

test("a market already stored as SUSPENDED is not rewritten and resumes to OPEN", async (t) => {
  const queries = setup(t, {
    marketRow: {
      marketid: "4.1-F2",
      fancyid: "4.1-F2",
      eventid: 9001,
      marketname: "Session Runs",
      mtype: "session",
      status: "SUSPENDED",
      isactive: true,
    },
  });
  await send(tick("S"));
  assert.equal(queries.length, 0);
  await send(tick("A"));
  assert.deepEqual(
    queries.map((query) => query.params[0]),
    ["OPEN"],
  );
});

test("rows that do not come from t_matchfancy are never written", async (t) => {
  const queries = setup(t, {
    marketRow: {
      marketid: "4.1-F2",
      eventid: 9001,
      marketname: "Session Runs",
      mtype: "session",
      status: "OPEN",
      isactive: true,
    },
  });
  const result = await send(tick("S"));
  assert.deepEqual(result.fancyStatusUpdates, []);
  assert.equal(queries.length, 0);
});

test("a failed status write is retried by the next tick", async (t) => {
  let failing = true;
  const queries = setup(t, {
    queryImpl: () => {
      if (failing) throw new Error("db down");
      return [{ affectedRows: 1 }];
    },
  });
  await send(tick("S"));
  failing = false;
  await send(tick("S"));
  assert.equal(queries.length, 2);
  assert.equal(queries[1].params[0], "SUSPENDED");
});
