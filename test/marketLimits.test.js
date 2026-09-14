const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");
const { providerLimits } = require("../src/utils/marketLimits");

test("provider stake limits accept numeric strings and zero, and ignore missing or invalid values", () => {
  assert.deepEqual(providerLimits({ ms: "50", mas: 200000 }), { providerMinBet: 50, providerMaxBet: 200000 });
  assert.deepEqual(providerLimits({ ms: 0 }), { providerMinBet: 0 });
  for (const value of [null, undefined, "", " ", false, [], {}, -1, "invalid", Infinity]) {
    assert.deepEqual(providerLimits({ ms: value, mas: value }), {});
  }
});

test("market settings persist both DB tables and preserve live prices across later ticks", async (t) => {
  const rows = new Map([
    [
      "1.9904",
      {
        marketid: "1.9904",
        eventid: 9904,
        marketname: "Runs Line",
        mtype: "line-market",
        isactive: true,
        minbet: 100,
        maxbet: 1000,
      },
    ],
    [
      "4.9904-F2",
      {
        marketid: "4.9904-F2",
        fancyid: "4.9904-F2",
        eventid: 9904,
        marketname: "Session Runs",
        mtype: "session",
        isactive: true,
        minbet: 100,
        maxbet: 1000,
      },
    ],
  ]);
  let staged = [];
  let failFancy = false;
  let commits = 0;
  let rollbacks = 0;
  const connection = {
    beginTransaction: async () => {
      staged = [];
    },
    query: async (sql, params) => {
      assert.match(sql, /target.eventid=limits_update.eventid/);
      if (failFancy && sql.includes("t_matchfancy")) throw new Error("DB failure");
      const [id, eventId, min, max] = params;
      const row = rows.get(id);
      if (row && String(row.eventid) === eventId && Boolean(row.fancyid) === sql.includes("t_matchfancy")) {
        staged.push(() => {
          if (min !== null) row.minbet = min;
          if (max !== null) row.maxbet = max;
        });
      }
      return [{ affectedRows: row ? 1 : 0 }];
    },
    commit: async () => {
      staged.forEach((apply) => apply());
      commits += 1;
    },
    rollback: async () => {
      staged = [];
      rollbacks += 1;
    },
    release: () => {},
  };
  t.mock.method(sourceDb, "getSourcePool", () => ({
    getConnection: async () => connection,
    query: async (sql, ids) => [
      [...rows.values()].filter(
        (row) => ids.includes(row.marketid) && Boolean(row.fancyid) === sql.includes("t_matchfancy"),
      ),
    ],
  }));
  const redis = require("../src/config/redis");
  const store = new Map();
  const client = {
    isOpen: true,
    get: async (key) => store.get(key) || null,
    set: async (key, value) => {
      store.set(key, value);
    },
    multi() {
      const ops = [];
      const transaction = {
        set(key, value) {
          ops.push([key, value]);
          return transaction;
        },
        async exec() {
          for (const [key, value] of ops) store.set(key, value);
        },
      };
      return transaction;
    },
  };
  redis.__testing__.reset();
  redis.__testing__.setRedisClient(client);
  t.after(() => redis.__testing__.reset());
  const tick = { eid: 9904, mid: "1.9904", s: "OPEN", r: [{ rid: 1, s: "ACTIVE", b1: 81 }] };
  await redis.writeTicks([tick]);
  const before = JSON.parse(store.get("Data-Rs:9904")).LineMarket[0];
  const settings = { eid: 9904, mid: "1.9904", settings: { ms: "50", mas: "200000" } };
  const update = await redis.writeMarketSettings(settings);
  assert.equal(update.eventId, "9904");
  assert.deepEqual(update.payload.LineMarket[0], { ...before, minBet: 50, maxBet: 200000 });
  assert.equal(rows.get("1.9904").minbet, 50);
  assert.equal(rows.get("1.9904").maxbet, 200000);
  assert.equal(await redis.writeMarketSettings(settings), null);
  await redis.writeTicks([{ ...tick, r: [{ rid: 1, s: "ACTIVE", b1: 82 }] }]);
  const after = JSON.parse(store.get("Data-Rs:9904")).LineMarket[0];
  assert.equal(after.minBet, 50);
  assert.equal(after.maxBet, 200000);
  assert.equal(after.runners[0].ex.availableToBack[0].price, 82);
  // Queue an odds tick while the settings write is still waiting to run.
  await Promise.all([
    redis.writeMarketSettings({ ...settings, settings: { ms: 75, mas: 250000 } }),
    redis.writeTicks([tick]),
  ]);
  const concurrent = JSON.parse(store.get("Data-Rs:9904")).LineMarket[0];
  assert.equal(concurrent.minBet, 75);
  assert.equal(concurrent.maxBet, 250000);
  await redis.writeMarketSettings({ eid: 9904, mid: "4.9904-F2", settings: { ms: 0 } });
  assert.equal(rows.get("4.9904-F2").minbet, 0);
  assert.equal(rows.get("4.9904-F2").maxbet, 1000);
  const count = commits;
  await redis.writeMarketSettings({ ...settings, settings: { ms: null, mas: "bad" } });
  assert.equal(commits, count);
  failFancy = true;
  const stored = store.get("Data-Rs:9904");
  await assert.rejects(redis.writeMarketSettings({ ...settings, settings: { ms: 500 } }), /DB failure/);
  assert.equal(rollbacks, 1);
  assert.equal(rows.get("1.9904").minbet, 75);
  assert.equal(store.get("Data-Rs:9904"), stored);
});
