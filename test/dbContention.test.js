const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const pending = require("../src/services/pendingResultQueue");
const subscriptions = require("../src/services/marketSubscriptionService");
const redis = require("../src/config/redis");
const frontend = require("../src/services/frontendSocketService");

test("discovery and retirement keep lock lifetimes bounded", async (t) => {
  let connection;
  t.mock.method(db, "getSourcePool", () => ({ getConnection: async () => connection }));
  const { retireCompletedEvents } = require("../src/cron/eventSync");
  const { upsertFancies } = require("../src/cron/marketDiscoverySync");
  let queued;
  let unsubscribed;
  let removed;
  let inTransaction;
  let calls;
  function reset() {
    queued = [];
    unsubscribed = [];
    removed = [];
    calls = [];
    inTransaction = false;
    connection = {
      beginTransaction: async () => { assert.equal(inTransaction, false); inTransaction = true; calls.push("begin"); },
      commit: async () => { assert.equal(inTransaction, true); inTransaction = false; calls.push("commit"); },
      rollback: async () => { inTransaction = false; calls.push("rollback"); },
      release: () => { assert.equal(inTransaction, false); calls.push("release"); },
    };
  }
  t.mock.method(pending, "enqueue", async (ids) => {
    assert.equal(inTransaction, false);
    queued.push(...ids);
  });
  t.mock.method(subscriptions, "unsubscribeEventMarkets", async (ids) => {
    assert.equal(inTransaction, false);
    unsubscribed.push(...ids);
  });
  t.mock.method(redis, "removeEvent", async (id) => { removed.push(id); });
  t.mock.method(frontend, "publishEventRemoved", () => {});

  await t.test("retirement updates only selected active IDs in batches and retries a deadlock", async () => {
    reset();
    const active = new Map(Array.from({ length: 205 }, (_, i) => [i + 1, true]));
    active.set(999, false);
    const sizes = [];
    let deadlock = true;
    connection.query = async (sql, args) => {
      if (sql.startsWith("SELECT")) {
        assert.equal(inTransaction, false);
        assert.match(sql, /WHERE eventid=\? AND isactive=\? ORDER BY id/);
        assert.deepEqual(args, [7, true]);
        return [sql.includes("t_matchfancy") ? [] : [...active].filter(([, value]) => value).map(([id]) => ({ id, marketid: String(id) }))];
      }
      assert.equal(inTransaction, true);
      assert.match(sql, /WHERE id IN \(.+\) AND isactive=\? ORDER BY id/);
      if (deadlock) { deadlock = false; throw Object.assign(new Error("deadlock"), { code: "ER_LOCK_DEADLOCK" }); }
      const ids = args.slice(0, -1);
      sizes.push(ids.length);
      ids.forEach((id) => active.set(id, false));
      return [{ affectedRows: ids.length }];
    };
    const result = await retireCompletedEvents([{ eventId: 7, gameOver: true }, { eventId: 8, gameOver: false }]);
    assert.deepEqual(sizes, [100, 100, 5]);
    assert.deepEqual(result, { events: 1, markets: 205 });
    assert.equal(calls.filter((c) => c === "rollback").length, 1);
    assert.equal(calls.filter((c) => c === "commit").length, 3);
    assert.equal(queued.length, 205);
    assert.equal(unsubscribed.length, 205);
    assert.equal(unsubscribed.includes("999"), false);
    assert.deepEqual(removed, [7]);
  });

  await t.test("already retired events do not open transactions or rewrite historical rows", async () => {
    reset();
    connection.query = async (sql) => { assert.match(sql, /^SELECT/); return [[]]; };
    assert.deepEqual(await retireCompletedEvents([{ eventId: 7, gameOver: true }]), { events: 1, markets: 0 });
    assert.deepEqual(calls, ["release"]);
    assert.deepEqual(queued, []);
    assert.deepEqual(removed, [7]);
  });

  await t.test("a later failed batch still unsubscribes earlier committed rows", async () => {
    reset();
    let writes = 0;
    connection.query = async (sql) => {
      if (sql.startsWith("SELECT")) return [Array.from({ length: 101 }, (_, i) => ({ id: i + 1, marketid: String(i + 1) }))];
      if (++writes > 1) throw new Error("database unavailable");
      return [{ affectedRows: 100 }];
    };
    await assert.rejects(retireCompletedEvents([{ eventId: 7, gameOver: true }]), /database unavailable/);
    assert.equal(unsubscribed.length, 100);
    assert.equal(calls.filter((c) => c === "commit").length, 1);
    assert.equal(calls.filter((c) => c === "rollback").length, 1);
    assert.deepEqual(removed, []);
  });

  await t.test("fallback repairs run after commit and deadlocked discovery batches are retried", async () => {
    reset();
    let attempts = 0;
    const repairs = [];
    connection.query = async (sql) => {
      if (sql.startsWith("SELECT")) return [[{ fancyid: "4.1-F2", name: "Fancy2", isactive: 1, status: "OPEN" }]];
      assert.equal(inTransaction, true);
      if (++attempts === 1) throw Object.assign(new Error("deadlock"), { errno: 1213 });
      return [[]];
    };
    connection.execute = async (sql) => {
      assert.equal(inTransaction, false);
      assert.ok(calls.includes("commit"));
      repairs.push(sql);
      return [[]];
    };
    await upsertFancies([{ marketId: "4.1-F2", marketName: "20 Over Runs", marketType: "session", isActive: true }]);
    assert.equal(attempts, 2);
    assert.equal(repairs.length, 2);
    assert.match(repairs[1], /UPDATE t_fancyresult/);
    assert.deepEqual(calls, ["begin", "rollback", "begin", "commit", "release"]);
  });

  await t.test("rediscovery of named markets does not scan historical results", async () => {
    reset();
    connection.query = async (sql) => sql.startsWith("SELECT")
      ? [[{ fancyid: "4.1-F2", name: "20 Over Runs", isactive: 1, status: "OPEN" }]] : [[]];
    connection.execute = async () => { throw new Error("unexpected name repair"); };
    await upsertFancies([{ marketId: "4.1-F2", marketName: "20 Over Runs", marketType: "session", isActive: true }]);
    assert.deepEqual(calls, ["begin", "commit", "release"]);
  });
});
