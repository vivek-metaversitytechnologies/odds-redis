const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const pending = require("../src/services/pendingResultQueue");
const subscriptions = require("../src/services/marketSubscriptionService");
const redis = require("../src/config/redis");
const frontend = require("../src/services/frontendSocketService");
const lifecycle = require("../src/services/eventLifecyclePolicy");

test("socket retirement isolates DB retries from lifecycle evidence and publication", async (t) => {
  let rows;
  let staged;
  let writes;
  let failure;
  let effects;
  let rollbacks;
  const previous = { confirmations: process.env.EVENT_TERMINAL_CONFIRMATIONS, dryRun: process.env.EVENT_TERMINAL_DRY_RUN };
  t.after(() => {
    for (const [key, value] of [["EVENT_TERMINAL_CONFIRMATIONS", previous.confirmations], ["EVENT_TERMINAL_DRY_RUN", previous.dryRun]]) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
    lifecycle.resetForTests();
  });
  function reset() {
    rows = { t_event: [], t_market: [], t_matchfancy: [] };
    staged = null;
    writes = [];
    effects = { queued: [], unsubscribed: [], removedMarkets: [], removedEvents: [], metadata: [], snapshots: [], notifications: [] };
    rollbacks = 0;
    failure = () => {};
    lifecycle.resetForTests();
    process.env.EVENT_TERMINAL_CONFIRMATIONS = "2";
    process.env.EVENT_TERMINAL_DRY_RUN = "false";
  }
  const connection = {
    beginTransaction: async () => { assert.equal(staged, null); staged = []; },
    commit: async () => { assert.notEqual(staged, null); staged.forEach((apply) => apply()); staged = null; },
    rollback: async () => { staged = null; rollbacks += 1; },
    release: () => assert.equal(staged, null),
    query: async (sql, params) => {
      const table = sql.match(/(?:FROM|UPDATE) (t_\w+)/)[1];
      if (sql.startsWith("SELECT")) {
        assert.equal(staged, null);
        const selected = rows[table].filter((row) => sql.includes("WHERE eventid=?")
          ? row.eventid === params[0] && (!sql.includes("AND isactive=?") || row.isactive === 1)
          : params.includes(row.marketid));
        return [selected.map((row) => ({ ...row }))];
      }
      assert.notEqual(staged, null);
      const ids = params.slice(3);
      assert.ok(ids.length <= 100);
      assert.deepEqual(ids, [...ids].sort((a, b) => a - b));
      assert.match(sql, /WHERE id(?: IN|=)/);
      writes.push({ table, ids, sql });
      failure(table, ids);
      const selected = rows[table].filter((row) => ids.includes(row.id));
      for (const row of selected) {
        staged.push(() => {
          const fields = table === "t_matchfancy" ? ["isshow", "is_show", "issubscribed"]
            : table === "t_event" ? ["isactive", "status", "in_play"] : ["isactive", "status", "issubscribed"];
          if (fields.some((key) => row[key] === 1)) {
            fields.forEach((key) => { row[key] = 0; });
            row.changed = true;
          }
        });
      }
      return [{ affectedRows: selected.length }];
    },
  };
  t.mock.method(db, "getSourcePool", () => ({ getConnection: async () => connection }));
  const { handleSocketGameOver } = require("../src/cron/resultSync");
  function external(fn) { return async (...args) => { assert.equal(staged, null); return fn(...args); }; }
  t.mock.method(pending, "enqueue", external((ids) => effects.queued.push(...ids)));
  t.mock.method(subscriptions, "unsubscribeResultMarkets", external((ids) => effects.unsubscribed.push(...ids)));
  t.mock.method(redis, "removeMarkets", external((eventId, ids) => { effects.removedMarkets.push(...ids); return new Set(ids); }));
  t.mock.method(redis, "removeEvent", external((id) => effects.removedEvents.push(id)));
  t.mock.method(redis, "removeEventsFromMetadata", external((events) => effects.metadata.push(...events.map((e) => e.eventid))));
  t.mock.method(frontend, "publishEventSnapshot", external((id) => effects.snapshots.push(id)));
  t.mock.method(frontend, "publishEventRemoved", external((id) => effects.notifications.push(id)));
  const fancy = (id, active = 1) => ({ id, marketid: `4.${id}-F2`, eventid: 7, marketname: "20 Over Runs", status: "OPEN", isactive: active, isshow: active, is_show: active, issubscribed: active });
  function primary() {
    rows.t_event.push({ id: 10, eventid: 7, sportid: 4, isactive: 1, status: 1, in_play: 1 });
    rows.t_market.push({ id: 1, marketid: "1.7", eventid: 7, marketname: "Match Odds", isactive: 1, status: 1, issubscribed: 1 });
  }

  await t.test("one primary message remains one observation despite a deadlock", async () => {
    reset(); primary();
    rows.t_matchfancy.push(fancy(2));
    let once = true;
    failure = () => { if (once) { once = false; throw Object.assign(new Error("deadlock"), { code: "ER_LOCK_DEADLOCK" }); } };
    await handleSocketGameOver(["1.7", "1.7"]);
    assert.equal(rollbacks, 1);
    assert.equal(lifecycle.isConfirmed(7), false);
    assert.equal(rows.t_event[0].isactive, 1);
    assert.equal(rows.t_matchfancy[0].isactive, 1);
    assert.deepEqual(effects.unsubscribed, ["1.7"]);
    assert.deepEqual(effects.removedEvents, []);
  });

  await t.test("confirmed events retire active fancies in bounded batches and preserve history", async () => {
    reset(); primary();
    rows.t_matchfancy = Array.from({ length: 205 }, (_, i) => fancy(i + 100));
    rows.t_matchfancy.push(fancy(999, 0));
    await handleSocketGameOver(["1.7"]);
    writes = [];
    await handleSocketGameOver(["1.7"]);
    assert.deepEqual(writes.filter((w) => w.table === "t_matchfancy").map((w) => w.ids.length), [100, 100, 5]);
    assert.equal(rows.t_event[0].isactive, 0);
    assert.ok(rows.t_matchfancy.slice(0, 205).every((row) => row.isactive === 1));
    assert.equal(rows.t_matchfancy.at(-1).isactive, 0);
    assert.ok(rows.t_matchfancy.every((row) => row.status === "OPEN"));
    assert.equal(rows.t_matchfancy.at(-1).changed, undefined);
    assert.equal(effects.unsubscribed.includes("4.999-F2"), false);
    assert.deepEqual(effects.removedEvents, ["7"]);
    assert.deepEqual(effects.metadata, [7]);
    assert.deepEqual(effects.notifications, ["7"]);
  });

  await t.test("ordinary fancy closure does not retire its event", async () => {
    reset(); primary();
    rows.t_matchfancy.push(fancy(2));
    await handleSocketGameOver(["4.2-F2"]);
    assert.equal(rows.t_matchfancy[0].isactive, 1);
    assert.equal(rows.t_matchfancy[0].status, "OPEN");
    assert.equal(rows.t_event[0].isactive, 1);
    assert.deepEqual(effects.queued, []);
    assert.deepEqual(effects.snapshots, ["7"]);
  });

  await t.test("partial failures publish only committed batches and can resume", async () => {
    reset();
    rows.t_matchfancy = Array.from({ length: 101 }, (_, i) => fancy(i + 1));
    failure = (_table, ids) => { if (ids.includes(101)) throw new Error("database unavailable"); };
    const ids = rows.t_matchfancy.map((row) => row.marketid);
    await assert.rejects(handleSocketGameOver(ids), /database unavailable/);
    assert.equal(effects.unsubscribed.length, 100);
    assert.equal(effects.removedMarkets.length, 100);
    assert.equal(rows.t_matchfancy.at(-1).isactive, 1);
    failure = () => {};
    await handleSocketGameOver(ids);
    assert.ok(rows.t_matchfancy.every((row) => row.isactive === 1));
    assert.equal(effects.unsubscribed.includes("4.101-F2"), true);
  });

  await t.test("exhausted deadlocks do not publish rolled-back rows", async () => {
    reset(); rows.t_matchfancy.push(fancy(2));
    failure = () => { throw Object.assign(new Error("deadlock"), { errno: 1213 }); };
    await assert.rejects(handleSocketGameOver(["4.2-F2"]), /deadlock/);
    assert.equal(rollbacks, 3);
    assert.equal(rows.t_matchfancy[0].isactive, 1);
    assert.deepEqual(effects.unsubscribed, []);
    assert.deepEqual(effects.removedMarkets, []);
  });

  await t.test("a committed terminal event is hidden even when a later market batch fails", async () => {
    reset(); primary(); rows.t_matchfancy.push(fancy(2));
    await handleSocketGameOver(["1.7"]);
    failure = (table) => { if (table === "t_matchfancy") throw new Error("database unavailable"); };
    await assert.rejects(handleSocketGameOver(["1.7"]), /database unavailable/);
    assert.equal(rows.t_event[0].isactive, 0);
    assert.equal(rows.t_matchfancy[0].isactive, 1);
    assert.deepEqual(effects.removedEvents, ["7"]);
    assert.deepEqual(effects.metadata, [7]);
    assert.equal(effects.unsubscribed.includes("4.2-F2"), false);
    failure = () => {};
    await handleSocketGameOver(["1.7"]);
    assert.equal(rows.t_matchfancy[0].isactive, 1);
  });

  await t.test("dry-run terminal evidence does not cascade to other markets", async () => {
    reset(); primary(); rows.t_matchfancy.push(fancy(2));
    process.env.EVENT_TERMINAL_DRY_RUN = "true";
    await handleSocketGameOver(["1.7"]);
    await handleSocketGameOver(["1.7"]);
    assert.equal(rows.t_event[0].isactive, 1);
    assert.equal(rows.t_matchfancy[0].isactive, 1);
    assert.deepEqual(effects.removedEvents, []);
  });
});
