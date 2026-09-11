const test = require("node:test");
const assert = require("node:assert/strict");
const provider = require("../src/services/providerApi");
const { resolveSessionName, repairSessionNames } = require("../src/services/fancyNameService");

test("inactive suspended session runners can repair a generic name", async (t) => {
  t.mock.method(provider, "runners", async () => ({ data: [{ marketId: "4.202342438123-F2", name: "Only 15 Over Run SL-W", sb: "S", active: false }] }));
  assert.equal(await resolveSessionName("4.202342438123-F2", "Fancy2"), "Only 15 Over Run SL-W");
});

test("descriptive names and other families do not fetch session names", async (t) => {
  t.mock.method(provider, "runners", () => { throw new Error("unexpected lookup"); });
  assert.equal(await resolveSessionName("4.1-F2", "Custom session name"), "Custom session name");
  assert.equal(await resolveSessionName("4.1-BB", "BallByBall"), "BallByBall");
  assert.equal(provider.runners.mock.callCount(), 0);
});

test("ambiguous or wrong-market runner metadata does not rename a session", async (t) => {
  t.mock.method(provider, "runners", async () => ({ data: [{ name: "One" }, { name: "Two" }] }));
  assert.equal(await resolveSessionName("4.1-F2", "Fancy2"), "Fancy2");
  t.mock.method(provider, "runners", async () => ({ data: [{ marketId: "4.2-F2", name: "Other" }] }));
  assert.equal(await resolveSessionName("4.1-F2", "Fancy2"), "Fancy2");
});

test("name repair guards generic names in both tables and never updates settlement fields", async () => {
  const calls = [];
  await repairSessionNames({ execute: async (sql, params) => calls.push({ sql, params }) }, "4.1-F2", "Only 15 Over Run SL-W");
  assert.equal(calls.length, 2);
  for (const { sql, params } of calls) {
    assert.match(sql, /WHERE fancyid=\? AND/);
    assert.match(sql, /IN \('','fancy2'\)/);
    assert.doesNotMatch(sql, /SET.*(?:status|isactive|result)=/);
    assert.deepEqual(params, ["Only 15 Over Run SL-W", "4.1-F2"]);
  }
});

test("rediscovery replaces a stored Fancy2 fallback with the runner name", async (t) => {
  const db = require("../src/config/sourceDb");
  const repairs = [];
  const connection = {
    query: async (sql) => sql.startsWith("SELECT") ? [[{ fancyid: "4.1-F2", name: "Fancy2", isactive: 1, status: "OPEN" }]] : [[]],
    execute: async (sql, params) => { repairs.push({ sql, params }); return [[]]; },
    beginTransaction: async () => {}, commit: async () => {}, rollback: async () => {}, release: () => {},
  };
  t.mock.method(db, "getSourcePool", () => ({ getConnection: async () => connection }));
  t.mock.method(provider, "runners", async () => ({ data: [{ marketId: "4.1-F2", name: "Only 15 Over Run SL-W", active: false, sb: "S" }] }));
  const { upsertFancies } = require("../src/cron/marketDiscoverySync");
  const market = { marketId: "4.1-F2", marketName: "Fancy2", marketType: "session", isActive: true };
  await upsertFancies([market]);
  assert.equal(market.marketName, "Only 15 Over Run SL-W");
  assert.equal(repairs.length, 2);
  assert.equal(repairs[0].params[0], market.marketName);
  assert.match(repairs[1].sql, /UPDATE t_fancyresult/);
});
