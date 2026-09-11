const test = require("node:test");
const assert = require("node:assert/strict");
const provider = require("../src/services/providerApi");
const { resolveFancyName, repairFancyNames } = require("../src/services/fancyNameService");

test("inactive suspended session runners can repair a generic name", async (t) => {
  t.mock.method(provider, "runners", async () => ({ data: [{ marketId: "4.202342438123-F2", name: "Only 15 Over Run SL-W", sb: "S", active: false }] }));
  assert.equal(await resolveFancyName("4.202342438123-F2", "Fancy2"), "Only 15 Over Run SL-W");
});

test("descriptive names and other families do not fetch session names", async (t) => {
  t.mock.method(provider, "runners", () => { throw new Error("unexpected lookup"); });
  assert.equal(await resolveFancyName("4.1-F2", "Custom session name"), "Custom session name");
  assert.equal(await resolveFancyName("4.1-BB", "BallByBall"), "BallByBall");
  assert.equal(provider.runners.mock.callCount(), 0);
});

test("ambiguous or wrong-market runner metadata does not rename a session", async (t) => {
  t.mock.method(provider, "runners", async () => ({ data: [{ name: "One" }, { name: "Two" }] }));
  assert.equal(await resolveFancyName("4.1-F2", "Fancy2"), "Fancy2");
  t.mock.method(provider, "runners", async () => ({ data: [{ marketId: "4.2-F2", name: "Other" }] }));
  assert.equal(await resolveFancyName("4.1-F2", "Fancy2"), "Fancy2");
});

test("name repair guards generic names in both tables and never updates settlement fields", async () => {
  const calls = [];
  await repairFancyNames({ execute: async (sql, params) => calls.push({ sql, params }) }, "4.1-F2", "Only 15 Over Run SL-W");
  assert.equal(calls.length, 2);
  for (const { sql, params } of calls) {
    assert.match(sql, /WHERE fancyid=\? AND/);
    assert.match(sql, /IN \('','fancy2','othermarket','oddeven','khado','meter','cricketcasino'\)/);
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

for (const [suffix, fallback] of [["F3", "OtherMarket"], ["OE", "OddEven"], ["KD", "Khado"], ["MT", "Meter"], ["CC", "CricketCasino"]]) {
  test(`${fallback} names resolve and repair both tables`, async (t) => {
    const id = `4.152968901145-${suffix}`;
    t.mock.method(provider, "runners", async () => ({ data: [{ marketId: id, name: "Verified provider market name", active: false, sb: "S" }] }));
    const name = await resolveFancyName(id, fallback);
    assert.equal(name, "Verified provider market name");
    const calls = [];
    await repairFancyNames({ execute: async (sql, params) => calls.push({ sql, params }) }, id, name);
    assert.equal(calls.length, 2);
    assert.match(calls[0].sql, /UPDATE t_matchfancy SET name=/);
    assert.match(calls[1].sql, /UPDATE t_fancyresult SET fancyname=/);
    assert.deepEqual(calls[1].params, [name, id]);
    assert.equal(await resolveFancyName(id, "Existing descriptive name"), "Existing descriptive name");
    assert.equal(provider.runners.mock.callCount(), 1);
  });
}

test("generic OtherMarket Redis names are repaired without resetting live prices", async () => {
  const redis = require("../src/config/redis");
  const store = new Map();
  redis.__testing__.reset();
  redis.__testing__.setRedisClient({ isOpen: true, get: async (key) => store.get(key), set: async (key, value) => store.set(key, value) });
  const payload = redis.emptyEventPayload();
  payload.OtherMarket.push({ mid: "4.152968901145-F3", nation: "OtherMarket", b1: 7, l1: 8, gstatus: "OPEN" });
  store.set("Data-Rs:36049024", JSON.stringify(payload));
  try {
    await redis.reconcileFancyDefinitions([{ marketId: "4.152968901145-F3", eventId: 36049024, marketType: "other-market", marketName: "Verified provider market name", isActive: true }]);
    const saved = JSON.parse(store.get("Data-Rs:36049024")).Fancy3[0];
    assert.equal(saved.nation, "Verified provider market name");
    assert.equal(saved.b1, 7);
    assert.equal(saved.gstatus, "OPEN");
  } finally { redis.__testing__.reset(); }
});
