const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");

// The discovery module takes getSourcePool at load time, so the stub is installed first.
let connection;
let inserts;
db.getSourcePool = () => ({ getConnection: async () => connection });
const { marketRows, upsertMarkets } = require("../src/cron/marketDiscoverySync");

function database({ markets = [], fancies = [] } = {}) {
  inserts = [];
  connection = {
    query: async (sql, params) => {
      if (sql.includes("FROM t_market")) return [markets.map((marketid) => ({ marketid, isactive: 1 }))];
      if (sql.includes("FROM t_matchfancy")) return [fancies.map((fancyid) => ({ fancyid }))];
      if (sql.includes("INSERT INTO t_market")) inserts.push({ sql, rows: params[0] });
      return [[]];
    },
    beginTransaction: async () => {},
    commit: async () => {},
    rollback: async () => {},
    release: () => {},
  };
}

const events = new Map([["10", { eventName: "A v B", sportId: 2 }]]);
const rows = (...items) => marketRows({ data: items.map((item) => ({ eventId: "10", sportId: 2, isActive: true, ...item })) }, events);

test("vendor markets without name or type are flagged unnamed", () => {
  const [nameless, named, bookmaker2] = rows(
    { id: "1.262980458", name: null, type: null },
    { id: "1.262980445", name: "Match Odds", type: "match-odd" },
    { id: "1.262980445-BM2", name: null, type: null },
  );
  assert.equal(nameless.unnamed, true);
  assert.equal(nameless.marketName, "Market 1.262980458");
  assert.equal(named.unnamed, false);
  assert.equal(bookmaker2.unnamed, false);
});

test("an unnamed vendor market never creates a t_market placeholder or a subscription", async () => {
  database();
  const result = await upsertMarkets(rows(
    { id: "1.262980458", name: null, type: null },
    { id: "1.262980445", name: "Match Odds", type: "match-odd" },
  ));
  assert.deepEqual(inserts.flatMap(({ rows }) => rows.map((row) => row[0])), ["1.262980445"]);
  assert.equal(result.inserted, 1);
  assert.deepEqual(result.marketIds, ["1.262980445"]);
});

test("an existing market is still updated but keeps its name when the vendor omits it", async () => {
  database({ markets: ["1.262980458"] });
  const result = await upsertMarkets(rows({ id: "1.262980458", name: null, type: null }));
  assert.equal(inserts.length, 1);
  assert.match(inserts[0].sql, /marketname=IF\(VALUES\(marketname\)=CONCAT\('Market ',marketid\),marketname,VALUES\(marketname\)\)/);
  assert.deepEqual(result.marketIds, ["1.262980458"]);
});
