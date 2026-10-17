const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const provider = require("../src/services/providerApi");

process.env.MARKET_TRACE_ENABLED = "false";

// resultRecall takes getSourcePool at load time, so the fake is installed before it is required.
const state = { fancy: new Map(), match: new Map(), abandoned: new Map(), rollback: [], queries: [] };
const connection = {
  release: () => {},
  query: async (sql, params) => {
    state.queries.push(sql);
    if (sql.includes("FROM t_fancyresult WHERE fancyid=?")) {
      return [state.fancy.has(params[0]) ? [{ markettype: state.fancy.get(params[0]) }] : []];
    }
    if (sql.includes("FROM t_matchresult WHERE marketid=?")) {
      return [state.match.has(params[0]) ? [{ markettype: state.match.get(params[0]) }] : []];
    }
    if (sql.includes("FROM t_matchabondendtie WHERE marketid=?")) {
      return [state.abandoned.has(params[0]) ? [{ markettype: state.abandoned.get(params[0]) }] : []];
    }
    if (sql.includes("INSERT INTO t_marketid_for_rollback")) {
      const [marketid, markettype, status] = params;
      if (state.rollback.some((row) => row.marketid === marketid && row.status === status)) {
        return [{ affectedRows: 0 }];
      }
      state.rollback.push({ marketid, markettype, status });
      return [{ affectedRows: 1 }];
    }
    if (sql.includes("SELECT fancyid AS marketid FROM t_fancyresult"))
      return [[...state.fancy.keys()].map((marketid) => ({ marketid }))];
    if (sql.includes("SELECT marketid FROM t_matchresult"))
      return [[...state.match.keys()].map((marketid) => ({ marketid }))];
    if (sql.includes("SELECT marketid FROM t_matchabondendtie"))
      return [[...state.abandoned.keys()].map((marketid) => ({ marketid }))];
    throw new Error(`unexpected query: ${sql}`);
  },
};
db.getSourcePool = () => ({ getConnection: async () => connection });
const recall = require("../src/services/resultRecall");
const resultSync = require("../src/cron/resultSync");

function reset() {
  state.fancy.clear();
  state.match.clear();
  state.abandoned.clear();
  state.rollback = [];
  state.queries = [];
  process.env.RESULT_RECALL_COOLDOWN_MS = "0";
}

test("a recalled fancy with a stored result gets one OPEN rollback row typed from t_fancyresult", async () => {
  reset();
  state.fancy.set("4.1-F2", "F2");
  await recall.recordRecalls([{ mid: "4.1-F2", rt: true }]);
  await recall.recordRecalls([{ mid: "4.1-F2", rt: true }]);
  assert.deepEqual(state.rollback, [{ marketid: "4.1-F2", markettype: "F2", status: "OPEN" }]);
  // Found in the fancy table, so the regular tables were never consulted.
  assert.ok(!state.queries.some((sql) => sql.includes("t_matchresult")));
});

test("regular and abandoned results are found in t_matchresult and t_matchabondendtie", async () => {
  reset();
  state.match.set("1.100", "Match Odds");
  state.abandoned.set("1.200", "Over/Under 2.5 Goals");
  const outcome = await recall.recordRecalls([
    { marketId: "1.100", rt: true },
    { marketId: "1.200", rt: true },
  ]);
  assert.deepEqual(outcome.recorded.sort(), ["1.100", "1.200"]);
  assert.deepEqual(
    state.rollback.map((row) => [row.marketid, row.markettype]),
    [
      ["1.100", "Match Odds"],
      ["1.200", "Over/Under 2.5 Goals"],
    ],
  );
});

test("no rollback row without a stored result or without rt", async () => {
  reset();
  state.match.set("1.300", "Match Odds");
  await recall.recordRecalls([{ mid: "1.999", rt: true }, { mid: "1.300", rt: false }, { mid: "1.300" }]);
  assert.deepEqual(state.rollback, []);
});

test("repeated socket recall ticks are looked up once per cooldown", async () => {
  reset();
  process.env.RESULT_RECALL_COOLDOWN_MS = "60000";
  await recall.recordRecalls([{ mid: "4.2-BB", rt: true }]);
  const lookups = state.queries.length;
  await recall.recordRecalls([{ mid: "4.2-BB", rt: true }]);
  assert.equal(state.queries.length, lookups);
});

test("recalled result rows are never settled and are recorded", async () => {
  reset();
  state.fancy.set("4.3-F2", "F2");
  const rows = resultSync.responseRows({
    data: [
      { marketId: "4.3-F2", result: "55", rt: true },
      { marketId: "4.4-F2", result: "60" },
    ],
  });
  assert.deepEqual(
    rows.map((row) => row.marketId),
    ["4.4-F2"],
  );
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(
    state.rollback.map((row) => row.marketid),
    ["4.3-F2"],
  );
});

test("the recheck asks the vendor about recently settled markets and records recalls", async (t) => {
  reset();
  state.fancy.set("4.5-F2", "F2");
  state.match.set("1.500", "Match Odds");
  let requested;
  t.mock.method(provider, "results", async (body) => {
    requested = body.mids;
    return {
      data: [
        { marketId: "1.500", result: null, rt: true },
        { marketId: "4.5-F2", result: "40" },
      ],
    };
  });
  const outcome = await recall.recheckRecentResults();
  assert.deepEqual(requested.sort(), ["1.500", "4.5-F2"]);
  assert.deepEqual(outcome.recalled, ["1.500"]);
  assert.deepEqual(state.rollback, [{ marketid: "1.500", markettype: "Match Odds", status: "OPEN" }]);
});
