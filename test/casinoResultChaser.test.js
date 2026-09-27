const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");

const casino = (index) => ({
  candidateid: index,
  marketid: `11.${900925862000 + index}-CC`,
  marketname: `1st Ing ${index * 10} Over Run (SA vs AUS)`,
  oddstype: "CC",
  mtype: "cricket-casino",
  eventid: 36111199,
  matchname: "South Africa v Australia",
  sportid: 4,
});
const state = { rows: [], queries: [] };
// The chaser takes getSourcePool at load time, so the fake is installed first.
db.getSourcePool = () => ({
  query: async (sql) => {
    state.queries.push(sql);
    return [state.rows];
  },
});
const provider = require("../src/services/providerApi");
const resultSync = require("../src/cron/resultSync");
const chaser = require("../src/cron/casinoResultChaser");

test("unsettled casino markets are requested in small batches and settled", async (t) => {
  state.rows = Array.from({ length: 12 }, (_, index) => casino(index + 1));
  t.mock.method(resultSync, "hasExceptionalTable", async () => true);
  const requests = [];
  t.mock.method(provider, "results", async ({ mids }, options) => {
    requests.push({ mids, options });
    // The vendor has a result only for the second market.
    return { data: mids.filter((id) => id === casino(2).marketid).map((marketId) => ({ marketId, result: "8" })) };
  });
  const applied = [];
  t.mock.method(resultSync, "applyResults", async (results, candidates) => {
    applied.push({ results, candidates });
    return { settled: results.map((row) => row.marketId) };
  });

  const run = await chaser.chaseOnce();

  assert.deepEqual(requests.map((request) => request.mids.length), [5, 5, 2]);
  assert.ok(requests.every((request) => request.mids.length <= chaser.CHASE_BATCH_SIZE));
  assert.equal(requests[0].options.source, "casino-result-chase");
  assert.deepEqual(applied[0].results.map((row) => row.marketId), [casino(2).marketid]);
  assert.equal(applied[0].candidates.fancies.length, 12);
  assert.deepEqual(run, { markets: 12, requests: 3, failedRequests: 0, results: 1, settled: 1, durationMs: run.durationMs });
  assert.match(state.queries.at(-1), /f\.mtype='cricket-casino'/);
  assert.match(state.queries.at(-1), /t_matchabondendtie/);
});

test("a failed request does not stop the other batches, and nothing to settle writes nothing", async (t) => {
  state.rows = Array.from({ length: 7 }, (_, index) => casino(index + 1));
  t.mock.method(resultSync, "hasExceptionalTable", async () => false);
  let calls = 0;
  t.mock.method(provider, "results", async () => {
    calls += 1;
    if (calls === 1) throw new Error("vendor 500");
    return { data: [] };
  });
  const applyResults = t.mock.method(resultSync, "applyResults", async () => ({ settled: [] }));

  const run = await chaser.chaseOnce();

  assert.equal(run.requests, 2);
  assert.equal(run.failedRequests, 1);
  assert.equal(applyResults.mock.callCount(), 0);
  assert.doesNotMatch(state.queries.at(-1), /t_matchabondendtie/);
});
