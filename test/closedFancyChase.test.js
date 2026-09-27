const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");

const state = { unsettled: [], queries: [] };
// Both modules take getSourcePool at load time, so the fake is installed first.
db.getSourcePool = () => ({
  query: async (sql, params) => {
    state.queries.push({ sql, params });
    return [state.unsettled.filter((row) => params.includes(row.marketid))];
  },
});
const provider = require("../src/services/providerApi");
const resultSync = require("../src/cron/resultSync");
const tracker = require("../src/services/closedFancyTracker");
const chaser = require("../src/cron/closedFancyResultChaser");

const row = (marketid) => ({ candidateid: 1, marketid, marketname: marketid, oddstype: "F2", mtype: "session", eventid: 7, matchname: "A v B", sportid: 4 });

test.beforeEach(() => tracker.__testing__.reset());

test("only an explicit s=false on a session-style fancy marks it closed; sb is ignored", () => {
  tracker.noteTick({ mid: "4.1-F2", eid: 7, s: false }, "Fancy2");
  tracker.noteTick({ mid: "4.2-F2", eid: 7, s: true, sb: "S" }, "Fancy2");
  tracker.noteTick({ mid: "4.3-F2", eid: 7, s: "SUSPENDED" }, "Fancy2");
  tracker.noteTick({ mid: "4.4-BB", eid: 7, s: false }, "BallByBall");
  tracker.noteTick({ mid: "4.5-KD", eid: 7, s: 0 }, "Khado");
  assert.deepEqual(tracker.pending(60000).map((entry) => entry.marketId).sort(), ["4.1-F2", "4.5-KD"]);

  tracker.noteTick({ mid: "4.1-F2", eid: 7, s: true }, "Fancy2");
  assert.deepEqual(tracker.pending(60000).map((entry) => entry.marketId), ["4.5-KD"]);
});

test("closed markets older than the chase window are dropped for the backlog sweep", () => {
  tracker.noteTick({ mid: "4.1-F2", eid: 7, s: false }, "Fancy2", 1000);
  tracker.noteTick({ mid: "4.2-F2", eid: 7, s: false }, "Fancy2", 50000);
  assert.deepEqual(tracker.pending(20000, 60000).map((entry) => entry.marketId), ["4.2-F2"]);
  assert.equal(tracker.size(), 1);
});

test("the chaser asks only for closed, unsettled fancies and settles what the vendor returns", async (t) => {
  tracker.noteTick({ mid: "4.1-F2", eid: 7, s: false }, "Fancy2");
  tracker.noteTick({ mid: "4.2-F2", eid: 7, s: false }, "Fancy2");
  tracker.noteTick({ mid: "4.3-F2", eid: 7, s: false }, "Fancy2");
  state.unsettled = [row("4.1-F2"), row("4.2-F2")]; // 4.3 was already settled elsewhere
  t.mock.method(resultSync, "hasExceptionalTable", async () => true);
  const requested = [];
  t.mock.method(provider, "results", async ({ mids }, options) => {
    requested.push({ mids, source: options.source });
    return { data: [{ marketId: "4.1-F2", result: "50" }] };
  });
  t.mock.method(resultSync, "applyResults", async (results) => ({ settled: results.map((result) => result.marketId) }));

  const run = await chaser.chaseOnce();

  assert.deepEqual(requested, [{ mids: ["4.1-F2", "4.2-F2"], source: "closed-fancy-chase" }]);
  assert.deepEqual(
    { tracked: run.tracked, unsettled: run.unsettled, requests: run.requests, results: run.results, settled: run.settled },
    { tracked: 3, unsettled: 2, requests: 1, results: 1, settled: 1 },
  );
  // Settled here or elsewhere: no longer chased. The unanswered one stays for the next run.
  assert.deepEqual(tracker.pending(60000).map((entry) => entry.marketId), ["4.2-F2"]);
});

test("nothing closed means no database query and no provider call", async (t) => {
  state.queries.length = 0;
  const results = t.mock.method(provider, "results", async () => ({ data: [] }));
  const run = await chaser.chaseOnce();
  assert.equal(run.requests, 0);
  assert.equal(results.mock.callCount(), 0);
  assert.equal(state.queries.length, 0);
});
