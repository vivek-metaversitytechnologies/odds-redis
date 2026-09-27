const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const db = require("../src/config/sourceDb");

const queries = [];
// resultSync takes getSourcePool at load time, so the fake is installed first.
db.getSourcePool = () => ({
  query: async (sql) => {
    queries.push(sql);
    return [[]];
  },
});
const resultSync = require("../src/cron/resultSync");

const isBacklogLane = (sql) => /ORDER BY CASE WHEN \? IS NULL/.test(sql);

test("without the backlog, only the recent lanes are queried", async () => {
  queries.length = 0;
  await resultSync.loadCandidates({ backlog: false });
  assert.equal(queries.filter(isBacklogLane).length, 0);
  assert.ok(queries.some((sql) => /ORDER BY m\.updatedon DESC/.test(sql)));
  assert.ok(queries.some((sql) => /ORDER BY f\.updatedon DESC/.test(sql)));

  queries.length = 0;
  await resultSync.loadCandidates();
  assert.equal(queries.filter(isBacklogLane).length, 2);
});

test("backlog runs are every Nth scheduled run and skipped runs leave the cursors alone", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/cron/resultSync.js"), "utf8");
  assert.match(source, /RESULT_BACKLOG_SWEEP_EVERY_RUNS \|\| 5/);
  assert.match(source, /const nextCursors = backlog\s*\? advanceCandidateCursors\(requested, activeRegular, candidates\.fancies\)\s*: \{ \.\.\.candidateCursors \}/);
});

test("the regular result lanes never pick up line markets left in t_market", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/cron/resultSync.js"), "utf8");
  const guard = /AND NOT EXISTS \(SELECT 1 FROM t_matchfancy lf WHERE lf\.fancyid=m\.marketid\)/g;
  assert.equal(source.match(guard).length, 2);
});
