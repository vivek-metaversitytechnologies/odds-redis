const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");

let queries;
let staleEvents;
db.getSourcePool = () => ({
  query: async (sql, params) => {
    queries.push({ sql, params });
    if (sql.trim().startsWith("SELECT")) return [staleEvents.map((eventid) => ({ eventid }))];
    return [{ affectedRows: 3 }];
  },
});
const { closeStaleFancies } = require("../src/cron/staleFancyClose");

test("open fancies of events finished over two hours ago are closed per event", async () => {
  queries = [];
  staleEvents = [101, 102];
  const result = await closeStaleFancies();

  const [select, ...updates] = queries;
  assert.match(select.sql, /e\.isactive=\? AND e\.status=\?/);
  // Within the carry-over window, an event that is in play or whose primary market was never
  // retired (multi-day match) is not picked; past it, event sync can no longer reopen it.
  assert.match(
    select.sql,
    /e\.open_date < DATE_SUB\(NOW\(\), INTERVAL \? DAY\)\s+OR \(\s*COALESCE\(e\.in_play,0\)=0\s+AND NOT EXISTS \(\s*SELECT 1 FROM t_market pm\s+WHERE pm\.eventid=e\.eventid AND pm\.status=\?/,
  );
  assert.deepEqual(select.params.slice(0, 7), [true, "OPEN", false, false, 120, 6, true]);
  assert.equal(updates.length, 2);
  for (const [index, update] of updates.entries()) {
    assert.match(update.sql, /UPDATE t_matchfancy SET isactive=\?,status=\?/);
    // Keyed on eventid, never the source auto-increment id, so it replays on the mirror.
    assert.doesNotMatch(update.sql, /\bid\s*(=|IN)/);
    assert.deepEqual(update.params, [false, "CLOSED", staleEvents[index], true, "OPEN"]);
  }
  assert.equal(result.events, 2);
  assert.equal(result.fancies, 6);
});

test("nothing is updated when no finished event has open fancies", async () => {
  queries = [];
  staleEvents = [];
  const result = await closeStaleFancies();
  assert.equal(queries.length, 1);
  assert.equal(result.fancies, 0);
});
