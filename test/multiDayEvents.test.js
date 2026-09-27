const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { activeMatchesFromProjection } = require("../src/services/dashboardService");

const now = Date.parse("2026-09-30T10:00:00Z");
const entry = (matchId, openDate, inPlay) => ({ matchId, openDate, inPlay, sportId: 4 });

test("an in-play Test match stays listed past the 48-hour start-date limit", () => {
  const listed = activeMatchesFromProjection(
    [
      entry(1, "2026-09-27T04:00:00Z", true), // day 4 of a Test, in play
      entry(2, "2026-09-27T04:00:00Z", false), // started days ago, not in play: stale
      entry(3, "2026-09-30T09:00:00Z", false), // today, not started yet
    ],
    48,
    now,
  );
  assert.deepEqual(listed.map((row) => row.matchId).sort(), [1, 3]);
});

test("subscription and active-match SQL keep in-play events regardless of start date", () => {
  const marketSync = fs.readFileSync(path.join(__dirname, "../src/cron/marketSync.js"), "utf8");
  const dashboard = fs.readFileSync(path.join(__dirname, "../src/services/dashboardService.js"), "utf8");
  const inPlayOrRecent = /COALESCE\(e\.in_play,0\)=1\s+OR e\.open_date >= DATE_SUB\(NOW\(\), INTERVAL \$\{maxAgeHours\} HOUR\)/g;
  assert.equal(marketSync.match(inPlayOrRecent).length, 2);
  assert.match(dashboard, /AND \(e\.in_play = TRUE OR e\.open_date >= DATE_SUB\(NOW\(\), INTERVAL \$\{maxAgeHours\} HOUR\)\)/);
  assert.match(dashboard, /if \(event\?\.inPlay === true\) return true;/);
});
