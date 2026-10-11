const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const provider = require("../src/services/providerApi");

process.env.EVENT_AUDIT_ENABLED = "false";

const rows = [
  // A Test between days: dropped from today=1, Match Odds suspended but unfinished.
  { eventid: 36140257, eventname: "South Africa v Australia", sportid: 4, seriesid: 11365612, open_date: "2026-10-11 13:00:00.000000" },
  // Finished at the vendor: every primary market is game-over.
  { eventid: 501, eventname: "Finished v Match", sportid: 4, seriesid: 9, open_date: "2026-10-10 13:00:00" },
  // Vendor lookup fails: kept, because a failure is not evidence that the match ended.
  { eventid: 502, eventname: "Unknown v State", sportid: 4, seriesid: 9, open_date: "2026-10-10 13:00:00" },
];

const vendor = {
  36140257: [
    { id: "1.263209775", name: "Match Odds", type: "match-odd", isActive: false, gameOver: false },
    { id: "1.263209775-BM2", name: "Bookmaker2", type: "bookmaker", isActive: false, gameOver: true },
    { id: "4.749785843375-BM", name: "TOSS", type: "bookmaker", isActive: false, gameOver: true },
  ],
  501: [
    { id: "1.501", name: "Match Odds", type: "match-odd", isActive: false, gameOver: true },
    // A suspended secondary bookmaker does not keep a finished match alive.
    { id: "1.501-BM2", name: "Bookmaker2", type: "bookmaker", isActive: false, gameOver: false },
  ],
};

test("events dropped from the today feed are kept only while a primary market is unfinished", async (t) => {
  let query;
  t.mock.method(db, "getSourcePool", () => ({
    query: async (sql, args) => {
      query = { sql, args };
      return [rows];
    },
  }));
  t.mock.method(provider, "markets", async ({ eids: [eventId] }) => {
    if (eventId === 502) throw new Error("timeout");
    return { data: vendor[eventId] };
  });
  const { carriedOverEvents } = require("../src/cron/eventSync");

  const kept = await carriedOverEvents([{ eventId: 777 }], [4]);

  assert.deepEqual(
    kept.map((event) => event.eventId),
    [36140257, 502],
  );
  assert.deepEqual(kept[0], {
    eventId: 36140257,
    eventName: "South Africa v Australia",
    sportId: 4,
    seriesId: 11365612,
    openDate: "2026-10-11 13:00:00",
    inPlay: false,
    gameOver: false,
  });
  // Listed events are excluded, and only events with a never-retired primary market qualify.
  assert.match(query.sql, /e\.eventid NOT IN \(\?\)/);
  assert.match(query.sql, /m\.status=\?/);
  assert.deepEqual(query.args, [4, 6, 777, true, 25]);
});

test("carry-over is skipped when no sport feed succeeded", async (t) => {
  t.mock.method(db, "getSourcePool", () => assert.fail("database must not be queried"));
  const { carriedOverEvents } = require("../src/cron/eventSync");
  assert.deepEqual(await carriedOverEvents([], []), []);
});
