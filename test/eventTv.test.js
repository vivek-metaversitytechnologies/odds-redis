const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");

const IFRAME =
  "<iframe style='height: 230px; width: 100%;' src='https://oddsdata.org/score/619d50045eb8d022a9cd0b2389b8a96f42884d74/35256'></iframe>";

let queries = [];
db.getSourcePool = () => ({
  getConnection: async () => ({
    query: async (sql, args) => {
      queries.push({ sql, args });
      return [[]];
    },
    release() {},
  }),
});
const { eventTv, eventRows, upsertEvents } = require("../src/cron/eventSync");

test("the vendor's tv iframe is read from the event feed as sent", () => {
  const original = process.env.SPORT_IDS;
  process.env.SPORT_IDS = "4";
  try {
    const feed = (tv) => ({
      id: "36241438",
      sportId: 4,
      name: "Pokhara Avengers v Sudurpaschim Royals",
      leagueId: "12706346",
      startTime: "2026-10-28T11:15:00.000Z",
      gameOver: false,
      inPlay: false,
      ...(tv === undefined ? {} : { tv }),
    });
    assert.equal(eventRows([{ data: [feed(IFRAME)] }])[0].tv, IFRAME);
    assert.equal(eventRows([{ data: [feed(undefined)] }])[0].tv, null);
  } finally {
    if (original === undefined) delete process.env.SPORT_IDS;
    else process.env.SPORT_IDS = original;
  }
});

test("an empty or oversized tv is not stored", () => {
  assert.equal(eventTv(""), null);
  assert.equal(eventTv("   "), null);
  assert.equal(eventTv(null), null);
  assert.equal(eventTv(42), null);
  // A truncated iframe would be broken HTML, and an overlong value would fail the batch upsert.
  assert.equal(eventTv(`<iframe src='${"x".repeat(600)}'></iframe>`), null);
});

test("the upsert writes tv and keeps the stored value when the feed has none", async () => {
  queries = [];
  const event = {
    eventId: 36241438,
    eventName: "Pokhara Avengers v Sudurpaschim Royals",
    sportId: 4,
    seriesId: 12706346,
    openDate: "2026-10-28 16:45:00",
    inPlay: false,
    gameOver: false,
  };
  await upsertEvents([{ ...event, tv: IFRAME }, { ...event, eventId: 2, tv: null }]);

  const upsert = queries.find(({ sql }) => /INSERT INTO t_event/.test(sql));
  assert.match(upsert.sql, /channel_id,tv\)/);
  assert.match(upsert.sql, /tv=COALESCE\(VALUES\(tv\),tv\)/);
  const [values] = upsert.args;
  assert.equal(values[0].at(-1), IFRAME);
  assert.equal(values[1].at(-1), null);
});
