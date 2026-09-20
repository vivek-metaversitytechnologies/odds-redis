const assert = require("node:assert/strict");
const test = require("node:test");
const request = require("supertest");
const { createPublicApiApp } = require("../src/publicApiApp");
const { publicApiPort } = require("../src/publicApiServer");
const redis = require("../src/config/redis");

test("public API exposes only the split service routes", async () => {
  const app = createPublicApiApp();
  await request(app).get("/betfair_api/fancy/not-an-id").expect(400);
  await request(app).get("/betfair_api/fancy/score/not-an-id").expect(400);
  await request(app).get("/betfair_api/active_match/not-an-id").expect(400);
  const missing = await request(app).get("/api/socket/status").expect(404);
  assert.equal(missing.body.message, "Route not found");
  await request(app).get("/admin/").expect(404);
});

test("public API rejects an invalid listen port", () => {
  const original = process.env.PUBLIC_API_PORT;
  process.env.PUBLIC_API_PORT = "invalid";
  assert.throws(() => publicApiPort(), /PUBLIC_API_PORT/);
  if (original === undefined) delete process.env.PUBLIC_API_PORT;
  else process.env.PUBLIC_API_PORT = original;
});

test("active-match response exposes its Redis and application timings", async (t) => {
  const originalGetEvents = redis.getEvents;
  const originalGetEventSnapshots = redis.getEventSnapshots;
  const originalGetActiveMatches = redis.getActiveMatches;
  t.after(() => {
    redis.getEvents = originalGetEvents;
    redis.getEventSnapshots = originalGetEventSnapshots;
    redis.getActiveMatches = originalGetActiveMatches;
  });
  redis.getActiveMatches = async () => null;
  redis.getEvents = async () => [];
  redis.getEventSnapshots = async () => new Map();

  const response = await request(createPublicApiApp()).get("/betfair_api/active_match/4").expect(200);
  assert.match(response.headers["server-timing"], /redis-events;dur=/);
  assert.match(response.headers["server-timing"], /redis-events-command;dur=/);
  assert.match(response.headers["server-timing"], /redis-snapshots;dur=/);
  assert.match(response.headers["server-timing"], /redis-snapshots-command;dur=/);
  assert.match(response.headers["server-timing"], /snapshots-parse;dur=/);
  assert.match(response.headers["server-timing"], /transform;dur=/);
  assert.match(response.headers["server-timing"], /controller;dur=/);
});

test("live-match API returns only in-play events for each configured sport", async (t) => {
  const originalGetEvents = redis.getEvents;
  const originalGetActiveMatches = redis.getActiveMatches;
  const originalCache = process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS;
  const originalSports = process.env.SPORT_IDS;
  t.after(() => {
    redis.getEvents = originalGetEvents;
    redis.getActiveMatches = originalGetActiveMatches;
    if (originalCache === undefined) delete process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS;
    else process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS = originalCache;
    if (originalSports === undefined) delete process.env.SPORT_IDS;
    else process.env.SPORT_IDS = originalSports;
  });
  process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS = "0";
  process.env.SPORT_IDS = "1,4";
  const metadata = {
    1: [
      { eventId: 11, eventName: "Alpha v Beta", sportId: 1, seriesId: 5, openDate: "2026-09-20T10:00:00.000Z", inPlay: true, gameOver: false },
      { eventId: 12, eventName: "Not started", sportId: 1, seriesId: 5, openDate: "2026-09-21T10:00:00.000Z", inPlay: false, gameOver: false },
    ],
    4: [
      { eventId: 42, eventName: "Later live", sportId: 4, seriesId: null, openDate: "2026-09-20T09:00:00.000Z", inPlay: true, gameOver: false },
      { eventId: 41, eventName: "Earlier live", sportId: 4, seriesId: 9, openDate: "2026-09-20T08:00:00.000Z", inPlay: true, gameOver: false },
      { eventId: 43, eventName: "Finished", sportId: 4, seriesId: 9, openDate: "2026-09-20T07:00:00.000Z", inPlay: true, gameOver: true },
    ],
  };
  redis.getEvents = async (sportId) => metadata[sportId] ?? null;
  // Event 11 has a displayable market, so the active-match projection holds its full row.
  const projectedRow = {
    matchName: "Alpha v Beta",
    openDate: "2026-09-20T10:00:00",
    inPlay: false,
    matchId: 11,
    marketId: "1.111",
    bm: true,
    GM: false,
    outright: false,
    team1Back: 1.5,
    team1Lay: 1.6,
    team2Back: 2.4,
    team2Lay: 2.5,
    drawBack: 3.3,
    drawLay: 3.4,
    li: 5,
  };
  redis.getActiveMatches = async (sportId) => (Number(sportId) === 1 ? [projectedRow] : null);
  const app = createPublicApiApp();

  const all = await request(app).get("/betfair_api/live_match").expect(200);
  assert.equal(all.body.status, true);
  assert.equal(all.headers["cache-control"].includes("no-store"), true);
  assert.deepEqual(
    all.body.data.map((sport) => [sport.sportId, sport.count, sport.events.map((event) => event.matchId)]),
    [
      [1, 1, [11]],
      [4, 2, [41, 42]],
    ],
    "in-play only, finished events dropped, oldest kickoff first",
  );
  // A live event with a market is exactly its active-match row (in play by definition).
  assert.deepEqual(all.body.data[0].events[0], { ...projectedRow, inPlay: true });
  // A live event without a displayable market keeps the same shape, with zero prices.
  assert.deepEqual(all.body.data[1].events[0], {
    matchName: "Earlier live",
    openDate: "2026-09-20T08:00:00.000Z",
    inPlay: true,
    matchId: 41,
    marketId: null,
    bm: false,
    GM: false,
    outright: false,
    team1Back: 0,
    team1Lay: 0,
    team2Back: 0,
    team2Lay: 0,
    drawBack: 0,
    drawLay: 0,
    li: 9,
  });
  assert.equal(all.body.data[1].events[1].li, null);
  assert.deepEqual(
    Object.keys(all.body.data[1].events[0]),
    Object.keys(all.body.data[0].events[0]),
    "every live row has the same fields as an active-match row",
  );

  const one = await request(app).get("/betfair_api/live_match/4").expect(200);
  assert.deepEqual(one.body.data.map((sport) => sport.sportId), [4]);

  await request(app).get("/betfair_api/live_match/not-an-id").expect(400);
  await request(app).get("/betfair_api/live_match/0").expect(400);
  const unknown = await request(app).get("/betfair_api/live_match/99").expect(404);
  assert.equal(unknown.body.message, "Sport is not configured");
});

test("live-match API reports missing Redis data instead of an empty list", async (t) => {
  const originalGetEvents = redis.getEvents;
  const originalGetActiveMatches = redis.getActiveMatches;
  const originalCache = process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS;
  t.after(() => {
    redis.getEvents = originalGetEvents;
    redis.getActiveMatches = originalGetActiveMatches;
    if (originalCache === undefined) delete process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS;
    else process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS = originalCache;
  });
  process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS = "0";
  redis.getEvents = async (sportId) => (Number(sportId) === 2 ? null : []);
  redis.getActiveMatches = async () => null;
  const response = await request(createPublicApiApp()).get("/betfair_api/live_match").expect(503);
  assert.equal(response.body.status, false);
  assert.deepEqual(response.body.data, []);
});
