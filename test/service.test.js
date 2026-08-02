const assert = require("node:assert/strict");
const test = require("node:test");
const Event = require("../src/models/Event");
const Market = require("../src/models/Market");
const { bookmakerPayload, oddsPayload, fancyPayload, payloadGroup, emptyEventPayload,
  runnerPrices, preserveRunnerNames } = require("../src/config/redis");
const { normalizeProviderAcknowledgement } = require("../src/services/marketSubscriptionService");
const { collectOddsTicks, collectScores, messageShape, logRawSocketPayload,
  payloadContainsMarket, isResultTick } = require("../src/services/websocketService");
const { parseJsonObjects, containsMarketId } = require("../src/services/logReaderService");
const { dashboardEntry } = require("../src/services/dashboardService");

test("database rows map to API event fields", () => {
  const event = Event.fromRow({ id: 1, eventid: 123, eventname: "A v B",
    open_date: "2026-07-31", isactive: 1, betlock: 0,
    is_redis_updated: 1, in_play: 1, fancylock: 0, channel_id: "cricket" });
  assert.equal(event.openDate, "2026-07-31");
  assert.equal(event.isActive, 1);
  assert.equal(event.betLock, 0);
  assert.equal(event.channelId, "cricket");
});

test("dashboard entries use first prices from the consolidated event snapshot", () => {
  const entry = dashboardEntry({ eventid: 99, marketid: "1.2", marketname: "Match Odds",
    matchname: "A v B", opendate: "2026-08-02 12:00:00", inplay: 1,
    seriesid: 7, isBookmaker: 1, isGoal: 0 }, { Odds: [{ marketId: "1.2", inplay: true,
    runners: [{ ex: { availableToBack: [{ price: 1.8 }], availableToLay: [{ price: 1.82 }] } },
      { ex: { availableToBack: [{ price: 2.1 }], availableToLay: [{ price: 2.12 }] } }] }] });
  assert.deepEqual(entry, { matchName: "A v B", openDate: "2026-08-02 12:00:00", inPlay: true,
    matchId: 99, marketId: "1.2", bm: true, GM: false, team1Back: 1.8, team1Lay: 1.82,
    team2Back: 2.1, team2Lay: 2.12, drawBack: 0, drawLay: 0, li: 7 });
});

test("market rows map betting configuration", () => {
  const market = Market.fromRow({ marketid: "1.2", betdelay: 3,
    minbetrate: "1.5", maxbetrate: "100", is_redis_updated: 0 });
  assert.equal(market.betDelay, 3);
  assert.equal(market.minBetRate, "1.5");
  assert.equal(market.isRedisUpdated, 0);
});

test("bookmaker ticks are transformed for Redis", () => {
  const output = bookmakerPayload({ mid: "BM-1", r: [
    { rid: 11, na: "India", b: 91, l: 92, sb: "S" },
    { rid: 12, na: "Australia", b: 93, l: 94, sb: "B" },
  ] }, { marketname: "MATCH_ODDS", minbet: 100, maxbet: 50000,
    betDelay: 2, eventid: 99, isactive: 1, sportid: 4 });
  assert.equal(output.length, 2);
  assert.equal(output[0].matchName, "India v Australia");
  assert.equal(output[0].gstatus, "SUSPENDED");
  assert.equal(output[1].gstatus, "Ball Running");
  assert.equal(output[0].matchId, 99);
});

test("toss bookmaker markets force odds to 95", () => {
  const [runner] = bookmakerPayload({ mid: "BM-TOSS", r: [
    { rid: 1, na: "Heads", b: 10, l: 20 },
  ] }, { marketname: "TOSS" });
  assert.equal(runner.b1, 95);
  assert.equal(runner.l1, 95);
});

test("standard odds ticks match the frontend Redis contract", () => {
  const output = oddsPayload({ eid: 99, mid: "1.2", s: "OPEN", ip: true, r: [{
    rid: 11, na: "India", b: [{ p: 1.8, s: 250 }], l: [{ p: 1.82, s: 300 }],
  }] }, { eventid: 99, matchname: "India v Australia", marketname: "Match Odds",
    minbet: 100, maxbet: 50000, betDelay: 3, inPlay: 1, isactive: 1, sportid: 4 });
  assert.equal(output.marketId, "1.2");
  assert.equal(output.matchName, "India v Australia");
  assert.deepEqual(output.runners[0].ex.availableToBack, [{ price: 1.8, size: 250 }]);
  assert.deepEqual(output.runners[0].ex.availableToLay, [{ price: 1.82, size: 300 }]);
});

test("odds runners use cached provider selection names", () => {
  const names = new Map([["11", "India"], ["12", "Australia"]]);
  const output = oddsPayload({ mid: "1.2", r: [{ rid: 11 }, { rid: 12 }] },
    { marketname: "Match Odds" }, names);
  assert.deepEqual(output.runners.map((runner) => runner.name), ["India", "Australia"]);
});

test("existing runner names survive unnamed socket updates", () => {
  const entries = [{ runners: [{ selectionId: 11, name: null }] }];
  preserveRunnerNames(entries, [{ runners: [{ selectionId: 11, name: "India" }] }]);
  assert.equal(entries[0].runners[0].name, "India");
});

test("compact socket odds fields become three-level price ladders", () => {
  const runner = { b1: "10.00", b2: "9.40", b3: "9.20", br1: 4140, br2: 10000, br3: 2451600,
    l1: "10.50", l2: "11.00", l3: "12.00", lr1: 331780, lr2: 9892560, lr3: 33810 };
  assert.deepEqual(runnerPrices(runner, "back"), [
    { price: 10, size: 4140 }, { price: 9.4, size: 10000 }, { price: 9.2, size: 2451600 },
  ]);
  assert.deepEqual(runnerPrices(runner, "lay"), [
    { price: 10.5, size: 331780 }, { price: 11, size: 9892560 }, { price: 12, size: 33810 },
  ]);
});

test("fancy ticks and market suffixes map to frontend groups", () => {
  const output = fancyPayload({ mid: "4.1-F2", na: "Six over runs", go: true,
    r: [{ b: 42, l: 40, bs: 100, ls: 100 }] },
  { eventid: 99, marketname: "Six over runs", minbet: 100, maxbet: 100000 });
  assert.equal(payloadGroup({ mid: "4.1-F2" }, {}), "Fancy2");
  assert.equal(payloadGroup({ mid: "4.1-OE" }, {}), "OddEven");
  assert.equal(output.gameover, true);
  assert.equal(output.nation, "Six over runs");
  assert.deepEqual(Object.keys(emptyEventPayload()),
    ["Odds", "Bookmaker", "Fancy2", "OddEven", "Fancy3", "CricketCasino", "BallByBall"]);
});

test("provider acknowledgement excludes skipped markets from subscription", () => {
  assert.deepEqual(normalizeProviderAcknowledgement({
    subscribed: ["1.2"], skipped: ["BM-1"],
  }, ["1.2", "BM-1", "missing"]), {
    subscribed: ["1.2"],
    skipped: ["BM-1", "missing"],
    providerResponse: { subscribed: ["1.2"], skipped: ["BM-1"] },
  });
});

test("only confirmed game-over ticks trigger result unsubscription", () => {
  assert.equal(isResultTick({ mid: "1.2", go: true, res: "11", s: "CLOSED" }), true);
  assert.equal(isResultTick({ mid: "1.2", go: false, s: "OPEN" }), false);
  assert.equal(isResultTick({ go: true, res: "11" }), false);
});

test("socket classifier separates score envelopes from nested odds ticks", () => {
  const score = { level: "info", message: { score: [{ eid: 123, data: "<html>" }] } };
  assert.equal(collectScores(score).length, 1);
  assert.deepEqual(collectOddsTicks(score), []);

  const tick = { level: "info", message: { markets: [{ eid: 123, mid: "1.2", r: [] }] } };
  assert.deepEqual(collectOddsTicks(tick), [tick.message.markets[0]]);
  assert.deepEqual(messageShape(tick), {
    type: "object", keys: ["level", "message"], messageKeys: ["markets"],
  });
});

test("raw socket payload logger can be disabled", () => {
  const previous = process.env.PROVIDER_LOG_SOCKET_PAYLOADS;
  process.env.PROVIDER_LOG_SOCKET_PAYLOADS = "false";
  assert.doesNotThrow(() => logRawSocketPayload({ eid: 123, mid: "1.2" }));
  if (previous === undefined) delete process.env.PROVIDER_LOG_SOCKET_PAYLOADS;
  else process.env.PROVIDER_LOG_SOCKET_PAYLOADS = previous;
});

test("raw socket logs can be matched by nested market ID", () => {
  const payload = { level: "info", message: { markets: [
    { eid: 123, mid: "1.2", r: [] }, { eid: 123, mid: "BM-1", r: [] },
  ] } };
  assert.equal(containsMarketId(payload, "BM-1"), true);
  assert.equal(containsMarketId(payload, "missing"), false);
  assert.equal(payloadContainsMarket(payload, "1.2"), true);
});

test("log reader parses consecutive pretty JSON records", () => {
  assert.deepEqual(parseJsonObjects('{\n"a":1,"nested":{"ok":true}\n}\n{\n"b":2\n}'), [
    { a: 1, nested: { ok: true } }, { b: 2 },
  ]);
});
