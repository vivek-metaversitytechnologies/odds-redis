const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const Event = require("../src/models/Event");
const Market = require("../src/models/Market");
const {
  bookmakerPayload,
  oddsPayload,
  fancyPayload,
  payloadGroup,
  emptyEventPayload,
  runnerPrices,
  preserveRunnerNames,
  shouldRemoveFromPayload,
  fancyDefinitionEntry,
  regularDefinitionEntries,
  normalizeEventPayload,
  validMarketIdentifier,
} = require("../src/config/redis");
const { normalizeProviderAcknowledgement } = require("../src/services/marketSubscriptionService");
const {
  collectOddsTicks,
  collectScores,
  messageShape,
  logRawSocketPayload,
  payloadContainsMarket,
  isResultTick,
} = require("../src/services/websocketService");
const { parseJsonObjects, containsMarketId } = require("../src/services/logReaderService");
const { dashboardEntry } = require("../src/services/dashboardService");
const { competitionRows } = require("../src/cron/competitionSync");
const { eventRows } = require("../src/cron/eventSync");
const {
  MARKET_TYPES,
  REGULAR_MARKET_TYPES,
  FANCY_MARKET_TYPES,
  FANCY_MARKET_REQUESTS,
  marketRows,
  mergeDiscoveredMarkets,
  bookmaker2BaseMarketId,
  runnerSourceMarketId,
  runnerLookupMarketIds,
  enforceBookmaker2Eligibility,
  inactiveLineMarkets,
  oddsType,
} = require("../src/cron/marketDiscoverySync");
const { responseRows: resultRows, fancyResultValue } = require("../src/cron/resultSync");
const { integer, boolean, csvIntegers } = require("../src/config/env");
const { setBounded } = require("../src/utils/boundedMap");
const { checksum, migrationFiles } = require("../scripts/migrate");

test("environment helpers reject invalid values and normalize lists", () => {
  const previous = {
    number: process.env.TEST_INTEGER,
    boolean: process.env.TEST_BOOLEAN,
    list: process.env.TEST_INTEGER_LIST,
  };
  process.env.TEST_INTEGER = "not-a-number";
  process.env.TEST_BOOLEAN = "yes";
  process.env.TEST_INTEGER_LIST = "4, 2, bad, 4";
  assert.equal(integer("TEST_INTEGER", 25, { min: 1 }), 25);
  assert.equal(boolean("TEST_BOOLEAN", false), true);
  assert.deepEqual(csvIntegers("TEST_INTEGER_LIST", []), [4, 2]);
  for (const [key, value] of Object.entries(previous)) {
    const name = key === "number" ? "TEST_INTEGER" : key === "boolean" ? "TEST_BOOLEAN" : "TEST_INTEGER_LIST";
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("bounded maps evict their oldest entry", () => {
  const cache = new Map([
    ["a", 1],
    ["b", 2],
  ]);
  setBounded(cache, "c", 3, 2);
  assert.deepEqual(
    [...cache.entries()],
    [
      ["b", 2],
      ["c", 3],
    ],
  );
});

test("migration runner discovers ordered SQL files and produces stable checksums", () => {
  const files = migrationFiles(path.join(__dirname, "../scripts/migrations"));
  assert.deepEqual(files, [...files].sort());
  assert.ok(files.includes("001_selection_runner_unique.sql"));
  assert.equal(checksum("SELECT 1"), checksum("SELECT 1"));
  assert.notEqual(checksum("SELECT 1"), checksum("SELECT 2"));
});

test("normal service shutdown preserves provider registrations", () => {
  const serverSource = fs.readFileSync(path.join(__dirname, "../src/server.js"), "utf8");
  assert.doesNotMatch(serverSource, /subscriptions\.unsubscribeAll\s*\(/);
  assert.match(serverSource, /RECONCILE_SUBSCRIPTIONS_ON_START\s*\|\|\s*"false"/);
});

test("database rows map to API event fields", () => {
  const event = Event.fromRow({
    id: 1,
    eventid: 123,
    eventname: "A v B",
    open_date: "2026-07-31",
    isactive: 1,
    betlock: 0,
    is_redis_updated: 1,
    in_play: 1,
    fancylock: 0,
    channel_id: "cricket",
  });
  assert.equal(event.openDate, "2026-07-31");
  assert.equal(event.isActive, 1);
  assert.equal(event.betLock, 0);
  assert.equal(event.channelId, "cricket");
});

test("dashboard entries use first prices from the consolidated event snapshot", () => {
  const entry = dashboardEntry(
    {
      eventid: 99,
      marketid: "1.2",
      marketname: "Match Odds",
      matchname: "A v B",
      opendate: "2026-08-02 12:00:00",
      inplay: 1,
      seriesid: 7,
      isBookmaker: 1,
      isGoal: 0,
    },
    {
      Odds: [
        {
          marketId: "1.2",
          inplay: true,
          runners: [
            { ex: { availableToBack: [{ price: 1.8 }], availableToLay: [{ price: 1.82 }] } },
            { ex: { availableToBack: [{ price: 2.1 }], availableToLay: [{ price: 2.12 }] } },
          ],
        },
      ],
    },
  );
  assert.deepEqual(entry, {
    matchName: "A v B",
    openDate: "2026-08-02 12:00:00",
    inPlay: true,
    matchId: 99,
    marketId: "1.2",
    bm: true,
    GM: false,
    team1Back: 1.8,
    team1Lay: 1.82,
    team2Back: 2.1,
    team2Lay: 2.12,
    drawBack: 0,
    drawLay: 0,
    li: 7,
  });
});

test("competition sync keeps only supported sports", () => {
  const rows = competitionRows({
    data: [
      { id: "10", name: "Football League", sportId: 1 },
      { id: "20", name: "Tennis Tour", sportId: 2 },
      { id: "30", name: "Cricket Cup", sportId: 4 },
      { id: "40", name: "Unsupported", sportId: 7 },
    ],
  });
  assert.deepEqual(
    rows.map((row) => row.seriesId),
    [10, 20, 30],
  );
});

test("event sync keeps supported unfinished events", () => {
  const rows = eventRows([
    {
      data: [
        {
          id: "10",
          name: "A v B",
          sportId: 1,
          leagueId: "5",
          startTime: "2026-08-02T12:00:00Z",
          gameOver: false,
        },
        {
          id: "20",
          name: "Finished",
          sportId: 2,
          leagueId: "6",
          startTime: "2026-08-02T13:00:00Z",
          gameOver: true,
        },
        {
          id: "30",
          name: "Unsupported",
          sportId: 7,
          leagueId: "7",
          startTime: "2026-08-02T14:00:00Z",
          gameOver: false,
        },
      ],
    },
  ]);
  assert.deepEqual(
    rows.map((row) => row.eventId),
    [10],
  );
  assert.equal(rows[0].openDate, "2026-08-02 17:30:00");
});

test("market discovery maps active unfinished markets with Java betting defaults", () => {
  const events = new Map([
    ["10", { eventName: "A v B", openDate: new Date("2026-08-02T12:00:00Z"), inPlay: false, seriesId: 5 }],
  ]);
  const rows = marketRows(
    {
      data: [
        {
          id: "4.1-BM",
          eventId: "10",
          sportId: 4,
          name: "Bookmaker 0%Comm",
          type: "bookmaker",
          isActive: true,
        },
        { id: "1.2", eventId: "10", sportId: 4, name: "Match Odds", type: "match-odd", isActive: true },
        { id: "1.3", eventId: "10", sportId: 4, name: "Closed", type: "match-odd", isActive: false },
      ],
    },
    events,
  );
  assert.deepEqual(
    rows.filter((row) => row.isActive).map((row) => [row.marketName, row.maxBet, row.betDelay]),
    [
      ["Bookmaker", 25000, 0],
      ["Match Odds", 1, 3],
    ],
  );
  assert.equal(rows.find((row) => row.marketName === "Closed")?.isActive, false);
});

test("vendor inPlayFilter maps to the persisted display message", () => {
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const [row] = marketRows(
    {
      data: [
        {
          id: "1.2",
          eventId: "10",
          sportId: 4,
          name: "Match Odds",
          type: "match-odd",
          isActive: true,
          inPlayFilter: "Winning bets only",
        },
      ],
    },
    events,
  );
  assert.equal(row.displayMessage, "Winning bets only");
});

test("explicitly inactive line markets remain in discovery for deactivation", () => {
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const rows = marketRows(
    {
      data: [
        {
          id: "1.3",
          eventId: "10",
          sportId: 4,
          name: "1st Innings 75 Balls Runs Line",
          type: "line-market",
          isActive: false,
          gameOver: false,
        },
      ],
    },
    events,
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].isActive, false);
  assert.equal(rows[0].marketType, "line-market");
  assert.equal(rows[0].betDelay, 5);
});

test("inactive line markets are selected for immediate reconciliation", () => {
  const rows = inactiveLineMarkets([
    { marketId: "1.1", marketType: "line-market", isActive: true, gameOver: false },
    { marketId: "1.2", marketType: "line-market", isActive: false, gameOver: false },
    { marketId: "1.3", marketType: "line-market", isActive: true, gameOver: true },
    { marketId: "4.1-F2", marketType: "session", isActive: false, gameOver: false },
  ]);
  assert.deepEqual(
    rows.map((row) => row.marketId),
    ["1.2", "1.3"],
  );
});

test("session market suffixes map to Java fancy odds types", () => {
  assert.equal(oddsType("4.1-F2"), "F2");
  assert.equal(oddsType("4.1-OE"), "OE");
  assert.equal(oddsType("4.1-F3"), "F3");
  assert.equal(oddsType("4.1-BB"), "BB");
  assert.equal(oddsType("4.1-KD"), "KD");
});

test("fancy discovery uses Java-compatible betting limits", () => {
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const [row] = marketRows(
    {
      data: [
        {
          id: "4.1-OE",
          eventId: "10",
          sportId: 4,
          name: "Odd Even",
          type: "odd-even",
          isActive: true,
          gameOver: false,
        },
      ],
    },
    events,
  );
  assert.equal(row.minBet, 100);
  assert.equal(row.maxBet, 100000);
  assert.equal(row.betDelay, 0);
});

test("inactive sessions remain available for fancy-table deactivation", () => {
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const rows = marketRows(
    {
      data: [
        {
          id: "4.1-F2",
          eventId: "10",
          sportId: 4,
          name: "Over runs",
          type: "session",
          isActive: false,
          gameOver: false,
        },
      ],
    },
    events,
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].isActive, false);
});

test("advanced fancy market types are requested and retained for deactivation", () => {
  for (const type of ["odd-even", "cricket-casino", "ball-by-ball"]) {
    assert.equal(MARKET_TYPES.includes(type), true);
    assert.equal(FANCY_MARKET_TYPES.has(type), true);
  }
  const events = new Map([["10", { eventName: "A v B", openDate: new Date(), inPlay: true }]]);
  const rows = marketRows(
    {
      data: [
        {
          id: "4.1-OE",
          eventId: "10",
          sportId: 4,
          name: "Odd Even",
          type: "odd-even",
          isActive: false,
          gameOver: true,
        },
        {
          id: "11.1-CC",
          eventId: "10",
          sportId: 4,
          name: "Cricket Casino",
          type: "cricket-casino",
          isActive: true,
          gameOver: false,
        },
        {
          id: "4.1-BB",
          eventId: "10",
          sportId: 4,
          name: "Ball By Ball",
          type: "ball-by-ball",
          isActive: true,
          gameOver: false,
        },
      ],
    },
    events,
  );
  assert.deepEqual(
    rows.map((row) => [row.marketId, oddsType(row.marketId)]),
    [
      ["4.1-OE", "OE"],
      ["11.1-CC", "CC"],
      ["4.1-BB", "BB"],
    ],
  );
});

test("undocumented vendor families are classified instead of dropped", () => {
  assert.equal(FANCY_MARKET_TYPES.has("other-market"), true);
  assert.equal(FANCY_MARKET_TYPES.has("meter"), true);
  assert.equal(REGULAR_MARKET_TYPES.includes("line-market"), true);
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const rows = marketRows(
    {
      data: [
        {
          id: "4.1-F3",
          eventId: "10",
          sportId: 4,
          name: "Caught Out",
          type: "other-market",
          isActive: true,
          gameOver: false,
        },
        {
          id: "4.2-MT",
          eventId: "10",
          sportId: 4,
          name: "Wicket Meter",
          type: "meter",
          isActive: true,
          gameOver: false,
        },
      ],
    },
    events,
  );
  assert.deepEqual(
    rows.map((row) => [row.marketId, row.marketType]),
    [
      ["4.1-F3", "other-market"],
      ["4.2-MT", "meter"],
    ],
  );
});

test("unnamed BM2 markets remain available for socket population", () => {
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const [row] = marketRows(
    {
      data: [
        { id: "1.2-BM2", eventId: "10", sportId: 4, name: null, type: null, isActive: true, gameOver: false },
      ],
    },
    events,
  );
  assert.equal(row.marketName, "Bookmaker2");
  assert.equal(row.marketType, "bookmaker");
  assert.equal(row.maxBet, 25000);
});

test("BM2 discovery inherits runner metadata from its base market", () => {
  assert.equal(bookmaker2BaseMarketId("1.260815092-BM2"), "1.260815092");
  assert.equal(bookmaker2BaseMarketId("4.1-BM"), null);
  assert.equal(runnerSourceMarketId("1.260815092-BM2"), "1.260815092");
  assert.deepEqual(runnerLookupMarketIds(["1.260815092-BM2", "4.1-BM"]), [
    "1.260815092-BM2",
    "1.260815092",
    "4.1-BM",
  ]);
  const seeded = regularDefinitionEntries(
    {
      marketId: "1.260815092-BM2",
      marketName: "Bookmaker2",
      eventId: 10,
      sportId: 4,
      matchName: "A v B",
      isActive: true,
      minBet: 100,
      maxBet: 25000,
      betDelay: 0,
    },
    [
      { selectionId: 1, runnerName: "A" },
      { selectionId: 2, runnerName: "B" },
    ],
  );
  assert.deepEqual(
    seeded.entries.map((entry) => entry.nation),
    ["A", "B"],
  );
  assert.deepEqual(
    seeded.entries.map((entry) => entry.b1),
    [null, null],
  );
});

test("only Match Odds BM2 remains active", () => {
  const rows = enforceBookmaker2Eligibility([
    { marketId: "1.1", marketType: "match-odd", isActive: true },
    { marketId: "1.1-BM2", marketType: "bookmaker", isActive: true },
    { marketId: "1.2", marketType: "tied-match", isActive: true },
    { marketId: "1.2-BM2", marketType: "bookmaker", isActive: true },
    { marketId: "1.3", marketType: "completed-match", isActive: false },
    { marketId: "1.3-BM2", marketType: "bookmaker", isActive: true },
  ]);
  assert.equal(rows.find((row) => row.marketId === "1.1-BM2").isActive, true);
  assert.equal(rows.find((row) => row.marketId === "1.2-BM2").isActive, false);
  assert.equal(rows.find((row) => row.marketId === "1.3-BM2").isActive, false);
});

test("advanced market families use separate vendor discovery requests", () => {
  assert.deepEqual(FANCY_MARKET_REQUESTS, [
    ["session"],
    ["khado"],
    ["odd-even"],
    ["cricket-casino"],
    ["ball-by-ball"],
  ]);
  assert.equal(REGULAR_MARKET_TYPES.includes("match-odd"), true);
  assert.equal(REGULAR_MARKET_TYPES.includes("odd-even"), false);
});

test("active vendor discovery wins over contradictory typed inactive records", () => {
  const base = {
    marketId: "4.1-F2",
    eventId: 10,
    sportId: 4,
    marketName: "Over runs",
    marketType: "session",
  };
  const rows = mergeDiscoveredMarkets([
    { ...base, isActive: true, gameOver: false },
    { ...base, isActive: false, gameOver: false },
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].isActive, true);
  assert.equal(rows[0].gameOver, false);
});

test("vendor discovery deactivates a market when every response agrees", () => {
  const base = {
    marketId: "4.1-F2",
    eventId: 10,
    sportId: 4,
    marketName: "Over runs",
    marketType: "session",
  };
  const [row] = mergeDiscoveredMarkets([
    { ...base, isActive: false, gameOver: false },
    { ...base, isActive: false, gameOver: true },
  ]);
  assert.equal(row.isActive, false);
  assert.equal(row.gameOver, true);
});

test("active fancy definitions create socket-updatable placeholder rows", () => {
  assert.deepEqual(
    fancyDefinitionEntry({
      marketId: "4.1-OE",
      marketName: "Odd runs",
      marketType: "odd-even",
      eventId: 10,
      matchName: "A v B",
      minBet: 100,
      maxBet: 100000,
      betDelay: 0,
    }),
    {
      mid: "4.1-OE",
      sid: "4.1-OE",
      nation: "Odd runs",
      b1: null,
      l1: null,
      bs1: 0,
      ls1: 0,
      gstatus: "WAITING",
      rem: "",
      srno: "",
      gameover: false,
      s: true,
      maxBet: 100000,
      minBet: 100,
      betDelay: 0,
      matchId: 10,
      isActive: true,
      isShow: true,
      matchName: "A v B",
      matchType: "odd-even",
      maxLiabilityPerMarket: 100000,
      display_message: null,
      rate: null,
    },
  );
});

test("market rows map betting configuration", () => {
  const market = Market.fromRow({
    marketid: "1.2",
    betdelay: 3,
    minbetrate: "1.5",
    maxbetrate: "100",
    is_redis_updated: 0,
  });
  assert.equal(market.betDelay, 3);
  assert.equal(market.minBetRate, "1.5");
  assert.equal(market.isRedisUpdated, 0);
});

test("bookmaker ticks are transformed for Redis", () => {
  const output = bookmakerPayload(
    {
      mid: "BM-1",
      r: [
        { rid: 11, na: "India", b: 91, l: 92, sb: "S" },
        { rid: 12, na: "Australia", b: 93, l: 94, sb: "B" },
      ],
    },
    {
      marketname: "MATCH_ODDS",
      minbet: 100,
      maxbet: 50000,
      betDelay: 2,
      eventid: 99,
      isactive: 1,
      sportid: 4,
    },
  );
  assert.equal(output.length, 2);
  assert.equal(output[0].matchName, "India v Australia");
  assert.equal(output[0].gstatus, "SUSPENDED");
  assert.equal(output[1].gstatus, "Ball Running");
  assert.equal(output[0].matchId, 99);
  assert.equal(output[0].betlock, 2);
});

test("toss bookmaker markets retain vendor runner odds", () => {
  const [runner] = bookmakerPayload(
    { mid: "BM-TOSS", r: [{ rid: 1, na: "Heads", back: 98, lay: 0 }] },
    { marketname: "TOSS" },
  );
  assert.equal(runner.b1, 98);
  assert.equal(runner.l1, 0);
});

test("standard odds ticks match the frontend Redis contract", () => {
  const output = oddsPayload(
    {
      eid: 99,
      mid: "1.2",
      s: "OPEN",
      ip: true,
      r: [
        {
          rid: 11,
          na: "India",
          b: [{ p: 1.8, s: 250 }],
          l: [{ p: 1.82, s: 300 }],
        },
      ],
    },
    {
      eventid: 99,
      matchname: "India v Australia",
      marketname: "Match Odds",
      minbet: 100,
      maxbet: 50000,
      betDelay: 3,
      inPlay: 1,
      isactive: 1,
      sportid: 4,
    },
  );
  assert.equal(output.marketId, "1.2");
  assert.equal(output.matchName, "India v Australia");
  assert.deepEqual(output.runners[0].ex.availableToBack, [{ price: 1.8, size: 250 }]);
  assert.deepEqual(output.runners[0].ex.availableToLay, [{ price: 1.82, size: 300 }]);
});

test("odds runners use cached provider selection names", () => {
  const names = new Map([
    ["11", "India"],
    ["12", "Australia"],
  ]);
  const output = oddsPayload(
    { mid: "1.2", r: [{ rid: 11 }, { rid: 12 }] },
    { marketname: "Match Odds" },
    names,
  );
  assert.deepEqual(
    output.runners.map((runner) => runner.name),
    ["India", "Australia"],
  );
});

test("existing runner names survive unnamed socket updates", () => {
  const entries = [{ runners: [{ selectionId: 11, name: null }] }];
  preserveRunnerNames(entries, [{ runners: [{ selectionId: 11, name: "India" }] }]);
  assert.equal(entries[0].runners[0].name, "India");
});

test("compact socket odds fields become three-level price ladders", () => {
  const runner = {
    b1: "10.00",
    b2: "9.40",
    b3: "9.20",
    br1: 4140,
    br2: 10000,
    br3: 2451600,
    l1: "10.50",
    l2: "11.00",
    l3: "12.00",
    lr1: 331780,
    lr2: 9892560,
    lr3: 33810,
  };
  assert.deepEqual(runnerPrices(runner, "back"), [
    { price: 10, size: 4140 },
    { price: 9.4, size: 10000 },
    { price: 9.2, size: 2451600 },
  ]);
  assert.deepEqual(runnerPrices(runner, "lay"), [
    { price: 10.5, size: 331780 },
    { price: 11, size: 9892560 },
    { price: 12, size: 33810 },
  ]);
});

test("fancy ticks and market suffixes map to frontend groups", () => {
  const output = fancyPayload(
    { mid: "4.1-F2", na: "Six over runs", go: true, r: [{ b: 42, l: 40, bs: 100, ls: 100 }] },
    {
      eventid: 99,
      marketname: "Six over runs",
      minbet: 100,
      maxbet: 100000,
      isactive: Buffer.from([0]),
      isShow: Buffer.from([1]),
    },
  );
  assert.equal(payloadGroup({ mid: "4.1-F2" }, {}), "Fancy2");
  assert.equal(payloadGroup({ mid: "4.1-OE" }, {}), "OddEven");
  assert.equal(payloadGroup({ mid: "4.1-F3" }, {}), "OtherMarket");
  assert.equal(payloadGroup({ mid: "4.1-KD" }, {}), "Khado");
  assert.equal(payloadGroup({ mid: "4.1" }, { mtype: "khado" }), "Khado");
  assert.equal(payloadGroup({ mid: "1.123" }, { marketname: "1st Innings 20 Overs Line" }), "LineMarket");
  assert.equal(output.gameover, true);
  assert.equal(output.nation, "Six over runs");
  assert.deepEqual(Object.keys(emptyEventPayload()), [
    "Odds",
    "Bookmaker",
    "LineMarket",
    "Fancy2",
    "Khado",
    "OddEven",
    "OtherMarket",
    "Fancy3",
    "CricketCasino",
    "BallByBall",
  ]);
});

test("legacy Fancy3 Redis rows migrate to OtherMarket", () => {
  const payload = normalizeEventPayload({ Fancy3: [{ mid: "4.1-F3", nation: "Caught Out" }] });
  assert.equal(payload.Fancy3.length, 0);
  assert.equal(payload.OtherMarket[0].mid, "4.1-F3");
});

test("legacy line markets migrate from Odds to their separate group", () => {
  const market = { marketId: "1.123", Name: "1st Innings 20 Overs Line" };
  const payload = normalizeEventPayload({ Odds: [market] });
  assert.equal(payload.Odds.length, 0);
  assert.deepEqual(payload.LineMarket, [market]);
});

test("legacy KD rows migrate from Fancy2 to Khado", () => {
  const row = { mid: "4.1-KD", nation: "Innings Khado" };
  const payload = normalizeEventPayload({ Fancy2: [row] });
  assert.equal(payload.Fancy2.length, 0);
  assert.deepEqual(payload.Khado, [row]);
});

test("invalid sentinel market IDs are rejected and removed from snapshots", () => {
  assert.equal(validMarketIdentifier("undefined"), false);
  assert.equal(validMarketIdentifier("1.123"), true);
  const payload = normalizeEventPayload({ Odds: [{ marketId: "undefined", Name: "Market undefined" }] });
  assert.equal(payload.Odds.length, 0);
});

test("regular API definitions seed line markets with suspended runner placeholders", () => {
  const output = regularDefinitionEntries(
    {
      marketId: "1.123",
      eventId: 10,
      sportId: 4,
      marketName: "1st Innings 20 Overs Line",
      matchName: "A v B",
      isActive: true,
      minBet: 100,
      maxBet: 1,
      betDelay: 3,
    },
    [
      { selectionId: 1, runnerName: "Over" },
      { selectionId: 2, runnerName: "Under" },
    ],
  );
  assert.equal(output.group, "LineMarket");
  assert.equal(output.entries[0].status, "WAITING");
  assert.deepEqual(
    output.entries[0].runners.map((runner) => [runner.name, runner.status]),
    [
      ["Over", "SUSPENDED"],
      ["Under", "SUSPENDED"],
    ],
  );
  assert.deepEqual(output.entries[0].runners[0].ex.availableToBack, []);
});

test("top-level provider fancy fields map to frontend prices and sizes", () => {
  const output = fancyPayload(
    {
      mid: "4.1-F2",
      na: "Six over runs",
      b: 75,
      l: 73,
      br: 100,
      lr: 120,
      res: "Ball Running",
      di: 6,
      s: true,
    },
    {
      eventid: 99,
      marketname: "Six over runs",
      minbet: 100,
      maxbet: 100000,
      isactive: Buffer.from([0]),
      isShow: Buffer.from([1]),
    },
  );
  assert.equal(output.b1, 75);
  assert.equal(output.l1, 73);
  assert.equal(output.bs1, 100);
  assert.equal(output.ls1, 120);
  assert.equal(output.rem, "Ball Running");
  assert.equal(output.srno, "6");
  assert.equal(output.isActive, false);
  assert.equal(output.isShow, true);
  assert.equal(payloadGroup({ mid: "4.1-CC" }, {}), "CricketCasino");
});

test("cricket casino rate maps to the visible back price", () => {
  const output = fancyPayload(
    { eid: 10, mid: "11.252631991096-CC", na: "1st Ing 1-100 Ball Run", ra: 9, s: true },
    { eventid: 10, marketname: "1st Ing 1-100 Ball Run", markettype: "cricket-casino" },
  );
  assert.equal(output.b1, 9);
  assert.equal(output.l1, null);
  assert.equal(output.rate, 9);
  assert.equal(output.bs1, null);
  assert.equal(output.ls1, null);
});

test("unavailable fancy ticks are removed without treating them as result ticks", () => {
  assert.equal(shouldRemoveFromPayload("Fancy2", { s: false, go: false }), true);
  assert.equal(shouldRemoveFromPayload("Fancy2", { s: true, go: false }), false);
  assert.equal(isResultTick({ mid: "4.1-F2", s: false, go: false }), false);
});

test("provider settlement rows retain winner and exceptional result semantics", () => {
  assert.deepEqual(
    resultRows({
      data: [
        { marketId: "1.2", marketType: "match-odd", result: "123", isTie: false, isAbandoned: false },
        { marketId: "4.1-F2", marketType: "session", result: "Abandoned", isAbandoned: true },
      ],
    }),
    [
      { marketId: "1.2", marketType: "match-odd", result: "123", isTie: false, isAbandoned: false },
      { marketId: "4.1-F2", marketType: "session", result: "Abandoned", isTie: false, isAbandoned: true },
    ],
  );
});

test("fancy settlement values follow the Java-compatible contract", () => {
  assert.equal(fancyResultValue("4.1-F2", "87"), 87);
  assert.equal(fancyResultValue("4.1-BB", "12"), 12);
  assert.equal(fancyResultValue("4.1-CC", "4"), 4);
  assert.equal(fancyResultValue("4.1-OE", "Back"), 1);
  assert.equal(fancyResultValue("4.1-OE", "Lay"), 0);
  assert.equal(fancyResultValue("4.1-F3", "back"), 1);
  assert.equal(fancyResultValue("4.1-F2", "Abandoned"), null);
});

test("provider acknowledgement excludes skipped markets from subscription", () => {
  assert.deepEqual(
    normalizeProviderAcknowledgement(
      {
        subscribed: ["1.2"],
        skipped: ["BM-1"],
      },
      ["1.2", "BM-1", "missing"],
    ),
    {
      subscribed: ["1.2"],
      skipped: ["BM-1", "missing"],
      providerResponse: { subscribed: ["1.2"], skipped: ["BM-1"] },
    },
  );
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
    type: "object",
    keys: ["level", "message"],
    messageKeys: ["markets"],
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
  const payload = {
    level: "info",
    message: {
      markets: [
        { eid: 123, mid: "1.2", r: [] },
        { eid: 123, mid: "BM-1", r: [] },
      ],
    },
  };
  assert.equal(containsMarketId(payload, "BM-1"), true);
  assert.equal(containsMarketId(payload, "missing"), false);
  assert.equal(payloadContainsMarket(payload, "1.2"), true);
});

test("log reader parses consecutive pretty JSON records", () => {
  assert.deepEqual(parseJsonObjects('{\n"a":1,"nested":{"ok":true}\n}\n{\n"b":2\n}'), [
    { a: 1, nested: { ok: true } },
    { b: 2 },
  ]);
});
