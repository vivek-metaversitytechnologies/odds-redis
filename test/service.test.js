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
  isFullySuspendedToss,
  moveTiedMatchLast,
  fancyDefinitionEntry,
  regularDefinitionEntries,
  normalizeEventPayload,
  frontendEventPayload,
  validMarketIdentifier,
  writeTicks,
  reconcileFancyDefinitions,
  __testing__: redisTesting,
} = require("../src/config/redis");
const { normalizeProviderAcknowledgement } = require("../src/services/marketSubscriptionService");
const { isFutureMarket } = require("../src/controllers/subscriptionController");

test("subscription admin separates future markets from actionable pending markets", () => {
  const now = Date.parse("2026-08-15T10:00:00.000Z");
  assert.equal(isFutureMarket({ inplay: 0, opendate: "2026-08-15T11:01:00.000Z" }, now), true);
  assert.equal(isFutureMarket({ inplay: 0, opendate: "2026-08-15T11:00:00.000Z" }, now), false);
  assert.equal(isFutureMarket({ inplay: 1, opendate: "2026-08-16T11:00:00.000Z" }, now), false);
});
const {
  collectOddsTicks,
  collectScores,
  messageShape,
  logRawSocketPayload,
  payloadContainsMarket,
  isResultTick,
} = require("../src/services/websocketService");
const { parseJsonObjects, containsMarketId } = require("../src/services/logReaderService");
const {
  activeMatchesFromCache,
  cachedDashboardRow,
  eventOnlyDashboardEntry,
  dashboardEntry,
  compareDashboardEntries,
  selectDashboardRows,
} = require("../src/services/dashboardService");
const { competitionRows } = require("../src/cron/competitionSync");
const { eventRows, eventInsertValues } = require("../src/cron/eventSync");
const {
  MARKET_TYPES,
  REGULAR_MARKET_TYPES,
  FANCY_MARKET_TYPES,
  FANCY_MARKET_REQUESTS,
  marketRows,
  isMarketSnapshotResponse,
  inferredMarketType,
  fallbackMarketName,
  mergeDiscoveredMarkets,
  bookmaker2BaseMarketId,
  runnerSourceMarketId,
  runnerLookupMarketIds,
  enforceBookmaker2Eligibility,
  inactiveLineMarkets,
  missingLineMarketIds,
  discoveryEventBatchSize,
  prioritizedDiscoveryEvents,
  discoveryEventBatches,
  typedDiscoveryRequests,
  discoveryPriority,
  oddsType,
  storedInFancyTable,
} = require("../src/cron/marketDiscoverySync");
const {
  responseRows: resultRows,
  fancyResultValue,
  isEventTerminalMarketName,
} = require("../src/cron/resultSync");
const { integer, boolean, csvIntegers } = require("../src/config/env");
const { setBounded } = require("../src/utils/boundedMap");
const { isDeadlock, retryDeadlock } = require("../src/utils/dbRetry");
const { checksum, migrationFiles } = require("../scripts/migrate");
const { lockName: migrationLockName } = require("../scripts/migration-lock");
const { eventIdFromKey } = require("../src/cron/redisEventCleanup");
const { eventInWindow, eventWindowSql, subscriptionEventWindowSql } = require("../src/utils/eventWindow");
const { subscriptionDiff } = require("../src/cron/marketSync");
const { resultFilters, resultWhere } = require("../src/utils/resultFilters");
const { environmentErrors, cronErrors } = require("../src/services/startupPreflight");
const { pipelineCheck } = require("../src/services/healthSupervisor");
const { getProviderRateLimitStatus, isVendorRateLimitResponse } = require("../src/services/providerApi");

test("provider limiter stays below the vendor rolling-window cap", () => {
  const limit = getProviderRateLimitStatus();
  assert.equal(limit.vendorWindowCap, 1000);
  assert.equal(limit.safeWindowCap, 800);
  assert.ok(limit.effectiveRequestsPerMinute <= 2400);
  assert.ok(limit.minTimeMs >= 25);
});

test("provider limiter recognizes the vendor temporary IP block", () => {
  assert.equal(isVendorRateLimitResponse(429, { message: "slow down" }), true);
  assert.equal(
    isVendorRateLimitResponse(403, "Access denied: IP is temporarily blocked due to exceeding 1000 requests"),
    true,
  );
  assert.equal(isVendorRateLimitResponse(403, "Forbidden"), false);
});

test("database deadlocks are recognized and retried with a bounded attempt count", async () => {
  assert.equal(isDeadlock({ code: "ER_LOCK_DEADLOCK" }), true);
  assert.equal(isDeadlock({ errno: 1213 }), true);
  assert.equal(isDeadlock({ sqlState: "40001" }), true);
  assert.equal(isDeadlock({ code: "ER_BAD_FIELD_ERROR" }), false);
  let calls = 0;
  const result = await retryDeadlock(
    async () => {
      calls += 1;
      if (calls < 3) throw Object.assign(new Error("deadlock"), { errno: 1213 });
      return "saved";
    },
    { attempts: 3, delayMs: 0 },
  );
  assert.equal(result, "saved");
  assert.equal(calls, 3);
});

test("health supervisor classifies failed and stuck pipelines", () => {
  const now = Date.parse("2026-08-22T12:00:00.000Z");
  assert.equal(pipelineCheck("events", { lastError: "provider timeout" }, now).status, "degraded");
  assert.equal(
    pipelineCheck("events", { running: true, lastStartedAt: "2026-08-22T11:55:00.000Z" }, now).status,
    "critical",
  );
  assert.equal(
    pipelineCheck("events", { running: false, lastCompletedAt: new Date(now).toISOString() }, now).status,
    "healthy",
  );
});

test("startup preflight rejects missing infrastructure configuration", () => {
  const errors = environmentErrors({ PORT: "70000", PROVIDER_SOCKET_URL: "invalid" });
  assert.ok(errors.some((error) => error.includes("SOURCE_DB_HOST")));
  assert.ok(errors.some((error) => error.includes("REDIS_URL")));
  assert.ok(errors.some((error) => error.includes("PROVIDER_TOKEN")));
  assert.ok(errors.some((error) => error.includes("PROVIDER_SOCKET_URL")));
  assert.ok(errors.some((error) => error.includes("PORT")));
});

test("startup preflight accepts complete infrastructure configuration", () => {
  assert.deepEqual(
    environmentErrors({
      SOURCE_DB_HOST: "db",
      SOURCE_DB_USER: "user",
      SOURCE_DB_NAME: "odds",
      REDIS_HOST: "redis",
      PROVIDER_TOKEN: "token",
      PROVIDER_SOCKET_URL: "wss://provider.example/stream",
      PROVIDER_BASE_URL: "https://provider.example",
      PORT: "5673",
    }),
    [],
  );
  assert.deepEqual(cronErrors({ sample: { expression: "*/5 * * * * *" } }), []);
  assert.equal(cronErrors({ sample: { expression: "not a cron" } }).length, 1);
});

function createFakeRedisClient({ gateFirstGet = false } = {}) {
  const store = new Map();
  let firstGetGated = gateFirstGet;
  let releaseGate = () => {};
  const gate = gateFirstGet
    ? new Promise((resolve) => {
        releaseGate = resolve;
      })
    : Promise.resolve();
  const client = {
    isOpen: true,
    async get(key) {
      // Snapshot the value immediately (like a real Redis server would, at the
      // moment it processes the command) and only delay handing the response back
      // — a delayed *response* must not silently pick up writes made in the meantime.
      const shouldGate = firstGetGated;
      firstGetGated = false;
      const value = store.has(key) ? store.get(key) : null;
      if (shouldGate) await gate;
      return value;
    },
    async set(key, value) {
      store.set(key, value);
      return "OK";
    },
    async mGet(keys) {
      return keys.map((key) => (store.has(key) ? store.get(key) : null));
    },
    multi() {
      const ops = [];
      const builder = {
        set(key, value) {
          ops.push([key, value]);
          return builder;
        },
        async exec() {
          for (const [key, value] of ops) store.set(key, value);
          return ops.map(() => "OK");
        },
      };
      return builder;
    },
  };
  return { client, store, releaseGate };
}

test("discovery reconciliation cannot overwrite a socket tick that lands mid-read", async () => {
  const { client: fakeClient, store, releaseGate } = createFakeRedisClient({ gateFirstGet: true });
  redisTesting.reset();
  redisTesting.setRedisClient(fakeClient);
  redisTesting.primeMarketCache([
    [
      "9001.F2",
      { marketid: "9001.F2", eventid: 9001, marketname: "Session Runs", isactive: true, matchname: "Alpha v Beta" },
    ],
  ]);

  const marketDefinition = {
    eventId: 9001,
    marketId: "9001.F2",
    marketName: "Session Runs",
    isActive: true,
    gameOver: false,
  };
  const tickItem = { eid: 9001, mid: "9001.F2", sb: "OPEN" };

  // Reconciliation starts first and blocks on its Redis read (simulating network
  // latency), which registers it as the event's lock holder. A live socket tick for
  // the same market arrives while it is still stuck there.
  const reconcilePromise = reconcileFancyDefinitions([marketDefinition]);
  await new Promise((resolve) => setImmediate(resolve));
  const tickPromise = writeTicks([tickItem]);

  // Give the tick every opportunity to run if it weren't blocked by the event lock.
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(
    store.has("Data-Rs:9001"),
    false,
    "the tick must not be able to persist while reconciliation still holds the event lock",
  );

  releaseGate();
  await Promise.all([reconcilePromise, tickPromise]);

  const stored = JSON.parse(store.get("Data-Rs:9001"));
  const entry = stored.Fancy2.find((row) => String(row.mid ?? row.sid) === "9001.F2");
  assert.ok(entry, "the market survives both operations");
  assert.notEqual(entry.gstatus, "WAITING", "the tick's live status must not be reverted by reconciliation");

  redisTesting.reset();
});

test("Redis API reads use a connection isolated from background writes", () => {
  const redisSource = fs.readFileSync(path.join(__dirname, "../src/config/redis.js"), "utf8");
  assert.match(redisSource, /async function getRedisReadClient\(\)/);
  assert.match(redisSource, /async function getEventSnapshots[\s\S]*?getRedisReadClient\(\)/);
  assert.match(redisSource, /readConnected: Boolean\(readClient\?\.isOpen\)/);
});

test("stored result filters validate and parameterize supported query fields", () => {
  const filters = resultFilters({
    type: "fancy",
    sportId: "4",
    eventId: "35916587",
    marketId: "4.123-F2",
    status: "OPEN",
    from: "2026-08-20",
    to: "2026-08-21",
    limit: "999",
  });
  assert.equal(filters.type, "fancy");
  assert.equal(filters.limit, 500);
  const where = resultWhere(filters, "fancy");
  assert.match(where.sql, /fancyid = \?/);
  assert.match(where.sql, /updatedon >= \?/);
  assert.match(where.sql, /updatedon < \?/);
  assert.deepEqual(where.params.slice(0, 4), [4, 35916587, "4.123-F2", "OPEN"]);
});

test("stored result filters reject unsupported types and malformed IDs", () => {
  assert.throws(() => resultFilters({ type: "all-results" }), /type must be/);
  assert.throws(() => resultFilters({ sportId: "cricket" }), /sportId must be/);
  assert.throws(() => resultFilters({ from: "2026-08-22", to: "2026-08-21" }), /from must be earlier/);
});

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

test("active market discovery isolates events while future discovery remains batched", () => {
  const previousFuture = process.env.MARKET_DISCOVERY_EVENT_BATCH_SIZE;
  delete process.env.MARKET_DISCOVERY_EVENT_BATCH_SIZE;
  assert.equal(discoveryEventBatchSize("active"), 1);
  assert.equal(discoveryEventBatchSize("future"), 10);
  process.env.MARKET_DISCOVERY_EVENT_BATCH_SIZE = "20";
  assert.equal(discoveryEventBatchSize("active"), 1);
  assert.equal(discoveryEventBatchSize("future"), 20);
  if (previousFuture === undefined) delete process.env.MARKET_DISCOVERY_EVENT_BATCH_SIZE;
  else process.env.MARKET_DISCOVERY_EVENT_BATCH_SIZE = previousFuture;
});

test("market discovery prioritizes cricket and limits non-cricket typed fan-out", () => {
  const events = [
    { eventId: 11, sportId: 1 },
    { eventId: 12, sportId: 2 },
    { eventId: 13, sportId: 4 },
  ];
  const prioritized = prioritizedDiscoveryEvents(events, "active", Date.now());
  assert.equal(prioritized[0].sportId, 4);
  assert.deepEqual(discoveryEventBatches(prioritized, "active")[0], { eids: [13], sportId: 4 });
  assert.equal(typedDiscoveryRequests(4).length, 1 + FANCY_MARKET_REQUESTS.length);
  assert.deepEqual(typedDiscoveryRequests(1), [["match-odd", "winner-market", "goals"]]);
  assert.deepEqual(typedDiscoveryRequests(2), [["match-odd", "winner-market", "goals"]]);
  assert.ok(discoveryPriority(4) < discoveryPriority(1));
  assert.ok(discoveryPriority(1, "active") < discoveryPriority(1, "future"));
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

test("Redis event cleanup extracts only numeric event IDs from configured key prefixes", () => {
  const prefixes = ["Data-Rs:", "Score-Rs:"];
  assert.equal(eventIdFromKey("Data-Rs:35913614", prefixes), "35913614");
  assert.equal(eventIdFromKey("Score-Rs:35913614", prefixes), "35913614");
  assert.equal(eventIdFromKey("Data-Rs:35913614:market", prefixes), null);
  assert.equal(eventIdFromKey("Other:35913614", prefixes), null);
  assert.equal(eventIdFromKey("Data-Rs:0", prefixes), null);
});

test("event workload lanes use mutually exclusive start-time predicates", () => {
  assert.match(eventWindowSql("e", "active"), /open_date <= DATE_ADD/);
  assert.match(eventWindowSql("e", "active"), /UTC_TIMESTAMP\(\).*INTERVAL 390 MINUTE/);
  assert.match(eventWindowSql("e", "active"), /in_play/);
  assert.match(eventWindowSql("e", "future"), /open_date > DATE_ADD/);
  assert.match(eventWindowSql("e", "future"), /in_play,0\)=0/);
  assert.equal(eventWindowSql("e", "all"), "1=1");
  const now = Date.parse("2026-08-20T10:00:00Z");
  assert.equal(eventInWindow({ openDate: "2026-08-20 16:29:00", inPlay: false }, "active", now), true);
  assert.equal(eventInWindow({ openDate: "2026-08-20 16:31:00", inPlay: false }, "active", now), false);
  assert.equal(eventInWindow({ openDate: "2026-08-20 16:31:00", inPlay: false }, "future", now), true);
  assert.equal(eventInWindow({ openDate: "2026-08-21 16:31:00", inPlay: true }, "future", now), false);
});

test("subscriptions use a wider lead only for non-cricket active markets", () => {
  const sql = subscriptionEventWindowSql("m.sportid", "e", "active");
  assert.match(sql, /\(m\.sportid\)=4.*INTERVAL 390 MINUTE/);
  assert.match(sql, /\(m\.sportid\)<>4.*INTERVAL 1050 MINUTE/);
  assert.equal(subscriptionEventWindowSql("m.sportid", "e", "all"), "1=1");
  assert.match(subscriptionEventWindowSql("m.sportid", "e", "future"), /open_date > DATE_ADD/);
});

test("market subscription batches retain cricket priority", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/cron/marketSync.js"), "utf8");
  assert.match(source, /CASE WHEN m\.sportid=.*THEN 0 ELSE 1 END/);
  assert.match(source, /CASE WHEN COALESCE\(f\.sportid,e\.sportid\)=.*THEN 0 ELSE 1 END/);
});

test("market discovery delegates subscription reconciliation to its standalone cron", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/cron/marketDiscoverySync.js"), "utf8");
  assert.doesNotMatch(source, /syncMarketSubscriptions\s*\(/);
  assert.match(source, /delegatedToMarketSync:\s*true/);
});

test("active subscription reconciliation finds missing and stale market IDs", () => {
  assert.deepEqual(subscriptionDiff(["current", "missing"], ["current", "stale"]), {
    pending: ["missing"],
    stale: ["stale"],
  });
  assert.deepEqual(
    subscriptionDiff(["settled"], [], (id) => id === "settled"),
    {
      pending: [],
      stale: [],
    },
  );
  assert.deepEqual(subscriptionDiff([undefined, null, "undefined", "valid"], ["null", "stale"]), {
    pending: ["valid"],
    stale: ["stale"],
  });
});

test("migration runner discovers ordered SQL files and produces stable checksums", () => {
  const files = migrationFiles(path.join(__dirname, "../scripts/migrations"));
  assert.deepEqual(files, [...files].sort());
  assert.equal(files[0], "000_prepare_selection_runner_index.sql");
  assert.ok(files.includes("001_selection_runner_unique.sql"));
  assert.equal(checksum("SELECT 1"), checksum("SELECT 1"));
  assert.notEqual(checksum("SELECT 1"), checksum("SELECT 2"));
  assert.equal(migrationLockName, "odds_socket_schema_migrations");
});

test("normal service shutdown preserves provider registrations", () => {
  const serverSource = fs.readFileSync(path.join(__dirname, "../src/server.js"), "utf8");
  assert.match(serverSource, /closeProviderRequests\(\)/);
  assert.doesNotMatch(serverSource, /subscriptions\.unsubscribeAll\s*\(/);
  assert.match(serverSource, /RECONCILE_SUBSCRIPTIONS_ON_START\s*\|\|\s*"false"/);
});

test("socket coalescing does not retain unused promise waiters", () => {
  const socketSource = fs.readFileSync(path.join(__dirname, "../src/services/websocketService.js"), "utf8");
  assert.doesNotMatch(socketSource, /pending\.waiters/);
  assert.match(socketSource, /const queuedEventWrites = new Map\(\)/);
  assert.doesNotMatch(socketSource, /const previous = eventWriteChains\.get/);
  assert.match(socketSource, /const pendingScores = new Map\(\)/);
  assert.doesNotMatch(socketSource, /void persistScores\(scores/);
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
      isOutright: 0,
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
    outright: false,
    team1Back: 1.8,
    team1Lay: 1.82,
    team2Back: 2.1,
    team2Lay: 2.12,
    drawBack: 0,
    drawLay: 0,
    li: 7,
  });
});

test("dashboard sorts outright winner markets before normal matches", () => {
  const entries = [
    { matchId: 1, outright: false, openDate: "2026-08-09 10:00:00" },
    { matchId: 2, outright: true, openDate: "2026-08-10 17:30:00" },
    { matchId: 3, outright: true, openDate: "2026-08-10 10:00:00" },
  ];
  entries.sort(compareDashboardEntries);
  assert.deepEqual(
    entries.map((entry) => entry.matchId),
    [3, 2, 1],
  );
});

test("Redis event metadata and snapshots produce the active-match API shape", () => {
  const events = [
    {
      eventId: 101,
      eventName: "Alpha v Beta",
      sportId: 4,
      seriesId: 55,
      openDate: "2026-08-20T12:00:00.000Z",
      inPlay: true,
      gameOver: false,
    },
  ];
  const snapshots = new Map([
    [
      "101",
      {
        Odds: [
          {
            marketId: "1.101",
            Name: "Match Odds",
            inplay: true,
            runners: [
              { ex: { availableToBack: [{ price: 1.8 }], availableToLay: [{ price: 1.9 }] } },
              { ex: { availableToBack: [{ price: 2.1 }], availableToLay: [{ price: 2.2 }] } },
            ],
          },
        ],
        Bookmaker: [{ mid: "4.101-BM", t: "Match Bookmaker" }],
      },
    ],
  ]);
  assert.equal(cachedDashboardRow(events[0], snapshots.get("101")).marketid, "1.101");
  assert.deepEqual(activeMatchesFromCache(events, snapshots, 48, Date.parse("2026-08-20T13:00:00Z")), [
    {
      matchName: "Alpha v Beta",
      openDate: "2026-08-20T12:00:00.000Z",
      inPlay: true,
      matchId: 101,
      marketId: "1.101",
      bm: true,
      GM: false,
      outright: false,
      team1Back: 1.8,
      team1Lay: 1.9,
      team2Back: 2.1,
      team2Lay: 2.2,
      drawBack: 0,
      drawLay: 0,
      li: 55,
    },
  ]);
});

test("Redis events remain visible before a usable market snapshot arrives", () => {
  const event = {
    eventId: 102,
    eventName: "Waiting Home v Waiting Away",
    sportId: 1,
    seriesId: 56,
    openDate: "2026-08-20T14:00:00.000Z",
    inPlay: false,
    gameOver: false,
  };
  const expected = {
    matchName: "Waiting Home v Waiting Away",
    openDate: "2026-08-20T14:00:00.000Z",
    inPlay: false,
    matchId: 102,
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
    li: 56,
  };
  const now = Date.parse("2026-08-20T13:00:00Z");
  assert.deepEqual(eventOnlyDashboardEntry(event), expected);
  assert.deepEqual(
    activeMatchesFromCache([event], new Map([["102", { Odds: [], Bookmaker: [] }]]), 48, now),
    [expected],
  );
  assert.deepEqual(activeMatchesFromCache([event], new Map(), 48, now), [expected]);
});

test("Redis active-match cache excludes completed and expired events", () => {
  const snapshot = {
    Odds: [{ marketId: "1.101", Name: "Match Odds", runners: [] }],
    Bookmaker: [],
  };
  const events = [
    { eventId: 1, sportId: 4, openDate: "2026-08-17T12:00:00Z", gameOver: false },
    { eventId: 2, sportId: 4, openDate: "2026-08-20T12:00:00Z", gameOver: true },
  ];
  const snapshots = new Map([
    ["1", snapshot],
    ["2", snapshot],
  ]);
  assert.deepEqual(activeMatchesFromCache(events, snapshots, 48, Date.parse("2026-08-20T13:00:00Z")), []);
});

test("database active-match rows are grouped without correlated market scans", () => {
  const common = {
    eventid: 101,
    matchname: "Alpha v Beta",
    sportid: 4,
    isactive: 1,
  };
  const selected = selectDashboardRows([
    { ...common, marketid: "goal", marketname: "First Goal" },
    { ...common, marketid: "winner", marketname: "Winner" },
    { ...common, marketid: "book", marketname: "Match Bookmaker" },
    { ...common, marketid: "odds", marketname: "Match Odds" },
    { ...common, marketid: "settled", marketname: "Match Odds", settled_marketid: "settled" },
  ]);
  assert.equal(selected.length, 1);
  assert.equal(selected[0].marketid, "odds");
  assert.equal(selected[0].isBookmaker, true);
  assert.equal(selected[0].isGoal, true);
  assert.equal(selected[0].isOutright, false);
});

test("competition sync keeps only supported sports", () => {
  const previousSportIds = process.env.SPORT_IDS;
  process.env.SPORT_IDS = "1,2,4";
  const rows = competitionRows({
    data: [
      { id: "10", name: "Football League", sportId: 1 },
      { id: "20", name: "Tennis Tour", sportId: 2 },
      { id: "30", name: "Cricket Cup", sportId: 4 },
      { id: "40", name: "Unsupported", sportId: 7 },
    ],
  });
  if (previousSportIds === undefined) delete process.env.SPORT_IDS;
  else process.env.SPORT_IDS = previousSportIds;
  assert.deepEqual(
    rows.map((row) => row.seriesId),
    [10, 20, 30],
  );
});

test("event sync keeps supported events including completed lifecycle updates", () => {
  const previousSportIds = process.env.SPORT_IDS;
  process.env.SPORT_IDS = "1,2,4";
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
  if (previousSportIds === undefined) delete process.env.SPORT_IDS;
  else process.env.SPORT_IDS = previousSportIds;
  assert.deepEqual(
    rows.map((row) => row.eventId),
    [10, 20],
  );
  assert.equal(rows[0].openDate, "2026-08-02 17:30:00");
  assert.equal(rows[1].gameOver, true);
});

test("event sync deduplicates conflicting lifecycle rows with game-over taking precedence", () => {
  const rows = eventRows([
    {
      data: [
        {
          id: 77,
          name: "Old Match",
          sportId: 4,
          leagueId: 9,
          startTime: "2026-08-23T10:00:00Z",
          inPlay: true,
          gameOver: false,
        },
        {
          id: 77,
          name: "Old Match",
          sportId: 4,
          leagueId: 9,
          startTime: "2026-08-23T10:00:00Z",
          inPlay: false,
          gameOver: true,
        },
      ],
    },
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].gameOver, true);
  assert.equal(rows[0].inPlay, false);
});

test("new event rows default fancylock to false", () => {
  const values = eventInsertValues({
    seriesId: 10,
    sportId: 4,
    eventId: 20,
    eventName: "A v B",
    openDate: "2026-08-23 12:00:00",
    inPlay: true,
    gameOver: false,
  });
  assert.equal(values[13], false);
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

test("market discovery infers fancy families from vendor suffixes", () => {
  assert.equal(inferredMarketType("4.1-F3", null), "other-market");
  assert.equal(inferredMarketType("4.1-MT", null), "meter");
  assert.equal(inferredMarketType("4.1-BB", null), "ball-by-ball");
  assert.equal(inferredMarketType("4.1-KD", null), "khado");
  assert.equal(inferredMarketType("4.1-OE", null), "odd-even");
  assert.equal(inferredMarketType("4.1-F2", null), "session");
  assert.equal(inferredMarketType("4.1-BM2", null), "bookmaker");
  assert.equal(inferredMarketType("4.1-BM2", "bookmaker2"), "bookmaker");
});

test("unnamed fancy markets use their Redis section name", () => {
  assert.equal(fallbackMarketName("odd-even", "4.1-OE"), "OddEven");
  assert.equal(fallbackMarketName("other-market", "4.1-F3"), "OtherMarket");
  assert.equal(fallbackMarketName("meter", "4.1-MT"), "Meter");
  assert.equal(fallbackMarketName("unknown", "1.2"), "Market 1.2");
});

test("ball-by-ball discovery prefixes the vendor ball line", () => {
  const events = new Map([["10", { eventName: "A v B", sportId: 4 }]]);
  const [row] = marketRows(
    {
      data: [
        {
          id: "4.1-BB",
          eventId: "10",
          sportId: 4,
          name: "Ball Run SB",
          type: "ball-by-ball",
          ballLine: 29,
          isActive: true,
        },
      ],
    },
    events,
  );
  assert.equal(row.marketName, "29 Ball Run SB");
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
  assert.equal(storedInFancyTable({ marketType: "line-market" }), true);
  assert.equal(storedInFancyTable({ marketType: "match-odd" }), false);
  assert.equal(oddsType("1.261062382"), "LINE");
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

test("BM2 discovery fetches and stores runner metadata under its exact market ID", () => {
  assert.equal(bookmaker2BaseMarketId("1.260815092-BM2"), "1.260815092");
  assert.equal(bookmaker2BaseMarketId("4.1-BM"), null);
  assert.equal(runnerSourceMarketId("1.260815092-BM2"), "1.260815092-BM2");
  assert.deepEqual(runnerLookupMarketIds(["1.260815092-BM2", "4.1-BM"]), ["1.260815092-BM2", "4.1-BM"]);
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

test("only valid vendor market snapshots are authoritative for missing-market removal", () => {
  assert.equal(isMarketSnapshotResponse([]), true);
  assert.equal(isMarketSnapshotResponse({ status: true, data: [] }), true);
  assert.equal(isMarketSnapshotResponse({ status: false, data: [] }), false);
  assert.equal(isMarketSnapshotResponse(null), false);
  assert.equal(isMarketSnapshotResponse("temporary upstream error"), false);
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

test("missing line markets require consecutive authoritative discovery passes", () => {
  const counts = new Map();
  const stored = [{ marketId: "1.2" }, { marketId: "1.3" }];
  const vendor = [{ marketId: "1.3" }];
  assert.deepEqual(missingLineMarketIds(stored, vendor, counts), []);
  assert.deepEqual(missingLineMarketIds(stored, vendor, counts), ["1.2"]);
  assert.equal(counts.has("1.3"), false);
});

test("a reappearing line market clears its missing-pass counter", () => {
  const counts = new Map();
  const stored = [{ marketId: "1.2" }];
  assert.deepEqual(missingLineMarketIds(stored, [], counts), []);
  assert.equal(counts.get("1.2"), 1);
  assert.deepEqual(missingLineMarketIds(stored, [{ marketId: "1.2" }], counts), []);
  assert.equal(counts.has("1.2"), false);
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

test("top-level socket suspension overrides open line-market and runner statuses", () => {
  const output = oddsPayload(
    {
      eid: 99,
      mid: "1.2",
      s: "OPEN",
      sb: "S",
      r: [{ rid: 11, s: "ACTIVE", b1: 79, l1: 78 }],
    },
    { eventid: 99, marketname: "75 Balls Runs Line", isactive: 1 },
  );
  assert.equal(output.status, "SUSPENDED");
  assert.equal(output.runners[0].status, "SUSPENDED");
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
  assert.equal(payloadGroup({ mid: "4.1-F3" }, {}), "Fancy3");
  assert.equal(payloadGroup({ mid: "4.1-KD" }, {}), "Khado");
  assert.equal(payloadGroup({ mid: "4.1-MT" }, {}), "Meter");
  assert.equal(payloadGroup({ mid: "4.1" }, { mtype: "meter" }), "Meter");
  assert.equal(payloadGroup({ mid: "4.1" }, { mtype: "khado" }), "Khado");
  assert.equal(payloadGroup({ mid: "1.123" }, { marketname: "1st Innings 20 Overs Line" }), "LineMarket");
  assert.equal(output.gameover, true);
  assert.equal(output.nation, "Six over runs");
  assert.deepEqual(Object.keys(emptyEventPayload()), [
    "Odds",
    "Bookmaker",
    "LineMarket",
    "Fancy2",
    "Meter",
    "Khado",
    "OddEven",
    "OtherMarket",
    "Fancy3",
    "CricketCasino",
    "BallByBall",
  ]);
});

test("ball-by-ball socket ticks retain the numbered discovery name", () => {
  const output = fancyPayload(
    { mid: "4.1-BB", na: "Ball Run SB", r: [{ na: "Ball Run SB", b: 1, l: 2 }] },
    { marketname: "29 Ball Run SB", mtype: "ball-by-ball", isactive: 1 },
  );
  assert.equal(output.nation, "29 Ball Run SB");
});

test("legacy F3 Redis rows migrate from OtherMarket to Fancy3", () => {
  const other = { mid: "4.2-OM", nation: "Genuine Other Market" };
  const payload = normalizeEventPayload({
    OtherMarket: [{ mid: "4.1-F3", nation: "Caught Out" }, other],
  });
  assert.equal(payload.Fancy3[0].mid, "4.1-F3");
  assert.deepEqual(payload.OtherMarket, [other]);
});

test("legacy line markets migrate from Odds to their separate group", () => {
  const market = { marketId: "1.123", Name: "1st Innings 20 Overs Line" };
  const payload = normalizeEventPayload({ Odds: [market] });
  assert.equal(payload.Odds.length, 0);
  assert.deepEqual(payload.LineMarket, [market]);
});

test("Tied Match is always the final market in the Odds section", () => {
  const tied = { marketId: "1.2", Name: "Tied Match" };
  const matchOdds = { marketId: "1.1", Name: "Match Odds" };
  const completed = { marketId: "1.3", Name: "Completed Match" };
  const payload = moveTiedMatchLast({ Odds: [tied, matchOdds, completed] });
  assert.deepEqual(payload.Odds, [matchOdds, completed, tied]);
  assert.deepEqual(normalizeEventPayload({ Odds: [tied, matchOdds] }).Odds, [matchOdds, tied]);
});

test("legacy KD rows migrate from Fancy2 to Khado", () => {
  const row = { mid: "4.1-KD", nation: "Innings Khado" };
  const payload = normalizeEventPayload({ Fancy2: [row] });
  assert.equal(payload.Fancy2.length, 0);
  assert.deepEqual(payload.Khado, [row]);
});

test("legacy MT rows migrate from Fancy2 to Meter", () => {
  const row = { mid: "4.1-MT", nation: "Wicket Meter" };
  const payload = normalizeEventPayload({ Fancy2: [row] });
  assert.equal(payload.Fancy2.length, 0);
  assert.deepEqual(payload.Meter, [row]);
});

test("invalid sentinel market IDs are rejected and removed from snapshots", () => {
  assert.equal(validMarketIdentifier("undefined"), false);
  assert.equal(validMarketIdentifier("1.123"), true);
  const payload = normalizeEventPayload({ Odds: [{ marketId: "undefined", Name: "Market undefined" }] });
  assert.equal(payload.Odds.length, 0);
});

test("frontend snapshots hide waiting markets but retain live and suspended markets", () => {
  const payload = frontendEventPayload({
    Odds: [
      { marketId: "1.waiting", status: "WAITING" },
      { marketId: "1.open", status: "OPEN" },
      { marketId: "1.suspended", status: "SUSPENDED" },
    ],
    Fancy2: [
      { mid: "4.waiting-F2", gstatus: "waiting" },
      { mid: "4.open-F2", gstatus: "ACTIVE" },
    ],
  });

  assert.deepEqual(payload.Odds.map((market) => market.marketId), ["1.open", "1.suspended"]);
  assert.deepEqual(payload.Fancy2.map((market) => market.mid), ["4.open-F2"]);
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
  assert.equal(output.difference, 6);
  assert.equal(output.d, 6);
  assert.equal(output.di, 6);
  assert.equal(output.isActive, false);
  assert.equal(output.isShow, true);
  assert.equal(payloadGroup({ mid: "4.1-CC" }, {}), "CricketCasino");
});

test("Khado maps vendor d to the visible badge and bet payload fields", () => {
  const eight = fancyPayload(
    { mid: "4.1-KD", na: "6 Over Run Khado SKNP Adv", b: 45, br: 100, d: 8, s: true },
    { marketname: "6 Over Run Khado SKNP Adv", markettype: "khado", isactive: 1 },
  );
  const twentyNine = fancyPayload(
    { mid: "4.2-KD", na: "20 Over Run Khado SKNP Adv", b: 155, br: 100, d: 29, s: true },
    { marketname: "20 Over Run Khado SKNP Adv", markettype: "khado", isactive: 1 },
  );
  assert.deepEqual(
    [eight.srno, eight.difference, eight.d, eight.di, twentyNine.srno, twentyNine.difference],
    ["8", 8, 8, 8, "29", 29],
  );
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

test("toss bookmaker markets are removed only after every runner is suspended", () => {
  const market = { marketname: "TOSS" };
  assert.equal(
    isFullySuspendedToss("Bookmaker", { r: [{ sb: "S" }, { status: "SUSPENDED" }] }, market),
    true,
  );
  assert.equal(
    shouldRemoveFromPayload("Bookmaker", { go: false, r: [{ sb: "S" }, { sb: "S" }] }, market),
    true,
  );
  assert.equal(
    shouldRemoveFromPayload("Bookmaker", { go: false, r: [{ sb: "S" }, { sb: "" }] }, market),
    false,
  );
  assert.equal(
    shouldRemoveFromPayload(
      "Bookmaker",
      { go: false, r: [{ sb: "S" }, { sb: "S" }] },
      { marketname: "BOOKMAKER" },
    ),
    false,
  );
  assert.equal(
    shouldRemoveFromPayload(
      "Bookmaker",
      { go: false, r: [{ sb: "" }, { sb: "" }] },
      { marketname: "TOSS", isActive: false },
    ),
    true,
  );
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
  assert.equal(fancyResultValue("1.261062382", "15316", "line-market"), 15316);
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

test("Match Odds and Bookmaker game-over ticks are event-terminal signals", () => {
  assert.equal(isEventTerminalMarketName("Match Odds"), true);
  assert.equal(isEventTerminalMarketName("Match Bookmaker"), true);
  assert.equal(isEventTerminalMarketName("BOOKMAKER"), true);
  assert.equal(isEventTerminalMarketName("Over/Under 2.5 Goals"), false);
  assert.equal(isEventTerminalMarketName("15 Over Run LF"), false);
});

test("socket game-over performs durable cleanup without dropping result candidates", () => {
  const resultSource = fs.readFileSync(path.join(__dirname, "../src/cron/resultSync.js"), "utf8");
  const discoverySource = fs.readFileSync(path.join(__dirname, "../src/cron/marketDiscoverySync.js"), "utf8");
  const serverSource = fs.readFileSync(path.join(__dirname, "../src/server.js"), "utf8");
  assert.match(serverSource, /setResultHandler\(handleSocketGameOver\)/);
  assert.match(resultSource, /UPDATE t_market SET isactive=\?,status=\?,issubscribed=\?/);
  assert.match(resultSource, /UPDATE t_matchfancy SET isactive=\?,status=\?,isshow=\?,is_show=\?,issubscribed=\?/);
  assert.match(resultSource, /redis\.removeMarkets/);
  assert.match(resultSource, /redis\.removeEvent/);
  assert.match(resultSource, /redis\.removeEventsFromMetadata/);
  assert.match(resultSource, /publishEventRemoved\(eventId, "primary-market-game-over"\)/);
  assert.match(resultSource, /m\.updatedon >= DATE_SUB\(NOW\(\), INTERVAL 48 HOUR\)/);
  assert.match(resultSource, /f\.updatedon >= DATE_SUB\(NOW\(\), INTERVAL 48 HOUR\)/);
  assert.match(resultSource, /UPPER\(f\.status\) IN \('SUSPENDED','CLOSED'\)/);
  assert.match(discoverySource, /isactive=IF\(status=0,0,VALUES\(isactive\)\)/);
  assert.match(discoverySource, /status=IF\(status='CLOSED','CLOSED',VALUES\(status\)\)/);
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
