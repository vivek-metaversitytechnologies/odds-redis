const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const pending = require("../src/services/pendingResultQueue");
const subscriptions = require("../src/services/marketSubscriptionService");
const redis = require("../src/config/redis");
const frontend = require("../src/services/frontendSocketService");
const fancyNames = require("../src/services/fancyNameService");
const marketTrace = require("../src/utils/marketTrace");

process.env.MARKET_TRACE_ENABLED = "false";

const BALL = {
  id: 501,
  marketid: "4.256883965552-BB",
  eventid: 36111199,
  marketname: "19.2 Ball Run SA",
  oddstype: "BB",
  mtype: "ball-by-ball",
  matchname: "South Africa v Australia",
  sportid: 4,
};
const SESSION = { ...BALL, id: 503, marketid: "4.150272715748-F2", marketname: "10 Over Run ENG", oddstype: "F2", mtype: "session" };
const F3 = { ...BALL, id: 504, marketid: "4.764368529190-F3", marketname: "1st Wkt Caught Out ZIM-W", oddstype: "F3", mtype: "other-market" };
const REGULAR = { id: 900, marketid: "1.262893000", eventid: 36111199, marketname: "Match Odds" };
const LINE = { ...BALL, id: 502, marketid: "1.262893097", marketname: "1st Innings 20 Overs Line", oddstype: "LINE", mtype: "line-market" };

// resultSync takes getSourcePool at load time, so the fake is installed before it is required.
const state = { inserts: [], regularWrites: 0 };
const connection = {
  beginTransaction: async () => {},
  commit: async () => {},
  rollback: async () => {},
  release: () => {},
  query: async (sql, params) => {
    if (sql.includes("FROM t_matchfancy")) return [[BALL, LINE, SESSION, F3].filter((row) => params.includes(row.marketid)).map((row) => ({ ...row }))];
    if (sql.includes("FROM t_market WHERE marketid IN")) return [[REGULAR].filter((row) => params.includes(row.marketid)).map((row) => ({ ...row }))];
    if (sql.startsWith("SELECT")) return [[]];
    return [{ affectedRows: 1 }];
  },
  execute: async (sql, params) => {
    if (sql.includes("INSERT INTO t_fancyresult")) state.inserts.push({ marketId: params[1], value: params[8] });
    if (sql.includes("t_matchresult")) state.regularWrites += 1;
    return [{ affectedRows: 1 }];
  },
};
db.getSourcePool = () => ({ getConnection: async () => connection, query: connection.query });
const resultSync = require("../src/cron/resultSync");

function stubEffects(t) {
  state.inserts = [];
  state.regularWrites = 0;
  t.mock.method(pending, "enqueue", async () => {});
  t.mock.method(pending, "remove", async () => {});
  t.mock.method(subscriptions, "unsubscribeResultMarkets", async () => {});
  t.mock.method(redis, "removeMarkets", async (_eventId, ids) => new Set(ids));
  t.mock.method(frontend, "publishEventSnapshot", async () => {});
  t.mock.method(fancyNames, "resolveFancyName", async (_id, name) => name);
  t.mock.method(fancyNames, "repairFancyNames", async () => {});
  t.after(() => {
    delete process.env.BALL_BY_BALL_SOCKET_SETTLEMENT;
    delete process.env.FANCY_SOCKET_SETTLEMENT;
    delete process.env.REGULAR_SOCKET_SETTLEMENT;
  });
}

// The provider results API must not be needed for a ball whose game-over tick carries `res`.
function forbidResultsApi(t) {
  const provider = require("../src/services/providerApi");
  return t.mock.method(provider, "results", async () => ({ data: [] }));
}

const gameOver = (market, res) => ({ eid: market.eventid, mid: market.marketid, s: false, go: true, res });

test("a ball-by-ball game-over tick with a result settles immediately from the socket", async (t) => {
  stubEffects(t);
  forbidResultsApi(t);
  const outcome = await resultSync.handleSocketGameOver([gameOver(BALL, "117")]);

  assert.deepEqual(state.inserts, [{ marketId: BALL.marketid, value: 117 }]);
  assert.deepEqual(outcome.fancies, { mode: "on", results: 1, settled: 1 });
});

test("shadow mode records the socket result but leaves settlement to the API poller", async (t) => {
  stubEffects(t);
  process.env.BALL_BY_BALL_SOCKET_SETTLEMENT = "shadow";
  const outcome = await resultSync.handleSocketGameOver([gameOver(BALL, "117")]);

  assert.deepEqual(state.inserts, []);
  assert.deepEqual(outcome.fancies, { mode: "shadow", results: 1, settled: 0 });
});

test("off mode and result-less game-over ticks never settle a ball", async (t) => {
  stubEffects(t);
  process.env.BALL_BY_BALL_SOCKET_SETTLEMENT = "off";
  await resultSync.handleSocketGameOver([gameOver(BALL, "117")]);
  delete process.env.BALL_BY_BALL_SOCKET_SETTLEMENT;
  await resultSync.handleSocketGameOver([gameOver(BALL, "")]);
  await resultSync.handleSocketGameOver([gameOver(BALL, "not-a-number")]);

  assert.deepEqual(state.inserts, []);
});

test("ball-by-ball socket rows are separate from line rows and keep an abandoned result", () => {
  const items = [gameOver(BALL, "Abandoned"), gameOver(LINE, "118")];
  assert.deepEqual(
    resultSync.socketResultRows(items, [BALL, LINE], "ball-by-ball").map((row) => [row.marketId, row.isAbandoned, row.source]),
    [[BALL.marketid, true, "socket"]],
  );
  assert.deepEqual(resultSync.socketLineResultRows(items, [BALL, LINE]).map((row) => row.marketId), [LINE.marketid]);
});

test("every market family's game-over is recorded with the result it carried", async (t) => {
  stubEffects(t);
  const census = [];
  t.mock.method(marketTrace, "traceGameOver", (fields) => census.push(fields));
  await resultSync.handleSocketGameOver([gameOver(SESSION, "50"), gameOver(BALL, "117")]);

  assert.deepEqual(
    census.map((row) => [row.kind, row.marketId, row.res]).sort(),
    [
      ["BB", BALL.marketid, "117"],
      ["F2", SESSION.marketid, "50"],
    ],
  );
});

test("session and back/lay fancies settle from the socket; line markets keep their own path", async (t) => {
  stubEffects(t);
  forbidResultsApi(t);
  const outcome = await resultSync.handleSocketGameOver([gameOver(SESSION, "50"), gameOver(F3, "lay"), gameOver(BALL, "117")]);

  assert.deepEqual(
    state.inserts.sort((a, b) => a.marketId.localeCompare(b.marketId)),
    [
      { marketId: SESSION.marketid, value: 50 },
      { marketId: BALL.marketid, value: 117 },
      { marketId: F3.marketid, value: 0 },
    ],
  );
  assert.deepEqual(outcome.fancies, { mode: "on", results: 3, settled: 3 });
  const fancyRows = resultSync.socketResultRows([gameOver(LINE, "118"), gameOver(SESSION, "50")], [LINE, SESSION], (fancy) => fancy.mtype !== "line-market");
  assert.deepEqual(fancyRows.map((row) => row.marketId), [SESSION.marketid]);
});

test("regular markets are only watched: the socket value is recorded, nothing is written", async (t) => {
  stubEffects(t);
  const census = [];
  t.mock.method(marketTrace, "traceGameOver", (fields) => census.push(fields));
  await resultSync.handleSocketGameOver([gameOver(REGULAR, "19924829"), gameOver(SESSION, "50")]);

  assert.equal(state.regularWrites, 0);
  const regular = census.find((row) => row.marketId === REGULAR.marketid);
  assert.deepEqual([regular.kind, regular.res, regular.socketSettlement], ["REGULAR", "19924829", "shadow"]);
  assert.equal(census.find((row) => row.marketId === SESSION.marketid).socketSettlement, "on");
});

test("the older ball-by-ball switch still controls fancy socket settlement", async (t) => {
  stubEffects(t);
  process.env.BALL_BY_BALL_SOCKET_SETTLEMENT = "off";
  await resultSync.handleSocketGameOver([gameOver(SESSION, "50")]);
  assert.deepEqual(state.inserts, []);
  process.env.FANCY_SOCKET_SETTLEMENT = "on";
  await resultSync.handleSocketGameOver([gameOver(SESSION, "50")]);
  assert.deepEqual(state.inserts, [{ marketId: SESSION.marketid, value: 50 }]);
});
