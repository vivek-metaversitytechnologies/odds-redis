const test = require("node:test");
const assert = require("node:assert/strict");
const db = require("../src/config/sourceDb");
const pending = require("../src/services/pendingResultQueue");
const subscriptions = require("../src/services/marketSubscriptionService");
const redis = require("../src/config/redis");
const frontend = require("../src/services/frontendSocketService");
const fancyNames = require("../src/services/fancyNameService");

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
const LINE = { ...BALL, id: 502, marketid: "1.262893097", marketname: "1st Innings 20 Overs Line", oddstype: "LINE", mtype: "line-market" };

// resultSync takes getSourcePool at load time, so the fake is installed before it is required.
const state = { inserts: [] };
const connection = {
  beginTransaction: async () => {},
  commit: async () => {},
  rollback: async () => {},
  release: () => {},
  query: async (sql, params) => {
    if (sql.includes("FROM t_matchfancy")) return [[BALL, LINE].filter((row) => params.includes(row.marketid)).map((row) => ({ ...row }))];
    if (sql.startsWith("SELECT")) return [[]];
    return [{ affectedRows: 1 }];
  },
  execute: async (sql, params) => {
    if (sql.includes("INSERT INTO t_fancyresult")) state.inserts.push({ marketId: params[1], value: params[8] });
    return [{ affectedRows: 1 }];
  },
};
db.getSourcePool = () => ({ getConnection: async () => connection, query: connection.query });
const resultSync = require("../src/cron/resultSync");

function stubEffects(t) {
  state.inserts = [];
  t.mock.method(pending, "enqueue", async () => {});
  t.mock.method(pending, "remove", async () => {});
  t.mock.method(subscriptions, "unsubscribeResultMarkets", async () => {});
  t.mock.method(redis, "removeMarkets", async (_eventId, ids) => new Set(ids));
  t.mock.method(frontend, "publishEventSnapshot", async () => {});
  t.mock.method(fancyNames, "resolveFancyName", async (_id, name) => name);
  t.mock.method(fancyNames, "repairFancyNames", async () => {});
  t.after(() => delete process.env.BALL_BY_BALL_SOCKET_SETTLEMENT);
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
  assert.deepEqual(outcome.ballByBall, { mode: "on", results: 1, settled: 1 });
});

test("shadow mode records the socket result but leaves settlement to the API poller", async (t) => {
  stubEffects(t);
  process.env.BALL_BY_BALL_SOCKET_SETTLEMENT = "shadow";
  const outcome = await resultSync.handleSocketGameOver([gameOver(BALL, "117")]);

  assert.deepEqual(state.inserts, []);
  assert.deepEqual(outcome.ballByBall, { mode: "shadow", results: 1, settled: 0 });
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
