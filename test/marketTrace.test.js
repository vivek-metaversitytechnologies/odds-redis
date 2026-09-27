const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

sourceDb.getSourcePool = () => ({ query: async () => [[]] });
const redisStore = require("../src/config/redis");
const marketTrace = require("../src/utils/marketTrace");

function capture(t) {
  const records = [];
  marketTrace.__testing__.reset();
  marketTrace.__testing__.setLogger({ info: (_message, { record }) => records.push(record) });
  t.after(() => {
    marketTrace.__testing__.setLogger(undefined);
    marketTrace.__testing__.reset();
    delete process.env.MARKET_TRACE_ENABLED;
  });
  return records;
}

function fakeRedis() {
  const store = new Map();
  const sets = new Map();
  return {
    isOpen: true,
    get: async (key) => store.get(key) ?? null,
    set: async (key, value) => store.set(key, value),
    async sAdd(key, values) {
      const members = sets.get(key) || new Set();
      for (const value of values) members.add(String(value));
      sets.set(key, members);
    },
    async sRem(key, values) {
      for (const value of values) sets.get(key)?.delete(String(value));
    },
    sMembers: async (key) => [...(sets.get(key) || [])],
    expire: async () => 1,
    multi() {
      const ops = [];
      const builder = {
        set(key, value) {
          ops.push([key, value]);
          return builder;
        },
        exec: async () => ops.forEach(([key, value]) => store.set(key, value)),
      };
      return builder;
    },
  };
}

test("only ball-by-ball, line and cricket-casino markets are traced, and a line id is remembered once identified", (t) => {
  const records = capture(t);

  marketTrace.trace("definition", { eventId: 1, marketId: "4.1-F2" }, { marketType: "session" });
  marketTrace.trace("definition", { eventId: 1, marketId: "4.1-BB" });
  marketTrace.trace("definition", { eventId: 1, marketId: "1.500" }, { marketType: "line-market" });
  marketTrace.trace("subscription", { eventId: null, marketId: "1.500", outcome: "subscribed" });
  marketTrace.trace("subscription", { eventId: null, marketId: "1.999", outcome: "subscribed" });
  marketTrace.trace("result", { eventId: 1, marketId: "11.900925862394-CC", outcome: "persisted" });

  assert.deepEqual(
    records.map((record) => [record.kind, record.marketId, record.stage]),
    [
      ["BB", "4.1-BB", "definition"],
      ["LINE", "1.500", "definition"],
      ["LINE", "1.500", "subscription"],
      ["CC", "11.900925862394-CC", "result"],
    ],
  );
});

test("state changes are deduplicated and repeated rejections report what was suppressed", (t) => {
  const records = capture(t);
  const fields = { eventId: 1, marketId: "4.1-BB" };

  marketTrace.traceChange("tick", "tick.state", fields, { s: true });
  marketTrace.traceChange("tick", "tick.state", fields, { s: true });
  marketTrace.traceChange("tick", "tick.state", fields, { s: false });
  marketTrace.traceThrottled("tick.rejected", fields, "no-db-row");
  marketTrace.traceThrottled("tick.rejected", fields, "no-db-row");
  marketTrace.traceThrottled("tick.rejected", fields, "no-db-row");

  assert.equal(records.filter((record) => record.stage === "tick.state").length, 2);
  assert.equal(records.filter((record) => record.stage === "tick.rejected").length, 1);

  process.env.MARKET_TRACE_ENABLED = "false";
  marketTrace.trace("definition", fields);
  assert.equal(records.length, 3);
});

test("a ball-by-ball market's tick lifecycle records shown, hidden with reason, and blocked re-opens", async (t) => {
  const records = capture(t);
  const testing = redisStore.__testing__;
  testing.reset();
  testing.setRedisClient(fakeRedis());
  testing.primeMarketCache([
    [
      "4.17-BB",
      {
        marketid: "4.17-BB",
        fancyid: "4.17-BB",
        eventid: 9001,
        marketname: "17.1 Ball Run",
        mtype: "ball-by-ball",
        status: "OPEN",
        isactive: true,
      },
    ],
  ]);
  t.after(() => testing.reset());
  const tick = (extra) => ({ eid: 9001, mid: "4.17-BB", s: true, r: [{ rid: 1, na: "Runs", s: "ACTIVE", b1: 1 }], ...extra });

  await redisStore.writeTicks([tick()]);
  await redisStore.writeTicks([tick({ r: [{ rid: 1, na: "Runs", s: "ACTIVE", b1: 2 }] })]);
  await redisStore.writeTicks([tick({ go: true, res: "4" })]);
  await redisStore.writeTicks([tick()]);
  await redisStore.writeTicks([{ eid: 9001, mid: "4.unknown-BB", s: true, r: [] }]);

  const summary = records.map((record) => [record.stage, record.visibility ?? record.reason ?? record.source ?? null]);
  assert.deepEqual(summary, [
    ["tick.state", null],
    ["visibility", "shown"],
    ["bb.terminal", "tick"],
    ["tick.state", null],
    ["visibility", "hidden"],
    ["tick.state", null],
    ["tick.blocked", "bb-terminal-set"],
    ["tick.rejected", "no-db-row"],
  ]);
  assert.equal(records[4].reason, "go");
  // A price-only change is not a new state.
  assert.equal(records.filter((record) => record.stage === "tick.state").length, 3);
});

test("a line market hidden by an unavailable tick records the set and the reason", async (t) => {
  const records = capture(t);
  const testing = redisStore.__testing__;
  testing.reset();
  testing.setRedisClient(fakeRedis());
  testing.primeMarketCache([
    [
      "1.777",
      { marketid: "1.777", eventid: 9002, marketname: "Runs Line", mtype: "line-market", isactive: true, status: "OPEN" },
    ],
  ]);
  t.after(() => testing.reset());

  await redisStore.writeTicks([{ eid: 9002, mid: "1.777", s: true, r: [{ rid: 1, s: "ACTIVE", b1: 81 }] }]);
  await redisStore.writeTicks([{ eid: 9002, mid: "1.777", s: false, r: [{ rid: 1, s: "ACTIVE", b1: 81 }] }]);

  const lineSets = records.filter((record) => record.stage === "line.set").map((record) => record.set);
  const hidden = records.find((record) => record.stage === "visibility" && record.visibility === "hidden");
  assert.deepEqual(lineSets, ["available", "unavailable"]);
  assert.equal(hidden.kind, "LINE");
  assert.equal(hidden.reason, "s-false");
});

test("the game-over census records every market family, not only BB/LINE/CC", (t) => {
  const records = capture(t);
  marketTrace.traceGameOver({ kind: "F2", eventId: 7, marketId: "4.1-F2", res: "87" });
  marketTrace.traceGameOver({ kind: "REGULAR", eventId: 7, marketId: "1.555", res: null });
  assert.deepEqual(
    records.map((record) => [record.stage, record.kind, record.marketId, record.res]),
    [
      ["socket.gameover", "F2", "4.1-F2", "87"],
      ["socket.gameover", "REGULAR", "1.555", null],
    ],
  );
});

test("session-style fancies are traced only when MARKET_TRACE_KINDS enables them", (t) => {
  const records = capture(t);
  t.after(() => delete process.env.MARKET_TRACE_KINDS);

  marketTrace.trace("definition", { eventId: 1, marketId: "4.1-F2" }, { marketType: "session" });
  assert.equal(records.length, 0);

  process.env.MARKET_TRACE_KINDS = "BB,LINE,CC,F2,KD,OE,F3,MT";
  marketTrace.trace("definition", { eventId: 1, marketId: "4.1-F2" }, { marketType: "session" });
  marketTrace.trace("definition", { eventId: 1, marketId: "4.2-KD" });
  marketTrace.trace("definition", { eventId: 1, marketId: "4.3-OE" });
  marketTrace.trace("definition", { eventId: 1, marketId: "4.4-F3" });
  marketTrace.trace("definition", { eventId: 1, marketId: "4.5-MT" });
  assert.deepEqual(records.map((record) => record.kind), ["F2", "KD", "OE", "F3", "MT"]);

  process.env.MARKET_TRACE_KINDS = "BB";
  marketTrace.trace("definition", { eventId: 1, marketId: "4.6-CC" });
  assert.equal(records.length, 5);
});
