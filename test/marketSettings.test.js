const test = require("node:test");
const assert = require("node:assert/strict");
const provider = require("../src/services/providerApi");
const redisStore = require("../src/config/redis");
const marketSettings = require("../src/services/marketSettingsService");
const { providerLimits } = require("../src/utils/marketLimits");

test.beforeEach(() => marketSettings.__testing__.reset());

test("initial settings are fetched once per market and applied", async (t) => {
  const calls = [];
  t.mock.method(provider, "marketSettings", async (mids) => {
    calls.push(mids);
    return mids.map((mid) => ({ eid: 77, mid, settings: { ms: "10", mas: 5000 } }));
  });
  const applied = [];
  const apply = async (item) => applied.push(item);

  const first = await marketSettings.loadInitialSettings(["1.1", "1.2", "1.1"], apply);
  assert.deepEqual(first, { requested: 2, applied: 2, failed: 0 });
  assert.deepEqual(calls, [["1.1", "1.2"]]);
  assert.deepEqual(
    applied.map((item) => item.mid),
    ["1.1", "1.2"],
  );

  const second = await marketSettings.loadInitialSettings(["1.1", "1.2"], apply);
  assert.equal(second.requested, 0);
  assert.equal(calls.length, 1);

  await marketSettings.loadInitialSettings(["1.1", "1.3"], apply);
  assert.deepEqual(calls.at(-1), ["1.3"]);
});

test("failed fetches are retried on a later subscription", async (t) => {
  let fail = true;
  const fetch = t.mock.method(provider, "marketSettings", async (mids) => {
    if (fail) throw new Error("provider down");
    return { data: mids.map((mid) => ({ eid: 5, mid, settings: { ms: 1, mas: 2 } })) };
  });
  const applied = [];
  const apply = async (item) => applied.push(item.mid);

  assert.deepEqual(await marketSettings.loadInitialSettings(["1.9"], apply), {
    requested: 1,
    applied: 0,
    failed: 1,
  });
  fail = false;
  assert.deepEqual(await marketSettings.loadInitialSettings(["1.9"], apply), {
    requested: 1,
    applied: 1,
    failed: 0,
  });
  assert.equal(fetch.mock.callCount(), 2);
  assert.deepEqual(applied, ["1.9"]);
});

test("a market room update is not overwritten by the older initial snapshot", async (t) => {
  t.mock.method(provider, "marketSettings", async (mids) =>
    mids.map((mid) => ({ eid: 1, mid, settings: { ms: 1, mas: 2 } })),
  );
  const applied = [];
  marketSettings.noteRoomUpdate("2.1");
  await marketSettings.loadInitialSettings(["2.1", "2.2"], async (item) => applied.push(item.mid));
  assert.deepEqual(applied, ["2.2"]);
});

test("missing event ids are resolved from the market table and unusable items are skipped", async (t) => {
  t.mock.method(provider, "marketSettings", async () => [
    { mid: "3.1", ms: 5, mas: 50 },
    { mid: "3.2" },
    { mid: "3.3", settings: { ms: 5 } },
  ]);
  t.mock.method(redisStore, "findMarkets", async () => new Map([["3.1", { eventid: 42 }]]));
  const applied = [];
  await marketSettings.loadInitialSettings(["3.1", "3.2", "3.3"], async (item) => applied.push(item));
  assert.deepEqual(applied, [{ eid: 42, mid: "3.1", settings: { mid: "3.1", ms: 5, mas: 50 } }]);
});

test("provider client posts mids to the settings endpoint", async (t) => {
  const seen = [];
  t.mock.method(global, "fetch", async (url, init) => {
    seen.push({ url: String(url), method: init.method, body: JSON.parse(init.body) });
    return new Response("[]", { status: 200 });
  });
  await provider.marketSettings(["1.1", "1.2"]);
  assert.equal(new URL(seen[0].url).pathname, "/v1/markets/settings");
  assert.equal(seen[0].method, "POST");
  assert.deepEqual(seen[0].body, { mids: ["1.1", "1.2"] });
});

test("the real provider response shape (flat items under data) is applied with its limits", async (t) => {
  t.mock.method(provider, "marketSettings", async () => ({
    data: [
      {
        mid: "4.206215583029-F2",
        ms: 50,
        mas: 25000,
        mol: 10,
        mpl: 300000,
        bd: 1,
        me: 500000000,
        m: 0,
        ic: false,
        ty: "session",
        opl: 0,
        eid: 36082557,
        si: 4,
      },
    ],
  }));
  const applied = [];
  await marketSettings.loadInitialSettings(["4.206215583029-F2"], async (item) => applied.push(item));
  assert.equal(applied.length, 1);
  assert.equal(String(applied[0].eid), "36082557");
  assert.equal(applied[0].mid, "4.206215583029-F2");
  assert.deepEqual(providerLimits(applied[0].settings), { providerMinBet: 50, providerMaxBet: 25000 });
});
