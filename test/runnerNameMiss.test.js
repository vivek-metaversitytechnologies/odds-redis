const test = require("node:test");
const assert = require("node:assert/strict");
const sourceDb = require("../src/config/sourceDb");

let queries = 0;
let storedNames = [];
sourceDb.getSourcePool = () => ({
  query: async () => {
    queries += 1;
    return [storedNames];
  },
});
const redisStore = require("../src/config/redis");
const provider = require("../src/services/providerApi");

function setup(t) {
  redisStore.__testing__.reset();
  queries = 0;
  storedNames = [];
  t.after(() => {
    redisStore.__testing__.reset();
  });
}

test("a market without runner names is not looked up again on every tick", async (t) => {
  setup(t);
  const runners = t.mock.method(provider, "runners", async () => ({ data: [] }));
  for (let tick = 0; tick < 5; tick += 1) await redisStore.primeRunnerNames(["1.264600001"]);
  assert.equal(queries, 1);
  assert.equal(runners.mock.callCount(), 1);
  // Tagged, so the provider request metrics show where these come from.
  assert.equal(runners.mock.calls[0].arguments[1].source, "runner-names");
});

test("a miss is retried once its minute passes, and a provider error counts as a miss", async (t) => {
  setup(t);
  let now = Date.parse("2026-10-31T06:00:00Z");
  t.mock.method(Date, "now", () => now);
  let fail = true;
  const runners = t.mock.method(provider, "runners", async () => {
    if (fail) throw new Error("provider down");
    return { data: [{ runnerId: 7, name: "Team A" }] };
  });
  await redisStore.primeRunnerNames(["1.2"]);
  fail = false;
  await redisStore.primeRunnerNames(["1.2"]);
  assert.equal(runners.mock.callCount(), 1, "the error is remembered as a miss");

  now += 60001;
  await redisStore.primeRunnerNames(["1.2"]);
  await redisStore.primeRunnerNames(["1.2"]);
  // Found names are cached for good.
  assert.equal(runners.mock.callCount(), 2);
});
