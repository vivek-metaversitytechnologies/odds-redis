const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { ownedByBallByBallPoller } = require("../src/cron/marketDiscoverySync");

test("the dedicated poller owns ball-by-ball rows only in the active lane", () => {
  assert.equal(ownedByBallByBallPoller({ marketType: "ball-by-ball" }, "active"), true);
  assert.equal(ownedByBallByBallPoller({ marketType: "ball-by-ball" }, "future"), false);
  assert.equal(ownedByBallByBallPoller({ marketType: "session" }, "active"), false);
  assert.equal(ownedByBallByBallPoller({ marketType: "line-market" }, "active"), false);
});

// The broad snapshot's ball-by-ball rows lag the poller; every writer in the broad active pass
// and in live cleanup must leave them to the poller (production: live balls flickered every
// discovery cycle and were unsubscribed by cleanup).
test("broad discovery and live cleanup never persist, reconcile or remove poller-owned rows", () => {
  const source = fs.readFileSync(path.join(__dirname, "../src/cron/marketDiscoverySync.js"), "utf8");
  const discovery = source.match(/async function syncMarketDiscovery[\s\S]*?\n}\n/)[0];
  const cleanup = source.match(/async function syncLiveMarketCleanup[\s\S]*?\n}\n/)[0];

  assert.match(discovery, /primaryStoredFancies = primaryUnique\.filter\(\s*\(market\) => storedInFancyTable\(market\) && !ownedByBallByBallPoller\(market, lane\)/);
  assert.match(discovery, /const fancies = unique\.filter\(\s*\(market\) => FANCY_MARKET_TYPES\.has\(market\.marketType\) && !ownedByBallByBallPoller\(market, lane\)/);
  assert.match(discovery, /upsertFancies\(\s*changedMarkets\.filter\(\(market\) => storedInFancyTable\(market\) && !ownedByBallByBallPoller\(market, lane\)\)/);
  assert.match(cleanup, /!ownedByBallByBallPoller\(market, "active"\)/);
});
