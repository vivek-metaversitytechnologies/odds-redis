const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const Market = require("../models/Market");
const subscriptions = require("../services/marketSubscriptionService");
const websocket = require("../services/websocketService");
const logger = require("../utils/logger");
const cronConfig = require("../config/cron");
const { integer, csvIntegers } = require("../config/env");

const activeMarketIds = new Set();
let running = false;
const syncState = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
  recent: [],
};

function record(type, details = {}) {
  syncState.recent.unshift({ at: new Date().toISOString(), type, ...details });
  syncState.recent = syncState.recent.slice(0, 50);
}

async function fetchActiveMarkets() {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  const [rows] = await getSourcePool().query(
    `SELECT m.* FROM t_market m WHERE m.isactive = ? AND m.sportid IN (${sportIds.map(() => "?").join(",")})
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ORDER BY sportid ASC, id DESC`,
    [true, ...sportIds],
  );
  const [fancyRows] = await getSourcePool().query(
    `SELECT f.*, f.fancyid AS marketid, f.name AS marketname,
       COALESCE(f.sportid,e.sportid) AS sportid, e.eventname AS matchname,
       e.open_date AS opendate, e.in_play AS inplay
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE f.isactive=? AND COALESCE(f.sportid,e.sportid) IN (${sportIds.map(() => "?").join(",")})
       AND COALESCE(UPPER(f.status),'') NOT IN ('SUSPENDED','CLOSED')
       AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
     ORDER BY sportid ASC, f.id DESC`,
    [true, ...sportIds],
  );
  return [...rows, ...fancyRows].map(Market.fromRow);
}

async function syncMarketSubscriptions() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  syncState.running = true;
  syncState.lastStartedAt = new Date().toISOString();
  syncState.lastError = null;
  record("sync.started");
  try {
    const markets = await fetchActiveMarkets();
    record("source.loaded", { markets: markets.length });
    const discovered = [
      ...new Set(
        markets
          .map((market) => market.marketid)
          .filter(Boolean)
          .map(String),
      ),
    ];
    const socketSubscriptions = new Set(websocket.getSubscribedMarketIds());
    const pending = discovered.filter(
      (id) => !socketSubscriptions.has(id) && !subscriptions.isMarketSuppressed(id),
    );
    const batchSize = integer("MARKET_SUBSCRIPTION_BATCH_SIZE", 10, { min: 1, max: 100 });
    const accepted = [];
    const skipped = [];
    for (let index = 0; index < pending.length; index += batchSize) {
      const batch = pending.slice(index, index + batchSize);
      record("batch.started", { batch: Math.floor(index / batchSize) + 1, size: batch.length });
      const result = await subscriptions.subscribeMarkets(batch);
      const subscribedIds = Array.isArray(result.subscribed) ? result.subscribed : [];
      const skippedIds = Array.isArray(result.skipped) ? result.skipped : [];
      subscribedIds.forEach((id) => activeMarketIds.add(id));
      accepted.push(...subscribedIds);
      skipped.push(...skippedIds);
      record("batch.completed", {
        batch: Math.floor(index / batchSize) + 1,
        requested: batch.length,
        subscribed: subscribedIds.length,
        skipped: skippedIds.length,
      });
    }
    const currentActiveMarketIds = websocket.getSubscribedMarketIds();
    const result = {
      skipped: false,
      total: markets.length,
      requested: pending.length,
      newlySubscribed: accepted.length,
      providerSkipped: skipped.length,
      activeMarketIds: currentActiveMarketIds,
      skippedMarketIds: [...new Set(skipped)],
    };
    syncState.lastResult = result;
    syncState.lastCompletedAt = new Date().toISOString();
    record("sync.completed", {
      total: result.total,
      subscribed: result.newlySubscribed,
      providerSkipped: result.providerSkipped,
    });
    return result;
  } catch (error) {
    syncState.lastError = error.message;
    syncState.lastCompletedAt = new Date().toISOString();
    record("sync.failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    syncState.running = false;
  }
}

function getMarketSyncStatus() {
  return {
    ...syncState,
    activeMarketCount: websocket.getSubscribedMarketIds().length,
    skippedRetry: subscriptions.getSkippedRetryStatus(),
    recent: syncState.recent.map((entry) => ({ ...entry })),
  };
}

function recordMarketSyncActivity(type, details) {
  record(type, details);
}

function startMarketSync() {
  const { expression } = cronConfig.marketSubscription;
  const task = cron.schedule(expression, async () => {
    try {
      const result = await syncMarketSubscriptions();
      logger.info("[MarketSync] completed", { ...result, socket: websocket.getSocketStatus() });
    } catch (error) {
      logger.error("[MarketSync] failed", { error: error.message });
    }
  });
  logger.info("[MarketSync] scheduled", { expression });
  return task;
}

module.exports = {
  fetchActiveMarkets,
  syncMarketSubscriptions,
  startMarketSync,
  getMarketSyncStatus,
  recordMarketSyncActivity,
};
