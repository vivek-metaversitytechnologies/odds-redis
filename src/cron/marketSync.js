const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const Market = require("../models/Market");
const subscriptions = require("../services/marketSubscriptionService");
const websocket = require("../services/websocketService");
const logger = require("../utils/logger");

const activeMarketIds = new Set();
let running = false;
const syncState = { running: false, lastStartedAt: null, lastCompletedAt: null,
  lastError: null, lastResult: null, recent: [] };

function record(type, details = {}) {
  syncState.recent.unshift({ at: new Date().toISOString(), type, ...details });
  syncState.recent = syncState.recent.slice(0, 50);
}

async function fetchActiveMarkets() {
  const sportId = Number(process.env.MARKET_SPORT_ID || 4);
  const [rows] = await getSourcePool().query(
    "SELECT * FROM t_market WHERE isactive = ? AND sportid = ? ORDER BY sportid ASC, id DESC",
    [true, sportId],
  );
  return rows.map(Market.fromRow);
}

async function syncMarketSubscriptions() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true; syncState.running = true; syncState.lastStartedAt = new Date().toISOString();
  syncState.lastError = null; record("sync.started");
  try {
    const markets = await fetchActiveMarkets();
    record("source.loaded", { markets: markets.length });
    const discovered = [...new Set(markets.map((market) => market.marketid).filter(Boolean).map(String))];
    const socketSubscriptions = new Set(websocket.getSubscribedMarketIds());
    const pending = discovered.filter((id) => !socketSubscriptions.has(id)
      && !subscriptions.isMarketCompleted(id));
    const batchSize = Math.max(1, Number(process.env.MARKET_SUBSCRIPTION_BATCH_SIZE || 10));
    const accepted = []; const skipped = [];
    for (let index = 0; index < pending.length; index += batchSize) {
      const batch = pending.slice(index, index + batchSize);
      record("batch.started", { batch: Math.floor(index / batchSize) + 1, size: batch.length });
      const result = await subscriptions.subscribeMarkets(batch);
      const subscribedIds = Array.isArray(result.subscribed) ? result.subscribed : [];
      const skippedIds = Array.isArray(result.skipped) ? result.skipped : [];
      subscribedIds.forEach((id) => activeMarketIds.add(id));
      accepted.push(...subscribedIds); skipped.push(...skippedIds);
      record("batch.completed", { batch: Math.floor(index / batchSize) + 1,
        requested: batch.length, subscribed: subscribedIds.length, skipped: skippedIds.length });
    }
    const currentActiveMarketIds = websocket.getSubscribedMarketIds();
    const result = { skipped: false, total: markets.length, requested: pending.length,
      newlySubscribed: accepted.length, providerSkipped: skipped.length,
      activeMarketIds: currentActiveMarketIds, skippedMarketIds: [...new Set(skipped)] };
    syncState.lastResult = result; syncState.lastCompletedAt = new Date().toISOString();
    record("sync.completed", { total: result.total, subscribed: result.newlySubscribed,
      providerSkipped: result.providerSkipped });
    return result;
  } catch (error) {
    syncState.lastError = error.message; syncState.lastCompletedAt = new Date().toISOString();
    record("sync.failed", { error: error.message });
    throw error;
  } finally {
    running = false; syncState.running = false;
  }
}

function getMarketSyncStatus() {
  return { ...syncState, activeMarketCount: websocket.getSubscribedMarketIds().length,
    skippedRetry: subscriptions.getSkippedRetryStatus(),
    recent: syncState.recent.map((entry) => ({ ...entry })) };
}

function recordMarketSyncActivity(type, details) {
  record(type, details);
}

function startMarketSync() {
  const expression = process.env.MARKET_SYNC_CRON || "*/5 * * * * *";
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

module.exports = { fetchActiveMarkets, syncMarketSubscriptions, startMarketSync, getMarketSyncStatus,
  recordMarketSyncActivity };
