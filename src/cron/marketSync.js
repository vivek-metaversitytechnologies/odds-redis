const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const Market = require("../models/Market");
const subscriptions = require("../services/marketSubscriptionService");
const websocket = require("../services/websocketService");
const logger = require("../utils/logger");
const redisStore = require("../config/redis");
const cronConfig = require("../config/cron");
const { integer, csvIntegers } = require("../config/env");
const { subscriptionEventWindowSql } = require("../utils/eventWindow");

const activeMarketIds = new Set();
const noTickRecovery = new Map();
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

function subscriptionDiff(desiredIds, subscribedIds, isSuppressed = () => false) {
  const normalize = (ids) =>
    (ids || []).map(String).map((id) => id.trim()).filter(redisStore.validMarketIdentifier);
  const desired = new Set(normalize(desiredIds));
  const subscribed = new Set(normalize(subscribedIds));
  return {
    pending: [...desired].filter((id) => !subscribed.has(id) && !isSuppressed(id)),
    stale: [...subscribed].filter((id) => !desired.has(id)),
  };
}

function noTickRecoveryCandidates(markets, subscribedIds, now = Date.now()) {
  const subscribed = new Set((subscribedIds || []).map(String));
  const graceMs = integer("MARKET_FIRST_TICK_GRACE_MS", 60000, { min: 10000 });
  const cooldownMs = integer("MARKET_RECOVERY_COOLDOWN_MS", 300000, { min: 60000 });
  const maxAttempts = integer("MARKET_RECOVERY_MAX_ATTEMPTS", 3, { min: 1, max: 10 });
  return (markets || []).filter((market) => {
    const id = String(market.marketid || "");
    if (!subscribed.has(id) || redisStore.getTickActivity(id)) return false;
    const subscribedAt = websocket.getMarketSubscribedAt(id);
    if (!subscribedAt || now - subscribedAt < graceMs) return false;
    const startsAt = new Date(market.opendate).getTime();
    if (Number(market.inplay) !== 1 && Number.isFinite(startsAt) && startsAt > now) return false;
    const recovery = noTickRecovery.get(id);
    return (!recovery || now - recovery.lastAttemptAt >= cooldownMs) && (recovery?.attempts || 0) < maxAttempts;
  });
}

async function fetchActiveMarkets(lane = "active") {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  const cricketSportId = integer("CRICKET_SPORT_ID", 4, { min: 1 });
  const [rows] = await getSourcePool().query(
    `SELECT m.* FROM t_market m LEFT JOIN t_event e ON e.eventid=m.eventid
     WHERE m.isactive = ? AND m.sportid IN (${sportIds.map(() => "?").join(",")})
       AND ${subscriptionEventWindowSql("m.sportid", "e", lane)}
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ORDER BY CASE WHEN m.sportid=${cricketSportId} THEN 0 ELSE 1 END, m.sportid ASC, m.id DESC`,
    [true, ...sportIds],
  );
  const [fancyRows] = await getSourcePool().query(
    `SELECT f.*, f.fancyid AS marketid, f.name AS marketname,
       COALESCE(f.sportid,e.sportid) AS sportid, e.eventname AS matchname,
       e.open_date AS opendate, e.in_play AS inplay
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE f.isactive=? AND COALESCE(f.sportid,e.sportid) IN (${sportIds.map(() => "?").join(",")})
       AND ${subscriptionEventWindowSql("COALESCE(f.sportid,e.sportid)", "e", lane)}
       AND COALESCE(UPPER(f.status),'') NOT IN ('SUSPENDED','CLOSED')
       AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
     ORDER BY CASE WHEN COALESCE(f.sportid,e.sportid)=${cricketSportId} THEN 0 ELSE 1 END,
       sportid ASC, f.id DESC`,
    [true, ...sportIds],
  );
  return [...rows, ...fancyRows].map(Market.fromRow);
}

async function syncMarketSubscriptions(lane = "active") {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  syncState.running = true;
  syncState.lastStartedAt = new Date().toISOString();
  syncState.lastError = null;
  record("sync.started");
  try {
    const markets = await fetchActiveMarkets(lane);
    record("source.loaded", { markets: markets.length });
    const discovered = [
      ...new Set(
        markets
          .map((market) => market.marketid)
          .filter(redisStore.validMarketIdentifier)
          .map(String),
      ),
    ];
    const currentSubscriptions = websocket.getSubscribedMarketIds();
    const initialDiff = subscriptionDiff(discovered, currentSubscriptions, subscriptions.isMarketSuppressed);
    // Subscribe missing live markets before doing slow stale cleanup. Provider calls
    // are kept sequential, but new markets no longer wait for an unsubscribe round trip.
    const pending = initialDiff.pending;
    const batchSize = integer("MARKET_SUBSCRIPTION_BATCH_SIZE", 50, { min: 1, max: 100 });
    const batches = [];
    for (let index = 0; index < pending.length; index += batchSize) {
      batches.push(pending.slice(index, index + batchSize));
    }
    batches.forEach((batch, index) => record("batch.started", { batch: index + 1, size: batch.length }));
    const accepted = [];
    const skipped = [];
    // Keep provider registration batches ordered. Concurrent subscribe requests can
    // overwrite one another on providers that replace registration state per request.
    for (let index = 0; index < batches.length; index += 1) {
      const batch = batches[index];
      try {
        const result = await subscriptions.subscribeMarkets(batch);
        const subscribedIds = Array.isArray(result.subscribed) ? result.subscribed : [];
        const skippedIds = Array.isArray(result.skipped) ? result.skipped : [];
        subscribedIds.forEach((id) => activeMarketIds.add(id));
        accepted.push(...subscribedIds);
        skipped.push(...skippedIds);
        record("batch.completed", {
          batch: index + 1,
          requested: batch.length,
          subscribed: subscribedIds.length,
          skipped: skippedIds.length,
        });
      } catch (error) {
        skipped.push(...batch);
        record("batch.failed", { batch: index + 1, size: batch.length, error: error.message });
      }
    }
    let unsubscribed = [];
    if (initialDiff.stale.length) {
      const staleResult = await subscriptions.unsubscribeEventMarkets(initialDiff.stale);
      unsubscribed = staleResult.unsubscribed || [];
      record("stale.unsubscribed", { markets: unsubscribed.length });
    }
    const recoveryLimit = integer("MARKET_RECOVERY_BATCH_SIZE", 20, { min: 1, max: 100 });
    const recoveryIds = noTickRecoveryCandidates(markets, websocket.getSubscribedMarketIds())
      .slice(0, recoveryLimit)
      .map((market) => String(market.marketid));
    let recovered = { requested: 0, subscribed: [], skipped: [] };
    if (recoveryIds.length) {
      const attemptedAt = Date.now();
      for (const id of recoveryIds) {
        const previous = noTickRecovery.get(id);
        noTickRecovery.set(id, { attempts: (previous?.attempts || 0) + 1, lastAttemptAt: attemptedAt });
      }
      try {
        recovered = await subscriptions.refreshMarkets(recoveryIds);
        record("no-tick.recovered", {
          requested: recovered.requested,
          subscribed: recovered.subscribed.length,
          skipped: recovered.skipped.length,
        });
      } catch (error) {
        recovered = { requested: recoveryIds.length, subscribed: [], skipped: recoveryIds, error: error.message };
        record("no-tick.recovery-failed", { requested: recoveryIds.length, error: error.message });
        logger.warn("[MarketSync] no-tick recovery failed", {
          marketIds: recoveryIds,
          error: error.message,
        });
      }
    }
    for (const id of [...noTickRecovery.keys()]) {
      if (redisStore.getTickActivity(id) || !discovered.includes(id)) noTickRecovery.delete(id);
    }
    const currentActiveMarketIds = websocket.getSubscribedMarketIds();
    const result = {
      skipped: false,
      lane,
      total: markets.length,
      desired: discovered.length,
      requested: pending.length,
      stale: initialDiff.stale.length,
      unsubscribed: unsubscribed.length,
      newlySubscribed: accepted.length,
      providerSkipped: skipped.length,
      activeMarketIds: currentActiveMarketIds,
      skippedMarketIds: [...new Set(skipped)],
      noTickRecovery: {
        requested: recovered.requested,
        subscribed: recovered.subscribed.length,
        skipped: recovered.skipped.length,
        tracked: noTickRecovery.size,
        error: recovered.error || null,
      },
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
  subscriptionDiff,
  noTickRecoveryCandidates,
  syncMarketSubscriptions,
  startMarketSync,
  getMarketSyncStatus,
  recordMarketSyncActivity,
};
