const cron = require("node-cron");
const { getSourcePool } = require("../config/sourceDb");
const Market = require("../models/Market");
const subscriptions = require("../services/marketSubscriptionService");
const websocket = require("../services/websocketService");
const resourceMonitor = require("../services/resourceMonitor");
const logger = require("../utils/logger");
const redisStore = require("../config/redis");
const cronConfig = require("../config/cron");
const { integer, csvIntegers } = require("../config/env");

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
    (ids || [])
      .map(String)
      .map((id) => id.trim())
      .filter(redisStore.validMarketIdentifier);
  const desired = new Set(normalize(desiredIds));
  const subscribed = new Set(normalize(subscribedIds));
  return {
    pending: [...desired].filter((id) => !subscribed.has(id) && !isSuppressed(id)),
    stale: [...subscribed].filter((id) => !desired.has(id)),
  };
}

function chunk(ids, size) {
  const result = [];
  for (let index = 0; index < ids.length; index += size) result.push(ids.slice(index, index + size));
  return result;
}

// Live/in-play markets are always admitted. Not-yet-live future markets are admitted
// in priority order (pending is expected pre-sorted nearest-kickoff-first, see
// fetchActiveMarkets) only while isHealthy() reports resource headroom; the moment it
// doesn't, the remaining future markets are deferred to the next sync cycle instead of
// starving live coverage or spending headroom that live ticks need.
function admissionBatches(pending, isLive, batchSize, isHealthy) {
  const livePending = pending.filter((id) => isLive(id));
  const futurePending = pending.filter((id) => !isLive(id));
  const batches = chunk(livePending, batchSize).map((batch) => ({ batch, tier: "live" }));
  let deferredFuture = futurePending.length;
  for (const batch of chunk(futurePending, batchSize)) {
    if (!isHealthy()) break;
    batches.push({ batch, tier: "future" });
    deferredFuture -= batch.length;
  }
  return { batches, deferredFuture };
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
    return (
      (!recovery || now - recovery.lastAttemptAt >= cooldownMs) && (recovery?.attempts || 0) < maxAttempts
    );
  });
}

async function fetchActiveMarkets() {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  const cricketSportId = integer("CRICKET_SPORT_ID", 4, { min: 1 });
  const maxAgeHours = integer("ACTIVE_MATCH_MAX_AGE_HOURS", 48, { min: 1, max: 720 });
  const futureHours = integer("MARKET_SUBSCRIPTION_FUTURE_HOURS", 24, { min: 1, max: 720 });
  // Cricket markets are eligible regardless of how far in the future the event
  // starts. Other sports remain bounded by the configured future horizon so
  // historical/far-future rows cannot flood the provider control endpoint.
  const [rows] = await getSourcePool().query(
    `SELECT m.*,
       CAST((COALESCE(e.in_play,0)=1 OR COALESCE(m.inplay,0)=1) AS UNSIGNED) AS inplay
     FROM t_market m INNER JOIN t_event e ON e.eventid=m.eventid
     WHERE m.isactive = ? AND m.sportid IN (${sportIds.map(() => "?").join(",")})
       AND e.isactive = ?
       AND (e.open_date IS NULL OR e.open_date >= DATE_SUB(NOW(), INTERVAL ${maxAgeHours} HOUR))
       AND (COALESCE(m.sportid,e.sportid)=${cricketSportId}
         OR COALESCE(e.in_play,0)=1 OR e.open_date IS NULL
         OR e.open_date <= DATE_ADD(NOW(), INTERVAL ${futureHours} HOUR))
       AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=m.marketid)
     ORDER BY CASE WHEN m.sportid=${cricketSportId} THEN 0 ELSE 1 END,
       COALESCE(e.in_play,0) DESC, e.open_date ASC, m.id DESC`,
    [true, ...sportIds, true],
  );
  const [fancyRows] = await getSourcePool().query(
    `SELECT f.*, f.fancyid AS marketid, f.name AS marketname,
       COALESCE(f.sportid,e.sportid) AS sportid, e.eventname AS matchname,
       e.open_date AS opendate,
       CAST((COALESCE(e.in_play,0)=1 OR COALESCE(f.isplay,0)=1) AS UNSIGNED) AS inplay
     FROM t_matchfancy f INNER JOIN t_event e ON e.eventid=f.eventid
     WHERE f.isactive=? AND COALESCE(f.sportid,e.sportid) IN (${sportIds.map(() => "?").join(",")})
       AND e.isactive=?
       AND (e.open_date IS NULL OR e.open_date >= DATE_SUB(NOW(), INTERVAL ${maxAgeHours} HOUR))
       AND (COALESCE(f.sportid,e.sportid)=${cricketSportId}
         OR COALESCE(e.in_play,0)=1 OR e.open_date IS NULL
         OR e.open_date <= DATE_ADD(NOW(), INTERVAL ${futureHours} HOUR))
       AND COALESCE(UPPER(f.status),'') <> 'CLOSED'
       AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
     ORDER BY CASE WHEN COALESCE(f.sportid,e.sportid)=${cricketSportId} THEN 0 ELSE 1 END,
       COALESCE(e.in_play,0) DESC, e.open_date ASC, f.id DESC`,
    [true, ...sportIds, true],
  );
  return [...rows, ...fancyRows].map(Market.fromRow);
}

async function syncMarketSubscriptions(lane = "active") {
  if (running) return { skipped: true, reason: "already-running" };
  if (subscriptions.isSubscriptionRecoveryRunning()) {
    return { skipped: true, reason: "subscription-recovery-running" };
  }
  running = true;
  syncState.running = true;
  syncState.lastStartedAt = new Date().toISOString();
  syncState.lastError = null;
  record("sync.started");
  try {
    const markets = await fetchActiveMarkets();
    record("source.loaded", { markets: markets.length });
    const marketsById = new Map(markets.map((market) => [String(market.marketid), market]));
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
    // `pending` is already priority-ordered by fetchActiveMarkets (cricket first, live
    // events first, then soonest kickoff first). In-play markets always get admitted;
    // not-yet-live future markets are admitted in that same order only while
    // resourceMonitor reports headroom, so a busy moment simply defers the furthest-out
    // events to the next cycle instead of starving live coverage.
    const pending = initialDiff.pending;
    const batchSize = integer("PROVIDER_SUBSCRIPTION_BATCH_SIZE", 5, { min: 1, max: 20 });
    const isLive = (id) => Number(marketsById.get(id)?.inplay) === 1;
    const admission = admissionBatches(pending, isLive, batchSize, resourceMonitor.isHealthyForAdmission);
    const maxBatches = integer("MARKET_SUBSCRIPTION_MAX_BATCHES_PER_RUN", 10, { min: 1, max: 100 });
    const batches = admission.batches.slice(0, maxBatches);
    const deferredForRunLimit = admission.batches
      .slice(maxBatches)
      .reduce((total, item) => total + item.batch.length, 0);
    const accepted = [];
    const skipped = [];
    const maxFailures = integer("MARKET_SUBSCRIPTION_MAX_CONSECUTIVE_FAILURES", 2, {
      min: 1,
      max: 10,
    });
    let consecutiveFailures = 0;
    // Keep provider registration batches ordered. Concurrent subscribe requests can
    // overwrite one another on providers that replace registration state per request.
    for (let index = 0; index < batches.length; index += 1) {
      const { batch, tier } = batches[index];
      record("batch.started", { batch: index + 1, size: batch.length, tier });
      try {
        const result = await subscriptions.subscribeMarkets(batch, { scheduleRetry: false });
        const subscribedIds = Array.isArray(result.attached)
          ? result.attached
          : Array.isArray(result.subscribed)
            ? result.subscribed
            : [];
        const skippedIds = Array.isArray(result.skipped) ? result.skipped : [];
        subscribedIds.forEach((id) => activeMarketIds.add(id));
        accepted.push(...subscribedIds);
        skipped.push(...skippedIds);
        consecutiveFailures = 0;
        record("batch.completed", {
          batch: index + 1,
          requested: batch.length,
          subscribed: subscribedIds.length,
          skipped: skippedIds.length,
          tier,
        });
      } catch (error) {
        skipped.push(...batch);
        subscriptions.queueSkippedMarkets(batch, { schedule: false });
        consecutiveFailures += 1;
        record("batch.failed", { batch: index + 1, size: batch.length, error: error.message, tier });
        if (consecutiveFailures >= maxFailures) {
          record("sync.circuit-open", { consecutiveFailures, remainingBatches: batches.length - index - 1 });
          break;
        }
      }
    }
    subscriptions.scheduleSkippedRetry();
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
        recovered = {
          requested: recoveryIds.length,
          subscribed: [],
          skipped: recoveryIds,
          error: error.message,
        };
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
      deferredForResourceHeadroom: Math.max(0, admission.deferredFuture),
      deferredForRunLimit,
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
      deferredForResourceHeadroom: result.deferredForResourceHeadroom,
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
  admissionBatches,
  noTickRecoveryCandidates,
  syncMarketSubscriptions,
  startMarketSync,
  getMarketSyncStatus,
  recordMarketSyncActivity,
};
