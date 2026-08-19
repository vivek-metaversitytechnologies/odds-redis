const provider = require("./providerApi");
const websocket = require("./websocketService");
const logger = require("../utils/logger");
const redisStore = require("../config/redis");
const { integer } = require("../config/env");

const skippedMarketIds = new Set();
const completedMarketIds = new Set();
const pendingResultUnsubscriptions = new Set();
let retryTimer = null;
let retryPromise = null;
let unsubscribeRetryTimer = null;
let unsubscribePromise = null;
let retriesStopped = false;
const retryState = {
  attempts: 0,
  lastAttemptAt: null,
  lastCompletedAt: null,
  lastAccepted: 0,
  lastSkipped: 0,
  lastError: null,
};

function getRetryDelayMs() {
  return integer("PROVIDER_SKIPPED_RETRY_MS", 1000, { min: 100 });
}

function normalizeProviderAcknowledgement(response, requested) {
  const requestedSet = new Set(requested);
  const hasAcknowledgement =
    response &&
    typeof response === "object" &&
    (Array.isArray(response.subscribed) || Array.isArray(response.skipped));
  const subscribed = hasAcknowledgement
    ? [...new Set((response.subscribed || []).map(String))].filter((id) => requestedSet.has(id))
    : requested;
  const subscribedSet = new Set(subscribed);
  const explicitSkipped = hasAcknowledgement
    ? [...new Set((response.skipped || []).map(String))].filter((id) => requestedSet.has(id))
    : [];
  const skippedSet = new Set(explicitSkipped);
  for (const id of requested) if (!subscribedSet.has(id)) skippedSet.add(id);
  return { subscribed, skipped: [...skippedSet], providerResponse: response };
}

async function subscribeMarkets(ids) {
  const marketIds = [
    ...new Set(
      (ids || [])
        .map(String)
        .map((id) => id.trim())
        .filter(redisStore.validMarketIdentifier),
    ),
  ].filter((id) => !completedMarketIds.has(id));
  if (!marketIds.length) return { subscribed: [], skipped: [] };
  const response = await provider.subscribe(marketIds);
  const acknowledgement = normalizeProviderAcknowledgement(response, marketIds);
  websocket.subscribeMarkets(acknowledgement.subscribed);
  acknowledgement.subscribed.forEach((id) => skippedMarketIds.delete(id));
  acknowledgement.skipped.forEach((id) => skippedMarketIds.add(id));
  scheduleSkippedRetry();
  return acknowledgement;
}

function scheduleResultUnsubscribeRetry() {
  if (
    retriesStopped ||
    unsubscribeRetryTimer ||
    unsubscribePromise ||
    pendingResultUnsubscriptions.size === 0
  )
    return;
  unsubscribeRetryTimer = setTimeout(() => {
    unsubscribeRetryTimer = null;
    void flushResultUnsubscriptions();
  }, getRetryDelayMs());
  unsubscribeRetryTimer.unref?.();
}

async function flushResultUnsubscriptions() {
  if (unsubscribePromise) return unsubscribePromise;
  const queued = [...pendingResultUnsubscriptions];
  if (!queued.length || retriesStopped) return undefined;
  unsubscribePromise = (async () => {
    const batchSize = integer("MARKET_SUBSCRIPTION_BATCH_SIZE", 50, { min: 1, max: 100 });
    for (let index = 0; index < queued.length && !retriesStopped; index += batchSize) {
      const batch = queued.slice(index, index + batchSize);
      try {
        await provider.unsubscribe(batch);
        websocket.unsubscribeMarkets(batch);
        batch.forEach((id) => pendingResultUnsubscriptions.delete(id));
        logger.info("[MarketSubscription] markets unsubscribed", { marketIds: batch });
      } catch (error) {
        logger.warn("[MarketSubscription] result unsubscribe remains queued", {
          marketIds: batch,
          error: error.message,
        });
      }
    }
  })().finally(() => {
    unsubscribePromise = null;
    scheduleResultUnsubscribeRetry();
  });
  return unsubscribePromise;
}

async function unsubscribeResultMarkets(ids) {
  const marketIds = [
    ...new Set(
      (ids || [])
        .map(String)
        .map((id) => id.trim())
        .filter(redisStore.validMarketIdentifier),
    ),
  ];
  marketIds.forEach((id) => {
    completedMarketIds.add(id);
    skippedMarketIds.delete(id);
    pendingResultUnsubscriptions.add(id);
  });
  return flushResultUnsubscriptions();
}

async function unsubscribeEventMarkets(ids) {
  const marketIds = [
    ...new Set(
      (ids || [])
        .map(String)
        .map((id) => id.trim())
        .filter(redisStore.validMarketIdentifier),
    ),
  ];
  if (!marketIds.length) return { requested: [], unsubscribed: [] };
  websocket.unsubscribeMarkets(marketIds);

  const batchSize = integer("MARKET_SUBSCRIPTION_BATCH_SIZE", 50, { min: 1, max: 100 });
  const unsubscribed = [];
  for (let index = 0; index < marketIds.length; index += batchSize) {
    const batch = marketIds.slice(index, index + batchSize);
    await provider.unsubscribe(batch);
    unsubscribed.push(...batch);
  }
  logger.info("[MarketSubscription] event markets manually unsubscribed", {
    count: unsubscribed.length,
    marketIds: unsubscribed,
  });
  return { requested: marketIds, unsubscribed };
}

async function reconcileProviderSubscriptions(ids) {
  const marketIds = [
    ...new Set(
      (ids || [])
        .map(String)
        .map((id) => id.trim())
        .filter(redisStore.validMarketIdentifier),
    ),
  ];
  if (!marketIds.length) return { requested: 0, unsubscribed: 0 };
  const batchSize = integer("MARKET_SUBSCRIPTION_BATCH_SIZE", 50, { min: 1, max: 100 });
  let unsubscribed = 0;
  for (let index = 0; index < marketIds.length; index += batchSize) {
    const batch = marketIds.slice(index, index + batchSize);
    await provider.unsubscribe(batch);
    websocket.unsubscribeMarkets(batch);
    batch.forEach((id) => skippedMarketIds.delete(id));
    unsubscribed += batch.length;
  }
  logger.info("[MarketSubscription] startup reconciliation completed", {
    requested: marketIds.length,
    unsubscribed,
  });
  return { requested: marketIds.length, unsubscribed };
}

function scheduleSkippedRetry() {
  if (retriesStopped || retryTimer || retryPromise || skippedMarketIds.size === 0) return;
  retryTimer = setTimeout(() => {
    retryTimer = null;
    retryPromise = retrySkippedMarkets()
      .catch((error) =>
        logger.error("[MarketSubscription] skipped retry failed", {
          error: error.message,
          queued: skippedMarketIds.size,
        }),
      )
      .finally(() => {
        retryPromise = null;
        scheduleSkippedRetry();
      });
  }, getRetryDelayMs());
  retryTimer.unref?.();
}

async function retrySkippedMarkets() {
  const queued = [...skippedMarketIds];
  if (queued.length === 0 || retriesStopped) return;

  retryState.attempts += 1;
  retryState.lastAttemptAt = new Date().toISOString();
  retryState.lastError = null;
  let accepted = 0;
  let skipped = 0;
  const batchSize = integer("MARKET_SUBSCRIPTION_BATCH_SIZE", 50, { min: 1, max: 100 });

  logger.info("[MarketSubscription] retrying provider-skipped markets", {
    attempt: retryState.attempts,
    queued: queued.length,
    retryDelayMs: getRetryDelayMs(),
  });

  for (let index = 0; index < queued.length && !retriesStopped; index += batchSize) {
    const batch = queued.slice(index, index + batchSize).filter((id) => !completedMarketIds.has(id));
    if (!batch.length) continue;
    try {
      // A provider "skipped" response can mean the IDs remain registered from an
      // earlier subscription while this socket is not attached to them. Clear that
      // stale provider state before retrying, otherwise the same IDs can skip forever.
      await provider.unsubscribe(batch);
      websocket.unsubscribeMarkets(batch);
      const response = await provider.subscribe(batch);
      const acknowledgement = normalizeProviderAcknowledgement(response, batch);
      websocket.subscribeMarkets(acknowledgement.subscribed);
      acknowledgement.subscribed.forEach((id) => skippedMarketIds.delete(id));
      acknowledgement.skipped.forEach((id) => {
        if (!completedMarketIds.has(id)) skippedMarketIds.add(id);
      });
      accepted += acknowledgement.subscribed.length;
      skipped += acknowledgement.skipped.length;
    } catch (error) {
      skipped += batch.length;
      retryState.lastError = error.message;
      logger.warn("[MarketSubscription] provider-skipped batch remains queued", {
        error: error.message,
        marketIds: batch,
      });
    }
  }

  retryState.lastCompletedAt = new Date().toISOString();
  retryState.lastAccepted = accepted;
  retryState.lastSkipped = skipped;
  logger.info("[MarketSubscription] skipped retry completed", {
    attempt: retryState.attempts,
    accepted,
    stillQueued: skippedMarketIds.size,
  });
}

function getSkippedRetryStatus() {
  return {
    ...retryState,
    retryDelayMs: getRetryDelayMs(),
    running: Boolean(retryPromise),
    queued: skippedMarketIds.size,
    marketIds: [...skippedMarketIds],
    completedMarketIds: [...completedMarketIds],
    pendingResultUnsubscriptions: [...pendingResultUnsubscriptions],
  };
}

function startSkippedRetries() {
  retriesStopped = false;
  scheduleSkippedRetry();
  scheduleResultUnsubscribeRetry();
}

async function stopSkippedRetries() {
  retriesStopped = true;
  if (retryTimer) clearTimeout(retryTimer);
  retryTimer = null;
  if (unsubscribeRetryTimer) clearTimeout(unsubscribeRetryTimer);
  unsubscribeRetryTimer = null;
  if (retryPromise) await retryPromise;
  if (unsubscribePromise) await unsubscribePromise;
}

async function unsubscribeAll() {
  const ids = websocket.getSubscribedMarketIds();
  if (ids.length) await provider.unsubscribe(ids);
  websocket.unsubscribeMarkets(ids);
  ids.forEach((id) => skippedMarketIds.delete(id));
  return { requested: ids.length, unsubscribed: ids };
}

module.exports = {
  subscribeMarkets,
  unsubscribeAll,
  normalizeProviderAcknowledgement,
  getSkippedRetryStatus,
  startSkippedRetries,
  stopSkippedRetries,
  unsubscribeResultMarkets,
  unsubscribeEventMarkets,
  reconcileProviderSubscriptions,
  isMarketSuppressed: (id) => completedMarketIds.has(String(id)),
};
