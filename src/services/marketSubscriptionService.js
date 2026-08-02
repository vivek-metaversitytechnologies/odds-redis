const provider = require("./providerApi");
const websocket = require("./websocketService");
const logger = require("../utils/logger");

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
  const configured = Number(process.env.PROVIDER_SKIPPED_RETRY_MS || 1000);
  return Number.isFinite(configured) ? Math.max(100, configured) : 1000;
}

function normalizeProviderAcknowledgement(response, requested) {
  const requestedSet = new Set(requested);
  const hasAcknowledgement = response && typeof response === "object"
    && (Array.isArray(response.subscribed) || Array.isArray(response.skipped));
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
  const marketIds = [...new Set((ids || []).map(String).map((id) => id.trim()).filter(Boolean))]
    .filter((id) => !completedMarketIds.has(id));
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
  if (retriesStopped || unsubscribeRetryTimer || unsubscribePromise
    || pendingResultUnsubscriptions.size === 0) return;
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
    const batchSize = Math.max(1, Number(process.env.MARKET_SUBSCRIPTION_BATCH_SIZE || 10));
    for (let index = 0; index < queued.length && !retriesStopped; index += batchSize) {
      const batch = queued.slice(index, index + batchSize);
      try {
        await provider.unsubscribe(batch);
        websocket.unsubscribeMarkets(batch);
        batch.forEach((id) => pendingResultUnsubscriptions.delete(id));
        logger.info("[MarketSubscription] result markets unsubscribed", { marketIds: batch });
      } catch (error) {
        logger.warn("[MarketSubscription] result unsubscribe remains queued", {
          marketIds: batch, error: error.message,
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
  const marketIds = [...new Set((ids || []).map(String).map((id) => id.trim()).filter(Boolean))];
  marketIds.forEach((id) => {
    completedMarketIds.add(id);
    skippedMarketIds.delete(id);
    pendingResultUnsubscriptions.add(id);
  });
  return flushResultUnsubscriptions();
}

function scheduleSkippedRetry() {
  if (retriesStopped || retryTimer || retryPromise || skippedMarketIds.size === 0) return;
  retryTimer = setTimeout(() => {
    retryTimer = null;
    retryPromise = retrySkippedMarkets()
      .catch((error) => logger.error("[MarketSubscription] skipped retry failed", {
        error: error.message,
        queued: skippedMarketIds.size,
      }))
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
  const batchSize = Math.max(1, Number(process.env.MARKET_SUBSCRIPTION_BATCH_SIZE || 10));

  logger.info("[MarketSubscription] retrying provider-skipped markets", {
    attempt: retryState.attempts,
    queued: queued.length,
    retryDelayMs: getRetryDelayMs(),
  });

  for (let index = 0; index < queued.length && !retriesStopped; index += batchSize) {
    const batch = queued.slice(index, index + batchSize)
      .filter((id) => !completedMarketIds.has(id));
    if (!batch.length) continue;
    try {
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
}

module.exports = { subscribeMarkets, unsubscribeAll, normalizeProviderAcknowledgement,
  getSkippedRetryStatus, startSkippedRetries, stopSkippedRetries, unsubscribeResultMarkets,
  isMarketCompleted: (id) => completedMarketIds.has(String(id)) };
