const provider = require("./providerApi");
const redisStore = require("../config/redis");
const logger = require("../utils/logger");
const { writeMarketLimitsOutcome } = require("../utils/marketLimitsFileLogger");
const { providerLimits } = require("../utils/marketLimits");
const { integer } = require("../config/env");
const { setBounded } = require("../utils/boundedMap");

// Settings are fetched over HTTP only once per market. After that the provider's
// `market` room is the sole source of changes. A failed fetch is not recorded, so
// the market is retried the next time it is subscribed.
const fetched = new Map();
const inFlight = new Set();
const roomUpdated = new Map();
const pendingRetries = new Set();
let retryTimer;
let retryPromise;
let retryApply;
let retriesStopped = false;
const TRACK_LIMIT = integer("MARKET_SETTINGS_TRACK_LIMIT", 100000, { min: 1000 });

// The settings API answers at most 50 markets per request and silently drops the rest
// (measured with scripts/benchmarkMarketSettings.js), so no caller may send more.
const SETTINGS_MAX_IDS_PER_REQUEST = 50;

function settingsBatchSize() {
  return integer("PROVIDER_SETTINGS_BATCH_SIZE", 20, { min: 1, max: SETTINGS_MAX_IDS_PER_REQUEST });
}

function noteRoomUpdate(marketId) {
  const id = String(marketId ?? "");
  if (id) setBounded(roomUpdated, id, Date.now(), TRACK_LIMIT);
}

// True when a `market` room message for this market arrived at or after `sinceMs`, meaning an
// HTTP settings response requested at `sinceMs` may be older than what is already applied.
function roomUpdatedSince(marketId, sinceMs) {
  const at = roomUpdated.get(String(marketId ?? ""));
  return typeof at === "number" && at >= sinceMs;
}

function responseItems(response) {
  if (Array.isArray(response)) return response;
  if (!response || typeof response !== "object") return [];
  for (const key of ["data", "markets", "settings"]) if (Array.isArray(response[key])) return response[key];
  return [];
}

// Accept the room message shape ({ eid, mid, settings }) as well as a flat { eid, mid, ms, mas }.
function normalizeItem(item, eventIds) {
  const marketId = String(item?.mid ?? "");
  if (!redisStore.validMarketIdentifier(marketId)) return null;
  const settings = item.settings ?? (item.ms !== undefined || item.mas !== undefined ? item : null);
  if (!settings || !Object.keys(providerLimits(settings)).length) return null;
  const eventId = item.eid ?? eventIds.get(marketId);
  return eventId == null ? null : { eid: eventId, mid: marketId, settings };
}

function retryDelayMs() {
  return integer("MARKET_SETTINGS_RETRY_MS", 5000, { min: 100, max: 300000 });
}

function scheduleRetry(apply) {
  if (retriesStopped || retryTimer || retryPromise || !pendingRetries.size) return;
  retryApply = apply || retryApply;
  if (typeof retryApply !== "function") return;
  retryTimer = setTimeout(() => {
    retryTimer = undefined;
    const ids = [...pendingRetries];
    retryPromise = loadInitialSettings(ids, retryApply, { force: true })
      .catch((error) => logger.error("[MarketSettings] retry failed", { error: error.message }))
      .finally(() => {
        retryPromise = undefined;
        scheduleRetry(retryApply);
      });
  }, retryDelayMs());
  retryTimer.unref?.();
}

function queueRefresh(ids, apply) {
  for (const value of ids || []) {
    const id = String(value || "").trim();
    if (!redisStore.validMarketIdentifier(id)) continue;
    fetched.delete(id);
    roomUpdated.delete(id);
    pendingRetries.add(id);
  }
  scheduleRetry(apply);
}

async function loadInitialSettings(ids, apply, { force = false } = {}) {
  const wanted = [
    ...new Set((ids || []).map((id) => String(id).trim()).filter(redisStore.validMarketIdentifier)),
  ].filter((id) => (force || !fetched.has(id)) && !inFlight.has(id));
  if (!wanted.length) return { requested: 0, applied: 0, failed: 0 };
  wanted.forEach((id) => inFlight.add(id));
  let applied = 0;
  let failed = 0;
  const size = settingsBatchSize();
  for (let index = 0; index < wanted.length; index += size) {
    const batch = wanted.slice(index, index + size);
    try {
      const response = await provider.marketSettings(batch, { source: "market-settings" });
      const items = responseItems(response);
      const missingEvent = items.filter((item) => item?.eid == null).map((item) => item?.mid);
      const eventIds = new Map();
      if (missingEvent.length) {
        for (const [id, market] of await redisStore.findMarkets(missingEvent)) {
          if (market?.eventid != null) eventIds.set(id, market.eventid);
        }
      }
      const completed = new Set();
      const responded = new Set();
      for (const raw of items) {
        const item = normalizeItem(raw, eventIds);
        // A room update is newer than this snapshot and must not be overwritten by it.
        if (!item || !batch.includes(item.mid)) continue;
        responded.add(item.mid);
        // refreshSettings clears old room markers before fetching. A marker seen
        // here therefore represents a newer live update and must always win.
        if (roomUpdated.has(item.mid)) {
          completed.add(item.mid);
          setBounded(fetched, item.mid, true, TRACK_LIMIT);
          pendingRetries.delete(item.mid);
          continue;
        }
        try {
          const recovering = pendingRetries.has(item.mid);
          await apply(item);
          applied += 1;
          completed.add(item.mid);
          setBounded(fetched, item.mid, true, TRACK_LIMIT);
          pendingRetries.delete(item.mid);
          if (recovering) writeMarketLimitsOutcome("RECOVERED", item);
        } catch (error) {
          failed += 1;
          pendingRetries.add(item.mid);
          logger.error("[MarketSettings] initial settings apply failed", {
            marketId: item.mid,
            error: error.message,
          });
        }
      }
      for (const id of batch) {
        if (completed.has(id) || responded.has(id)) continue;
        failed += 1;
        pendingRetries.add(id);
        logger.warn("[MarketSettings] provider response omitted usable settings", { marketId: id });
      }
    } catch (error) {
      failed += batch.length;
      batch.forEach((id) => pendingRetries.add(id));
      logger.warn("[MarketSettings] initial settings fetch failed", {
        marketIds: batch,
        error: error.message,
      });
    } finally {
      batch.forEach((id) => inFlight.delete(id));
    }
  }
  scheduleRetry(apply);
  logger.info("[MarketSettings] initial settings loaded", { requested: wanted.length, applied, failed });
  return { requested: wanted.length, applied, failed };
}

async function refreshSettings(ids, apply) {
  const marketIds = [
    ...new Set((ids || []).map(String).map((id) => id.trim()).filter(redisStore.validMarketIdentifier)),
  ];
  marketIds.forEach((id) => {
    fetched.delete(id);
    roomUpdated.delete(id);
  });
  return loadInitialSettings(marketIds, apply, { force: true });
}

async function stopRetries() {
  retriesStopped = true;
  if (retryTimer) clearTimeout(retryTimer);
  retryTimer = undefined;
  if (retryPromise) await retryPromise;
}

module.exports = {
  SETTINGS_MAX_IDS_PER_REQUEST,
  loadInitialSettings,
  noteRoomUpdate,
  queueRefresh,
  refreshSettings,
  responseItems,
  roomUpdatedSince,
  stopRetries,
  __testing__: {
    reset() {
      if (retryTimer) clearTimeout(retryTimer);
      retryTimer = undefined;
      retryPromise = undefined;
      retryApply = undefined;
      retriesStopped = false;
      fetched.clear();
      inFlight.clear();
      roomUpdated.clear();
      pendingRetries.clear();
    },
    pendingRetries,
  },
};
