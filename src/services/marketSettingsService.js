const provider = require("./providerApi");
const redisStore = require("../config/redis");
const logger = require("../utils/logger");
const { integer } = require("../config/env");
const { setBounded } = require("../utils/boundedMap");

// Settings are fetched over HTTP only once per market. After that the provider's
// `market` room is the sole source of changes. A failed fetch is not recorded, so
// the market is retried the next time it is subscribed.
const fetched = new Map();
const inFlight = new Set();
const roomUpdated = new Map();
const TRACK_LIMIT = integer("MARKET_SETTINGS_TRACK_LIMIT", 100000, { min: 1000 });

function settingsBatchSize() {
  return integer("PROVIDER_SETTINGS_BATCH_SIZE", 20, { min: 1, max: 100 });
}

function noteRoomUpdate(marketId) {
  const id = String(marketId ?? "");
  if (id) setBounded(roomUpdated, id, true, TRACK_LIMIT);
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
  if (!settings) return null;
  const eventId = item.eid ?? eventIds.get(marketId);
  return eventId == null ? null : { eid: eventId, mid: marketId, settings };
}

async function loadInitialSettings(ids, apply) {
  const wanted = [
    ...new Set((ids || []).map((id) => String(id).trim()).filter(redisStore.validMarketIdentifier)),
  ].filter((id) => !fetched.has(id) && !inFlight.has(id));
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
      for (const raw of items) {
        const item = normalizeItem(raw, eventIds);
        // A room update is newer than this snapshot and must not be overwritten by it.
        if (!item || roomUpdated.has(item.mid)) continue;
        try {
          await apply(item);
          applied += 1;
        } catch (error) {
          logger.error("[MarketSettings] initial settings apply failed", {
            marketId: item.mid,
            error: error.message,
          });
        }
      }
      batch.forEach((id) => setBounded(fetched, id, true, TRACK_LIMIT));
    } catch (error) {
      failed += batch.length;
      logger.warn("[MarketSettings] initial settings fetch failed", {
        marketIds: batch,
        error: error.message,
      });
    } finally {
      batch.forEach((id) => inFlight.delete(id));
    }
  }
  logger.info("[MarketSettings] initial settings loaded", { requested: wanted.length, applied, failed });
  return { requested: wanted.length, applied, failed };
}

module.exports = {
  loadInitialSettings,
  noteRoomUpdate,
  __testing__: {
    reset() {
      fetched.clear();
      inFlight.clear();
      roomUpdated.clear();
    },
  },
};
