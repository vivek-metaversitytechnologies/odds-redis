const { createClient } = require("redis");
const logger = require("../utils/logger");

let client;
let connecting;

function cacheKey(item) {
  const eventId = String(item?.eid ?? "").trim();
  const marketId = String(item?.mid ?? "").trim();
  if (!/^\d+$/.test(eventId) || !marketId || /[\s\x00-\x1f]/.test(marketId)) return null;
  const prefix = process.env.BET_PAUSE_REDIS_KEY_PREFIX || "kalyanexch_com_redis";
  return `${prefix}${eventId}_${marketId}_bp`;
}

async function getClient() {
  if (client?.isOpen) return client;
  if (connecting) return connecting;
  const url = String(process.env.BET_PAUSE_REDIS_URL || "").trim();
  if (!url) return null;
  client = createClient({
    url,
    socket: { connectTimeout: Number(process.env.BET_PAUSE_REDIS_TIMEOUT_MS || 5000) },
  });
  client.on("error", (error) =>
    logger.error("[BetPauseCache] Redis client error", { error: error.message }),
  );
  connecting = client
    .connect()
    .then(() => {
      logger.info("[BetPauseCache] Redis connected");
      return client;
    })
    .catch((error) => {
      logger.error("[BetPauseCache] Redis connection failed", { error: error.message });
      client = undefined;
      return null;
    })
    .finally(() => {
      connecting = undefined;
    });
  return connecting;
}

async function deleteMarketBetPause(item) {
  const key = cacheKey(item);
  if (!key) return false;
  const redis = await getClient();
  if (!redis?.isOpen) return false;
  await redis.del(key);
  return true;
}

async function closeBetPauseCache() {
  const current = client?.isOpen ? client : null;
  client = undefined;
  connecting = undefined;
  if (current) await current.quit();
}

module.exports = {
  deleteMarketBetPause,
  closeBetPauseCache,
  __testing__: {
    cacheKey,
    setClient(value) {
      client = value;
    },
    reset() {
      client = undefined;
      connecting = undefined;
    },
  },
};
