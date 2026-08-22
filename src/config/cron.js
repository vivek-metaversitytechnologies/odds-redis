function booleanEnv(name, fallback = true) {
  const value = process.env[name];
  if (value === undefined || value === "") return fallback;
  return String(value).toLowerCase() === "true";
}

module.exports = Object.freeze({
  competition: Object.freeze({
    expression: process.env.COMPETITION_SYNC_CRON || "0 0 0 * * *",
    runOnStart: booleanEnv("RUN_COMPETITION_SYNC_ON_START"),
  }),
  event: Object.freeze({
    expression: process.env.EVENT_SYNC_CRON || "0 0 * * * *",
    runOnStart: booleanEnv("RUN_EVENT_SYNC_ON_START"),
  }),
  marketDiscovery: Object.freeze({
    expression: process.env.MARKET_DISCOVERY_CRON || "*/5 * * * * *",
  }),
  futureMarketDiscovery: Object.freeze({
    expression: process.env.FUTURE_MARKET_DISCOVERY_CRON || "0 */10 * * * *",
  }),
  liveMarketCleanup: Object.freeze({
    expression: process.env.LIVE_MARKET_CLEANUP_CRON || "*/3 * * * * *",
  }),
  redisEventCleanup: Object.freeze({
    expression: process.env.REDIS_EVENT_CLEANUP_CRON || "0 */10 * * * *",
    runOnStart: booleanEnv("RUN_REDIS_EVENT_CLEANUP_ON_START", false),
  }),
  marketSubscription: Object.freeze({
    expression: process.env.MARKET_SYNC_CRON || "*/5 * * * * *",
    runOnStart: booleanEnv("RUN_MARKET_SYNC_ON_START"),
  }),
  result: Object.freeze({
    expression: process.env.RESULT_SYNC_CRON || "0 * * * * *",
    runOnStart: booleanEnv("RUN_RESULT_SYNC_ON_START"),
  }),
});
