require("dotenv").config();
const { createApp } = require("./app");
const { checkSourceDbConnection, closeSourceDb } = require("./config/sourceDb");
const { closeRedis } = require("./config/redis");
const { fetchActiveMarkets, startMarketSync, syncMarketSubscriptions } = require("./cron/marketSync");
const { startCompetitionSync, syncCompetitions } = require("./cron/competitionSync");
const { startEventSync, syncEvents } = require("./cron/eventSync");
const { startMarketDiscoverySync } = require("./cron/marketDiscoverySync");
const { startResultSync, syncResults } = require("./cron/resultSync");
const { startRedisEventCleanup } = require("./cron/redisEventCleanup");
const websocket = require("./services/websocketService");
const subscriptions = require("./services/marketSubscriptionService");
const frontendSocket = require("./services/frontendSocketService");
const logger = require("./utils/logger");
const { closeProviderLog } = require("./utils/providerFileLogger");
const cronConfig = require("./config/cron");

async function startServer() {
  await checkSourceDbConnection();
  const app = createApp();
  const server = await new Promise((resolve, reject) => {
    const listener = app.listen(Number(process.env.PORT || 3000), () => resolve(listener));
    listener.once("error", reject);
  });
  frontendSocket.attachFrontendSocket(server);
  logger.info("HTTP server started", { port: server.address().port });
  websocket.setResultHandler((marketIds) => subscriptions.unsubscribeResultMarkets(marketIds));
  // Establish the replacement socket before any optional provider cleanup. Redis remains
  // the durable frontend snapshot and must not be touched during a process restart.
  websocket.connectSocket();
  subscriptions.startSkippedRetries();
  const competitionCronTask = startCompetitionSync();
  const eventCronTask = startEventSync();
  const marketDiscoveryCronTask = startMarketDiscoverySync();
  const resultCronTask = startResultSync();
  const redisEventCleanupCronTask = startRedisEventCleanup();
  if (cronConfig.competition.runOnStart) {
    syncCompetitions()
      .then(() => {
        if (cronConfig.event.runOnStart) return syncEvents();
        return null;
      })
      .catch(() => {});
  } else if (cronConfig.event.runOnStart) {
    syncEvents().catch(() => {});
  }
  const cronTask = startMarketSync();
  // Provider reconciliation can take minutes for thousands of markets. It must not
  // block cron initialization, otherwise a slow provider leaves every worker idle.
  if (String(process.env.RECONCILE_SUBSCRIPTIONS_ON_START || "false").toLowerCase() === "true") {
    setImmediate(() => {
      fetchActiveMarkets("active")
        .then((markets) =>
          subscriptions.reconcileProviderSubscriptions(
            markets.map((market) => market.marketid).filter(Boolean),
          ),
        )
        .catch((error) =>
          logger.error("Startup subscription reconciliation failed", { error: error.message }),
        );
    });
  }
  if (cronConfig.marketSubscription.runOnStart) {
    syncMarketSubscriptions().catch((error) =>
      logger.error("Initial market sync failed", { error: error.message }),
    );
  }
  if (cronConfig.result.runOnStart) {
    syncResults().catch((error) => logger.error("Initial result sync failed", { error: error.message }));
  }

  let shutdownPromise;
  const shutdown = (signal) => {
    if (shutdownPromise) return shutdownPromise;
    shutdownPromise = (async () => {
      logger.info("Shutdown started", { signal });
      cronTask.stop();
      competitionCronTask.stop();
      eventCronTask.stop();
      marketDiscoveryCronTask.stop();
      resultCronTask.stop();
      redisEventCleanupCronTask.stop();
      await subscriptions.stopSkippedRetries();
      websocket.setResultHandler(null);
      await frontendSocket.closeFrontendSocket();
      await new Promise((resolve) => server.close(resolve));
      // Do not bulk-unsubscribe on SIGTERM/SIGINT. PM2 restarts replace this process and
      // the new socket immediately restores subscriptions. Explicit admin/result/event
      // unsubscribe operations still use the provider unsubscribe API.
      await websocket.stopSocket();
      await closeRedis();
      await closeSourceDb();
      await closeProviderLog();
      await logger.close();
    })();
    return shutdownPromise;
  };
  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
  return { app, server, shutdown };
}

if (require.main === module) {
  startServer().catch((error) => {
    logger.error("Startup failed", { error: error.message });
    process.exitCode = 1;
  });
}

module.exports = { startServer };
