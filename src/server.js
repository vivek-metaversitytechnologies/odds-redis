require("dotenv").config();
const { createApp } = require("./app");
const { closeSourceDb } = require("./config/sourceDb");
const { closeRedis } = require("./config/redis");
const { fetchActiveMarkets, startMarketSync, syncMarketSubscriptions } = require("./cron/marketSync");
const { startCompetitionSync, syncCompetitions } = require("./cron/competitionSync");
const { startEventSync, syncEvents } = require("./cron/eventSync");
const { startMarketDiscoverySync } = require("./cron/marketDiscoverySync");
const { startResultSync, syncResults, handleSocketGameOver } = require("./cron/resultSync");
const { startRedisEventCleanup } = require("./cron/redisEventCleanup");
const websocket = require("./services/websocketService");
const subscriptions = require("./services/marketSubscriptionService");
const frontendSocket = require("./services/frontendSocketService");
const logger = require("./utils/logger");
const { closeProviderLog } = require("./utils/providerFileLogger");
const cronConfig = require("./config/cron");
const { closeProviderRequests } = require("./services/providerApi");
const { runStartupPreflight } = require("./services/startupPreflight");
const { startHealthSupervisor, stopHealthSupervisor } = require("./services/healthSupervisor");

async function startServer() {
  const preflight = await runStartupPreflight();
  logger.info("Startup preflight passed", preflight);
  const app = createApp();
  const server = await new Promise((resolve, reject) => {
    const listener = app.listen(Number(process.env.PORT || 3000), () => resolve(listener));
    listener.once("error", reject);
  });
  frontendSocket.attachFrontendSocket(server);
  logger.info("HTTP server started", { port: server.address().port });
  websocket.setResultHandler(handleSocketGameOver);
  // Establish the replacement socket before any optional provider cleanup. Redis remains
  // the durable frontend snapshot and must not be touched during a process restart.
  websocket.connectSocket();
  subscriptions.startSkippedRetries();
  const competitionCronTask = startCompetitionSync();
  const eventCronTask = startEventSync();
  const marketDiscoveryCronTask = startMarketDiscoverySync();
  const resultCronTask = startResultSync();
  const redisEventCleanupCronTask = startRedisEventCleanup();
  startHealthSupervisor();
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
  // Keep startup non-blocking, but serialize provider cleanup and subscription.
  // Running these concurrently can unsubscribe IDs that the initial sync just added.
  const reconcileOnStart =
    String(process.env.RECONCILE_SUBSCRIPTIONS_ON_START || "false").toLowerCase() === "true";
  if (reconcileOnStart || cronConfig.marketSubscription.runOnStart) {
    setImmediate(() => {
      void (async () => {
        if (reconcileOnStart) {
          const markets = await fetchActiveMarkets();
          await subscriptions.reconcileProviderSubscriptions(
            markets.map((market) => market.marketid).filter(Boolean),
          );
        }
        if (cronConfig.marketSubscription.runOnStart) await syncMarketSubscriptions("active");
      })().catch((error) =>
        logger.error("Initial market subscription reconciliation failed", { error: error.message }),
      );
    });
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
      stopHealthSupervisor();
      // Requests owned by the outgoing process must not keep PM2 waiting through
      // provider timeouts and retries. The replacement process establishes fresh work.
      await closeProviderRequests();
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
  startServer().catch(async (error) => {
    logger.error("Startup failed", { error: error.message });
    await Promise.allSettled([closeRedis(), closeSourceDb(), closeProviderLog(), logger.close()]);
    process.exitCode = 1;
  });
}

module.exports = { startServer };
