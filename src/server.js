require("dotenv").config();
const { createApp } = require("./app");
const { checkSourceDbConnection, closeSourceDb } = require("./config/sourceDb");
const { closeRedis } = require("./config/redis");
const { startMarketSync, syncMarketSubscriptions } = require("./cron/marketSync");
const websocket = require("./services/websocketService");
const subscriptions = require("./services/marketSubscriptionService");
const frontendSocket = require("./services/frontendSocketService");
const logger = require("./utils/logger");
const { closeProviderLog } = require("./utils/providerFileLogger");

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
  websocket.connectSocket();
  subscriptions.startSkippedRetries();
  const cronTask = startMarketSync();
  if (String(process.env.RUN_MARKET_SYNC_ON_START || "true").toLowerCase() === "true") {
    syncMarketSubscriptions().catch((error) => logger.error("Initial market sync failed", { error: error.message }));
  }

  let shutdownPromise;
  const shutdown = (signal) => {
    if (shutdownPromise) return shutdownPromise;
    shutdownPromise = (async () => {
      logger.info("Shutdown started", { signal });
      cronTask.stop();
      await subscriptions.stopSkippedRetries();
      websocket.setResultHandler(null);
      await frontendSocket.closeFrontendSocket();
      await new Promise((resolve) => server.close(resolve));
      await subscriptions.unsubscribeAll().catch((error) => logger.warn("Provider unsubscribe failed", { error: error.message }));
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
