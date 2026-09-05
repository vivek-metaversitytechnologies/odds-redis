require("dotenv").config();
const { createPublicApiApp } = require("./publicApiApp");
const { closeRedis } = require("./config/redis");
const logger = require("./utils/logger");

function publicApiPort() {
  const port = Number(process.env.PUBLIC_API_PORT || 3001);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PUBLIC_API_PORT must be an integer between 1 and 65535");
  }
  return port;
}

async function startPublicApiServer() {
  const app = createPublicApiApp();
  const server = await new Promise((resolve, reject) => {
    const listener = app.listen(publicApiPort(), () => resolve(listener));
    listener.once("error", reject);
  });
  logger.info("Public API server started", { port: server.address().port });

  let shutdownPromise;
  const shutdown = (signal) => {
    if (shutdownPromise) return shutdownPromise;
    shutdownPromise = (async () => {
      logger.info("Public API shutdown started", { signal });
      await new Promise((resolve) => server.close(resolve));
      await closeRedis();
      await logger.close();
    })();
    return shutdownPromise;
  };
  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
  return { app, server, shutdown };
}

if (require.main === module) {
  startPublicApiServer().catch(async (error) => {
    logger.error("Public API startup failed", { error: error.message });
    await Promise.allSettled([closeRedis(), logger.close()]);
    process.exitCode = 1;
  });
}

module.exports = { startPublicApiServer, publicApiPort };
