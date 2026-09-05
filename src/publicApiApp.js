const express = require("express");
const helmet = require("helmet");
const redis = require("./config/redis");
const redisController = require("./controllers/redisController");
const { notFound, errorHandler } = require("./middleware/errorHandler");

function allowedOrigins() {
  return String(process.env.FRONTEND_ORIGINS || "http://localhost:5173")
    .split(",")
    .map((value) => value.trim().replace(/\/$/, ""))
    .filter(Boolean);
}

function redisHealthTimeoutMs() {
  const configured = Number(process.env.PUBLIC_API_REDIS_HEALTH_TIMEOUT_MS || 2000);
  return Number.isFinite(configured) && configured > 0 ? configured : 2000;
}

function createPublicApiApp() {
  const app = express();
  const startedAt = Date.now();
  app.disable("x-powered-by");
  app.disable("etag");
  app.use(helmet());
  app.use((req, res, next) => {
    const origins = allowedOrigins();
    const origin = req.headers.origin;
    if (origin && (origins.includes("*") || origins.includes(origin))) {
      res.setHeader("Access-Control-Allow-Origin", origins.includes("*") ? "*" : origin);
      res.setHeader("Vary", "Origin");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
      res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    }
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });

  app.get("/health", async (req, res) => {
    const before = Date.now();
    try {
      const client = await redis.getRedisReadClient();
      if (!client?.isOpen) throw new Error("Redis read connection is unavailable");
      await Promise.race([
        client.ping(),
        new Promise((resolve, reject) => {
          const timer = setTimeout(
            () => reject(new Error("Redis health check timed out")),
            redisHealthTimeoutMs(),
          );
          timer.unref?.();
        }),
      ]);
      res.json({
        status: "ok",
        service: "public-api",
        redis: { connected: true, latencyMs: Date.now() - before },
        uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
      });
    } catch (error) {
      res.status(503).json({
        status: "critical",
        service: "public-api",
        redis: { connected: false, latencyMs: Date.now() - before },
        message: error.message,
      });
    }
  });
  app.get("/betfair_api/fancy/score/:eventId", redisController.eventScore);
  app.get("/betfair_api/fancy/:eventId", redisController.eventSnapshot);
  app.get("/betfair_api/active_match/:sportId", redisController.activeMatchesRedisOnly);
  app.use(notFound);
  app.use(errorHandler);
  return app;
}

module.exports = { createPublicApiApp };
