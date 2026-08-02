const express = require("express");
const helmet = require("helmet");
const path = require("node:path");
const { checkSourceDbConnection, getSourcePool } = require("./config/sourceDb");
const websocket = require("./services/websocketService");
const redis = require("./config/redis");
const eventRoutes = require("./routes/eventRoutes");
const providerRoutes = require("./routes/providerRoutes");
const sourceMarketRoutes = require("./routes/sourceMarketRoutes");
const logRoutes = require("./routes/logRoutes");
const redisRoutes = require("./routes/redisRoutes");
const redisController = require("./controllers/redisController");
const { notFound, errorHandler } = require("./middleware/errorHandler");

function createApp() {
  const app = express();
  app.disable("x-powered-by");
  app.use(helmet({
    contentSecurityPolicy: {
      directives: {
        // TLS is terminated by Nginx. Forcing upgrades here breaks direct-port admin assets.
        "upgrade-insecure-requests": null,
      },
    },
  }));
  app.use(express.json({ limit: "1mb" }));
  app.use(express.static(path.join(__dirname, "../public")));
  app.use((req, res, next) => {
    const origins = String(process.env.FRONTEND_ORIGINS || "http://localhost:5173")
      .split(",").map((value) => value.trim());
    const origin = req.headers.origin;
    if (origin && (origins.includes("*") || origins.includes(origin))) {
      res.setHeader("Access-Control-Allow-Origin", origins.includes("*") ? "*" : origin);
      res.setHeader("Vary", "Origin");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Internal-API-Key");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    }
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });
  app.use(["/api/events/subscriptions", "/api/provider", "/api/source", "/api/logs", "/api/redis"], (req, res, next) => {
    const required = process.env.INTERNAL_API_KEY;
    if (!required || req.get("x-internal-api-key") === required) return next();
    res.status(401).json({ status: "error", message: "Unauthorized" });
  });
  app.get("/health", async (req, res) => {
    try {
      await checkSourceDbConnection();
      res.json({ status: "ok", sourceDatabase: "connected",
        redis: redis.getRedisStatus(), websocket: websocket.getSocketStatus() });
    } catch (error) {
      res.status(503).json({ status: "error", sourceDatabase: "disconnected",
        redis: redis.getRedisStatus(), websocket: websocket.getSocketStatus(), message: error.message });
    }
  });
  app.get("/db-time", async (req, res, next) => {
    try {
      const [rows] = await getSourcePool().query("SELECT NOW() AS currentTime");
      res.json({ status: "ok", data: rows[0] });
    } catch (error) { next(error); }
  });
  app.get("/api/socket/status", (req, res) => res.json({ status: "ok", data: websocket.getSocketStatus() }));
  app.get("/betfair_api/fancy/:eventId", redisController.eventSnapshot);
  app.get("/betfair_api/active_match/:sportId", redisController.activeMatches);
  app.use("/api/provider", providerRoutes);
  app.use("/api/source", sourceMarketRoutes);
  app.use("/api/logs", logRoutes);
  app.use("/api/redis", redisRoutes);
  app.use("/api/events", eventRoutes);
  app.use(notFound);
  app.use(errorHandler);
  return app;
}

module.exports = { createApp };
