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
const adminAuth = require("./middleware/adminAuth");
const { getCompetitionSyncStatus } = require("./cron/competitionSync");
const { getEventSyncStatus } = require("./cron/eventSync");
const { getMarketDiscoveryStatus } = require("./cron/marketDiscoverySync");
const { getMarketSyncStatus } = require("./cron/marketSync");
const { getResultSyncStatus } = require("./cron/resultSync");
const { getRedisEventCleanupStatus } = require("./cron/redisEventCleanup");
const { providerLimiter } = require("./services/providerApi");
const { getHealthStatus } = require("./services/healthSupervisor");

function createApp() {
  const app = express();
  app.disable("x-powered-by");
  app.disable("etag");
  app.use(
    helmet({
      contentSecurityPolicy: {
        directives: {
          // TLS is terminated by Nginx. Forcing upgrades here breaks direct-port admin assets.
          "upgrade-insecure-requests": null,
        },
      },
    }),
  );
  app.use(express.json({ limit: "1mb" }));
  app.post("/admin/auth/login", (req, res) => {
    if (!process.env.ADMIN_PANEL_PASSWORD) {
      return res.status(503).json({ status: "error", message: "ADMIN_PANEL_PASSWORD is not configured" });
    }
    if (!adminAuth.matchesPassword(req.body?.password)) {
      return res.status(401).json({ status: "error", message: "Incorrect password" });
    }
    adminAuth.setSessionCookie(req, res);
    return res.json({ status: "ok" });
  });
  app.post("/admin/auth/logout", (req, res) => {
    adminAuth.clearSessionCookie(req, res);
    res.json({ status: "ok" });
  });
  app.get("/admin/auth/session", (req, res) =>
    res.json({ status: "ok", authenticated: adminAuth.validSession(req) }),
  );
  app.use("/admin", (req, res, next) => {
    if (req.path.startsWith("/login/") || req.path === "/tailwind.css") return next();
    return adminAuth.requireAdminPage(req, res, next);
  });
  app.use(express.static(path.join(__dirname, "../public")));
  app.use((req, res, next) => {
    const origins = String(process.env.FRONTEND_ORIGINS || "http://localhost:5173")
      .split(",")
      .map((value) => value.trim().replace(/\/$/, ""))
      .filter(Boolean);
    const origin = req.headers.origin;
    if (origin && (origins.includes("*") || origins.includes(origin))) {
      res.setHeader("Access-Control-Allow-Origin", origins.includes("*") ? "*" : origin);
      res.setHeader("Vary", "Origin");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Internal-API-Key");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    }
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
  });
  app.use(
    ["/api/events/subscriptions", "/api/provider", "/api/source", "/api/logs", "/api/redis"],
    adminAuth.requireAdminApi,
  );
  app.get("/health", async (req, res) => {
    try {
      await checkSourceDbConnection();
      const supervisor = getHealthStatus();
      res.status(supervisor.status === "critical" ? 503 : 200).json({
        status: supervisor.status === "critical" ? "critical" : "ok",
        supervisor,
        sourceDatabase: "connected",
        redis: redis.getRedisStatus(),
        websocket: websocket.getSocketStatus(),
        pipelines: {
          competition: getCompetitionSyncStatus(),
          events: getEventSyncStatus(),
          discovery: getMarketDiscoveryStatus(),
          subscriptions: getMarketSyncStatus(),
          results: getResultSyncStatus(),
          redisEventCleanup: getRedisEventCleanupStatus(),
          providerQueue: providerLimiter.counts(),
        },
      });
    } catch (error) {
      const supervisor = getHealthStatus();
      res.status(503).json({
        status: "error",
        supervisor,
        sourceDatabase: "disconnected",
        redis: redis.getRedisStatus(),
        websocket: websocket.getSocketStatus(),
        pipelines: {
          competition: getCompetitionSyncStatus(),
          events: getEventSyncStatus(),
          discovery: getMarketDiscoveryStatus(),
          subscriptions: getMarketSyncStatus(),
          results: getResultSyncStatus(),
          redisEventCleanup: getRedisEventCleanupStatus(),
          providerQueue: providerLimiter.counts(),
        },
        message: error.message,
      });
    }
  });
  app.get("/db-time", async (req, res, next) => {
    try {
      const [rows] = await getSourcePool().query("SELECT NOW() AS currentTime");
      res.json({ status: "ok", data: rows[0] });
    } catch (error) {
      next(error);
    }
  });
  app.get("/api/socket/status", (req, res) => res.json({ status: "ok", data: websocket.getSocketStatus() }));
  app.get("/betfair_api/fancy/score/:eventId", redisController.eventScore);
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
