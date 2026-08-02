const { Server } = require("socket.io");
const redis = require("../config/redis");
const providerSocket = require("./websocketService");
const logger = require("../utils/logger");
const { validSession } = require("../middleware/adminAuth");

let io;

function validEventId(value) {
  return /^\d+$/.test(String(value)) && Number(value) > 0;
}

function validMarketId(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 100
    && /^[A-Za-z0-9._-]+$/.test(value);
}

function allowedOrigins() {
  const configured = String(process.env.FRONTEND_ORIGINS || "http://localhost:5173")
    .split(",").map((value) => value.trim().replace(/\/$/, "")).filter(Boolean);
  return configured.includes("*") ? true : configured;
}

function adminSocketAuthorized(socket) {
  const required = process.env.INTERNAL_API_KEY;
  return validSession(socket.request) || (required && socket.handshake.auth?.apiKey === required)
    || (!required && !process.env.ADMIN_PANEL_PASSWORD);
}

function attachFrontendSocket(server) {
  io = new Server(server, { path: process.env.FRONTEND_SOCKET_PATH || "/socket.io",
    cors: { origin: allowedOrigins(), credentials: true } });
  io.on("connection", (client) => {
    client.on("subscribe:event", async (eventId, acknowledge = () => {}) => {
      if (!validEventId(eventId)) return acknowledge({ status: "error", message: "Invalid event ID" });
      const room = `event:${eventId}`;
      await client.join(room);
      try {
        const snapshot = await redis.getEventSnapshot(eventId);
        const stale = Object.values(snapshot).every((items) => !Array.isArray(items) || items.length === 0);
        acknowledge({ status: "ok", eventId: String(eventId), snapshot,
          snapshotAt: new Date().toISOString(), stale });
      } catch (error) {
        acknowledge({ status: "error", message: error.message });
      }
    });
    client.on("unsubscribe:event", (eventId) => {
      if (validEventId(eventId)) void client.leave(`event:${eventId}`);
    });
    client.on("subscribe:admin:market", async (marketId, acknowledge = () => {}) => {
      if (!adminSocketAuthorized(client)) return acknowledge({ status: "error", message: "Unauthorized" });
      const normalized = String(marketId || "").trim();
      if (!validMarketId(normalized)) return acknowledge({ status: "error", message: "Invalid market ID" });
      await client.join(`admin:market:${normalized}`);
      acknowledge({ status: "ok", marketId: normalized });
    });
    client.on("unsubscribe:admin:market", (marketId) => {
      const normalized = String(marketId || "").trim();
      if (validMarketId(normalized)) void client.leave(`admin:market:${normalized}`);
    });
  });
  providerSocket.setTickPublisher((eventId, payload) => {
    if (eventId != null) io.to(`event:${eventId}`).emit("tick", {
      eventId: String(eventId), data: payload, receivedAt: new Date().toISOString(),
    });
  });
  providerSocket.setRawTickPublisher((tick, receivedAtMs) => {
    const marketId = String(tick?.mid || "").trim();
    if (!validMarketId(marketId)) return;
    io.to(`admin:market:${marketId}`).emit("raw:market", {
      type: "provider.socket.raw", marketId,
      timestamp: new Date(receivedAtMs || Date.now()).toISOString(), payload: tick,
    });
  });
  logger.info("Frontend Socket.IO attached", { path: process.env.FRONTEND_SOCKET_PATH || "/socket.io" });
  return io;
}

async function publishEventSnapshot(eventId) {
  if (!io || !validEventId(eventId)) return false;
  const snapshot = await redis.getEventSnapshot(eventId);
  io.to(`event:${eventId}`).emit("tick", {
    eventId: String(eventId), data: snapshot, receivedAt: new Date().toISOString(),
    snapshot: true,
  });
  return true;
}

async function closeFrontendSocket() {
  if (!io) return;
  const current = io; io = undefined;
  providerSocket.setTickPublisher(null);
  providerSocket.setRawTickPublisher(null);
  await new Promise((resolve) => current.close(resolve));
}

module.exports = { attachFrontendSocket, publishEventSnapshot, closeFrontendSocket };
