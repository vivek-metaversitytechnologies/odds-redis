const { io } = require("socket.io-client");
const crypto = require("node:crypto");
const redisStore = require("../config/redis");
const logger = require("../utils/logger");
const { writeProviderLog } = require("../utils/providerFileLogger");
const { integer } = require("../config/env");
const { setBounded } = require("../utils/boundedMap");

let socket;
const eventWriteChains = new Map();
const pendingEventTicks = new Map();
// Coalesce each event within a short fixed window. Result ticks bypass the delay.
const TICK_COALESCE_MS = integer("PROVIDER_TICK_COALESCE_MS", 100, { min: 0, max: 250 });
let tickPublisher = () => {};
let scorePublisher = () => {};
let rawTickPublisher = () => {};
let resultHandler = async () => {};
const loggedShapes = new Set();
const rawSocketActivity = [];
const subscribedMarketIds = new Set();
const scoreHashes = new Map();
const SCORE_HASH_LIMIT = integer("SCORE_HASH_CACHE_LIMIT", 50000, { min: 1000 });
const state = {
  connectionRequested: false,
  connected: false,
  socketId: null,
  lastConnectedAt: null,
  lastDisconnectedAt: null,
  lastConnectError: null,
  socketMessageCount: 0,
  tickCount: 0,
  scoreUpdateCount: 0,
  scorePersistedCount: 0,
  scoreUnchangedCount: 0,
  unknownMessageCount: 0,
  persistedTickCount: 0,
  unchangedTickCount: 0,
  failedTickCount: 0,
  lastTickAt: null,
  lastTickSummary: null,
};

function summarize(item) {
  if (!item || typeof item !== "object") return { type: typeof item };
  return {
    eid: item.eid ?? null,
    mid: item.mid ?? null,
    name: item.na ?? null,
    status: item.s ?? null,
    ts: item.t ?? null,
    level: item.level ?? null,
  };
}

function isResultTick(item) {
  return Boolean(item && item.mid != null && item.go === true);
}

function collectOddsTicks(value, output = [], visited = new Set()) {
  if (!value || typeof value !== "object" || visited.has(value)) return output;
  visited.add(value);
  if (Array.isArray(value)) {
    value.forEach((item) => collectOddsTicks(item, output, visited));
    return output;
  }
  if (value.eid != null && value.mid != null) {
    output.push(value);
    return output;
  }
  Object.values(value).forEach((item) => collectOddsTicks(item, output, visited));
  return output;
}

function collectScores(value) {
  const score = value?.message?.score;
  return Array.isArray(score) ? score : score && typeof score === "object" ? [score] : [];
}

async function persistScores(scores, receivedAtMs = Date.now()) {
  // A provider packet can contain the same event more than once. Keep only its
  // final scorecard and avoid rewriting/re-emitting unchanged HTML heartbeats.
  const latestByEvent = new Map();
  for (const score of scores || []) {
    const eventId = String(score?.eid ?? score?.eventId ?? "").trim();
    const html = score?.data ?? score?.html ?? score?.scorecard;
    if (/^\d+$/.test(eventId) && typeof html === "string" && html.trim()) {
      latestByEvent.set(eventId, score);
    }
  }
  const writes = [...latestByEvent.entries()].map(async ([eventId, score]) => {
    const html = score?.data ?? score?.html ?? score?.scorecard;
    const hash = crypto.createHash("sha1").update(html).digest("base64url");
    if (scoreHashes.get(eventId) === hash) {
      state.scoreUnchangedCount += 1;
      return false;
    }
    setBounded(scoreHashes, eventId, hash, SCORE_HASH_LIMIT);
    try {
      const payload = await redisStore.writeScore(score);
      if (!payload) {
        if (scoreHashes.get(eventId) === hash) scoreHashes.delete(eventId);
        return false;
      }
      state.scorePersistedCount += 1;
      scorePublisher(String(payload.eid), payload, receivedAtMs);
      return true;
    } catch (error) {
      if (scoreHashes.get(eventId) === hash) scoreHashes.delete(eventId);
      logger.error("[ProviderWS] scorecard write failed", { error: error.message });
      return false;
    }
  });
  await Promise.allSettled(writes);
}

function messageShape(value) {
  const keys = value && typeof value === "object" && !Array.isArray(value) ? Object.keys(value).sort() : [];
  const messageKeys =
    value?.message && typeof value.message === "object" && !Array.isArray(value.message)
      ? Object.keys(value.message).sort()
      : [];
  return { type: Array.isArray(value) ? "array" : typeof value, keys, messageKeys };
}

function logShape(classification, value) {
  const shape = messageShape(value);
  const signature = `${classification}:${shape.type}:${shape.keys.join(",")}:${shape.messageKeys.join(",")}`;
  if (loggedShapes.has(signature) || loggedShapes.size >= 20) return;
  loggedShapes.add(signature);
  writeProviderLog("provider.socket.shape", { classification, ...shape });
}

function logRawSocketPayload(data) {
  if (String(process.env.PROVIDER_LOG_SOCKET_PAYLOADS || "false").toLowerCase() !== "true") return;
  rawSocketActivity.unshift({ timestamp: new Date().toISOString(), payload: data });
  rawSocketActivity.splice(500);
  writeProviderLog("provider.socket.raw", { payload: data });
}

function payloadContainsMarket(value, marketId, visited = new Set()) {
  if (!value || typeof value !== "object" || visited.has(value)) return false;
  visited.add(value);
  if (value.mid != null && String(value.mid) === String(marketId)) return true;
  return Object.values(value).some((item) => payloadContainsMarket(item, marketId, visited));
}

function getRawSocketPayloads(marketId, limit = 20) {
  const boundedLimit = Math.max(1, Math.min(100, Number(limit) || 20));
  return rawSocketActivity
    .filter((record) => payloadContainsMarket(record.payload, marketId))
    .slice(0, boundedLimit);
}

function logSocketTiming(details) {
  if (String(process.env.PROVIDER_LOG_SOCKET_TIMINGS || "false").toLowerCase() !== "true") return;
  writeProviderLog("provider.socket.timing", details);
}

async function persist(items, receivedAtMs = Date.now()) {
  const resultedMarketIds = new Set();
  const writeStartedAt = Date.now();
  try {
    const result = await redisStore.writeTicks(items);
    const accepted = result.accepted || [];
    if (result.changed) state.persistedTickCount += accepted.length;
    else state.unchangedTickCount += accepted.length;
    state.failedTickCount += (result.rejected || []).length;
    if (result.payload && accepted.length) {
      const lastWriteCompletedAt = Date.now();
      for (const item of accepted) {
        const providerTimestamp = Number(item.t);
        logSocketTiming({
          eventId: String(item.eid),
          marketId: String(item.mid),
          providerTimestamp: Number.isFinite(providerTimestamp) ? providerTimestamp : null,
          receivedAt: new Date(receivedAtMs).toISOString(),
          providerToBackendMs: Number.isFinite(providerTimestamp)
            ? Math.max(0, receivedAtMs - providerTimestamp)
            : null,
          queueDelayMs: writeStartedAt - receivedAtMs,
          redisWriteMs: lastWriteCompletedAt - writeStartedAt,
          batchMarkets: accepted.length,
        });
        if (isResultTick(item)) resultedMarketIds.add(String(item.mid));
      }
      if (result.changed) {
        const eventId = String(accepted[0].eid);
        const emitStartedAt = Date.now();
        tickPublisher(eventId, result.payload);
        logSocketTiming({
          eventId,
          marketId: String(accepted.at(-1).mid),
          stage: "frontend.emit",
          backendProcessingMs: emitStartedAt - receivedAtMs,
          postRedisToEmitMs: emitStartedAt - lastWriteCompletedAt,
          emitCallMs: Date.now() - emitStartedAt,
          batchMarkets: accepted.length,
        });
      }
    }
  } catch (error) {
    state.failedTickCount += items.length;
    logger.error("[ProviderWS] Redis batch write failed", { error: error.message, ticks: items.length });
  }
  if (resultedMarketIds.size) {
    try {
      await resultHandler([...resultedMarketIds]);
    } catch (error) {
      logger.error("[ProviderWS] result unsubscribe handler failed", {
        marketIds: [...resultedMarketIds],
        error: error.message,
      });
    }
  }
}

function enqueueEventTicks(eventId, items, receivedAtMs) {
  const key = String(eventId);
  let pending = pendingEventTicks.get(key);
  if (!pending) {
    pending = { items: new Map(), receivedAtMs, waiters: [], timer: null, immediate: false };
    pendingEventTicks.set(key, pending);
  }
  pending.receivedAtMs = Math.min(pending.receivedAtMs, receivedAtMs);
  for (const item of items || []) {
    const marketId = String(item.mid);
    const current = pending.items.get(marketId);
    // Settlement must never be replaced by a late non-result update in the same window.
    if (!isResultTick(current) || isResultTick(item)) pending.items.set(marketId, item);
  }
  const resultTick = (items || []).some(isResultTick);
  pending.immediate ||= resultTick;
  const completion = new Promise((resolve, reject) => pending.waiters.push({ resolve, reject }));
  if (pending.immediate) {
    if (pending.timer) clearTimeout(pending.timer);
    pending.timer = setTimeout(() => flushPendingEvent(key), 0);
    pending.timer.unref?.();
  } else if (!pending.timer) {
    // This is a fixed window from the first tick, not a debounce. Restarting the
    // timer for every update can starve a busy event indefinitely.
    pending.timer = setTimeout(() => flushPendingEvent(key), TICK_COALESCE_MS);
    pending.timer.unref?.();
  }
  return completion;
}

function flushPendingEvent(key) {
  const pending = pendingEventTicks.get(key);
  if (!pending) return eventWriteChains.get(key) || Promise.resolve();
  pendingEventTicks.delete(key);
  if (pending.timer) clearTimeout(pending.timer);
  const previous = eventWriteChains.get(key) || Promise.resolve();
  const next = previous
    .catch(() => {})
    .then(() => persist([...pending.items.values()], pending.receivedAtMs));
  eventWriteChains.set(key, next);
  const cleanup = () => {
    if (eventWriteChains.get(key) === next) eventWriteChains.delete(key);
  };
  next.then(
    (value) => pending.waiters.forEach(({ resolve }) => resolve(value)),
    (error) => pending.waiters.forEach(({ reject }) => reject(error)),
  );
  next.then(cleanup, cleanup);
  return next;
}

function connectSocket() {
  state.connectionRequested = true;
  if (socket) {
    if (!socket.connected && !socket.active) socket.connect();
    return socket;
  }
  const url = process.env.PROVIDER_SOCKET_URL;
  if (!url) {
    logger.warn("[ProviderWS] PROVIDER_SOCKET_URL is not set");
    return null;
  }
  socket = io(url, {
    path: process.env.PROVIDER_SOCKET_PATH || "/stream",
    transports: ["websocket"],
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: integer("PROVIDER_SOCKET_RECONNECT_DELAY_MS", 1000, { min: 100 }),
    reconnectionDelayMax: integer("PROVIDER_SOCKET_RECONNECT_DELAY_MAX_MS", 10000, { min: 1000 }),
    timeout: integer("PROVIDER_SOCKET_TIMEOUT_MS", 15000, { min: 1000 }),
    extraHeaders: {
      "x-api-key": process.env.PROVIDER_TOKEN || process.env.PROVIDER_X_API_KEY || "dummy_key",
    },
  });
  socket.on("connect", () => {
    state.connected = true;
    state.socketId = socket.id;
    state.lastConnectedAt = new Date().toISOString();
    state.lastConnectError = null;
    logger.info("[ProviderWS] connected", { socketId: socket.id });
    if (subscribedMarketIds.size) socket.emit("subscribe", [...subscribedMarketIds]);
  });
  socket.on("tick", (data) => {
    const receivedAtMs = Date.now();
    const messages = Array.isArray(data) ? data : [data];
    state.socketMessageCount += messages.length;
    const oddsTicks = collectOddsTicks(data);
    const scores = messages.flatMap(collectScores);
    if (scores.length || oddsTicks.length) logRawSocketPayload(data);
    if (scores.length) {
      state.scoreUpdateCount += scores.length;
      logShape("score", messages[0]);
      void persistScores(scores, receivedAtMs);
    }
    if (oddsTicks.length) {
      for (const item of oddsTicks) {
        try {
          rawTickPublisher(item, receivedAtMs);
        } catch (error) {
          logger.error("[ProviderWS] raw tick publish failed", { error: error.message });
        }
      }
      state.tickCount += oddsTicks.length;
      state.lastTickAt = new Date().toISOString();
      state.lastTickSummary = summarize(oddsTicks.at(-1));
      logShape("odds", messages[0]);
      const ticksByEvent = new Map();
      for (const item of oddsTicks) {
        const eventId = String(item.eid);
        if (!ticksByEvent.has(eventId)) ticksByEvent.set(eventId, []);
        ticksByEvent.get(eventId).push(item);
      }
      for (const [eventId, eventTicks] of ticksByEvent) {
        enqueueEventTicks(eventId, eventTicks, receivedAtMs);
      }
    }
    if (!scores.length && !oddsTicks.length) {
      state.unknownMessageCount += messages.length;
      logShape("unknown", messages[0]);
    }
  });
  socket.on("disconnect", (reason) => {
    state.connected = false;
    state.socketId = null;
    state.lastDisconnectedAt = new Date().toISOString();
    logger.warn("[ProviderWS] disconnected", { reason });
  });
  socket.on("connect_error", (error) => {
    state.connected = false;
    state.lastConnectError = error.message;
    logger.error("[ProviderWS] connection error", { error: error.message });
  });
  return socket;
}

function subscribeMarkets(ids) {
  const fresh = [
    ...new Set(
      (ids || [])
        .map(String)
        .map((id) => id.trim())
        .filter(Boolean),
    ),
  ].filter((id) => !subscribedMarketIds.has(id));
  fresh.forEach((id) => subscribedMarketIds.add(id));
  if (!fresh.length) return [];
  const current = connectSocket();
  if (current?.connected) current.emit("subscribe", fresh);
  return fresh;
}

function unsubscribeMarkets(ids) {
  const removed = [
    ...new Set(
      (ids || [])
        .map(String)
        .map((id) => id.trim())
        .filter(Boolean),
    ),
  ].filter((id) => subscribedMarketIds.delete(id));
  if (!removed.length) return [];
  if (socket?.connected) socket.emit("unsubscribe", removed);
  return removed;
}

async function stopSocket() {
  state.connectionRequested = false;
  if (socket) {
    socket.disconnect();
    socket = undefined;
  }
  await Promise.allSettled([...pendingEventTicks.keys()].map(flushPendingEvent));
  await Promise.allSettled([...eventWriteChains.values()]);
}

function getSocketStatus() {
  return {
    ...state,
    subscribedCount: subscribedMarketIds.size,
    pendingEventCount: pendingEventTicks.size,
    pendingTickCount: [...pendingEventTicks.values()].reduce((total, pending) => total + pending.items.size, 0),
    activeEventWriteCount: eventWriteChains.size,
    tickCoalesceMs: TICK_COALESCE_MS,
  };
}

module.exports = {
  connectSocket,
  subscribeMarkets,
  unsubscribeMarkets,
  stopSocket,
  getSocketStatus,
  getSubscribedMarketIds: () => [...subscribedMarketIds],
  collectOddsTicks,
  collectScores,
  messageShape,
  logRawSocketPayload,
  logSocketTiming,
  getRawSocketPayloads,
  payloadContainsMarket,
  isResultTick,
  enqueueEventTicks,
  setTickPublisher: (publisher) => {
    tickPublisher = typeof publisher === "function" ? publisher : () => {};
  },
  setScorePublisher: (publisher) => {
    scorePublisher = typeof publisher === "function" ? publisher : () => {};
  },
  setRawTickPublisher: (publisher) => {
    rawTickPublisher = typeof publisher === "function" ? publisher : () => {};
  },
  setResultHandler: (handler) => {
    resultHandler = typeof handler === "function" ? handler : async () => {};
  },
};
