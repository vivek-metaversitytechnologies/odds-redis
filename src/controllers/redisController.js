const redis = require("../config/redis");
const dashboard = require("../services/dashboardService");

const activeMatchCache = new Map();
const activeMatchLoads = new Map();

function activeMatchCacheMs() {
  const value = Number(process.env.PUBLIC_API_ACTIVE_MATCH_CACHE_MS || 350);
  return Number.isFinite(value) && value >= 0 ? value : 350;
}

async function loadActiveMatches(sportId, timings) {
  const cached = activeMatchCache.get(sportId);
  if (cached && cached.expiresAt > Date.now()) {
    timings.activeMatchSource = "memory";
    return cached.data;
  }
  if (activeMatchLoads.has(sportId)) {
    timings.activeMatchSource = "coalesced";
    return activeMatchLoads.get(sportId);
  }
  const loading = dashboard
    .activeMatchesFromRedis(sportId, timings)
    .then((data) => {
      if (data !== null) {
        activeMatchCache.set(sportId, { data, expiresAt: Date.now() + activeMatchCacheMs() });
      }
      return data;
    })
    .finally(() => activeMatchLoads.delete(sportId));
  activeMatchLoads.set(sportId, loading);
  return loading;
}

function disableCaching(res) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
}

async function list(req, res, next) {
  try {
    const result = await redis.inspectTicks({
      eventId: req.query.eventId,
      marketId: req.query.marketId,
      limit: req.query.limit,
      includePayload: req.query.includePayload === "true",
    });
    res.json({ status: "ok", data: result });
  } catch (error) {
    next(error);
  }
}

async function market(req, res, next) {
  try {
    const result = await redis.inspectTicks({
      marketId: req.params.marketId,
      limit: 20,
      includePayload: true,
    });
    if (!result.items.length)
      return res.status(404).json({ status: "error", message: "Market data not found in Redis" });
    res.json({ status: "ok", data: result.items[0] });
  } catch (error) {
    next(error);
  }
}

async function eventSnapshot(req, res, next) {
  try {
    disableCaching(res);
    const eventId = String(req.params.eventId || "").trim();
    if (!/^\d+$/.test(eventId) || Number(eventId) <= 0) {
      return res.status(400).json({ status: "error", message: "A positive numeric event ID is required" });
    }
    const snapshot = await redis.getEventSnapshot(eventId);
    res.json(snapshot);
  } catch (error) {
    next(error);
  }
}

async function eventScore(req, res, next) {
  try {
    disableCaching(res);
    const eventId = String(req.params.eventId || "").trim();
    if (!/^\d+$/.test(eventId) || Number(eventId) <= 0) {
      return res
        .status(400)
        .json({ success: false, message: "A positive numeric event ID is required", data: null });
    }
    const score = await redis.getScore(eventId);
    res.json({
      success: true,
      message: "Data Fetch Successfully",
      data: score || { eid: Number(eventId), data: "" },
    });
  } catch (error) {
    next(error);
  }
}

async function activeMatches(req, res, next) {
  try {
    disableCaching(res);
    const sportId = Number(req.params.sportId);
    if (!Number.isInteger(sportId) || sportId <= 0) {
      return res
        .status(400)
        .json({ status: false, message: "A positive numeric sport ID is required", data: [] });
    }
    const data = await dashboard.activeMatches(sportId);
    res.json({ status: true, message: "Data Fetch Successfully", data });
  } catch (error) {
    next(error);
  }
}

async function activeMatchesRedisOnly(req, res, next) {
  const startedAt = process.hrtime.bigint();
  const timings = {};
  try {
    disableCaching(res);
    const sportId = Number(req.params.sportId);
    if (!Number.isInteger(sportId) || sportId <= 0) {
      return res
        .status(400)
        .json({ status: false, message: "A positive numeric sport ID is required", data: [] });
    }
    const data = await loadActiveMatches(sportId, timings);
    timings.controllerMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
    res.set(
      "Server-Timing",
      [
        `redis-events;dur=${(timings.redisEventsMs || 0).toFixed(1)}`,
        `redis-events-command;dur=${(timings.redisEventsCommandMs || 0).toFixed(1)}`,
        `events-parse;dur=${(timings.eventsParseMs || 0).toFixed(1)}`,
        `redis-snapshots;dur=${(timings.redisSnapshotsMs || 0).toFixed(1)}`,
        `redis-snapshots-command;dur=${(timings.redisSnapshotsCommandMs || 0).toFixed(1)}`,
        `snapshots-parse;dur=${(timings.snapshotsParseMs || 0).toFixed(1)}`,
        `transform;dur=${(timings.transformMs || 0).toFixed(1)}`,
        `controller;dur=${timings.controllerMs.toFixed(1)}`,
        `active-match;desc="${timings.activeMatchSource || "unknown"}"`,
        `compact-command;dur=${(timings.activeMatchCommandMs || 0).toFixed(1)}`,
        `compact-parse;dur=${(timings.activeMatchParseMs || 0).toFixed(1)}`,
      ].join(", "),
    );
    if (data === null) {
      return res.status(503).json({
        status: false,
        message: "Active-match data is not available in Redis",
        data: [],
      });
    }
    res.json({ status: true, message: "Data Fetch Successfully", data });
  } catch (error) {
    next(error);
  }
}

module.exports = { list, market, eventSnapshot, eventScore, activeMatches, activeMatchesRedisOnly };
