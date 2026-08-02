const redis = require("../config/redis");
const dashboard = require("../services/dashboardService");

function disableCaching(res) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
}

async function list(req, res, next) {
  try {
    const result = await redis.inspectTicks({ eventId: req.query.eventId, marketId: req.query.marketId,
      limit: req.query.limit, includePayload: req.query.includePayload === "true" });
    res.json({ status: "ok", data: result });
  } catch (error) { next(error); }
}

async function market(req, res, next) {
  try {
    const result = await redis.inspectTicks({ marketId: req.params.marketId, limit: 20, includePayload: true });
    if (!result.items.length) return res.status(404).json({ status: "error", message: "Market data not found in Redis" });
    res.json({ status: "ok", data: result.items[0] });
  } catch (error) { next(error); }
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
  } catch (error) { next(error); }
}

async function activeMatches(req, res, next) {
  try {
    disableCaching(res);
    const sportId = Number(req.params.sportId);
    if (!Number.isInteger(sportId) || sportId <= 0) {
      return res.status(400).json({ status: false, message: "A positive numeric sport ID is required", data: [] });
    }
    const data = await dashboard.activeMatches(sportId);
    res.json({ status: true, message: "Data Fetch Successfully", data });
  } catch (error) { next(error); }
}

module.exports = { list, market, eventSnapshot, activeMatches };
