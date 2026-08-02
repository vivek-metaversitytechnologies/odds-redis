const redis = require("../config/redis");

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

module.exports = { list, market };
