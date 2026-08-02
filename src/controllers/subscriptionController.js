const redis = require("../config/redis");
const { getSourcePool } = require("../config/sourceDb");
const websocket = require("../services/websocketService");
const { getMarketSyncStatus } = require("../cron/marketSync");

async function listSubscriptions(req, res, next) {
  try {
    const [rows] = await getSourcePool().query(
      "SELECT * FROM t_market WHERE isactive = ? AND sportid = ? ORDER BY sportid ASC, id DESC",
      [true, Number(process.env.MARKET_SPORT_ID || 4)],
    );
    const subscribed = new Set(websocket.getSubscribedMarketIds());
    const retryStatus = getMarketSyncStatus().skippedRetry || {};
    const skipped = new Set(retryStatus.marketIds || []);
    const completed = new Set(retryStatus.completedMarketIds || []);
    const data = rows.map((market) => {
      const marketId = String(market.marketid || ""); const activity = redis.getTickActivity(marketId);
      const ageMs = activity ? Date.now() - new Date(activity.lastUpdatedAt).getTime() : null;
      const providerState = completed.has(marketId) ? "completed"
        : subscribed.has(marketId) ? "subscribed" : skipped.has(marketId) ? "skipped" : "pending";
      const freshness = activity ? ageMs <= 60000 ? "healthy" : ageMs <= 300000 ? "delayed" : "stale" : "waiting";
      return { id: market.id, marketId, marketName: market.marketname, eventId: String(market.eventid || ""),
        eventName: market.matchname, sportId: market.sportid, providerState,
        receiving: Boolean(activity), freshness, lastTickAt: activity?.lastUpdatedAt || null,
        tickCount: activity?.tickCount || 0 };
    });
    res.json({ status: "ok", data, meta: { total: data.length, socket: websocket.getSocketStatus() } });
  } catch (error) { next(error); }
}

module.exports = { listSubscriptions };
