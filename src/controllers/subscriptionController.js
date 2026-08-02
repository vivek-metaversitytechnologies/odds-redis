const redis = require("../config/redis");
const { getSourcePool } = require("../config/sourceDb");
const websocket = require("../services/websocketService");
const { getMarketSyncStatus } = require("../cron/marketSync");

async function listSubscriptions(req, res, next) {
  try {
    const sportIds = String(process.env.SPORT_IDS || "1,2,4").split(",")
      .map((value) => Number(value.trim())).filter(Number.isFinite);
    const placeholders = sportIds.map(() => "?").join(",");
    const [regular] = await getSourcePool().query(
      `SELECT id,marketid,marketname,eventid,matchname,sportid FROM t_market
       WHERE isactive=? AND sportid IN (${placeholders})
         AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid=t_market.marketid)
       ORDER BY id DESC`,
      [true, ...sportIds],
    );
    const [fancies] = await getSourcePool().query(
      `SELECT f.id,f.fancyid AS marketid,f.name AS marketname,f.eventid,
              COALESCE(f.matchname,e.eventname) AS matchname,COALESCE(f.sportid,e.sportid) AS sportid
       FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
       WHERE f.isactive=? AND COALESCE(f.sportid,e.sportid) IN (${placeholders})
         AND COALESCE(UPPER(f.status),'') NOT IN ('SUSPENDED','CLOSED')
         AND NOT EXISTS (SELECT 1 FROM t_fancyresult r WHERE r.fancyid=f.fancyid)
       ORDER BY f.id DESC`,
      [true, ...sportIds],
    );
    const rows = [...regular, ...fancies];
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
