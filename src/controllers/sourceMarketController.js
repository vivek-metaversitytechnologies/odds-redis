const { getSourcePool } = require("../config/sourceDb");
const { getMarketSyncStatus, syncMarketSubscriptions,
  recordMarketSyncActivity } = require("../cron/marketSync");
const subscriptions = require("../services/marketSubscriptionService");

async function list(req, res, next) {
  try {
    const [rows] = await getSourcePool().query(
      "SELECT * FROM t_market WHERE isactive = ? AND sportid = ? ORDER BY sportid ASC, id DESC",
      [true, 4],
    );
    res.json({ status: "ok", data: rows, meta: { total: rows.length, sportId: 4 } });
  } catch (error) { next(error); }
}

function syncStatus(req, res) {
  res.json({ status: "ok", data: getMarketSyncStatus() });
}

async function runSync(req, res, next) {
  try {
    const result = await syncMarketSubscriptions();
    res.json({ status: "ok", data: { result, sync: getMarketSyncStatus() } });
  } catch (error) { next(error); }
}

async function subscribeManual(req, res, next) {
  try {
    const marketIds = [...new Set((Array.isArray(req.body?.marketIds) ? req.body.marketIds : [])
      .map(String).map((id) => id.trim()).filter(Boolean))];
    if (!marketIds.length || marketIds.length > 100) {
      const error = new Error("Provide between 1 and 100 market IDs"); error.statusCode = 400; throw error;
    }
    recordMarketSyncActivity("manual.started", { marketIds: marketIds.join(",") });
    const result = await subscriptions.subscribeMarkets(marketIds);
    recordMarketSyncActivity("manual.completed", {
      requested: marketIds.length,
      subscribed: Array.isArray(result.subscribed) ? result.subscribed.length : 0,
      skipped: Array.isArray(result.skipped) ? result.skipped.length : 0,
    });
    res.json({ status: "ok", data: { requested: marketIds, ...result } });
  } catch (error) {
    recordMarketSyncActivity("manual.failed", { error: error.message });
    next(error);
  }
}

module.exports = { list, syncStatus, runSync, subscribeManual };
