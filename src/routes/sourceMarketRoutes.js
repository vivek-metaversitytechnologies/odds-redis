const express = require("express");
const sourceMarkets = require("../controllers/sourceMarketController");

const router = express.Router();
router.get("/results/review", async (req, res, next) => {
  try {
    const cursor = String(req.query.cursor || "0");
    if (!/^\d+$/.test(cursor)) return res.status(400).json({ message: "Invalid cursor" });
    const data = await require("../services/pendingResultQueue").listReview(cursor);
    res.json({ status: "ok", data });
  } catch (error) { next(error); }
});
router.post("/results/review/reconcile", async (req, res, next) => {
  try {
    const data = await require("../cron/resultSync").reconcileReviewed(req.body?.marketIds);
    res.json({ status: "ok", data });
  } catch (error) {
    if (error.status) return res.status(error.status).json({ message: error.message });
    next(error);
  }
});
router.get("/overview", sourceMarkets.overview);
router.get("/markets", sourceMarkets.list);
router.get("/fancies", sourceMarkets.listFancies);
router.get("/markets/discovery", sourceMarkets.marketDiscoveryStatus);
router.get("/competitions", sourceMarkets.listCompetitions);
router.get("/competitions/sync", sourceMarkets.competitionSyncStatus);
router.post("/competitions/sync", sourceMarkets.runCompetitionSync);
router.get("/events", sourceMarkets.listEvents);
router.get("/events/sync", sourceMarkets.eventSyncStatus);
router.post("/events/sync", sourceMarkets.runEventSync);
router.get("/sync", sourceMarkets.syncStatus);
router.post("/sync", sourceMarkets.runSync);
router.get("/results/sync", sourceMarkets.resultSyncStatus);
router.post("/results/sync", sourceMarkets.runResultSync);
router.get("/results", sourceMarkets.listResults);
router.post("/subscribe", sourceMarkets.subscribeManual);
router.post("/unsubscribe-all", sourceMarkets.unsubscribeAllMarkets);
router.post("/events/:eventId/unsubscribe", sourceMarkets.unsubscribeEvent);
module.exports = router;
