const express = require("express");
const sourceMarkets = require("../controllers/sourceMarketController");

const router = express.Router();
router.get("/markets", sourceMarkets.list);
router.get("/sync", sourceMarkets.syncStatus);
router.post("/sync", sourceMarkets.runSync);
router.post("/subscribe", sourceMarkets.subscribeManual);
module.exports = router;
