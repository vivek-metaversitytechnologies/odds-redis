const express = require("express");
const provider = require("../controllers/providerController");

const router = express.Router();
router.get("/sports", provider.sports);
router.get("/competitions", provider.competitions);
router.get("/events", provider.events);
router.post("/markets", provider.markets);
router.get("/markets/:marketId/runners", provider.runners);
router.post("/markets/results", provider.results);
module.exports = router;
