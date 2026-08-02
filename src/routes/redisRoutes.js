const express = require("express");
const redis = require("../controllers/redisController");

const router = express.Router();
router.get("/ticks", redis.list);
router.get("/markets/:marketId", redis.market);
module.exports = router;
