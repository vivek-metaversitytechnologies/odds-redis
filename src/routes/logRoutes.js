const express = require("express");
const logs = require("../controllers/logController");

const router = express.Router();
router.get("/socket", logs.rawSocketMarket);
router.get("/", logs.list);
module.exports = router;
