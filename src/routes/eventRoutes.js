const express = require("express");
const events = require("../controllers/eventController");
const subscriptions = require("../controllers/subscriptionController");
const router = express.Router();
router.get("/", events.list);
router.get("/subscriptions", subscriptions.listSubscriptions);
router.get("/:id", events.get);
module.exports = router;
