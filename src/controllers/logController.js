const logs = require("../services/logReaderService");
const websocket = require("../services/websocketService");

async function list(req, res, next) {
  try {
    const source = ["all", "application", "provider"].includes(req.query.source) ? req.query.source : "all";
    const data = await logs.readLogs({ source, limit: req.query.limit });
    res.json({ status: "ok", data });
  } catch (error) { next(error); }
}

async function rawSocketMarket(req, res, next) {
  try {
    const marketId = String(req.query.marketId || "").trim();
    if (!marketId || marketId.length > 100) {
      return res.status(400).json({ status: "error", message: "A valid market ID is required" });
    }
    const liveRecords = websocket.getRawSocketPayloads(marketId, req.query.limit);
    if (liveRecords.length) {
      return res.json({ status: "ok", data: {
        marketId, records: liveRecords, file: "Live socket buffer", live: true,
      } });
    }
    const data = await logs.readRawSocketLogs({ marketId, limit: req.query.limit });
    res.json({ status: "ok", data });
  } catch (error) { next(error); }
}

module.exports = { list, rawSocketMarket };
