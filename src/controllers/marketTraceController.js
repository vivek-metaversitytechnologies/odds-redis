const reader = require("../services/marketTraceReader");

// Reports read whole daily files, so concurrent reads are capped to protect the ingest process.
const MAX_CONCURRENT_READS = 2;
let activeReads = 0;

function bounded(value, fallback, min, max) {
  const number = Number(value);
  return Number.isInteger(number) ? Math.min(max, Math.max(min, number)) : fallback;
}

function identifier(value) {
  const id = String(value ?? "").trim();
  return id && id.length <= 100 ? id : null;
}

// GET /api/market-trace?view=anomalies|event|market|recent|files&id=&days=&kind=&stage=&limit=
async function report(req, res, next) {
  const view = String(req.query.view || "anomalies");
  const days = bounded(req.query.days, 1, 1, reader.MAX_DAYS);
  const kind = ["BB", "LINE"].includes(req.query.kind) ? req.query.kind : undefined;
  const id = identifier(req.query.id);
  if (["event", "market"].includes(view) && !id) {
    return res.status(400).json({ status: "error", message: `view=${view} requires id` });
  }
  if (!["anomalies", "event", "market", "recent", "files"].includes(view)) {
    return res.status(400).json({ status: "error", message: "view must be anomalies, event, market, recent or files" });
  }
  if (view === "files") return res.json({ status: "ok", data: reader.files() });
  if (activeReads >= MAX_CONCURRENT_READS) {
    return res.status(429).json({ status: "error", message: "Trace reports are busy; retry shortly" });
  }
  activeReads += 1;
  const startedAt = Date.now();
  try {
    let data;
    if (view === "market") data = await reader.marketTimeline(id, { days, limit: bounded(req.query.limit, 500, 1, 5000) });
    else if (view === "event") data = await reader.eventSummary(id, { days });
    else if (view === "recent") {
      data = await reader.recent({
        days,
        kind,
        stage: identifier(req.query.stage) ?? undefined,
        eventId: identifier(req.query.event) ?? undefined,
        limit: bounded(req.query.limit, 200, 1, 2000),
      });
    } else {
      data = await reader.anomalies({
        days,
        kind,
        eventId: identifier(req.query.event) ?? undefined,
        noTickMs: bounded(req.query.noTickMs, 120000, 1000, 86400000),
        noResultMs: bounded(req.query.noResultMs, 1800000, 1000, 86400000),
        perIssue: bounded(req.query.perIssue, 25, 1, 500),
      });
    }
    res.json({ status: "ok", view, days, durationMs: Date.now() - startedAt, data });
  } catch (error) {
    next(error);
  } finally {
    activeReads -= 1;
  }
}

module.exports = { report };
