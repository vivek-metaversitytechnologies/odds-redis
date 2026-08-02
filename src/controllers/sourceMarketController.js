const { getSourcePool } = require("../config/sourceDb");
const { getMarketSyncStatus, syncMarketSubscriptions,
  recordMarketSyncActivity } = require("../cron/marketSync");
const subscriptions = require("../services/marketSubscriptionService");
const { getCompetitionSyncStatus, syncCompetitions } = require("../cron/competitionSync");
const { getEventSyncStatus, syncEvents } = require("../cron/eventSync");
const { getMarketDiscoveryStatus } = require("../cron/marketDiscoverySync");
const { getResultSyncStatus, syncResults } = require("../cron/resultSync");

async function list(req, res, next) {
  try {
    const sportIds = String(process.env.SPORT_IDS || "1,2,4").split(",")
      .map((value) => Number(value.trim())).filter(Number.isFinite);
    const [rows] = await getSourcePool().query(
      `SELECT * FROM t_market WHERE isactive = ? AND sportid IN (${sportIds.map(() => "?").join(",")})
       ORDER BY sportid ASC, id DESC`,
      [true, ...sportIds],
    );
    res.json({ status: "ok", data: rows, meta: { total: rows.length, sportIds } });
  } catch (error) { next(error); }
}

async function overview(req, res, next) {
  try {
    const [[counts]] = await getSourcePool().query(
      `SELECT
        (SELECT COUNT(*) FROM t_series WHERE sportid IN (1,2,4)) AS competitions,
        (SELECT COUNT(*) FROM t_event WHERE sportid IN (1,2,4)) AS events,
        ((SELECT COUNT(*) FROM t_market WHERE isactive = 1 AND sportid IN (1,2,4))
         + (SELECT COUNT(*) FROM t_matchfancy WHERE isactive = 1 AND sportid IN (1,2,4))) AS activeMarkets`,
    );
    res.json({ status: "ok", data: counts });
  } catch (error) { next(error); }
}

async function listCompetitions(req, res, next) {
  try {
    const [rows] = await getSourcePool().query(
      `SELECT id, seriesid, seriesname, sportid, isactive, status, updatedon
       FROM t_series WHERE sportid IN (?, ?, ?) ORDER BY sportid ASC, seriesname ASC`,
      [1, 2, 4],
    );
    res.json({ status: "ok", data: rows, meta: { total: rows.length, sportIds: [1, 2, 4] } });
  } catch (error) { next(error); }
}

function competitionSyncStatus(req, res) {
  res.json({ status: "ok", data: getCompetitionSyncStatus() });
}

async function runCompetitionSync(req, res, next) {
  try {
    const result = await syncCompetitions();
    res.json({ status: "ok", data: { result, sync: getCompetitionSyncStatus() } });
  } catch (error) { next(error); }
}

async function listEvents(req, res, next) {
  try {
    const [rows] = await getSourcePool().query(
      `SELECT e.id, e.eventid, e.eventname, e.seriesid, e.sportid, e.open_date,
              e.in_play, e.isactive, e.status, e.updatedon, s.seriesname
       FROM t_event e LEFT JOIN t_series s ON s.seriesid = e.seriesid
       WHERE e.sportid IN (?, ?, ?) ORDER BY e.open_date ASC`,
      [1, 2, 4],
    );
    res.json({ status: "ok", data: rows, meta: { total: rows.length, sportIds: [1, 2, 4] } });
  } catch (error) { next(error); }
}

async function listFancies(req, res, next) {
  try {
    const [rows] = await getSourcePool().query(
      `SELECT f.id, f.fancyid, f.name, f.oddstype, f.status, f.eventid, f.isactive,
              f.isshow, f.minbet, f.maxbet, f.betdelay, f.updatedon,
              COALESCE(f.sportid,e.sportid) AS sportid, e.eventname AS matchname
       FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
       WHERE COALESCE(f.sportid,e.sportid) IN (?, ?, ?)
       ORDER BY f.eventid DESC, f.id DESC`, [1, 2, 4],
    );
    res.json({ status: "ok", data: rows, meta: { total: rows.length, sportIds: [1, 2, 4] } });
  } catch (error) { next(error); }
}

function marketDiscoveryStatus(req, res) {
  res.json({ status: "ok", data: getMarketDiscoveryStatus() });
}

function eventSyncStatus(req, res) { res.json({ status: "ok", data: getEventSyncStatus() }); }

async function runEventSync(req, res, next) {
  try {
    const result = await syncEvents();
    res.json({ status: "ok", data: { result, sync: getEventSyncStatus() } });
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

function resultSyncStatus(req, res) {
  res.json({ status: "ok", data: getResultSyncStatus() });
}

async function runResultSync(req, res, next) {
  try {
    const result = await syncResults();
    res.json({ status: "ok", data: { result, sync: getResultSyncStatus() } });
  } catch (error) { next(error); }
}

async function listResults(req, res, next) {
  try {
    const limit = Math.max(1, Math.min(500, Number(req.query.limit) || 250));
    const [marketRows] = await getSourcePool().query(
      `SELECT 'market' AS resulttype, id, marketid, marketname, matchid, matchname,
              selectionid, selectionname, result, resultstatus, status, date AS declaredat
       FROM t_matchresult ORDER BY id DESC LIMIT ?`, [limit],
    );
    const [fancyRows] = await getSourcePool().query(
      `SELECT 'fancy' AS resulttype, id, fancyid AS marketid, fancyname AS marketname,
              matchid, matchname, NULL AS selectionid, NULL AS selectionname, result,
              resultstatus, isresult AS status, updatedon AS declaredat
       FROM t_fancyresult ORDER BY id DESC LIMIT ?`, [limit],
    );
    const [exceptionalRows] = await getSourcePool().query(
      `SELECT 'exceptional' AS resulttype, id, marketid, marketname, matchid, matchname,
              NULL AS selectionid, NULL AS selectionname, result, result AS resultstatus,
              status, date AS declaredat
       FROM t_matchabondendtie ORDER BY id DESC LIMIT ?`, [limit],
    );
    const [[counts]] = await getSourcePool().query(
      `SELECT (SELECT COUNT(*) FROM t_matchresult) AS market,
              (SELECT COUNT(*) FROM t_fancyresult) AS fancy,
              (SELECT COUNT(*) FROM t_matchabondendtie) AS exceptional`,
    );
    const rows = [...marketRows, ...fancyRows, ...exceptionalRows]
      .sort((left, right) => Number(right.id) - Number(left.id)).slice(0, limit);
    res.json({ status: "ok", data: rows, meta: counts });
  } catch (error) { next(error); }
}

async function unsubscribeAllMarkets(req, res, next) {
  try {
    const result = await subscriptions.unsubscribeAll();
    recordMarketSyncActivity("all.unsubscribe.completed", { markets: result.unsubscribed.length });
    res.json({ status: "ok", data: result });
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

async function unsubscribeEvent(req, res, next) {
  try {
    const eventId = String(req.params.eventId || "").trim();
    if (!/^\d+$/.test(eventId) || Number(eventId) <= 0) {
      const error = new Error("A positive numeric event ID is required"); error.statusCode = 400; throw error;
    }
    const [rows] = await getSourcePool().query(
      `SELECT marketid FROM t_market WHERE eventid = ? AND marketid IS NOT NULL
       UNION SELECT fancyid AS marketid FROM t_matchfancy WHERE eventid = ? AND fancyid IS NOT NULL`,
      [eventId, eventId],
    );
    const marketIds = [...new Set(rows.map((row) => String(row.marketid).trim()).filter(Boolean))];
    if (!marketIds.length) {
      const error = new Error("No markets found for this event"); error.statusCode = 404; throw error;
    }
    recordMarketSyncActivity("event.unsubscribe.started", { eventId, markets: marketIds.length });
    const result = await subscriptions.unsubscribeEventMarkets(marketIds);
    recordMarketSyncActivity("event.unsubscribe.completed", {
      eventId, markets: result.unsubscribed.length,
    });
    res.json({ status: "ok", data: { eventId, ...result } });
  } catch (error) {
    recordMarketSyncActivity("event.unsubscribe.failed", {
      eventId: req.params.eventId, error: error.message,
    });
    next(error);
  }
}

module.exports = { list, listFancies, overview, listCompetitions, competitionSyncStatus, runCompetitionSync,
  marketDiscoveryStatus,
  listEvents, eventSyncStatus, runEventSync,
  syncStatus, runSync, resultSyncStatus, runResultSync, listResults,
  subscribeManual, unsubscribeEvent, unsubscribeAllMarkets };
