const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const { supportedSportIds } = require("./competitionSync");
const logger = require("../utils/logger");
const redis = require("../config/redis");
const subscriptions = require("../services/marketSubscriptionService");
const frontendSocket = require("../services/frontendSocketService");
const cronConfig = require("../config/cron");
const { utcToIstSql } = require("../utils/dateTime");

let running = false;
const state = {
  running: false,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastError: null,
  lastResult: null,
};

function eventRows(responses) {
  const sports = supportedSportIds();
  const rows = (responses || [])
    .flatMap((response) =>
      Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [],
    )
    .map((item) => ({
      eventId: Number(item?.id),
      eventName: String(item?.name || "").trim(),
      sportId: Number(item?.sportId),
      seriesId: Number(item?.leagueId),
      // Provider start times are UTC; persist an IST wall-clock DATETIME for consumers.
      openDate: utcToIstSql(item?.startTime),
      inPlay: Boolean(item?.inPlay),
      gameOver: Boolean(item?.gameOver),
    }))
    .filter(
      (item) =>
        Number.isInteger(item.eventId) &&
        item.eventName &&
        sports.has(item.sportId) &&
        Number.isInteger(item.seriesId) &&
        item.openDate,
    );
  const byId = new Map();
  for (const event of rows) {
    const current = byId.get(event.eventId);
    if (!current || event.gameOver || (!current.inPlay && event.inPlay)) byId.set(event.eventId, event);
  }
  return [...byId.values()].map((event) =>
    event.gameOver ? { ...event, inPlay: false } : event,
  );
}

function eventInsertValues(event) {
  return [
    event.seriesId,
    event.sportId,
    event.eventId,
    event.eventName,
    event.openDate,
    !event.gameOver,
    !event.gameOver,
    new Date(),
    new Date(),
    false,
    false,
    true,
    event.gameOver ? false : event.inPlay,
    false,
    true,
    true,
    "1",
  ];
}

async function upsertEvents(events) {
  if (!events.length) return { inserted: 0, updated: 0 };
  const connection = await getSourcePool().getConnection();
  try {
    const ids = events.map((event) => event.eventId);
    const [existingRows] = await connection.query(
      `SELECT eventid FROM t_event WHERE eventid IN (${ids.map(() => "?").join(",")})`,
      ids,
    );
    const existing = new Set(existingRows.map((row) => Number(row.eventid)));
    const values = events.map(eventInsertValues);
    await connection.query(
      `INSERT INTO t_event
        (seriesid,sportid,eventid,eventname,open_date,status,isactive,createdon,updatedon,
         fancypause,betlock,is_redis_updated,in_play,fancylock,bookmaker,fancy,channel_id)
       VALUES ?
       ON DUPLICATE KEY UPDATE eventname=VALUES(eventname),seriesid=VALUES(seriesid),
         sportid=VALUES(sportid),open_date=VALUES(open_date),in_play=VALUES(in_play),
         isactive=VALUES(isactive),status=VALUES(status),updatedon=NOW()`,
      [values],
    );
    return {
      inserted: events.filter((event) => !existing.has(event.eventId)).length,
      updated: events.filter((event) => existing.has(event.eventId)).length,
    };
  } finally {
    connection.release();
  }
}

async function restoreActiveEventMarkets(events) {
  const eventIds = [...new Set((events || [])
    .filter((event) => !event.gameOver)
    .map((event) => Number(event.eventId))
    .filter(Number.isInteger))];
  if (!eventIds.length) return 0;
  const placeholders = eventIds.map(() => "?").join(",");
  const [markets] = await getSourcePool().query(
    `SELECT marketid FROM t_market WHERE eventid IN (${placeholders})
     UNION SELECT fancyid AS marketid FROM t_matchfancy WHERE eventid IN (${placeholders})`,
    [...eventIds, ...eventIds],
  );
  const marketIds = markets.map((market) => String(market.marketid));
  subscriptions.restoreMarketEligibility(marketIds);
  return marketIds.length;
}

async function terminalPrimaryEventIds(events) {
  const eventIds = [...new Set((events || []).map((event) => Number(event.eventId)).filter(Number.isInteger))];
  if (!eventIds.length) return new Set();
  const placeholders = eventIds.map(() => "?").join(",");
  const [rows] = await getSourcePool().query(
    `SELECT eventid
       FROM t_market
     WHERE eventid IN (${placeholders})
        AND (LOWER(marketname)='match odds' OR LOWER(marketname) LIKE '%bookmaker%')
      GROUP BY eventid
     HAVING SUM(CASE WHEN isactive=? THEN 1 ELSE 0 END)=0
        AND SUM(CASE WHEN isactive=? AND status=? THEN 1 ELSE 0 END)>0`,
    [...eventIds, true, false, false],
  );
  return new Set(rows.map((row) => Number(row.eventid)).filter(Number.isInteger));
}

async function retireCompletedEvents(events) {
  const completed = events.filter((event) => event.gameOver);
  if (!completed.length) return { events: 0, markets: 0 };
  const eventIds = [...new Set(completed.map((event) => event.eventId))];
  const placeholders = eventIds.map(() => "?").join(",");
  const connection = await getSourcePool().getConnection();
  let marketIds = [];
  try {
    await connection.beginTransaction();
    const [markets] = await connection.query(
      `SELECT marketid FROM t_market WHERE eventid IN (${placeholders}) AND isactive = ?`,
      [...eventIds, true],
    );
    const [fancies] = await connection.query(
      `SELECT fancyid AS marketid FROM t_matchfancy WHERE eventid IN (${placeholders}) AND isactive = ?`,
      [...eventIds, true],
    );
    marketIds = [...markets, ...fancies].map((row) => String(row.marketid));
    if (!marketIds.length) {
      await connection.rollback();
    } else {
      await connection.query(
        `UPDATE t_market SET isactive = ?, status = ?, issubscribed = ?, updatedon = NOW()
         WHERE eventid IN (${placeholders})`,
        [false, false, false, ...eventIds],
      );
      await connection.query(
        `UPDATE t_matchfancy SET isactive = ?, isshow = ?, is_show = ?, issubscribed = ?,
           status = ?, updatedon = NOW() WHERE eventid IN (${placeholders})`,
        [false, false, false, false, "CLOSED", ...eventIds],
      );
      await connection.commit();
    }
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
  if (marketIds.length) {
    try {
      await subscriptions.unsubscribeEventMarkets(marketIds);
    } catch (error) {
      logger.warn("[EventSync] provider unsubscribe failed while retiring completed event", {
        eventIds,
        marketCount: marketIds.length,
        error: error.message,
      });
    }
  }
  await Promise.allSettled(
    eventIds.map(async (eventId) => {
      await redis.removeEvent(eventId);
      frontendSocket.publishEventRemoved(eventId, "completed");
    }),
  );
  return { events: eventIds.length, markets: marketIds.length };
}

async function syncEvents() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const sportIds = [...supportedSportIds()];
    const responseResults = await Promise.allSettled(
      sportIds.map((si) => provider.events({ si, today: 1 })),
    );
    const successfulSportIds = sportIds.filter(
      (_sportId, index) => responseResults[index]?.status === "fulfilled",
    );
    const responses = responseResults
      .filter((result) => result.status === "fulfilled")
      .map((result) => result.value);
    const received = responses.reduce(
      (total, response) =>
        total +
        (Array.isArray(response?.data)
          ? response.data.length
          : Array.isArray(response)
            ? response.length
            : 0),
      0,
    );
    const events = eventRows(responses);
    // Market lifecycle is more specific than the event feed. Some completed
    // matches remain inPlay=true at event level after their only primary market
    // has emitted gameOver. Preserve that terminal state until discovery sees a
    // replacement active Match Odds or Bookmaker market.
    const terminalEventIds = await terminalPrimaryEventIds(events);
    const effectiveEvents = events.map((event) =>
      terminalEventIds.has(event.eventId) ? { ...event, gameOver: true, inPlay: false } : event,
    );
    // Populate the public read cache before MySQL writes. Database lock or storage
    // pressure must not prevent fresh provider events from reaching public APIs.
    // Current vendor lifecycle state is authoritative. Completed rows never enter
    // the public cache, while a corrected active row can reopen a previously closed event.
    const cacheableEvents = effectiveEvents.filter((event) => !event.gameOver);
    const cached = await redis.writeEvents(cacheableEvents, successfulSportIds);
    const persisted = await upsertEvents(effectiveEvents);
    const restoredMarketEligibility = await restoreActiveEventMarkets(effectiveEvents);
    const retired = await retireCompletedEvents(effectiveEvents);
    const result = {
      skipped: false,
      received,
      supported: events.length,
      sportIds,
      ...persisted,
      cached,
      restoredMarketEligibility,
      terminalMarketEvents: terminalEventIds.size,
      retired,
      failedSports: responseResults.filter((item) => item.status === "rejected").length,
    };
    state.lastResult = result;
    state.lastCompletedAt = new Date().toISOString();
    logger.info("[EventSync] completed", result);
    return result;
  } catch (error) {
    state.lastError = error.message;
    state.lastCompletedAt = new Date().toISOString();
    logger.error("[EventSync] failed", { error: error.message });
    throw error;
  } finally {
    running = false;
    state.running = false;
  }
}

function startEventSync() {
  const { expression } = cronConfig.event;
  const task = cron.schedule(expression, () => void syncEvents().catch(() => {}));
  logger.info("[EventSync] scheduled", { expression, sportIds: [...supportedSportIds()] });
  return task;
}

function getEventSyncStatus() {
  return { ...state, sportIds: [...supportedSportIds()] };
}

module.exports = {
  eventRows,
  eventInsertValues,
  terminalPrimaryEventIds,
  restoreActiveEventMarkets,
  upsertEvents,
  retireCompletedEvents,
  syncEvents,
  startEventSync,
  getEventSyncStatus,
};
