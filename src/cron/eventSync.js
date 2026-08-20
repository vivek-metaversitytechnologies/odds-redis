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
  return (responses || [])
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
}

async function upsertEvents(events) {
  if (!events.length) return { inserted: 0, updated: 0 };
  const connection = await getSourcePool().getConnection();
  let inserted = 0;
  let updated = 0;
  try {
    await connection.beginTransaction();
    const ids = events.map((event) => event.eventId);
    const [existingRows] = await connection.query(
      `SELECT eventid FROM t_event WHERE eventid IN (${ids.map(() => "?").join(",")})`,
      ids,
    );
    const existing = new Set(existingRows.map((row) => Number(row.eventid)));
    for (const event of events) {
      if (existing.has(event.eventId)) {
        await connection.execute(
          `UPDATE t_event SET eventname = ?, seriesid = ?, sportid = ?, open_date = ?,
             in_play = ?, isactive = ?, status = ?, updatedon = NOW()
           WHERE eventid = ?`,
          [
            event.eventName,
            event.seriesId,
            event.sportId,
            event.openDate,
            event.gameOver ? false : event.inPlay,
            !event.gameOver,
            !event.gameOver,
            event.eventId,
          ],
        );
        updated += 1;
        continue;
      }
      await connection.execute(
        `INSERT INTO t_event
          (seriesid, sportid, eventid, eventname, open_date, status, isactive, createdon, updatedon,
           fancypause, betlock, is_redis_updated, in_play, fancylock, bookmaker, fancy, channel_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW(), ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          event.seriesId,
          event.sportId,
          event.eventId,
          event.eventName,
          event.openDate,
          !event.gameOver,
          !event.gameOver,
          false,
          false,
          true,
          event.gameOver ? false : event.inPlay,
          true,
          true,
          true,
          "1",
        ],
      );
      inserted += 1;
    }
    await connection.commit();
    return { inserted, updated };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

async function retireCompletedEvents(events) {
  const completed = events.filter((event) => event.gameOver);
  if (!completed.length) return { events: 0, markets: 0 };
  const eventIds = completed.map((event) => event.eventId);
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
    // Populate the public read cache before MySQL writes. Database lock or storage
    // pressure must not prevent fresh provider events from reaching public APIs.
    const cached = await redis.writeEvents(events, successfulSportIds);
    const persisted = await upsertEvents(events);
    const retired = await retireCompletedEvents(events);
    const result = {
      skipped: false,
      received,
      supported: events.length,
      sportIds,
      ...persisted,
      cached,
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
  upsertEvents,
  retireCompletedEvents,
  syncEvents,
  startEventSync,
  getEventSyncStatus,
};
