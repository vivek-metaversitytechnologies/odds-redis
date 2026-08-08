const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const { supportedSportIds } = require("./competitionSync");
const logger = require("../utils/logger");
const { syncMarketDiscovery } = require("./marketDiscoverySync");
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
        item.openDate &&
        !item.gameOver,
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
             in_play = IF(in_play = 1, 1, ?), updatedon = NOW()
           WHERE eventid = ?`,
          [event.eventName, event.seriesId, event.sportId, event.openDate, event.inPlay, event.eventId],
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
          true,
          true,
          false,
          false,
          true,
          event.inPlay,
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

async function syncEvents() {
  if (running) return { skipped: true, reason: "already-running" };
  running = true;
  state.running = true;
  state.lastStartedAt = new Date().toISOString();
  state.lastError = null;
  try {
    const sportIds = [...supportedSportIds()];
    const responses = await Promise.all(sportIds.map((si) => provider.events({ si, today: 1 })));
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
    const persisted = await upsertEvents(events);
    const marketDiscovery = await syncMarketDiscovery(events);
    const result = {
      skipped: false,
      received,
      supported: events.length,
      sportIds,
      ...persisted,
      marketDiscovery,
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

module.exports = { eventRows, upsertEvents, syncEvents, startEventSync, getEventSyncStatus };
