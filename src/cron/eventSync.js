const cron = require("node-cron");
const provider = require("../services/providerApi");
const { getSourcePool } = require("../config/sourceDb");
const { queryWithMirror } = require("../config/mirrorDb");
const { supportedSportIds } = require("./competitionSync");
const logger = require("../utils/logger");
const redis = require("../config/redis");
const subscriptions = require("../services/marketSubscriptionService");
const frontendSocket = require("../services/frontendSocketService");
const cronConfig = require("../config/cron");
const { utcToIstSql } = require("../utils/dateTime");
const lifecycle = require("../services/eventLifecyclePolicy");
const pendingResults = require("../services/pendingResultQueue");
const { retryDeadlock } = require("../utils/dbRetry");
const { PRIMARY_MARKET_SQL, isPrimaryMarket } = require("../utils/primaryMarket");
const { integer } = require("../config/env");
const { audit } = require("../utils/eventAudit");

const CARRY_OVER_CONCURRENCY = 4;

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

// Every primary market must have been terminally retired (status=0 is written only by
// result settlement, socket game-over and event retirement). Discovery deactivates a
// suspended market with status=1, so a Test suspended at stumps is not a finished event.
async function terminalPrimaryEventIds(events) {
  const eventIds = [...new Set((events || []).map((event) => Number(event.eventId)).filter(Number.isInteger))];
  if (!eventIds.length) return new Set();
  const placeholders = eventIds.map(() => "?").join(",");
  const [rows] = await getSourcePool().query(
    `SELECT eventid
       FROM t_market
     WHERE eventid IN (${placeholders})
        AND ${PRIMARY_MARKET_SQL}
        AND NOT EXISTS (
          SELECT 1 FROM t_market active_market
           WHERE active_market.eventid=t_market.eventid AND active_market.isactive=?
        )
        AND NOT EXISTS (
          SELECT 1 FROM t_matchfancy active_fancy
           WHERE active_fancy.eventid=t_market.eventid AND active_fancy.isactive=?
        )
      GROUP BY eventid
     HAVING SUM(CASE WHEN isactive=? THEN 1 ELSE 0 END)=0
        AND SUM(CASE WHEN status=? THEN 1 ELSE 0 END)=0`,
    [...eventIds, true, true, true, true],
  );
  return new Set(rows.map((row) => Number(row.eventid)).filter(Number.isInteger));
}

function vendorRows(response) {
  return Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
}

// The vendor's today=1 feed drops a multi-day match (a Test between days of play) while it is
// still running. Without carry-over its metadata, Redis payload and market discovery would all
// lapse until it is relisted. A recent event that is missing from the feed but still has a
// primary market never terminally retired (status=1) is kept while the vendor reports an
// unfinished primary market for it. This also reopens an event that was closed only because its
// primary markets were suspended.
async function carriedOverEvents(feedEvents, sportIds) {
  if (!sportIds.length) return [];
  const maxDays = integer("EVENT_CARRY_OVER_MAX_DAYS", 6, { min: 1, max: 30 });
  const maxEvents = integer("EVENT_CARRY_OVER_MAX_EVENTS", 25, { min: 1, max: 200 });
  const listedIds = [...new Set((feedEvents || []).map((event) => Number(event.eventId)))];
  const [rows] = await getSourcePool().query(
    `SELECT e.eventid,e.eventname,e.sportid,e.seriesid,e.open_date
       FROM t_event e
      WHERE e.sportid IN (${sportIds.map(() => "?").join(",")})
        AND e.open_date >= DATE_SUB(NOW(), INTERVAL ? DAY)
        ${listedIds.length ? `AND e.eventid NOT IN (${listedIds.map(() => "?").join(",")})` : ""}
        AND EXISTS (
          SELECT 1 FROM t_market m
           WHERE m.eventid=e.eventid AND m.status=? AND ${PRIMARY_MARKET_SQL}
        )
      ORDER BY e.open_date DESC
      LIMIT ?`,
    [...sportIds, maxDays, ...listedIds, true, maxEvents],
  );
  const candidates = rows
    .map((row) => ({
      eventId: Number(row.eventid),
      eventName: String(row.eventname || "").trim(),
      sportId: Number(row.sportid),
      seriesId: Number(row.seriesid),
      openDate: String(row.open_date || "").slice(0, 19) || null,
      inPlay: false,
      gameOver: false,
    }))
    .filter((event) => Number.isInteger(event.eventId) && event.eventName && Number.isInteger(event.seriesId));
  const kept = [];
  for (let offset = 0; offset < candidates.length; offset += CARRY_OVER_CONCURRENCY) {
    const batch = candidates.slice(offset, offset + CARRY_OVER_CONCURRENCY);
    const responses = await Promise.allSettled(
      batch.map((event) => provider.markets({ eids: [event.eventId] }, { source: "event-carry-over" })),
    );
    batch.forEach((event, index) => {
      const response = responses[index];
      // A failed lookup is not evidence that the match finished; keep it until the next pass.
      if (response.status !== "fulfilled") {
        kept.push(event);
        audit("carry.kept", event.eventId, { reason: "vendor-unavailable", error: response.reason?.message });
        return;
      }
      const unfinished = vendorRows(response.value)
        .filter((market) => isPrimaryMarket({
          marketId: market?.id,
          marketName: market?.name,
          marketType: market?.type,
        }))
        .filter((market) => market?.gameOver !== true)
        .map((market) => String(market.id));
      if (unfinished.length) {
        kept.push(event);
        audit("carry.kept", event.eventId, { reason: "unfinished-primary", marketIds: unfinished });
      } else {
        audit("carry.dropped", event.eventId, { reason: "no-unfinished-primary" });
      }
    });
  }
  return kept;
}

async function retireCompletedEvents(events) {
  const completed = events.filter((event) => event.gameOver);
  if (!completed.length) return { events: 0, markets: 0 };
  const eventIds = [...new Set(completed.map((event) => event.eventId))];
  // Select only active rows, one event at a time. Never rewrite the entire
  // historical event list when just one of its markets still needs retirement.
  const marketIds = [];
  try {
    for (const eventId of eventIds.sort((a, b) => a - b)) {
      const connection = await getSourcePool().getConnection();
      try {
        for (const { table, column, assignments } of [
          { table: "t_market", column: "marketid", assignments: "status=false,issubscribed=false" },
          {
            table: "t_matchfancy",
            column: "fancyid",
            assignments: "isshow=false,is_show=false,issubscribed=false",
          },
        ]) {
          const visibleFancyFilter = table === "t_matchfancy"
            ? " AND (isshow=1 OR is_show=1 OR issubscribed=1)"
            : "";
          const [rows] = await connection.query(
            `SELECT id,${column} AS marketid FROM ${table} WHERE eventid=? AND isactive=?${visibleFancyFilter} ORDER BY id`,
            [eventId, true],
          );
          for (let offset = 0; offset < rows.length; offset += 100) {
            const batch = rows.slice(offset, offset + 100);
            // Redis work must not extend an open database transaction.
            if (table === "t_market") await pendingResults.enqueue(batch.map((row) => row.marketid), { revive: true });
            await retryDeadlock(async () => {
              await connection.beginTransaction();
              try {
                const set = `${table === "t_matchfancy" ? "" : "isactive=false,"}${assignments},updatedon=NOW()`;
                const placeholders = batch.map(() => "?").join(",");
                await queryWithMirror(
                  connection,
                  `UPDATE ${table} SET ${set}
                 WHERE id IN (${placeholders}) AND isactive=? ORDER BY id`,
                  [...batch.map((row) => row.id), true],
                  `UPDATE ${table} SET ${set} WHERE ${column} IN (${placeholders}) AND isactive=?`,
                  [...batch.map((row) => row.marketid), true],
                );
                await connection.commit();
              } catch (error) {
                await connection.rollback();
                throw error;
              }
            });
            const retiredIds = batch.map((row) => String(row.marketid));
            marketIds.push(...retiredIds);
          }
        }
      } finally {
        connection.release();
      }
    }
  } finally {
    // Earlier batches remain committed if a later batch exhausts its retries.
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
  }
  await Promise.allSettled(
    eventIds.map(async (eventId) => {
      await redis.removeEvent(eventId, "event-retired");
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
    const listedEvents = eventRows(responses);
    const carried = await carriedOverEvents(listedEvents, successfulSportIds);
    const events = [...listedEvents, ...carried];
    // Market lifecycle is more specific than the event feed. Some completed
    // matches remain inPlay=true at event level after their only primary market
    // has emitted gameOver. Preserve that terminal state until discovery sees a
    // replacement active Match Odds or Bookmaker market.
    const terminalEventIds = await terminalPrimaryEventIds(events);
    const confirmedVendorTerminalIds = new Set();
    for (const event of events) {
      const decision = lifecycle.observe({
        eventId: event.eventId,
        source: "event-feed",
        terminal: event.gameOver,
        evidence: event.gameOver ? { eventGameOver: true } : null,
      });
      if (decision.confirmed) confirmedVendorTerminalIds.add(event.eventId);
    }
    const terminalDryRun = lifecycle.dryRun();
    const effectiveEvents = events.map((event) =>
      terminalEventIds.has(event.eventId) ||
      confirmedVendorTerminalIds.has(event.eventId) ||
      lifecycle.isConfirmed(event.eventId)
        ? { ...event, gameOver: true, inPlay: false }
        : { ...event, gameOver: false },
    );
    // Closing an event the vendor still reports as running is the risky decision; record why.
    for (const event of events) {
      if (event.gameOver || !(terminalEventIds.has(event.eventId) || lifecycle.isConfirmed(event.eventId))) continue;
      audit("terminal.override", event.eventId, {
        reason: terminalEventIds.has(event.eventId) ? "primary-markets-retired" : "lifecycle-confirmed",
        vendorInPlay: event.inPlay,
        dryRun: terminalDryRun,
      });
    }
    // Populate the public read cache before MySQL writes. Database lock or storage
    // pressure must not prevent fresh provider events from reaching public APIs.
    // Current vendor lifecycle state is authoritative. Completed rows never enter
    // the public cache, while a corrected active row can reopen a previously closed event.
    const cacheableEvents = effectiveEvents.filter((event) => !event.gameOver);
    const discoveryCached = await redis.writeDiscoveryEvents(
      events.filter((event) => !event.gameOver),
      successfulSportIds,
    );
    const cached = await redis.writeEvents(cacheableEvents, successfulSportIds);
    // Dry-run suppresses confirmed terminal events from public metadata but does
    // not mutate their database lifecycle or delete Redis payloads.
    const writableEvents = terminalDryRun
      ? effectiveEvents.filter((event) => !event.gameOver)
      : effectiveEvents;
    const persisted = await upsertEvents(writableEvents);
    const restoredMarketEligibility = await restoreActiveEventMarkets(writableEvents);
    const retired = terminalDryRun
      ? { events: 0, markets: 0, dryRunCandidates: effectiveEvents.length - writableEvents.length }
      : await retireCompletedEvents(effectiveEvents);
    const result = {
      skipped: false,
      received,
      supported: listedEvents.length,
      carriedOver: carried.length,
      sportIds,
      ...persisted,
      cached,
      discoveryCached,
      restoredMarketEligibility,
      terminalMarketEvents: terminalEventIds.size,
      terminalDryRun,
      confirmedVendorTerminalEvents: confirmedVendorTerminalIds.size,
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
  carriedOverEvents,
  restoreActiveEventMarkets,
  upsertEvents,
  retireCompletedEvents,
  syncEvents,
  startEventSync,
  getEventSyncStatus,
};
