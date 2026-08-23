const { createClient } = require("redis");
const { getSourcePool } = require("./sourceDb");
const Market = require("../models/Market");
const provider = require("../services/providerApi");
const logger = require("../utils/logger");
const { integer } = require("./env");
const { setBounded } = require("../utils/boundedMap");

let client;
let connecting;
let readClient;
let readConnecting;
const marketCache = new Map();
const runnerNameCache = new Map();
const runnerNameLoads = new Map();
const eventPayloadCache = new Map();
const tickActivity = new Map();
const eventLocks = new Map();
const eventMetadataLocks = new Map();
const CACHE_LIMIT = integer("REDIS_MEMORY_CACHE_LIMIT", 50000, { min: 1000 });
const EVENT_TTL_SECONDS = integer("REDIS_EVENT_TTL_SECONDS", 86400, { min: 60 });
const EVENT_METADATA_TTL_SECONDS = integer("REDIS_EVENT_METADATA_TTL_SECONDS", 172800, { min: 300 });
const SCORE_TTL_SECONDS = integer("REDIS_SCORE_TTL_SECONDS", 86400, { min: 60 });

const PAYLOAD_GROUPS = [
  "Odds",
  "Bookmaker",
  "LineMarket",
  "Fancy2",
  "Meter",
  "Khado",
  "OddEven",
  "OtherMarket",
  "Fancy3",
  "CricketCasino",
  "BallByBall",
];

// Serializes every read-modify-write against one event's Redis payload (tick writes,
// market removals, event deletion) so concurrent callers can't race and clobber
// each other's update. Keyed per event so unrelated events never block each other.
function withEventLock(eventId, fn) {
  const key = String(eventId);
  const previousTail = (eventLocks.get(key) || Promise.resolve()).catch(() => {});
  const result = previousTail.then(fn);
  const tail = result.catch(() => {});
  eventLocks.set(key, tail);
  tail.finally(() => {
    if (eventLocks.get(key) === tail) eventLocks.delete(key);
  });
  return result;
}

function withEventMetadataLock(sportId, fn) {
  const key = String(sportId);
  const previousTail = (eventMetadataLocks.get(key) || Promise.resolve()).catch(() => {});
  const result = previousTail.then(fn);
  const tail = result.catch(() => {});
  eventMetadataLocks.set(key, tail);
  tail.finally(() => {
    if (eventMetadataLocks.get(key) === tail) eventMetadataLocks.delete(key);
  });
  return result;
}

function emptyEventPayload() {
  return Object.fromEntries(PAYLOAD_GROUPS.map((group) => [group, []]));
}

function scoreKey(eventId) {
  return `${process.env.REDIS_SCORE_KEY_PREFIX || "Score-Rs:"}${eventId}`;
}

function eventMetadataKey(sportId) {
  return `${process.env.REDIS_EVENT_METADATA_KEY_PREFIX || "Events-Rs:"}${sportId}`;
}

async function writeEvents(events, sportIds = []) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { sports: 0, events: 0 };
  const requestedSports = [...new Set((sportIds || []).map(Number).filter(Number.isInteger))];
  const bySport = new Map(requestedSports.map((sportId) => [sportId, []]));
  for (const event of events || []) {
    const sportId = Number(event?.sportId);
    if (!Number.isInteger(sportId) || !bySport.has(sportId)) continue;
    bySport.get(sportId).push({
      eventId: Number(event.eventId),
      eventName: String(event.eventName || "").trim(),
      sportId,
      seriesId: Number(event.seriesId),
      openDate: event.openDate ?? null,
      inPlay: Boolean(event.inPlay),
      gameOver: Boolean(event.gameOver),
    });
  }
  if (!bySport.size) return { sports: 0, events: 0 };
  await Promise.all(
    [...bySport].map(([sportId, rows]) =>
      withEventMetadataLock(sportId, () =>
        redis.set(eventMetadataKey(sportId), JSON.stringify(rows), {
          EX: EVENT_METADATA_TTL_SECONDS,
        }),
      ),
    ),
  );
  return {
    sports: bySport.size,
    events: [...bySport.values()].reduce((total, rows) => total + rows.length, 0),
  };
}

async function getEvents(sportId) {
  const normalized = Number(sportId);
  if (!Number.isInteger(normalized) || normalized <= 0) return null;
  const redis = await getRedisReadClient();
  if (!redis?.isOpen) return null;
  const value = await redis.get(eventMetadataKey(normalized));
  if (value == null) return null;
  try {
    const events = JSON.parse(value);
    return Array.isArray(events) ? events : null;
  } catch {
    return null;
  }
}

async function removeEventsFromMetadata(events) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return 0;
  const bySport = new Map();
  for (const event of events || []) {
    const sportId = Number(event?.sportid ?? event?.sportId);
    const eventId = String(event?.eventid ?? event?.eventId ?? "");
    if (!Number.isInteger(sportId) || !/^\d+$/.test(eventId)) continue;
    if (!bySport.has(sportId)) bySport.set(sportId, new Set());
    bySport.get(sportId).add(eventId);
  }
  const removals = await Promise.all(
    [...bySport].map(([sportId, eventIds]) =>
      withEventMetadataLock(sportId, async () => {
        const key = eventMetadataKey(sportId);
        const value = await redis.get(key);
        if (!value) return 0;
        let rows;
        try {
          rows = JSON.parse(value);
        } catch {
          return 0;
        }
        if (!Array.isArray(rows)) return 0;
        const filtered = rows.filter((event) => !eventIds.has(String(event?.eventId)));
        const removed = rows.length - filtered.length;
        if (removed) await redis.set(key, JSON.stringify(filtered), { EX: EVENT_METADATA_TTL_SECONDS });
        return removed;
      }),
    ),
  );
  return removals.reduce((total, count) => total + count, 0);
}

async function writeScore(score) {
  const eventId = String(score?.eid ?? score?.eventId ?? "").trim();
  const html = score?.data ?? score?.html ?? score?.scorecard;
  if (!/^\d+$/.test(eventId) || typeof html !== "string" || !html.trim()) return null;
  const redis = await getRedisClient();
  if (!redis?.isOpen) return null;
  const payload = { ...score, eid: Number(eventId), data: html, receivedAt: new Date().toISOString() };
  await redis.set(scoreKey(eventId), JSON.stringify(payload), { EX: SCORE_TTL_SECONDS });
  return payload;
}

async function getScore(eventId) {
  const normalized = String(eventId || "").trim();
  if (!/^\d+$/.test(normalized)) return null;
  const redis = await getRedisReadClient();
  if (!redis?.isOpen) return null;
  const value = await redis.get(scoreKey(normalized));
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return { eid: Number(normalized), data: value };
  }
}

function validMarketIdentifier(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  return Boolean(normalized) && !["undefined", "null", "nan"].includes(normalized);
}

function moveTiedMatchLast(payload) {
  if (!Array.isArray(payload?.Odds)) return payload;
  const tied = [];
  const other = [];
  for (const market of payload.Odds) {
    if (
      String(market?.Name ?? market?.name ?? "")
        .trim()
        .toLowerCase() === "tied match"
    )
      tied.push(market);
    else other.push(market);
  }
  payload.Odds = [...other, ...tied];
  return payload;
}

function normalizeEventPayload(payload) {
  const normalized = emptyEventPayload();
  for (const group of PAYLOAD_GROUPS) {
    normalized[group] = Array.isArray(payload?.[group])
      ? payload[group].filter((entry) => validMarketIdentifier(entryMarketId(entry)))
      : [];
  }
  // Move line markets written by older builds out of the generic Odds group immediately.
  const legacyLineMarkets = normalized.Odds.filter((market) => /\bline\b/i.test(String(market?.Name || "")));
  for (const market of legacyLineMarkets) {
    if (!normalized.LineMarket.some((item) => entryMarketId(item) === entryMarketId(market))) {
      normalized.LineMarket.push(market);
    }
  }
  normalized.Odds = normalized.Odds.filter((market) => !/\bline\b/i.test(String(market?.Name || "")));
  const legacyKhado = normalized.Fancy2.filter((entry) => /-KD$/i.test(entryMarketId(entry)));
  for (const entry of legacyKhado) {
    if (!normalized.Khado.some((item) => entryMarketId(item) === entryMarketId(entry))) {
      normalized.Khado.push(entry);
    }
  }
  normalized.Fancy2 = normalized.Fancy2.filter((entry) => !/-KD$/i.test(entryMarketId(entry)));
  const legacyMeter = normalized.Fancy2.filter((entry) => /-MT$/i.test(entryMarketId(entry)));
  for (const entry of legacyMeter) {
    if (!normalized.Meter.some((item) => entryMarketId(item) === entryMarketId(entry))) {
      normalized.Meter.push(entry);
    }
  }
  normalized.Fancy2 = normalized.Fancy2.filter((entry) => !/-MT$/i.test(entryMarketId(entry)));
  // Older builds stored vendor -F3 markets under OtherMarket. Keep genuine
  // OtherMarket rows in place, but migrate -F3 rows into their API contract group.
  const legacyFancy3 = normalized.OtherMarket.filter((entry) => /-F3$/i.test(entryMarketId(entry)));
  for (const entry of legacyFancy3) {
    if (!normalized.Fancy3.some((item) => entryMarketId(item) === entryMarketId(entry))) {
      normalized.Fancy3.push(entry);
    }
  }
  normalized.OtherMarket = normalized.OtherMarket.filter((entry) => !/-F3$/i.test(entryMarketId(entry)));
  return moveTiedMatchLast(normalized);
}

function frontendEventPayload(payload) {
  const normalized = normalizeEventPayload(payload);
  return Object.fromEntries(
    PAYLOAD_GROUPS.map((group) => [
      group,
      normalized[group].filter((entry) => {
        const state = String(entry?.status ?? entry?.gstatus ?? entry?.s ?? "")
          .trim()
          .toUpperCase();
        return state !== "WAITING";
      }),
    ]),
  );
}

function redisUrl() {
  if (process.env.REDIS_URL) return process.env.REDIS_URL;
  if (!process.env.REDIS_HOST) return null;
  const username = process.env.REDIS_USERNAME ? encodeURIComponent(process.env.REDIS_USERNAME) : "";
  const password = process.env.REDIS_PASSWORD ? encodeURIComponent(process.env.REDIS_PASSWORD) : "";
  const credentials = username || password ? `${username}:${password}@` : "";
  return `redis://${credentials}${process.env.REDIS_HOST}:${Number(process.env.REDIS_PORT || 6379)}`;
}

async function getRedisClient() {
  if (client?.isOpen) return client;
  if (connecting) return connecting;
  const url = redisUrl();
  if (!url) return null;
  client = createClient({
    url,
    database: Number(process.env.REDIS_DB || 0),
    socket: { connectTimeout: Number(process.env.REDIS_TIMEOUT_MS || 15000) },
  });
  client.on("error", (error) => logger.error("[Redis] client error", { error: error.message }));
  connecting = client
    .connect()
    .then(() => {
      logger.info("[Redis] connected");
      return client;
    })
    .catch((error) => {
      logger.error("[Redis] connection failed", { error: error.message });
      client = undefined;
      return null;
    })
    .finally(() => {
      connecting = undefined;
    });
  return connecting;
}

async function getRedisReadClient() {
  if (readClient?.isOpen) return readClient;
  if (readConnecting) return readConnecting;
  const url = redisUrl();
  if (!url) return null;
  readClient = createClient({
    url,
    database: Number(process.env.REDIS_DB || 0),
    socket: { connectTimeout: Number(process.env.REDIS_TIMEOUT_MS || 15000) },
  });
  readClient.on("error", (error) => logger.error("[RedisRead] client error", { error: error.message }));
  readConnecting = readClient
    .connect()
    .then(() => {
      logger.info("[RedisRead] connected");
      return readClient;
    })
    .catch((error) => {
      logger.error("[RedisRead] connection failed", { error: error.message });
      readClient = undefined;
      return null;
    })
    .finally(() => {
      readConnecting = undefined;
    });
  return readConnecting;
}

async function findMarkets(marketIds) {
  const ids = [...new Set((marketIds || []).map(String).filter(validMarketIdentifier))];
  const found = new Map();
  const missing = [];
  for (const id of ids) {
    if (marketCache.has(id)) found.set(id, marketCache.get(id));
    else missing.push(id);
  }
  if (!missing.length) return found;
  const placeholders = missing.map(() => "?").join(",");
  const [rows] = await getSourcePool().query(
    `SELECT * FROM t_market WHERE marketid IN (${placeholders})`,
    missing,
  );
  for (const row of rows) {
    const id = String(row.marketid);
    const market = Market.fromRow(row);
    found.set(id, market);
    setBounded(marketCache, id, market, CACHE_LIMIT);
  }
  const unresolved = missing.filter((id) => !found.has(id));
  if (!unresolved.length) return found;
  const fancyPlaceholders = unresolved.map(() => "?").join(",");
  const [fancies] = await getSourcePool().query(
    `SELECT f.*, f.fancyid AS marketid, f.name AS marketname,
       COALESCE(f.sportid,e.sportid) AS sportid, e.eventname AS matchname,
       e.open_date AS opendate, e.in_play AS inplay
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE f.fancyid IN (${fancyPlaceholders})`,
    unresolved,
  );
  for (const row of fancies) {
    const id = String(row.marketid);
    const market = Market.fromRow(row);
    found.set(id, market);
    setBounded(marketCache, id, market, CACHE_LIMIT);
  }
  // Discovery invalidates every inserted or changed ID, so caching a miss avoids
  // repeated database reads for unsupported provider ticks without hiding new markets.
  for (const id of unresolved) {
    if (!found.has(id)) setBounded(marketCache, id, null, CACHE_LIMIT);
  }
  return found;
}

function status(value) {
  if (String(value).toUpperCase() === "S") return "SUSPENDED";
  if (String(value).toUpperCase() === "B") return "Ball Running";
  return value == null ? "" : String(value);
}

function numberOr(value, fallback = null) {
  return value == null || value === "" || Number.isNaN(Number(value)) ? fallback : Number(value);
}

function booleanOr(value, fallback = null) {
  if (value == null) return fallback;
  if (Buffer.isBuffer(value)) return value.length ? value[0] !== 0 : fallback;
  if (Array.isArray(value?.data)) return value.data.length ? value.data[0] !== 0 : fallback;
  if (typeof value === "string") return !["0", "false", "no", "off"].includes(value.toLowerCase());
  return Boolean(value);
}

function prices(value) {
  if (!Array.isArray(value)) return [];
  return value.slice(0, 3).map((entry) => ({
    price: numberOr(entry?.price ?? entry?.p, 0),
    size: numberOr(entry?.size ?? entry?.s, 0),
  }));
}

function runnerPrices(runner, side) {
  const exchangeKey = side === "back" ? "availableToBack" : "availableToLay";
  const arrayValue = side === "back" ? runner?.b : runner?.l;
  if (Array.isArray(arrayValue)) return prices(arrayValue);
  if (Array.isArray(runner?.ex?.[exchangeKey])) return prices(runner.ex[exchangeKey]);
  const pricePrefix = side === "back" ? "b" : "l";
  const sizePrefix = side === "back" ? "br" : "lr";
  const hasCompactPrices = [1, 2, 3].some(
    (level) => runner?.[`${pricePrefix}${level}`] != null || runner?.[`${sizePrefix}${level}`] != null,
  );
  if (!hasCompactPrices) return [];
  return [1, 2, 3].map((level) => ({
    price: numberOr(runner?.[`${pricePrefix}${level}`], 0),
    size: numberOr(runner?.[`${sizePrefix}${level}`], 0),
  }));
}

function marketMatchName(item, market) {
  return (
    market.matchname ||
    item.mn ||
    item.matchName ||
    (Array.isArray(item.r)
      ? item.r
          .map((runner) => runner?.na ?? runner?.name)
          .filter(Boolean)
          .join(" v ")
      : "") ||
    null
  );
}

function bookmakerPayload(item, market) {
  const runners = Array.isArray(item.r) ? item.r : [];
  const matchName = marketMatchName(item, market);
  return runners.map((runner) => ({
    mid: String(item.mid),
    t: market.marketname || item.na || "",
    sid: runner?.rid ?? runner?.selectionId ?? null,
    nation: runner?.na ?? runner?.name ?? null,
    b1: numberOr(runner?.b ?? runner?.b1 ?? runner?.back),
    bs1: numberOr(runner?.bs ?? runner?.bs1, 0),
    l1: numberOr(runner?.l ?? runner?.l1 ?? runner?.lay),
    ls1: numberOr(runner?.ls ?? runner?.ls1, 0),
    gstatus: status(runner?.sb ?? runner?.gstatus),
    rem: runner?.rem ?? item.rem ?? null,
    display_message: market.displayMessage ?? null,
    betlock: numberOr(market.betDelay, 0),
    minBet: numberOr(market.minbet),
    maxBet: numberOr(market.maxbet),
    betDelay: numberOr(market.betDelay, 0),
    minBetRate: numberOr(market.minBetRate, 0),
    maxBetRate: numberOr(market.maxBetRate, 0),
    matchName,
    matchId: numberOr(market.eventid, market.eventid),
    isActive: booleanOr(market.isactive),
    isPause: booleanOr(market.isPause ?? market.fancypause, false),
    sportId: numberOr(market.sportid, market.sportid),
  }));
}

function oddsPayload(item, market, runnerNames = runnerNameCache.get(String(item.mid))) {
  const runners = Array.isArray(item.r) ? item.r : [];
  return {
    matchName: marketMatchName(item, market),
    marketId: String(item.mid),
    status: status(item.sb ?? item.s ?? item.status),
    inplay: booleanOr(item.ip ?? market.inPlay, false),
    eventTime: market.opendate ?? null,
    lastMatchTime: item.tm ?? null,
    maxBetRate: numberOr(market.maxBetRate, 0),
    minBetRate: numberOr(market.minBetRate, 0),
    betDelay: numberOr(market.betDelay, 0),
    maxBet: numberOr(market.maxbet),
    minBet: numberOr(market.minbet),
    betlock: booleanOr(market.betlock),
    matchId: numberOr(market.eventid, market.eventid),
    display_message: market.displayMessage ?? null,
    runners: runners.map((runner) => ({
      selectionId: runner?.rid ?? runner?.selectionId ?? null,
      handicap: numberOr(runner?.hc ?? runner?.handicap, 0),
      status: status(item.sb ?? runner?.s ?? runner?.status ?? "ACTIVE"),
      lastPriceTraded: numberOr(runner?.ltp ?? runner?.lastPriceTraded, 0),
      totalMatched: numberOr(runner?.tv ?? runner?.totalMatched, 0),
      adjustmentFactor: numberOr(runner?.af ?? runner?.adjustmentFactor, 0),
      ex: { availableToBack: runnerPrices(runner, "back"), availableToLay: runnerPrices(runner, "lay") },
      name:
        runner?.na ?? runner?.name ?? runnerNames?.get(String(runner?.rid ?? runner?.selectionId)) ?? null,
    })),
    marketDataDelayed: booleanOr(item.delay ?? item.marketDataDelayed, false),
    Name: market.marketname || item.na || "",
    isActive: booleanOr(market.isactive),
    isPause: booleanOr(market.isPause ?? market.fancypause, false),
    sportId: numberOr(market.sportid, market.sportid),
  };
}

function fancyPayload(item, market) {
  const runner = Array.isArray(item.r) ? item.r[0] || {} : {};
  const marketType = String(market.markettype || market.mtype || item.type || "").toLowerCase();
  const ballByBall =
    marketType === "ball-by-ball" ||
    String(item.mid || "")
      .toUpperCase()
      .endsWith("-BB");
  const cricketCasino = String(item.mid || "")
    .toUpperCase()
    .includes("-CC");
  const casinoRate = cricketCasino ? numberOr(runner.ra ?? item.ra) : null;
  const difference = numberOr(item.d ?? item.di ?? item.srno ?? runner.d ?? runner.di ?? runner.srno);
  return {
    mid: String(item.mid),
    sid: String(runner.rid ?? item.mid),
    nation: ballByBall
      ? market.marketname || runner.na || item.na || null
      : (runner.na ?? item.na ?? market.marketname ?? null),
    b1: numberOr(runner.b ?? runner.b1 ?? item.b ?? item.b1, casinoRate),
    l1: numberOr(runner.l ?? runner.l1 ?? item.l ?? item.l1),
    bs1: numberOr(runner.bs ?? runner.bs1 ?? item.br ?? item.bs ?? item.bs1, cricketCasino ? null : 0),
    ls1: numberOr(runner.ls ?? runner.ls1 ?? item.lr ?? item.ls ?? item.ls1, cricketCasino ? null : 0),
    gstatus: status(runner.sb ?? item.sb),
    rem: runner.rem ?? item.rem ?? item.res ?? "",
    // Khado's 8/29-style badge arrives as top-level `d`.
    srno: difference == null ? "" : String(difference),
    difference,
    d: difference,
    di: difference,
    gameover: booleanOr(item.go, false),
    s: booleanOr(item.s, true),
    maxBet: numberOr(market.maxbet),
    minBet: numberOr(market.minbet),
    betDelay: numberOr(market.betDelay, 0),
    matchId: numberOr(market.eventid, market.eventid),
    isActive: booleanOr(market.isactive),
    isShow: booleanOr(market.isShow, true),
    matchName: market.matchname ?? null,
    matchType: market.markettype ?? null,
    maxLiabilityPerMarket: numberOr(market.maximumProfit),
    display_message: market.displayMessage ?? market.remarks ?? null,
    rate: casinoRate,
  };
}

function payloadGroup(item, market) {
  const id = String(item.mid).toUpperCase();
  const name = String(market.marketname || "").toLowerCase();
  const marketType = String(market.markettype || market.mtype || item.type || "").toLowerCase();
  if (id.includes("BM") || name.includes("bookmaker") || name === "toss") return "Bookmaker";
  // Vendor line-market IDs look like normal exchange IDs (for example 1.260761724),
  // so their persisted market name is the stable discriminator available on socket ticks.
  if (/\bline\b/.test(name)) return "LineMarket";
  if (id.includes("OE") || name.includes("odd even")) return "OddEven";
  if (id.endsWith("-F3")) return "Fancy3";
  if (name.includes("other market")) return "OtherMarket";
  if (id.includes("BB") || name.includes("ball by ball")) return "BallByBall";
  if (id.includes("-CC") || id.includes("CASINO") || name.includes("casino")) return "CricketCasino";
  if (marketType === "khado" || id.includes("KD") || name.includes("khado")) return "Khado";
  if (marketType === "meter" || id.endsWith("-MT") || /\bmeter\b/.test(name)) return "Meter";
  if (id.includes("F2")) return "Fancy2";
  return "Odds";
}

function transformedTick(item, market) {
  const group = payloadGroup(item, market);
  const entries =
    group === "Bookmaker"
      ? bookmakerPayload(item, market)
      : ["Odds", "LineMarket"].includes(group)
        ? [oddsPayload(item, market)]
        : [fancyPayload(item, market)];
  return { group, entries };
}

function fancyDefinitionEntry(market) {
  return {
    mid: String(market.marketId),
    sid: String(market.marketId),
    nation: market.marketName,
    b1: null,
    l1: null,
    bs1: 0,
    ls1: 0,
    gstatus: "WAITING",
    rem: "",
    srno: "",
    gameover: false,
    s: true,
    maxBet: numberOr(market.maxBet, 100000),
    minBet: numberOr(market.minBet, 100),
    betDelay: numberOr(market.betDelay, 0),
    matchId: numberOr(market.eventId, market.eventId),
    isActive: true,
    isShow: true,
    matchName: market.matchName ?? null,
    matchType: market.marketType ?? null,
    maxLiabilityPerMarket: 100000,
    display_message: market.displayMessage ?? null,
    rate: null,
  };
}

async function reconcileFancyDefinitions(markets) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { events: 0, added: 0, removed: 0, changedEventIds: [] };
  const byEvent = new Map();
  for (const market of markets || []) {
    if (!market?.eventId || !market?.marketId) continue;
    const group = payloadGroup({ mid: market.marketId }, { marketname: market.marketName });
    if (["Odds", "Bookmaker"].includes(group)) continue;
    const eventId = String(market.eventId);
    if (!byEvent.has(eventId)) byEvent.set(eventId, []);
    byEvent.get(eventId).push({ market, group });
  }
  let added = 0;
  let removed = 0;
  const changedEventIds = [];
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  // Each event's read-modify-write runs inside withEventLock so it can never race a
  // concurrent tick write or removal for that same event (see writeTicks/removeMarkets).
  // That rules out batching all events into one mGet/MULTI round trip: a stale
  // snapshot read outside the lock is exactly what let a reconciler resurrect a
  // market a tick had just updated.
  await Promise.all(
    [...byEvent.entries()].map(([eventId, definitions]) =>
      withEventLock(eventId, async () => {
        const key = `${prefix}${eventId}`;
        let payload = eventPayloadCache.get(eventId);
        if (!payload) {
          payload = emptyEventPayload();
          const current = await redis.get(key);
          if (current) {
            try {
              payload = normalizeEventPayload(JSON.parse(current));
            } catch {
              /* replace malformed payload */
            }
          }
        }
        let changed = false;
        for (const { market, group } of definitions) {
          const marketId = String(market.marketId);
          const index = payload[group].findIndex((entry) => entryMarketId(entry) === marketId);
          const active = market.isActive && !market.gameOver;
          if (active && index < 0) {
            payload[group].push(fancyDefinitionEntry(market));
            added += 1;
            changed = true;
          } else if (active && index >= 0 && payload[group][index]?.gstatus === "WAITING") {
            const definition = fancyDefinitionEntry(market);
            if (JSON.stringify(payload[group][index]) !== JSON.stringify(definition)) {
              payload[group][index] = definition;
              changed = true;
            }
          } else if (
            active &&
            index >= 0 &&
            payload[group][index]?.display_message !== (market.displayMessage ?? null)
          ) {
            payload[group][index].display_message = market.displayMessage ?? null;
            changed = true;
          } else if (!active && index >= 0) {
            payload[group].splice(index, 1);
            removed += 1;
            changed = true;
          }
        }
        if (!changed) return;
        moveTiedMatchLast(payload);
        await redis.set(key, JSON.stringify(payload), { EX: EVENT_TTL_SECONDS });
        setBounded(eventPayloadCache, eventId, payload, CACHE_LIMIT);
        changedEventIds.push(eventId);
      }),
    ),
  );
  return { events: byEvent.size, added, removed, changedEventIds };
}

function regularDefinitionEntries(market, runners = []) {
  const marketRow = {
    eventid: market.eventId,
    sportid: market.sportId,
    marketid: market.marketId,
    marketname: market.marketName,
    matchname: market.matchName,
    opendate: market.openDate,
    inplay: market.inPlay,
    isactive: market.isActive,
    minbet: market.minBet,
    maxbet: market.maxBet,
    betDelay: market.betDelay,
    minBetRate: 1,
    maxBetRate: 500,
    displayMessage: market.displayMessage ?? null,
  };
  const itemRunners = runners.map((runner) => ({
    rid: runner.selectionId,
    na: runner.runnerName,
    s: "SUSPENDED",
    sb: "S",
    b1: null,
    l1: null,
    bs1: 0,
    ls1: 0,
  }));
  const group = payloadGroup({ mid: market.marketId }, marketRow);
  if (group === "Bookmaker") {
    const entries = bookmakerPayload(
      { eid: market.eventId, mid: market.marketId, na: market.marketName, s: "WAITING", r: itemRunners },
      marketRow,
    );
    return {
      group,
      entries: entries.length
        ? entries
        : [
            {
              mid: String(market.marketId),
              t: market.marketName,
              sid: String(market.marketId),
              nation: market.marketName,
              b1: null,
              bs1: 0,
              l1: null,
              ls1: 0,
              gstatus: "WAITING",
              rem: null,
              display_message: market.displayMessage ?? null,
              betlock: 0,
              minBet: numberOr(market.minBet),
              maxBet: numberOr(market.maxBet),
              betDelay: numberOr(market.betDelay, 0),
              minBetRate: 1,
              maxBetRate: 500,
              matchName: market.matchName ?? null,
              matchId: numberOr(market.eventId, market.eventId),
              isActive: true,
              isPause: false,
              sportId: numberOr(market.sportId, market.sportId),
            },
          ],
    };
  }
  return {
    group,
    entries: [
      oddsPayload(
        {
          eid: market.eventId,
          mid: market.marketId,
          na: market.marketName,
          s: "WAITING",
          ip: market.inPlay,
          r: itemRunners,
        },
        marketRow,
      ),
    ],
  };
}

async function reconcileRegularDefinitions(markets) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { events: 0, added: 0, removed: 0, changedEventIds: [] };
  const byEvent = new Map();
  for (const market of markets || []) {
    if (!market?.eventId || !market?.marketId) continue;
    const eventId = String(market.eventId);
    if (!byEvent.has(eventId)) byEvent.set(eventId, []);
    byEvent.get(eventId).push(market);
  }
  let added = 0;
  let removed = 0;
  const changedEventIds = [];
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  // See reconcileFancyDefinitions: each event's read-modify-write runs inside
  // withEventLock instead of a batched mGet/MULTI so it can't race a concurrent
  // tick write or removal for that event.
  await Promise.all(
    [...byEvent.entries()].map(([eventId, definitions]) =>
      withEventLock(eventId, async () => {
        const key = `${prefix}${eventId}`;
        let payload = eventPayloadCache.get(eventId);
        if (!payload) {
          payload = emptyEventPayload();
          const current = await redis.get(key);
          if (current) {
            try {
              payload = normalizeEventPayload(JSON.parse(current));
            } catch {
              /* replace malformed payload */
            }
          }
        }
        let changed = false;
        for (const market of definitions) {
          const active = market.isActive && !market.gameOver;
          if (!active) {
            for (const group of PAYLOAD_GROUPS) {
              const filtered = payload[group].filter(
                (entry) => entryMarketId(entry) !== String(market.marketId),
              );
              if (filtered.length !== payload[group].length) {
                removed += 1;
                changed = true;
                payload[group] = filtered;
              }
            }
            continue;
          }
          if (payloadHasMarket(payload, market.marketId)) {
            // Replace the placeholder once runner names for this exact BM2 market are available.
            const marketId = String(market.marketId);
            const currentEntries = payload.Bookmaker.filter((entry) => entryMarketId(entry) === marketId);
            const syntheticBookmaker2 =
              /-BM2$/i.test(marketId) &&
              currentEntries.length === 1 &&
              String(currentEntries[0]?.nation || "").toLowerCase() === "bookmaker2";
            if (syntheticBookmaker2 && (market.runners || []).length) {
              for (const groupName of PAYLOAD_GROUPS) {
                payload[groupName] = payload[groupName].filter((entry) => entryMarketId(entry) !== marketId);
              }
              const seeded = regularDefinitionEntries(market, market.runners);
              payload[seeded.group].push(...seeded.entries);
              changed = true;
            }
            for (const groupName of PAYLOAD_GROUPS) {
              for (const entry of payload[groupName]) {
                if (
                  entryMarketId(entry) === marketId &&
                  entry.display_message !== (market.displayMessage ?? null)
                ) {
                  entry.display_message = market.displayMessage ?? null;
                  changed = true;
                }
              }
            }
            continue;
          }
          const { group, entries } = regularDefinitionEntries(market, market.runners || []);
          payload[group].push(...entries);
          added += 1;
          changed = true;
        }
        if (!changed) return;
        moveTiedMatchLast(payload);
        await redis.set(key, JSON.stringify(payload), { EX: EVENT_TTL_SECONDS });
        setBounded(eventPayloadCache, eventId, payload, CACHE_LIMIT);
        changedEventIds.push(eventId);
      }),
    ),
  );
  return { events: byEvent.size, added, removed, changedEventIds };
}

function entryMarketId(entry) {
  return String(entry.marketId ?? entry.mid ?? "");
}

function isFullySuspendedToss(group, item, market = {}) {
  if (group !== "Bookmaker") return false;
  const marketName = String(market.marketname ?? item.na ?? "")
    .trim()
    .toLowerCase();
  if (marketName !== "toss") return false;
  if (booleanOr(market.isactive ?? market.isActive, true) === false) return true;
  const runners = Array.isArray(item.r) ? item.r : [];
  return (
    runners.length > 0 &&
    runners.every((runner) =>
      ["S", "SUSPENDED"].includes(String(runner?.sb ?? runner?.s ?? runner?.status ?? "").toUpperCase()),
    )
  );
}

function shouldRemoveFromPayload(group, item, market) {
  if (isFullySuspendedToss(group, item, market)) return true;
  if (group === "Bookmaker") return booleanOr(item.go, false);
  if (group === "Odds") return false;
  return booleanOr(item.go, false) || !booleanOr(item.s, true);
}

async function loadRunnerNames(marketId) {
  const normalized = String(marketId);
  if (runnerNameCache.has(normalized)) return runnerNameCache.get(normalized);
  if (runnerNameLoads.has(normalized)) return runnerNameLoads.get(normalized);
  const loading = provider
    .runners(normalized)
    .then((response) => {
      const rows = Array.isArray(response?.data) ? response.data : Array.isArray(response) ? response : [];
      const names = new Map(
        rows
          .map((runner) => [
            String(runner?.runnerId ?? runner?.selectionId ?? runner?.id),
            runner?.name ?? runner?.nation ?? null,
          ])
          .filter(([selectionId, name]) => selectionId && name),
      );
      setBounded(runnerNameCache, normalized, names, CACHE_LIMIT);
      return names;
    })
    .catch((error) => {
      logger.warn("[Redis] runner metadata lookup failed", { marketId: normalized, error: error.message });
      return new Map();
    })
    .finally(() => runnerNameLoads.delete(normalized));
  runnerNameLoads.set(normalized, loading);
  return loading;
}

async function primeRunnerNames(marketIds) {
  await Promise.allSettled((marketIds || []).map((marketId) => loadRunnerNames(marketId)));
}

function preserveRunnerNames(entries, previousEntries) {
  const previous = new Map(
    (previousEntries || []).flatMap((entry) =>
      (entry.runners || []).map((runner) => [String(runner.selectionId), runner.name]),
    ),
  );
  for (const entry of entries) {
    for (const runner of entry.runners || []) {
      if (!runner.name) runner.name = previous.get(String(runner.selectionId)) ?? null;
    }
  }
}

async function writeTicks(items) {
  const candidates = (items || []).filter((item) => item && typeof item === "object" && item.eid && item.mid);
  if (!candidates.length) return { payload: false, accepted: [], rejected: candidates };
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { payload: false, accepted: [], rejected: candidates };
  const eventId = String(candidates[0].eid);
  const sameEvent = candidates.filter((item) => String(item.eid) === eventId);
  const rejected = candidates.filter((item) => String(item.eid) !== eventId);
  const markets = await findMarkets(sameEvent.map((item) => item.mid));
  const marketRows = sameEvent.map((item) => ({ item, market: markets.get(String(item.mid)) || null }));
  const acceptedRows = marketRows.filter(({ item, market }) => {
    if (!market) return false;
    if (booleanOr(market.isactive ?? market.isActive, false) !== false) return true;
    return isFullySuspendedToss(payloadGroup(item, market), item, market);
  });
  const acceptedItems = new Set(acceptedRows.map(({ item }) => item));
  rejected.push(...marketRows.filter(({ item }) => !acceptedItems.has(item)).map(({ item }) => item));
  if (!acceptedRows.length) return { payload: false, accepted: [], rejected };
  const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventId}`;
  await Promise.allSettled(
    acceptedRows
      .filter(({ item, market }) => payloadGroup(item, market) === "Odds")
      .map(({ item }) => loadRunnerNames(item.mid)),
  );
  const { payload, changed, serialized } = await withEventLock(eventId, async () => {
    let payload = eventPayloadCache.get(eventId);
    if (!payload) {
      payload = emptyEventPayload();
      const current = await redis.get(key);
      if (current) {
        try {
          const parsed = JSON.parse(current);
          if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            payload = normalizeEventPayload(parsed);
          }
        } catch {
          /* Replace malformed legacy data with a valid event payload. */
        }
      }
    }
    let changed = false;
    for (const { item, market } of acceptedRows) {
      const { group, entries } = transformedTick(item, market);
      const marketId = String(item.mid);
      const previousEntries = payload[group].filter((entry) => entryMarketId(entry) === marketId);
      // Detect whether this market's entries were sitting in a different group before
      // (grouping can change as discovery metadata improves), which is itself a change.
      const movedFromOtherGroup = PAYLOAD_GROUPS.some(
        (payloadGroupName) =>
          payloadGroupName !== group &&
          payload[payloadGroupName].some((entry) => entryMarketId(entry) === marketId),
      );
      if (group === "Odds") preserveRunnerNames(entries, previousEntries);
      const newEntries = shouldRemoveFromPayload(group, item, market) ? [] : entries;
      const marketChanged =
        movedFromOtherGroup ||
        previousEntries.length !== newEntries.length ||
        JSON.stringify(previousEntries) !== JSON.stringify(newEntries);
      if (!marketChanged) continue;
      changed = true;
      // A market can change grouping as discovery metadata improves; keep exactly one copy.
      for (const payloadGroupName of PAYLOAD_GROUPS) {
        payload[payloadGroupName] = payload[payloadGroupName].filter(
          (entry) => entryMarketId(entry) !== marketId,
        );
      }
      if (newEntries.length) payload[group].push(...newEntries);
    }
    moveTiedMatchLast(payload);
    const serialized = changed ? JSON.stringify(payload) : null;
    if (changed) await redis.set(key, serialized, { EX: EVENT_TTL_SECONDS });
    setBounded(eventPayloadCache, eventId, payload, CACHE_LIMIT);
    return { payload, changed, serialized };
  });
  for (const { item } of acceptedRows) recordTickActivity(`${key}:${item.mid}`, item.eid, item.mid);
  return {
    payload,
    accepted: acceptedRows.map(({ item }) => item),
    rejected,
    changed,
    persistedBytes: changed ? Buffer.byteLength(serialized) : 0,
  };
}

async function writeTick(item) {
  const result = await writeTicks([item]);
  return result.payload;
}

function invalidateMarkets(marketIds) {
  for (const marketId of marketIds || []) marketCache.delete(String(marketId));
}

function recordTickActivity(key, eventId, marketId) {
  const previous = tickActivity.get(key);
  setBounded(
    tickActivity,
    key,
    {
      eventId: String(eventId),
      marketId: String(marketId),
      lastUpdatedAt: new Date().toISOString(),
      tickCount: (previous?.tickCount || 0) + 1,
    },
    CACHE_LIMIT,
  );
}

function parseTickKey(key) {
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  if (!key.startsWith(prefix)) return null;
  const value = key.slice(prefix.length);
  const separator = value.indexOf(":");
  return separator < 0
    ? { eventId: value, marketId: null }
    : { eventId: value.slice(0, separator), marketId: value.slice(separator + 1) };
}

function payloadHasMarket(payload, marketId) {
  return PAYLOAD_GROUPS.some(
    (group) =>
      Array.isArray(payload?.[group]) &&
      payload[group].some((entry) => entryMarketId(entry) === String(marketId)),
  );
}

async function inspectTicks({ eventId, marketId, limit = 250, includePayload = false } = {}) {
  const redis = await getRedisReadClient();
  if (!redis?.isOpen) return { connected: false, scanned: 0, truncated: false, items: [] };
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  const boundedLimit = Math.max(1, Math.min(500, Number(limit) || 250));
  const pattern = eventId ? `${prefix}${eventId}*` : `${prefix}*`;
  let cursor = "0";
  const keys = [];
  do {
    const result = await redis.scan(cursor, { MATCH: pattern, COUNT: 200 });
    cursor = String(result.cursor);
    keys.push(...result.keys.slice(0, boundedLimit - keys.length));
  } while (cursor !== "0" && keys.length < boundedLimit);
  const values = keys.length ? await redis.mGet(keys) : [];
  const items = [];
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const parsedKey = parseTickKey(key);
    const eventActivity = [...tickActivity.values()].filter((entry) => entry.eventId === parsedKey.eventId);
    const activity = eventActivity.sort((left, right) =>
      String(right.lastUpdatedAt).localeCompare(String(left.lastUpdatedAt)),
    )[0];
    let payload = null;
    try {
      payload = values[index] ? JSON.parse(values[index]) : null;
    } catch {
      payload = values[index];
    }
    if (marketId && parsedKey.marketId !== String(marketId) && !payloadHasMarket(payload, marketId)) continue;
    items.push({
      key,
      ...parsedKey,
      ttl: await redis.ttl(key),
      lastUpdatedAt: activity?.lastUpdatedAt || null,
      tickCount: eventActivity.reduce((total, entry) => total + entry.tickCount, 0) || null,
      payload: includePayload ? payload : null,
    });
  }
  return { connected: true, scanned: items.length, truncated: cursor !== "0", items };
}

function getTickActivity(marketId) {
  const entries = [...tickActivity.values()].filter(
    (item) => !marketId || item.marketId === String(marketId),
  );
  return (
    entries.sort((left, right) => String(right.lastUpdatedAt).localeCompare(String(left.lastUpdatedAt)))[0] ||
    null
  );
}

async function getEventSnapshot(eventId) {
  const redis = await getRedisReadClient();
  if (!redis?.isOpen) return emptyEventPayload();
  const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventId}`;
  const value = await redis.get(key);
  if (!value) return emptyEventPayload();
  try {
    return frontendEventPayload(JSON.parse(value));
  } catch {
    return emptyEventPayload();
  }
}

async function getEventSnapshots(eventIds) {
  const redis = await getRedisReadClient();
  const ids = [...new Set((eventIds || []).map(String).filter(Boolean))];
  const snapshots = new Map();
  if (!redis?.isOpen || !ids.length) return snapshots;
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  const values = await redis.mGet(ids.map((eventId) => `${prefix}${eventId}`));
  ids.forEach((eventId, index) => {
    try {
      snapshots.set(eventId, values[index] ? frontendEventPayload(JSON.parse(values[index])) : null);
    } catch {
      snapshots.set(eventId, null);
    }
  });
  return snapshots;
}

async function removeMarkets(eventId, marketIds) {
  const normalizedIds = [...new Set((marketIds || []).map((id) => String(id)))];
  const redis = await getRedisClient();
  if (!redis?.isOpen || !normalizedIds.length) return new Set();
  const eventKey = String(eventId);
  return withEventLock(eventKey, async () => {
    const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventKey}`;
    let payload = eventPayloadCache.get(eventKey);
    if (!payload) {
      const value = await redis.get(key);
      if (!value) return new Set();
      try {
        payload = JSON.parse(value);
      } catch {
        return new Set();
      }
    }
    const idSet = new Set(normalizedIds);
    const removedIds = new Set();
    for (const group of PAYLOAD_GROUPS) {
      if (!Array.isArray(payload[group])) payload[group] = [];
      const filtered = [];
      for (const entry of payload[group]) {
        const entryId = entryMarketId(entry);
        if (idSet.has(entryId)) removedIds.add(entryId);
        else filtered.push(entry);
      }
      payload[group] = filtered;
    }
    if (!removedIds.size) return removedIds;
    await redis.set(key, JSON.stringify(payload), { EX: EVENT_TTL_SECONDS });
    setBounded(eventPayloadCache, eventKey, payload, CACHE_LIMIT);
    return removedIds;
  });
}

async function removeMarket(eventId, marketId) {
  const removedIds = await removeMarkets(eventId, [marketId]);
  return removedIds.size > 0;
}

async function removeEvent(eventId) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return false;
  const eventKey = String(eventId);
  return withEventLock(eventKey, async () => {
    const keys = [
      `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventKey}`,
      `${process.env.REDIS_SCORE_KEY_PREFIX || "Score-Rs:"}${eventKey}`,
    ];
    await redis.del(keys);
    eventPayloadCache.delete(eventKey);
    return true;
  });
}

async function closeRedis() {
  const current = client?.isOpen ? client : null;
  const currentRead = readClient?.isOpen ? readClient : null;
  client = undefined;
  readClient = undefined;
  await Promise.allSettled([current?.quit(), currentRead?.quit()].filter(Boolean));
}

function getRedisStatus() {
  return {
    configured: Boolean(redisUrl()),
    connected: Boolean(client?.isOpen),
    readConnected: Boolean(readClient?.isOpen),
    activityCount: tickActivity.size,
  };
}

module.exports = {
  getRedisClient,
  getRedisReadClient,
  writeTick,
  writeTicks,
  writeScore,
  getScore,
  writeEvents,
  getEvents,
  removeEventsFromMetadata,
  removeMarket,
  removeMarkets,
  removeEvent,
  getEventSnapshot,
  getEventSnapshots,
  inspectTicks,
  getTickActivity,
  getRedisStatus,
  closeRedis,
  bookmakerPayload,
  oddsPayload,
  fancyPayload,
  payloadGroup,
  runnerPrices,
  transformedTick,
  emptyEventPayload,
  loadRunnerNames,
  primeRunnerNames,
  preserveRunnerNames,
  shouldRemoveFromPayload,
  isFullySuspendedToss,
  moveTiedMatchLast,
  fancyDefinitionEntry,
  reconcileFancyDefinitions,
  regularDefinitionEntries,
  reconcileRegularDefinitions,
  normalizeEventPayload,
  frontendEventPayload,
  validMarketIdentifier,
  invalidateMarkets,
  __testing__: {
    setRedisClient(fakeClient) {
      client = fakeClient;
      readClient = fakeClient;
    },
    primeMarketCache(entries) {
      for (const [id, market] of entries) marketCache.set(String(id), market);
    },
    reset() {
      client = undefined;
      readClient = undefined;
      marketCache.clear();
      eventPayloadCache.clear();
      eventLocks.clear();
    },
  },
};
