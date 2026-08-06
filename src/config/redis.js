const { createClient } = require("redis");
const { getSourcePool } = require("./sourceDb");
const Market = require("../models/Market");
const provider = require("../services/providerApi");
const logger = require("../utils/logger");

let client;
let connecting;
const marketCache = new Map();
const runnerNameCache = new Map();
const runnerNameLoads = new Map();
const eventPayloadCache = new Map();
const tickActivity = new Map();

const PAYLOAD_GROUPS = ["Odds", "Bookmaker", "LineMarket", "Fancy2", "OddEven", "OtherMarket",
  "Fancy3", "CricketCasino", "BallByBall"];

function emptyEventPayload() {
  return Object.fromEntries(PAYLOAD_GROUPS.map((group) => [group, []]));
}

function normalizeEventPayload(payload) {
  const normalized = emptyEventPayload();
  for (const group of PAYLOAD_GROUPS) normalized[group] = Array.isArray(payload?.[group]) ? payload[group] : [];
  // Move line markets written by older builds out of the generic Odds group immediately.
  const legacyLineMarkets = normalized.Odds.filter((market) => /\bline\b/i.test(String(market?.Name || "")));
  for (const market of legacyLineMarkets) {
    if (!normalized.LineMarket.some((item) => entryMarketId(item) === entryMarketId(market))) {
      normalized.LineMarket.push(market);
    }
  }
  normalized.Odds = normalized.Odds.filter((market) => !/\bline\b/i.test(String(market?.Name || "")));
  for (const entry of normalized.Fancy3) {
    if (!normalized.OtherMarket.some((item) => entryMarketId(item) === entryMarketId(entry))) {
      normalized.OtherMarket.push(entry);
    }
  }
  normalized.Fancy3 = [];
  return normalized;
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
  client = createClient({ url, database: Number(process.env.REDIS_DB || 0),
    socket: { connectTimeout: Number(process.env.REDIS_TIMEOUT_MS || 15000) } });
  client.on("error", (error) => logger.error("[Redis] client error", { error: error.message }));
  connecting = client.connect().then(() => {
    logger.info("[Redis] connected"); return client;
  }).catch((error) => {
    logger.error("[Redis] connection failed", { error: error.message }); client = undefined; return null;
  }).finally(() => { connecting = undefined; });
  return connecting;
}

async function findMarket(mid) {
  if (marketCache.has(mid)) return marketCache.get(mid);
  const [rows] = await getSourcePool().query("SELECT * FROM t_market WHERE marketid = ? LIMIT 1", [mid]);
  if (rows.length) {
    const market = Market.fromRow(rows[0]); marketCache.set(mid, market); return market;
  }
  const [fancies] = await getSourcePool().query(
    `SELECT f.*, f.fancyid AS marketid, f.name AS marketname,
       COALESCE(f.sportid,e.sportid) AS sportid, e.eventname AS matchname,
       e.open_date AS opendate, e.in_play AS inplay
     FROM t_matchfancy f LEFT JOIN t_event e ON e.eventid=f.eventid
     WHERE f.fancyid=? LIMIT 1`, [mid],
  );
  if (!fancies.length) return null;
  const market = Market.fromRow(fancies[0]); marketCache.set(mid, market); return market;
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
    price: numberOr(entry?.price ?? entry?.p, 0), size: numberOr(entry?.size ?? entry?.s, 0),
  }));
}

function runnerPrices(runner, side) {
  const exchangeKey = side === "back" ? "availableToBack" : "availableToLay";
  const arrayValue = side === "back" ? runner?.b : runner?.l;
  if (Array.isArray(arrayValue)) return prices(arrayValue);
  if (Array.isArray(runner?.ex?.[exchangeKey])) return prices(runner.ex[exchangeKey]);
  const pricePrefix = side === "back" ? "b" : "l";
  const sizePrefix = side === "back" ? "br" : "lr";
  const hasCompactPrices = [1, 2, 3].some((level) =>
    runner?.[`${pricePrefix}${level}`] != null || runner?.[`${sizePrefix}${level}`] != null);
  if (!hasCompactPrices) return [];
  return [1, 2, 3].map((level) => ({
    price: numberOr(runner?.[`${pricePrefix}${level}`], 0),
    size: numberOr(runner?.[`${sizePrefix}${level}`], 0),
  }));
}

function marketMatchName(item, market) {
  return market.matchname || item.mn || item.matchName
    || (Array.isArray(item.r) ? item.r.map((runner) => runner?.na ?? runner?.name).filter(Boolean).join(" v ") : "")
    || null;
}

function bookmakerPayload(item, market) {
  const runners = Array.isArray(item.r) ? item.r : [];
  const matchName = marketMatchName(item, market);
  return runners.map((runner) => ({
    mid: String(item.mid), t: market.marketname || item.na || "", sid: runner?.rid ?? runner?.selectionId ?? null,
    nation: runner?.na ?? runner?.name ?? null, b1: numberOr(runner?.b ?? runner?.b1 ?? runner?.back),
    bs1: numberOr(runner?.bs ?? runner?.bs1, 0), l1: numberOr(runner?.l ?? runner?.l1 ?? runner?.lay),
    ls1: numberOr(runner?.ls ?? runner?.ls1, 0), gstatus: status(runner?.sb ?? runner?.gstatus),
    rem: runner?.rem ?? item.rem ?? null, display_message: market.displayMessage ?? null,
    betlock: numberOr(market.betDelay, 0), minBet: numberOr(market.minbet), maxBet: numberOr(market.maxbet),
    betDelay: numberOr(market.betDelay, 0), minBetRate: numberOr(market.minBetRate, 0),
    maxBetRate: numberOr(market.maxBetRate, 0), matchName, matchId: numberOr(market.eventid, market.eventid),
    isActive: booleanOr(market.isactive), isPause: booleanOr(market.isPause ?? market.fancypause, false),
    sportId: numberOr(market.sportid, market.sportid),
  }));
}

function oddsPayload(item, market, runnerNames = runnerNameCache.get(String(item.mid))) {
  const runners = Array.isArray(item.r) ? item.r : [];
  return {
    matchName: marketMatchName(item, market), marketId: String(item.mid), status: item.s ?? item.status ?? "",
    inplay: booleanOr(item.ip ?? market.inPlay, false), eventTime: market.opendate ?? null,
    lastMatchTime: item.tm ?? null, maxBetRate: numberOr(market.maxBetRate, 0),
    minBetRate: numberOr(market.minBetRate, 0), betDelay: numberOr(market.betDelay, 0),
    maxBet: numberOr(market.maxbet), minBet: numberOr(market.minbet), betlock: booleanOr(market.betlock),
    matchId: numberOr(market.eventid, market.eventid), display_message: market.displayMessage ?? null,
    runners: runners.map((runner) => ({
      selectionId: runner?.rid ?? runner?.selectionId ?? null, handicap: numberOr(runner?.hc ?? runner?.handicap, 0),
      status: runner?.s ?? runner?.status ?? "ACTIVE", lastPriceTraded: numberOr(runner?.ltp ?? runner?.lastPriceTraded, 0),
      totalMatched: numberOr(runner?.tv ?? runner?.totalMatched, 0),
      adjustmentFactor: numberOr(runner?.af ?? runner?.adjustmentFactor, 0),
      ex: { availableToBack: runnerPrices(runner, "back"),
        availableToLay: runnerPrices(runner, "lay") },
      name: runner?.na ?? runner?.name
        ?? runnerNames?.get(String(runner?.rid ?? runner?.selectionId)) ?? null,
    })),
    marketDataDelayed: booleanOr(item.delay ?? item.marketDataDelayed, false), Name: market.marketname || item.na || "",
    isActive: booleanOr(market.isactive), isPause: booleanOr(market.isPause ?? market.fancypause, false),
    sportId: numberOr(market.sportid, market.sportid),
  };
}

function fancyPayload(item, market) {
  const runner = Array.isArray(item.r) ? item.r[0] || {} : {};
  const cricketCasino = String(item.mid || "").toUpperCase().includes("-CC");
  const casinoRate = cricketCasino ? numberOr(runner.ra ?? item.ra) : null;
  return {
    mid: String(item.mid), sid: String(runner.rid ?? item.mid), nation: runner.na ?? item.na ?? market.marketname ?? null,
    b1: numberOr(runner.b ?? runner.b1 ?? item.b ?? item.b1, casinoRate),
    l1: numberOr(runner.l ?? runner.l1 ?? item.l ?? item.l1),
    bs1: numberOr(runner.bs ?? runner.bs1 ?? item.br ?? item.bs ?? item.bs1, cricketCasino ? null : 0),
    ls1: numberOr(runner.ls ?? runner.ls1 ?? item.lr ?? item.ls ?? item.ls1, cricketCasino ? null : 0),
    gstatus: status(runner.sb ?? item.sb), rem: runner.rem ?? item.rem ?? item.res ?? "",
    srno: String(item.srno ?? item.di ?? runner.srno ?? ""),
    gameover: booleanOr(item.go, false), s: booleanOr(item.s, true), maxBet: numberOr(market.maxbet),
    minBet: numberOr(market.minbet), betDelay: numberOr(market.betDelay, 0),
    matchId: numberOr(market.eventid, market.eventid), isActive: booleanOr(market.isactive),
    isShow: booleanOr(market.isShow, true), matchName: market.matchname ?? null,
    matchType: market.markettype ?? null, maxLiabilityPerMarket: numberOr(market.maximumProfit),
    rate: casinoRate,
  };
}

function payloadGroup(item, market) {
  const id = String(item.mid).toUpperCase();
  const name = String(market.marketname || "").toLowerCase();
  if (id.includes("BM") || name.includes("bookmaker") || name === "toss") return "Bookmaker";
  // Vendor line-market IDs look like normal exchange IDs (for example 1.260761724),
  // so their persisted market name is the stable discriminator available on socket ticks.
  if (/\bline\b/.test(name)) return "LineMarket";
  if (id.includes("OE") || name.includes("odd even")) return "OddEven";
  if (id.includes("F3") || name.includes("other market")) return "OtherMarket";
  if (id.includes("BB") || name.includes("ball by ball")) return "BallByBall";
  if (id.includes("-CC") || id.includes("CASINO") || name.includes("casino")) return "CricketCasino";
  if (id.includes("F2") || id.includes("KD") || id.includes("MT")) return "Fancy2";
  return "Odds";
}

function transformedTick(item, market) {
  const group = payloadGroup(item, market);
  const entries = group === "Bookmaker" ? bookmakerPayload(item, market)
    : ["Odds", "LineMarket"].includes(group) ? [oddsPayload(item, market)] : [fancyPayload(item, market)];
  return { group, entries };
}

function fancyDefinitionEntry(market) {
  return {
    mid: String(market.marketId), sid: String(market.marketId), nation: market.marketName,
    b1: null, l1: null, bs1: 0, ls1: 0, gstatus: "WAITING", rem: "", srno: "",
    gameover: false, s: true, maxBet: numberOr(market.maxBet, 100000),
    minBet: numberOr(market.minBet, 100), betDelay: numberOr(market.betDelay, 0),
    matchId: numberOr(market.eventId, market.eventId), isActive: true, isShow: true,
    matchName: market.matchName ?? null, matchType: market.marketType ?? null,
    maxLiabilityPerMarket: 100000, rate: null,
  };
}

async function reconcileFancyDefinitions(markets) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { events: 0, added: 0, removed: 0 };
  const byEvent = new Map();
  for (const market of markets || []) {
    if (!market?.eventId || !market?.marketId) continue;
    const group = payloadGroup({ mid: market.marketId }, { marketname: market.marketName });
    if (["Odds", "Bookmaker"].includes(group)) continue;
    const eventId = String(market.eventId);
    if (!byEvent.has(eventId)) byEvent.set(eventId, []);
    byEvent.get(eventId).push({ market, group });
  }
  let added = 0; let removed = 0;
  for (const [eventId, definitions] of byEvent) {
    const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventId}`;
    let payload = emptyEventPayload();
    const current = await redis.get(key);
    if (current) {
      try {
        const parsed = JSON.parse(current);
        payload = normalizeEventPayload(parsed);
      } catch { /* replace malformed payload */ }
    }
    let changed = false;
    for (const { market, group } of definitions) {
      const marketId = String(market.marketId);
      const index = payload[group].findIndex((entry) => entryMarketId(entry) === marketId);
      const active = market.isActive && !market.gameOver;
      if (active && index < 0) {
        payload[group].push(fancyDefinitionEntry(market)); added += 1; changed = true;
      } else if (active && index >= 0 && payload[group][index]?.gstatus === "WAITING") {
        payload[group][index] = fancyDefinitionEntry(market); changed = true;
      } else if (!active && index >= 0) {
        payload[group].splice(index, 1); removed += 1; changed = true;
      }
    }
    if (changed) {
      await redis.set(key, JSON.stringify(payload));
      eventPayloadCache.set(eventId, payload);
    }
  }
  return { events: byEvent.size, added, removed };
}

function regularDefinitionEntries(market, runners = []) {
  const marketRow = {
    eventid: market.eventId, sportid: market.sportId, marketid: market.marketId,
    marketname: market.marketName, matchname: market.matchName, opendate: market.openDate,
    inplay: market.inPlay, isactive: market.isActive, minbet: market.minBet,
    maxbet: market.maxBet, betDelay: market.betDelay, minBetRate: 1, maxBetRate: 500,
  };
  const itemRunners = runners.map((runner) => ({
    rid: runner.selectionId, na: runner.runnerName, s: "SUSPENDED", sb: "S",
    b: [], l: [], b1: null, l1: null, bs1: 0, ls1: 0,
  }));
  const group = payloadGroup({ mid: market.marketId }, marketRow);
  if (group === "Bookmaker") {
    const entries = bookmakerPayload({ eid: market.eventId, mid: market.marketId,
      na: market.marketName, s: "WAITING", r: itemRunners }, marketRow);
    return { group, entries: entries.length ? entries : [{
      mid: String(market.marketId), t: market.marketName, sid: String(market.marketId),
      nation: market.marketName, b1: null, bs1: 0, l1: null, ls1: 0,
      gstatus: "WAITING", rem: null, display_message: null, betlock: 0,
      minBet: numberOr(market.minBet), maxBet: numberOr(market.maxBet),
      betDelay: numberOr(market.betDelay, 0), minBetRate: 1, maxBetRate: 500,
      matchName: market.matchName ?? null, matchId: numberOr(market.eventId, market.eventId),
      isActive: true, isPause: false, sportId: numberOr(market.sportId, market.sportId),
    }] };
  }
  return { group, entries: [oddsPayload({ eid: market.eventId, mid: market.marketId,
    na: market.marketName, s: "WAITING", ip: market.inPlay, r: itemRunners }, marketRow)] };
}

async function reconcileRegularDefinitions(markets) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { events: 0, added: 0 };
  const byEvent = new Map();
  for (const market of markets || []) {
    if (!market?.eventId || !market?.marketId || !market.isActive || market.gameOver) continue;
    const eventId = String(market.eventId);
    if (!byEvent.has(eventId)) byEvent.set(eventId, []);
    byEvent.get(eventId).push(market);
  }
  let added = 0;
  for (const [eventId, definitions] of byEvent) {
    const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventId}`;
    let payload = emptyEventPayload();
    const current = await redis.get(key);
    if (current) {
      try { payload = normalizeEventPayload(JSON.parse(current)); } catch { /* replace malformed payload */ }
    }
    let changed = false;
    for (const market of definitions) {
      if (payloadHasMarket(payload, market.marketId)) continue;
      const { group, entries } = regularDefinitionEntries(market, market.runners || []);
      payload[group].push(...entries); added += 1; changed = true;
    }
    if (changed) {
      await redis.set(key, JSON.stringify(payload)); eventPayloadCache.set(eventId, payload);
    }
  }
  return { events: byEvent.size, added };
}

function entryMarketId(entry) { return String(entry.marketId ?? entry.mid ?? ""); }

function shouldRemoveFromPayload(group, item) {
  if (group === "Bookmaker") return booleanOr(item.go, false);
  if (group === "Odds") return false;
  return booleanOr(item.go, false) || !booleanOr(item.s, true);
}

async function loadRunnerNames(marketId) {
  const normalized = String(marketId);
  if (runnerNameCache.has(normalized)) return runnerNameCache.get(normalized);
  if (runnerNameLoads.has(normalized)) return runnerNameLoads.get(normalized);
  const loading = provider.runners(normalized).then((response) => {
    const rows = Array.isArray(response?.data) ? response.data
      : Array.isArray(response) ? response : [];
    const names = new Map(rows.map((runner) => [
      String(runner?.runnerId ?? runner?.selectionId ?? runner?.id), runner?.name ?? runner?.nation ?? null,
    ]).filter(([selectionId, name]) => selectionId && name));
    runnerNameCache.set(normalized, names);
    return names;
  }).catch((error) => {
    logger.warn("[Redis] runner metadata lookup failed", { marketId: normalized, error: error.message });
    return new Map();
  }).finally(() => runnerNameLoads.delete(normalized));
  runnerNameLoads.set(normalized, loading);
  return loading;
}

async function primeRunnerNames(marketIds) {
  await Promise.allSettled((marketIds || []).map((marketId) => loadRunnerNames(marketId)));
}

function preserveRunnerNames(entries, previousEntries) {
  const previous = new Map((previousEntries || []).flatMap((entry) =>
    (entry.runners || []).map((runner) => [String(runner.selectionId), runner.name])));
  for (const entry of entries) {
    for (const runner of entry.runners || []) {
      if (!runner.name) runner.name = previous.get(String(runner.selectionId)) ?? null;
    }
  }
}

async function writeTick(item) {
  if (!item || typeof item !== "object" || !item.eid || !item.mid) return false;
  const redis = await getRedisClient(); if (!redis?.isOpen) return false;
  const market = await findMarket(String(item.mid)); if (!market) return false;
  if (booleanOr(market.isactive, false) === false) return false;
  const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${item.eid}`;
  let payload = eventPayloadCache.get(String(item.eid));
  if (!payload) {
    payload = emptyEventPayload();
    const current = await redis.get(key);
    if (current) {
      try {
        const parsed = JSON.parse(current);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          payload = normalizeEventPayload(parsed);
        }
      } catch { /* Replace malformed legacy data with a valid event payload. */ }
    }
  }
  if (payloadGroup(item, market) === "Odds") await loadRunnerNames(item.mid);
  const { group, entries } = transformedTick(item, market);
  const previousEntries = payload[group].filter((entry) => entryMarketId(entry) === String(item.mid));
  if (group === "Odds") preserveRunnerNames(entries, previousEntries);
  // A market can change grouping as discovery metadata improves; keep exactly one copy.
  for (const payloadGroupName of PAYLOAD_GROUPS) {
    payload[payloadGroupName] = payload[payloadGroupName]
      .filter((entry) => entryMarketId(entry) !== String(item.mid));
  }
  if (!shouldRemoveFromPayload(group, item)) payload[group].push(...entries);
  await redis.set(key, JSON.stringify(payload));
  eventPayloadCache.set(String(item.eid), payload);
  recordTickActivity(`${key}:${item.mid}`, item.eid, item.mid); return payload;
}

function recordTickActivity(key, eventId, marketId) {
  const previous = tickActivity.get(key);
  tickActivity.set(key, { eventId: String(eventId), marketId: String(marketId),
    lastUpdatedAt: new Date().toISOString(), tickCount: (previous?.tickCount || 0) + 1 });
}

function parseTickKey(key) {
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  if (!key.startsWith(prefix)) return null;
  const value = key.slice(prefix.length); const separator = value.indexOf(":");
  return separator < 0 ? { eventId: value, marketId: null }
    : { eventId: value.slice(0, separator), marketId: value.slice(separator + 1) };
}

function payloadHasMarket(payload, marketId) {
  return PAYLOAD_GROUPS.some((group) => Array.isArray(payload?.[group])
    && payload[group].some((entry) => entryMarketId(entry) === String(marketId)));
}

async function inspectTicks({ eventId, marketId, limit = 250, includePayload = false } = {}) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return { connected: false, scanned: 0, truncated: false, items: [] };
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  const boundedLimit = Math.max(1, Math.min(500, Number(limit) || 250));
  const pattern = eventId ? `${prefix}${eventId}*` : `${prefix}*`;
  let cursor = "0"; const keys = [];
  do {
    const result = await redis.scan(cursor, { MATCH: pattern, COUNT: 200 }); cursor = String(result.cursor);
    keys.push(...result.keys.slice(0, boundedLimit - keys.length));
  } while (cursor !== "0" && keys.length < boundedLimit);
  const values = keys.length ? await redis.mGet(keys) : [];
  const items = [];
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index]; const parsedKey = parseTickKey(key);
    const eventActivity = [...tickActivity.values()].filter((entry) => entry.eventId === parsedKey.eventId);
    const activity = eventActivity.sort((left, right) => String(right.lastUpdatedAt).localeCompare(String(left.lastUpdatedAt)))[0];
    let payload = null;
    try { payload = values[index] ? JSON.parse(values[index]) : null; } catch { payload = values[index]; }
    if (marketId && parsedKey.marketId !== String(marketId) && !payloadHasMarket(payload, marketId)) continue;
    items.push({ key, ...parsedKey, ttl: await redis.ttl(key), lastUpdatedAt: activity?.lastUpdatedAt || null,
      tickCount: eventActivity.reduce((total, entry) => total + entry.tickCount, 0) || null,
      payload: includePayload ? payload : null });
  }
  return { connected: true, scanned: items.length, truncated: cursor !== "0", items };
}

function getTickActivity(marketId) {
  const entries = [...tickActivity.values()].filter((item) => !marketId || item.marketId === String(marketId));
  return entries.sort((left, right) => String(right.lastUpdatedAt).localeCompare(String(left.lastUpdatedAt)))[0] || null;
}

async function getEventSnapshot(eventId) {
  const redis = await getRedisClient(); if (!redis?.isOpen) return emptyEventPayload();
  const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventId}`;
  const value = await redis.get(key); if (!value) return emptyEventPayload();
  try { return normalizeEventPayload(JSON.parse(value)); } catch { return emptyEventPayload(); }
}

async function getEventSnapshots(eventIds) {
  const redis = await getRedisClient();
  const ids = [...new Set((eventIds || []).map(String).filter(Boolean))];
  const snapshots = new Map();
  if (!redis?.isOpen || !ids.length) return snapshots;
  const prefix = process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:";
  const values = await redis.mGet(ids.map((eventId) => `${prefix}${eventId}`));
  ids.forEach((eventId, index) => {
    try { snapshots.set(eventId, values[index] ? normalizeEventPayload(JSON.parse(values[index])) : null); }
    catch { snapshots.set(eventId, null); }
  });
  return snapshots;
}

async function removeMarket(eventId, marketId) {
  const redis = await getRedisClient();
  if (!redis?.isOpen) return false;
  const eventKey = String(eventId); const normalizedMarketId = String(marketId);
  const key = `${process.env.REDIS_TICK_KEY_PREFIX || "Data-Rs:"}${eventKey}`;
  let payload = eventPayloadCache.get(eventKey);
  if (!payload) {
    const value = await redis.get(key);
    if (!value) return false;
    try { payload = JSON.parse(value); } catch { return false; }
  }
  let removed = false;
  for (const group of PAYLOAD_GROUPS) {
    if (!Array.isArray(payload[group])) payload[group] = [];
    const filtered = payload[group].filter((entry) => entryMarketId(entry) !== normalizedMarketId);
    if (filtered.length !== payload[group].length) removed = true;
    payload[group] = filtered;
  }
  if (!removed) return false;
  await redis.set(key, JSON.stringify(payload));
  eventPayloadCache.set(eventKey, payload);
  return true;
}

async function closeRedis() {
  if (!client?.isOpen) return; const current = client; client = undefined; await current.quit();
}

function getRedisStatus() {
  return { configured: Boolean(redisUrl()), connected: Boolean(client?.isOpen), activityCount: tickActivity.size };
}

module.exports = { getRedisClient, writeTick, removeMarket, getEventSnapshot, getEventSnapshots, inspectTicks, getTickActivity,
  getRedisStatus, closeRedis, bookmakerPayload, oddsPayload, fancyPayload, payloadGroup, runnerPrices,
  transformedTick, emptyEventPayload, loadRunnerNames, primeRunnerNames, preserveRunnerNames,
  shouldRemoveFromPayload, fancyDefinitionEntry, reconcileFancyDefinitions, regularDefinitionEntries,
  reconcileRegularDefinitions, normalizeEventPayload };
