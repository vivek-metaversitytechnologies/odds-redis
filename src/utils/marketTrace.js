const path = require("node:path");
const winston = require("winston");
const logger = require("./logger");
const { integer } = require("../config/env");
const { setBounded } = require("./boundedMap");
require("winston-daily-rotate-file");

// Lifecycle trace for ball-by-ball, line and cricket-casino markets, written as JSON Lines (one event per line)
// for analysis with jq or scripts/marketTrace.js. Each line has ts, kind (BB|LINE), stage,
// eventId, marketId and stage-specific fields. Only transitions are written: repeated identical
// states are deduplicated and repeated rejections are throttled, so high-frequency ticks stay cheap.
//
// Stages:
//   discovery.row         vendor discovery row whose state changed
//   discovery.decision    new | changed | deactivate | retire
//   discovery.omitted     a known market missing from a successful discovery response (retained)
//   db.upsert             inserted | updated, with the persisted flags
//   definition            placeholder added | removed, or blocked by a terminal set
//   tick.state            socket/price tick whose status, go, result or runner states changed
//   tick.rejected         tick dropped before the payload (no-db-row | inactive-in-db)
//   tick.blocked          active-looking tick kept hidden by a terminal/unavailable set
//   visibility            shown | hidden transition in the event payload, with reason
//   line.set              line market added to / removed from the terminal or unavailable set
//   bb.terminal           ball-by-ball market added to the terminal set
//   bb.reopened           terminal ball-by-ball market reopened by a newer live socket tick
//   price.seed            HTTP runner price seed for a line market failed or returned nothing
//   subscription          subscribed | already-registered | unresolved | unsubscribed
//   result                result persisted or rejected
//   socket.gameover       any market type's socket game-over with its `res` (not limited to BB/LINE/CC)

const KIND_LIMIT = 200000;
const STATE_LIMIT = 400000;
const kinds = new Map();
const lastState = new Map();
const throttled = new Map();
let instance;

function enabled() {
  return String(process.env.MARKET_TRACE_ENABLED || "true").toLowerCase() === "true";
}

function getLogger() {
  if (instance) return instance;
  const transport = new winston.transports.DailyRotateFile({
    dirname: path.resolve(process.env.MARKET_TRACE_LOG_DIR || "logs/market-trace"),
    filename: "market-trace-%DATE%.jsonl",
    datePattern: "YYYY-MM-DD",
    maxSize: process.env.MARKET_TRACE_LOG_MAX_SIZE || "100m",
    maxFiles: process.env.MARKET_TRACE_LOG_MAX_FILES || "7d",
    zippedArchive: false,
  });
  instance = winston.createLogger({
    level: "info",
    format: winston.format.printf((info) => JSON.stringify(info.record)),
    transports: [transport],
    exitOnError: false,
  });
  instance.on("error", (error) => logger.error("[MarketTrace] write failed", { error: error.message }));
  return instance;
}

// Kinds traced by default. Session-style fancies (F2, KD, OE, F3, MT) change state on nearly every
// ball, so they are opt-in through MARKET_TRACE_KINDS for short investigations.
const DEFAULT_KINDS = "BB,LINE,CC";
let kindsSetting = { raw: null, set: new Set() };

function enabledKinds() {
  const raw = String(process.env.MARKET_TRACE_KINDS || DEFAULT_KINDS);
  if (kindsSetting.raw !== raw) {
    kindsSetting = { raw, set: new Set(raw.split(",").map((kind) => kind.trim().toUpperCase()).filter(Boolean)) };
  }
  return kindsSetting.set;
}

function kindFromHint({ marketId, marketType, group } = {}) {
  const type = String(marketType || "").toLowerCase();
  const id = String(marketId || "");
  if (type === "ball-by-ball" || group === "BallByBall" || /-BB$/i.test(id)) return "BB";
  if (type === "line-market" || group === "LineMarket") return "LINE";
  // Cricket casino results trail the over by a long way on the results API; tracing their few
  // markets shows whether the socket game-over already carries the result.
  if (type === "cricket-casino" || group === "CricketCasino" || /-CC$/i.test(id)) return "CC";
  if (type === "session" || group === "Fancy2" || /-F2$/i.test(id)) return "F2";
  if (type === "khado" || group === "Khado" || /-KD$/i.test(id)) return "KD";
  if (type === "odd-even" || group === "OddEven" || /-OE$/i.test(id)) return "OE";
  if (type === "other-market" || group === "Fancy3" || group === "OtherMarket" || /-F3$/i.test(id)) return "F3";
  if (type === "meter" || group === "Meter" || /-MT$/i.test(id)) return "MT";
  return null;
}

// Resolves whether a market is traced. Line market ids look like any Betfair id, so a market is
// remembered once any caller identifies it (discovery, DB row, payload group).
function kindOf(marketId, hint = {}) {
  const id = String(marketId ?? "");
  if (!id) return null;
  const hinted = kindFromHint({ ...hint, marketId: id });
  if (hinted && kinds.get(id) !== hinted) setBounded(kinds, id, hinted, KIND_LIMIT);
  const kind = hinted || kinds.get(id) || null;
  // Only enabled kinds are traced (and pay for tick-state building in the tick path).
  return kind && enabledKinds().has(kind) ? kind : null;
}

function write(stage, fields) {
  try {
    const { marketId, eventId, kind, ...rest } = fields;
    getLogger().info("trace", {
      record: {
        ts: new Date().toISOString(),
        kind,
        stage,
        eventId: eventId == null ? null : String(eventId),
        marketId: String(marketId),
        ...rest,
      },
    });
  } catch (error) {
    logger.error("[MarketTrace] write failed", { error: error.message });
  }
}

function resolve(fields, hint) {
  if (!enabled()) return null;
  const kind = kindOf(fields?.marketId, hint);
  return kind ? { ...fields, kind } : null;
}

function trace(stage, fields, hint = {}) {
  const resolved = resolve(fields, hint);
  if (resolved) write(stage, resolved);
}

// Every market type's socket game-over, bypassing the BB/LINE/CC filter: one line per market when
// it ends, so the volume stays small. Shows which families carry their result in `res`.
function traceGameOver(fields) {
  if (!enabled() || !fields?.marketId) return;
  write("socket.gameover", fields);
}

// Writes only when `state` differs from the last state written for this market and slot.
function traceChange(slot, stage, fields, state, hint = {}) {
  const resolved = resolve(fields, hint);
  if (!resolved) return false;
  const key = `${slot}|${resolved.marketId}`;
  const fingerprint = typeof state === "string" ? state : JSON.stringify(state);
  if (lastState.get(key) === fingerprint) return false;
  setBounded(lastState, key, fingerprint, STATE_LIMIT);
  write(stage, resolved);
  return true;
}

// Writes at most once per interval per market and reason, reporting how many were suppressed.
function traceThrottled(stage, fields, reason, hint = {}) {
  const resolved = resolve(fields, hint);
  if (!resolved) return;
  const intervalMs = integer("MARKET_TRACE_THROTTLE_MS", 30000, { min: 1000 });
  const key = `${stage}|${reason}|${resolved.marketId}`;
  const now = Date.now();
  const entry = throttled.get(key);
  if (entry && now - entry.at < intervalMs) {
    entry.suppressed += 1;
    return;
  }
  setBounded(throttled, key, { at: now, suppressed: 0 }, STATE_LIMIT);
  write(stage, { ...resolved, reason, ...(entry?.suppressed ? { suppressedSinceLast: entry.suppressed } : {}) });
}

function tickState(item) {
  return {
    s: item?.s ?? null,
    sb: item?.sb ?? null,
    go: item?.go ?? null,
    rt: item?.rt ?? null,
    res: item?.res ?? null,
    runners: Array.isArray(item?.r) ? item.r.map((runner) => [runner?.rid ?? null, runner?.s ?? runner?.sb ?? null]) : [],
  };
}

function tickPrices(item) {
  return Array.isArray(item?.r)
    ? item.r.map((runner) => ({
        rid: runner?.rid ?? runner?.selectionId ?? null,
        na: runner?.na ?? null,
        s: runner?.s ?? runner?.sb ?? null,
        b1: runner?.b1 ?? runner?.back ?? null,
        bs1: runner?.bs1 ?? null,
        l1: runner?.l1 ?? runner?.lay ?? null,
        ls1: runner?.ls1 ?? null,
      }))
    : [];
}

async function closeMarketTrace() {
  if (!instance) return;
  const current = instance;
  instance = undefined;
  const flushed = current.transports.map(
    (transport) =>
      new Promise((resolve) => {
        transport.logStream.once("finish", resolve);
        transport.logStream.once("error", resolve);
      }),
  );
  await new Promise((resolve) => {
    current.once("finish", resolve);
    current.once("error", resolve);
    current.end();
  });
  current.close();
  await Promise.all(flushed);
}

module.exports = {
  kindOf,
  trace,
  traceGameOver,
  traceChange,
  traceThrottled,
  tickState,
  tickPrices,
  closeMarketTrace,
  __testing__: {
    reset() {
      kinds.clear();
      lastState.clear();
      throttled.clear();
    },
    setLogger(fake) {
      instance = fake;
    },
  },
};
