const fs = require("node:fs");
const path = require("node:path");
const readline = require("node:readline");

// Analysis over logs/market-trace/*.jsonl (see src/utils/marketTrace.js). Shared by
// scripts/marketTrace.js and GET /api/market-trace; every function returns plain data.

const MAX_DAYS = 7;

function traceDir(dir) {
  return path.resolve(dir || process.env.MARKET_TRACE_LOG_DIR || "logs/market-trace");
}

function traceFiles({ dir, days = 1 } = {}) {
  const directory = traceDir(dir);
  if (!fs.existsSync(directory)) return [];
  const count = Math.min(MAX_DAYS, Math.max(1, Number(days) || 1));
  return fs
    .readdirSync(directory)
    .filter((name) => /^market-trace-.*\.jsonl$/.test(name))
    .sort()
    .slice(-count)
    .map((name) => path.join(directory, name));
}

async function* records(options, filter = () => true) {
  for (const file of traceFiles(options)) {
    const lines = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
    for await (const line of lines) {
      if (!line) continue;
      let record;
      try {
        record = JSON.parse(line);
      } catch {
        continue;
      }
      if (filter(record)) yield record;
    }
  }
}

function emptyMarket(record) {
  return {
    kind: record.kind,
    eventId: record.eventId,
    marketId: record.marketId,
    name: null,
    firstDiscoveredAt: null,
    discoveredActive: false,
    firstTickAt: null,
    lastTickAt: null,
    ticks: 0,
    visibility: null,
    visibilityReason: null,
    visibilityAt: null,
    shownCount: 0,
    hiddenCount: 0,
    terminalAt: null,
    terminalSource: null,
    discoveryActiveAfterTerminal: 0,
    shownAfterTerminal: 0,
    rejected: {},
    blocked: {},
    omitted: 0,
    goAt: null,
    resultAt: null,
    result: null,
    resultOutcome: null,
    subscription: null,
    seedFailures: 0,
  };
}

function bump(map, key, by = 1) {
  map[key] = (map[key] || 0) + by;
}

function counted(record) {
  return 1 + (record.suppressedSinceLast || 0);
}

async function collect(options, filter) {
  const markets = new Map();
  let lastTs = null;
  for await (const record of records(options, filter)) {
    lastTs = record.ts;
    const market = markets.get(record.marketId) || emptyMarket(record);
    markets.set(record.marketId, market);
    if (record.eventId && !market.eventId) market.eventId = record.eventId;
    const at = Date.parse(record.ts);
    switch (record.stage) {
      case "discovery.row":
        if (record.name) market.name = record.name;
        market.firstDiscoveredAt ??= at;
        if (record.isActive && !record.gameOver && !record.recalled) {
          market.discoveredActive = true;
          if (market.terminalAt && at > market.terminalAt) market.discoveryActiveAfterTerminal += 1;
        }
        break;
      case "tick.state":
        if (record.name) market.name ??= record.name;
        market.firstTickAt ??= at;
        market.lastTickAt = at;
        market.ticks += 1;
        if (record.go === true || record.go === 1 || record.go === "true") market.goAt ??= at;
        break;
      case "visibility":
        market.visibility = record.visibility;
        market.visibilityReason = record.reason;
        market.visibilityAt = at;
        if (record.visibility === "shown") {
          market.shownCount += 1;
          if (market.terminalAt && at > market.terminalAt) market.shownAfterTerminal += 1;
        } else market.hiddenCount += 1;
        break;
      case "bb.terminal":
        market.terminalAt ??= at;
        market.terminalSource ??= record.source;
        break;
      case "line.set":
        if (record.set === "terminal") {
          market.terminalAt ??= at;
          market.terminalSource ??= "line-go";
        }
        break;
      case "tick.rejected":
        bump(market.rejected, record.reason, counted(record));
        break;
      case "tick.blocked":
        bump(market.blocked, record.reason, counted(record));
        break;
      case "definition":
        if (record.action === "blocked") bump(market.blocked, `definition:${record.reason}`, counted(record));
        break;
      case "discovery.omitted":
        market.omitted += counted(record);
        break;
      case "result":
        if (record.outcome === "socket-game-over") market.goAt ??= at;
        else {
          market.resultAt ??= at;
          market.result = record.value ?? record.result ?? null;
          market.resultOutcome = record.outcome;
        }
        break;
      case "subscription":
        market.subscription = record.outcome;
        break;
      case "price.seed":
        market.seedFailures += counted(record);
        break;
      default:
    }
  }
  return { markets, lastTs };
}

function issues(market, { lastTs, noTickMs = 120000, noResultMs = 1800000 } = {}) {
  const found = [];
  const end = lastTs ? Date.parse(lastTs) : Date.now();
  if (market.shownAfterTerminal) found.push("shown-after-terminal");
  if (market.discoveryActiveAfterTerminal) found.push("discovery-active-after-terminal");
  if (Object.keys(market.blocked).length) found.push("blocked-while-live");
  if (market.rejected["no-db-row"]) found.push("ticks-before-db-row");
  if (market.rejected["inactive-in-db"]) found.push("ticks-for-inactive-db-row");
  if (market.discoveredActive && !market.firstTickAt && market.firstDiscoveredAt && end - market.firstDiscoveredAt > noTickMs)
    found.push("discovered-never-ticked");
  if (market.goAt && !market.resultAt && end - market.goAt > noResultMs) found.push("game-over-without-result");
  if (market.resultAt && market.visibility === "shown" && market.visibilityAt > market.resultAt) found.push("shown-after-result");
  if (market.subscription === "unresolved") found.push("subscription-unresolved");
  if (market.seedFailures) found.push("price-seed-failures");
  if (market.omitted >= 3) found.push("repeatedly-omitted-by-discovery");
  return found;
}

function seconds(ms) {
  return ms == null ? null : Number((ms / 1000).toFixed(1));
}

function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1)];
}

function marketSummary(market, context) {
  return {
    kind: market.kind,
    eventId: market.eventId,
    marketId: market.marketId,
    name: market.name,
    ticks: market.ticks,
    visibility: market.visibility,
    visibilityReason: market.visibilityReason,
    firstTickAfterS:
      market.firstDiscoveredAt && market.firstTickAt ? seconds(market.firstTickAt - market.firstDiscoveredAt) : null,
    result: market.result,
    resultAfterGoS: market.goAt && market.resultAt ? seconds(market.resultAt - market.goAt) : null,
    subscription: market.subscription,
    ...(Object.keys(market.rejected).length ? { rejected: market.rejected } : {}),
    ...(Object.keys(market.blocked).length ? { blocked: market.blocked } : {}),
    ...(market.omitted ? { omitted: market.omitted } : {}),
    issues: issues(market, context),
  };
}

// Newest `limit` records for one market, oldest first.
async function marketTimeline(marketId, { limit = 500, ...options } = {}) {
  const tail = [];
  for await (const record of records(options, (row) => row.marketId === String(marketId))) {
    tail.push(record);
    if (tail.length > limit) tail.shift();
  }
  return { marketId: String(marketId), records: tail, truncated: tail.length === limit };
}

async function eventSummary(eventId, options = {}) {
  const { markets, lastTs } = await collect(options, (row) => row.eventId === String(eventId));
  const rows = [...markets.values()]
    .sort((a, b) => (a.firstDiscoveredAt ?? a.firstTickAt ?? 0) - (b.firstDiscoveredAt ?? b.firstTickAt ?? 0))
    .map((market) => marketSummary(market, { lastTs }));
  return { eventId: String(eventId), lastTs, markets: rows };
}

async function anomalies({ kind, eventId, noTickMs = 120000, noResultMs = 1800000, perIssue = 25, ...options } = {}) {
  const { markets, lastTs } = await collect(
    options,
    (row) => (!kind || row.kind === kind) && (!eventId || row.eventId === String(eventId)),
  );
  const context = { lastTs, noTickMs, noResultMs };
  const byIssue = new Map();
  for (const market of markets.values()) {
    for (const issue of issues(market, context)) {
      if (!byIssue.has(issue)) byIssue.set(issue, []);
      byIssue.get(issue).push(market);
    }
  }
  const list = [...markets.values()];
  const firstTick = list.filter((m) => m.firstDiscoveredAt && m.firstTickAt).map((m) => m.firstTickAt - m.firstDiscoveredAt);
  const settle = list.filter((m) => m.goAt && m.resultAt).map((m) => m.resultAt - m.goAt);
  const hiddenByReason = {};
  for (const market of list) if (market.visibility === "hidden") bump(hiddenByReason, market.visibilityReason);
  return {
    marketsTraced: markets.size,
    lastTs,
    issues: Object.fromEntries(
      [...byIssue]
        .sort((a, b) => b[1].length - a[1].length)
        .map(([issue, affected]) => [
          issue,
          { count: affected.length, markets: affected.slice(0, perIssue).map((market) => marketSummary(market, context)) },
        ]),
    ),
    latencySeconds: {
      discoveryToFirstTick: { count: firstTick.length, p50: seconds(percentile(firstTick, 0.5)), p95: seconds(percentile(firstTick, 0.95)) },
      gameOverToResult: { count: settle.length, p50: seconds(percentile(settle, 0.5)), p95: seconds(percentile(settle, 0.95)) },
    },
    hiddenByReason,
  };
}

// Newest records across all markets, optionally narrowed by kind, stage or event.
async function recent({ kind, stage, eventId, limit = 200, ...options } = {}) {
  const tail = [];
  const filter = (row) =>
    (!kind || row.kind === kind) && (!stage || row.stage === stage) && (!eventId || row.eventId === String(eventId));
  for await (const record of records(options, filter)) {
    tail.push(record);
    if (tail.length > limit) tail.shift();
  }
  return { records: tail };
}

function files(options = {}) {
  return traceFiles({ ...options, days: MAX_DAYS }).map((file) => {
    const stat = fs.statSync(file);
    return { name: path.basename(file), bytes: stat.size, modifiedAt: stat.mtime.toISOString() };
  });
}

module.exports = { marketTimeline, eventSummary, anomalies, recent, files, MAX_DAYS };
