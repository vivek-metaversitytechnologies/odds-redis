// Reads logs/market-trace/*.jsonl (ball-by-ball and line market lifecycle) for analysis.
//
//   node scripts/marketTrace.js market <marketId> [--days=2]   full timeline of one market
//   node scripts/marketTrace.js event <eventId> [--days=2]     one row per market of an event
//   node scripts/marketTrace.js anomalies [--days=1] [--kind=BB|LINE] [--event=<id>]
//                                         [--no-tick-ms=120000] [--no-result-ms=1800000]
//
// --dir overrides MARKET_TRACE_LOG_DIR (default logs/market-trace). --days reads the newest N files.
require("dotenv").config({ quiet: true });

const fs = require("node:fs");
const path = require("node:path");
const readline = require("node:readline");

function option(name, fallback) {
  const match = process.argv.find((arg) => arg.startsWith(`--${name}=`));
  return match ? match.slice(name.length + 3) : fallback;
}

function traceFiles() {
  const dir = path.resolve(option("dir", process.env.MARKET_TRACE_LOG_DIR || "logs/market-trace"));
  const days = Number(option("days", "1"));
  if (!fs.existsSync(dir)) throw new Error(`Trace directory not found: ${dir}`);
  return fs
    .readdirSync(dir)
    .filter((name) => /^market-trace-.*\.jsonl$/.test(name))
    .sort()
    .slice(-days)
    .map((name) => path.join(dir, name));
}

async function* records(filter = () => true) {
  for (const file of traceFiles()) {
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

function time(ts) {
  return String(ts).slice(11, 23);
}

function describe(record) {
  const skip = new Set(["ts", "kind", "stage", "eventId", "marketId", "prices"]);
  const parts = Object.entries(record)
    .filter(([key, value]) => !skip.has(key) && value !== null && value !== undefined)
    .map(([key, value]) => `${key}=${typeof value === "object" ? JSON.stringify(value) : value}`);
  if (Array.isArray(record.prices) && record.prices.length) {
    parts.push(
      `prices=${record.prices.map((runner) => `${runner.na ?? runner.rid}:${runner.s ?? ""} ${runner.b1 ?? "-"}/${runner.l1 ?? "-"}`).join(" | ")}`,
    );
  }
  return parts.join(" ");
}

async function marketTimeline(marketId) {
  let count = 0;
  for await (const record of records((row) => row.marketId === marketId)) {
    if (!count) console.log(`${record.kind} market ${marketId}, event ${record.eventId}\n`);
    console.log(`${record.ts.slice(0, 10)} ${time(record.ts)}  ${record.stage.padEnd(18)} ${describe(record)}`);
    count += 1;
  }
  if (!count) console.log(`No trace records for market ${marketId}`);
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

async function collect(filter) {
  const markets = new Map();
  let lastTs = null;
  for await (const record of records(filter)) {
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
        bump(market.rejected, record.reason, 1 + (record.suppressedSinceLast || 0));
        break;
      case "tick.blocked":
        bump(market.blocked, record.reason, 1 + (record.suppressedSinceLast || 0));
        break;
      case "definition":
        if (record.action === "blocked") bump(market.blocked, `definition:${record.reason}`);
        break;
      case "discovery.omitted":
        market.omitted += 1 + (record.suppressedSinceLast || 0);
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
        market.seedFailures += 1 + (record.suppressedSinceLast || 0);
        break;
      default:
    }
  }
  return { markets, lastTs };
}

function seconds(ms) {
  return ms == null ? "" : (ms / 1000).toFixed(1);
}

async function eventSummary(eventId) {
  const { markets, lastTs } = await collect((row) => row.eventId === eventId);
  if (!markets.size) return console.log(`No trace records for event ${eventId}`);
  const rows = [...markets.values()]
    .sort((a, b) => (a.firstDiscoveredAt ?? a.firstTickAt ?? 0) - (b.firstDiscoveredAt ?? b.firstTickAt ?? 0))
    .map((market) => ({
      kind: market.kind,
      marketId: market.marketId,
      name: String(market.name ?? "").slice(0, 32),
      ticks: market.ticks,
      visibility: market.visibility ? `${market.visibility}${market.visibilityReason ? ` (${market.visibilityReason})` : ""}` : "",
      firstTickAfterS: market.firstDiscoveredAt && market.firstTickAt ? seconds(market.firstTickAt - market.firstDiscoveredAt) : "",
      result: market.result ?? "",
      resultAfterGoS: market.goAt && market.resultAt ? seconds(market.resultAt - market.goAt) : "",
      issues: issues(market, { lastTs }).join(", "),
    }));
  console.table(rows);
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

function percentile(values, fraction) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1)];
}

async function anomalies() {
  const kind = option("kind", null);
  const eventId = option("event", null);
  const noTickMs = Number(option("no-tick-ms", "120000"));
  const noResultMs = Number(option("no-result-ms", "1800000"));
  const { markets, lastTs } = await collect(
    (row) => (!kind || row.kind === kind) && (!eventId || row.eventId === eventId),
  );
  console.log(`Markets traced: ${markets.size} (last record ${lastTs ?? "none"})\n`);
  const byIssue = new Map();
  for (const market of markets.values()) {
    for (const issue of issues(market, { lastTs, noTickMs, noResultMs })) {
      if (!byIssue.has(issue)) byIssue.set(issue, []);
      byIssue.get(issue).push(market);
    }
  }
  if (!byIssue.size) console.log("No anomalies found.");
  for (const [issue, list] of [...byIssue].sort((a, b) => b[1].length - a[1].length)) {
    console.log(`${issue}: ${list.length}`);
    for (const market of list.slice(0, 10)) {
      const detail = {
        ...(Object.keys(market.blocked).length ? { blocked: market.blocked } : {}),
        ...(Object.keys(market.rejected).length ? { rejected: market.rejected } : {}),
        ...(market.omitted ? { omitted: market.omitted } : {}),
      };
      console.log(
        `  ${market.kind.padEnd(4)} ${market.marketId}  event=${market.eventId}  ${String(market.name ?? "").slice(0, 30)}` +
          (Object.keys(detail).length ? `  ${JSON.stringify(detail)}` : ""),
      );
    }
    if (list.length > 10) console.log(`  ... ${list.length - 10} more`);
  }
  const list = [...markets.values()];
  const firstTick = list.filter((m) => m.firstDiscoveredAt && m.firstTickAt).map((m) => m.firstTickAt - m.firstDiscoveredAt);
  const settle = list.filter((m) => m.goAt && m.resultAt).map((m) => m.resultAt - m.goAt);
  console.log("\nLatency (seconds)");
  console.table({
    "discovery -> first tick": { count: firstTick.length, p50: seconds(percentile(firstTick, 0.5)), p95: seconds(percentile(firstTick, 0.95)) },
    "game over -> result": { count: settle.length, p50: seconds(percentile(settle, 0.5)), p95: seconds(percentile(settle, 0.95)) },
  });
  const hiddenReasons = {};
  for (const market of list) if (market.visibility === "hidden") bump(hiddenReasons, market.visibilityReason);
  if (Object.keys(hiddenReasons).length) {
    console.log("Currently hidden markets by reason");
    console.table(hiddenReasons);
  }
}

async function main() {
  const [command, id] = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
  if (command === "market" && id) return marketTimeline(id);
  if (command === "event" && id) return eventSummary(id);
  if (command === "anomalies") return anomalies();
  console.log(fs.readFileSync(__filename, "utf8").split("\n").slice(0, 9).join("\n").replaceAll("// ", ""));
  process.exitCode = 1;
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
