// Reads logs/market-trace/*.jsonl (ball-by-ball and line market lifecycle) for analysis.
//
//   node scripts/marketTrace.js market <marketId> [--days=2]   full timeline of one market
//   node scripts/marketTrace.js event <eventId> [--days=2]     one row per market of an event
//   node scripts/marketTrace.js anomalies [--days=1] [--kind=BB|LINE] [--event=<id>]
//                                         [--no-tick-ms=120000] [--no-result-ms=1800000]
//
// --dir overrides MARKET_TRACE_LOG_DIR (default logs/market-trace). --days reads the newest N files.
// The same reports are served by GET /api/market-trace.
require("dotenv").config({ quiet: true });

const fs = require("node:fs");
const reader = require("../src/services/marketTraceReader");

function option(name, fallback) {
  const match = process.argv.find((arg) => arg.startsWith(`--${name}=`));
  return match ? match.slice(name.length + 3) : fallback;
}

const common = { dir: option("dir", undefined), days: Number(option("days", "1")) };

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

async function printMarket(marketId) {
  const { records } = await reader.marketTimeline(marketId, { ...common, limit: 5000 });
  if (!records.length) return console.log(`No trace records for market ${marketId}`);
  console.log(`${records[0].kind} market ${marketId}, event ${records[0].eventId}\n`);
  for (const record of records) {
    console.log(`${record.ts.slice(0, 10)} ${record.ts.slice(11, 23)}  ${record.stage.padEnd(18)} ${describe(record)}`);
  }
}

async function printEvent(eventId) {
  const { markets } = await reader.eventSummary(eventId, common);
  if (!markets.length) return console.log(`No trace records for event ${eventId}`);
  console.table(
    markets.map((market) => ({
      kind: market.kind,
      marketId: market.marketId,
      name: String(market.name ?? "").slice(0, 32),
      ticks: market.ticks,
      visibility: market.visibility ? `${market.visibility}${market.visibilityReason ? ` (${market.visibilityReason})` : ""}` : "",
      firstTickAfterS: market.firstTickAfterS ?? "",
      result: market.result ?? "",
      resultAfterGoS: market.resultAfterGoS ?? "",
      issues: market.issues.join(", "),
    })),
  );
}

async function printAnomalies() {
  const report = await reader.anomalies({
    ...common,
    kind: option("kind", undefined),
    eventId: option("event", undefined),
    noTickMs: Number(option("no-tick-ms", "120000")),
    noResultMs: Number(option("no-result-ms", "1800000")),
    perIssue: 10,
  });
  console.log(
    `Markets traced: ${report.marketsTraced} (${report.firstTs ?? "none"} .. ${report.lastTs ?? "none"}),` +
      ` ${report.preTraceMarkets} finished before the window and skipped\n`,
  );
  const issues = Object.entries(report.issues);
  if (!issues.length) console.log("No anomalies found.");
  for (const [issue, { count, markets }] of issues) {
    console.log(`${issue}: ${count}`);
    for (const market of markets) {
      const detail = {
        ...(market.blocked ? { blocked: market.blocked } : {}),
        ...(market.rejected ? { rejected: market.rejected } : {}),
        ...(market.omitted ? { omitted: market.omitted } : {}),
      };
      console.log(
        `  ${market.kind.padEnd(4)} ${market.marketId}  event=${market.eventId}  ${String(market.name ?? "").slice(0, 30)}` +
          (Object.keys(detail).length ? `  ${JSON.stringify(detail)}` : ""),
      );
    }
    if (count > markets.length) console.log(`  ... ${count - markets.length} more`);
  }
  console.log("\nLatency (seconds)");
  console.table({
    "discovery -> first tick": report.latencySeconds.discoveryToFirstTick,
    "game over -> result": report.latencySeconds.gameOverToResult,
  });
  if (Object.keys(report.hiddenByReason).length) {
    console.log("Currently hidden markets by reason");
    console.table(report.hiddenByReason);
  }
}

async function main() {
  const [command, id] = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
  if (command === "market" && id) return printMarket(id);
  if (command === "event" && id) return printEvent(id);
  if (command === "anomalies") return printAnomalies();
  console.log(fs.readFileSync(__filename, "utf8").split("\n").slice(0, 10).join("\n").replaceAll("// ", ""));
  process.exitCode = 1;
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
