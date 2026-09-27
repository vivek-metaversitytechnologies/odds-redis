// Measures the provider settings API against the markets the frontend currently lists, to pick
// LIMITS_*_POLL_BATCH_SIZE. Read-only: it sends settings requests and writes nothing.
//
//   node scripts/benchmarkMarketSettings.js [--sizes=1,10,25,50,100,200,500,1000] [--repeat=3]
//                                           [--gap-ms=300] [--group=all|market|fancy]
//
// Each request counts toward the provider's rate limit (defaults send about 25 requests), so run it
// when the live service has headroom.
require("dotenv").config({ quiet: true });

const redisStore = require("../src/config/redis");
const dashboard = require("../src/services/dashboardService");
const { csvIntegers } = require("../src/config/env");

const MARKET_GROUPS = ["Odds", "Bookmaker"];

function option(name, fallback) {
  const match = process.argv.find((arg) => arg.startsWith(`--${name}=`));
  return match ? match.slice(name.length + 3) : fallback;
}

function kind(mid) {
  const suffix = /-([A-Z0-9]+)$/i.exec(mid);
  return suffix ? suffix[1].toUpperCase() : "betfair-id";
}

async function listedIds(group) {
  const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
  const lists = await Promise.all(sportIds.map((sportId) => dashboard.activeMatchesFromRedis(sportId)));
  const eventIds = [...new Set(lists.flat().filter(Boolean).map((row) => String(row.matchId)))];
  const snapshots = await redisStore.getEventSnapshots(eventIds);
  const ids = new Set();
  for (const payload of snapshots.values()) {
    for (const [name, entries] of Object.entries(payload || {})) {
      const isMarket = MARKET_GROUPS.includes(name);
      if ((group === "market" && !isMarket) || (group === "fancy" && isMarket)) continue;
      for (const entry of entries || []) ids.add(String(entry?.marketId ?? entry?.mid ?? ""));
    }
  }
  ids.delete("");
  return { events: eventIds.length, ids: [...ids] };
}

async function post(mids) {
  const url = new URL("/v1/markets/settings", process.env.PROVIDER_BASE_URL);
  const body = JSON.stringify({ mids });
  const startedAt = performance.now();
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.PROVIDER_TOKEN || process.env.PROVIDER_X_API_KEY}`,
    },
    body,
  });
  const text = await response.text();
  const ms = performance.now() - startedAt;
  let items = [];
  try {
    const parsed = JSON.parse(text);
    items = Array.isArray(parsed) ? parsed : parsed?.data || parsed?.markets || parsed?.settings || [];
  } catch {
    // Non-JSON bodies are reported through status and preview.
  }
  return { status: response.status, ms, requestBytes: body.length, responseBytes: text.length, items, preview: text.slice(0, 200) };
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length ? sorted[Math.floor((sorted.length - 1) / 2)] : null;
}

async function main() {
  const sizes = option("sizes", "1,10,25,50,100,200,500,1000").split(",").map(Number).filter((n) => n > 0);
  const repeat = Number(option("repeat", "3"));
  const gapMs = Number(option("gap-ms", "300"));
  const group = option("group", "all");
  const { events, ids } = await listedIds(group);
  console.log(`Listed events: ${events}, ${group} market ids: ${ids.length}`);
  if (!ids.length) return;

  const rows = [];
  const coverage = new Map();
  let offset = 0;
  let requests = 0;
  // Sizes above the listed count collapse into one "everything" size.
  const effective = [...new Set(sizes.map((size) => Math.min(size, ids.length)))];
  sizeLoop: for (const size of effective) {
    const samples = [];
    for (let attempt = 0; attempt < repeat; attempt += 1) {
      const mids = Array.from({ length: size }, (_, index) => ids[(offset + index) % ids.length]);
      offset = (offset + mids.length) % ids.length;
      const result = await post(mids);
      requests += 1;
      const answered = new Set(
        result.items.filter((item) => item?.ms !== undefined || item?.mas !== undefined || item?.settings).map((item) => String(item.mid)),
      );
      for (const mid of mids) {
        const current = coverage.get(kind(mid)) || { requested: 0, answered: 0 };
        current.requested += 1;
        if (answered.has(mid)) current.answered += 1;
        coverage.set(kind(mid), current);
      }
      samples.push({ ...result, sent: mids.length, answered: answered.size });
      if (result.status === 429 || result.status === 403) {
        console.error(`Stopping: provider returned ${result.status}: ${result.preview}`);
        rows.push(summary(size, samples));
        break sizeLoop;
      }
      await new Promise((resolve) => setTimeout(resolve, gapMs));
    }
    rows.push(summary(size, samples));
    if (samples.some((sample) => sample.status >= 400)) {
      console.error(`Stopping at size ${size}: ${samples.find((s) => s.status >= 400).preview}`);
      break;
    }
  }

  console.log("\nLatency and payload by batch size");
  console.table(rows);
  console.log("\nWhich ID kinds the settings API answers for");
  console.table(
    Object.fromEntries(
      [...coverage].map(([name, value]) => [
        name,
        { ...value, answeredPercent: Math.round((value.answered / value.requested) * 100) },
      ]),
    ),
  );
  console.log(`\nProvider requests sent: ${requests}`);
}

function summary(size, samples) {
  const ms = samples.map((sample) => sample.ms);
  const medianMs = median(ms);
  const sent = samples[0]?.sent ?? size;
  return {
    size: sent,
    statuses: [...new Set(samples.map((sample) => sample.status))].join(","),
    medianMs: Math.round(medianMs),
    maxMs: Math.round(Math.max(...ms)),
    msPerId: Number((medianMs / sent).toFixed(2)),
    answeredAvg: Math.round(samples.reduce((sum, sample) => sum + sample.answered, 0) / samples.length),
    responseKb: Number((median(samples.map((sample) => sample.responseBytes)) / 1024).toFixed(1)),
  };
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(() => redisStore.closeRedis().catch(() => {}));
