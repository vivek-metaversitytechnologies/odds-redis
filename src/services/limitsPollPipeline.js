const provider = require("./providerApi");
const redisStore = require("../config/redis");
const dashboard = require("./dashboardService");
const marketSettings = require("./marketSettingsService");
const websocket = require("./websocketService");
const logger = require("../utils/logger");
const { writeMarketLimitsOutcome } = require("../utils/marketLimitsFileLogger");
const { providerLimits } = require("../utils/marketLimits");
const { boolean, csvIntegers, integer } = require("../config/env");
const { setBounded } = require("../utils/boundedMap");

// Keeps stake limits of everything the frontend lists in step with the provider's settings API.
// The `market` room is not reliable on its own: the provider has pushed placeholder limits (1)
// and corrected them over HTTP without a follow-up room message.
//
// Resource rules:
// - Targets come from the active-match list and this process's cached event payloads, so a
//   run costs a few Redis reads and no database reads.
// - Each pipeline has its own provider request budget. When the listed markets need more
//   requests than one run may spend, a rotating cursor covers them over several runs.
// - Runs yield entirely when overall provider traffic is near the application cap, and use a
//   low queue priority so subscriptions, discovery and results dispatch first.
// - Only limits that differ from what the frontend already has are written.
// - Markets the provider omits from its response are not asked for again for a while.

const MARKET_GROUPS = Object.freeze(["Odds", "Bookmaker"]);
const FANCY_GROUPS = Object.freeze([
  "LineMarket",
  "Fancy2",
  "Meter",
  "Khado",
  "OddEven",
  "OtherMarket",
  "Fancy3",
  "CricketCasino",
  "BallByBall",
]);
const PROVIDER_PRIORITY = 8;
const LATENCY_SAMPLE_LIMIT = 200;
const UNSUPPORTED_TRACK_LIMIT = 50000;

let listed = { at: 0, ids: [], loading: null };

// Both pipelines share one read of the active-match list per TTL.
async function listedEventIds(now = Date.now()) {
  const ttlMs = integer("LIMITS_POLL_EVENT_LIST_TTL_MS", 5000, { min: 500, max: 60000 });
  if (listed.loading) return listed.loading;
  if (listed.at && now - listed.at < ttlMs) return listed.ids;
  listed.loading = (async () => {
    const sportIds = csvIntegers("SPORT_IDS", [1, 2, 4]);
    const lists = await Promise.all(sportIds.map((sportId) => dashboard.activeMatchesFromRedis(sportId)));
    const ids = [
      ...new Set(
        lists
          .flat()
          .filter(Boolean)
          .map((row) => String(row.matchId ?? ""))
          .filter((id) => /^\d+$/.test(id)),
      ),
    ];
    listed = { at: Date.now(), ids, loading: null };
    return ids;
  })();
  try {
    return await listed.loading;
  } finally {
    listed.loading = null;
  }
}

async function collectTargets(groups) {
  const eventIds = await listedEventIds();
  const payloads = await redisStore.getFrontendEventPayloads(eventIds);
  const targets = new Map();
  for (const [eventId, payload] of payloads) {
    for (const group of groups) {
      for (const entry of payload?.[group] || []) {
        // Odds rows repeat per runner; the first row carries the market's limits.
        const mid = String(entry?.marketId ?? entry?.mid ?? "").trim();
        if (!redisStore.validMarketIdentifier(mid) || targets.has(mid)) continue;
        targets.set(mid, { mid, eid: eventId, minBet: entry.minBet ?? null, maxBet: entry.maxBet ?? null });
      }
    }
  }
  // Stable order keeps the rotating cursor fair between runs.
  return { events: eventIds.length, targets: [...targets.values()].sort((a, b) => a.mid.localeCompare(b.mid)) };
}

function sameLimit(current, next) {
  return next == null || (current != null && Number(current) === next);
}

function percentile(sorted, fraction) {
  if (!sorted.length) return null;
  return sorted[Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1)];
}

function createPipeline({ name, groups, defaults }) {
  const prefix = `LIMITS_${name.toUpperCase()}_POLL`;
  const source = `limits-${name}`;
  const unsupported = new Map();
  // Last market id covered by a partial run. Resuming by id rather than index stays fair while
  // markets are listed, settled or backed off between runs.
  let cursor = null;
  let timer;
  let stopped = true;
  let halted = false;
  let running = null;
  const state = {
    name,
    enabled: false,
    running: false,
    lastStartedAt: null,
    lastCompletedAt: null,
    lastError: null,
    lastRun: null,
    totals: {
      runs: 0,
      skippedForHeadroom: 0,
      requests: 0,
      failedRequests: 0,
      idsRequested: 0,
      changed: 0,
      applied: 0,
      applyFailed: 0,
    },
  };

  function config() {
    return {
      enabled: boolean(`${prefix}_ENABLED`, true),
      intervalMs: integer(`${prefix}_INTERVAL_MS`, defaults.intervalMs, { min: 1000, max: 3600000 }),
      batchSize: Math.min(
        marketSettings.SETTINGS_MAX_IDS_PER_REQUEST,
        integer(`${prefix}_BATCH_SIZE`, defaults.batchSize, { min: 1, max: 1000 }),
      ),
      concurrency: integer(`${prefix}_CONCURRENCY`, defaults.concurrency, { min: 1, max: 10 }),
      maxRequestsPerMinute: integer(`${prefix}_MAX_REQUESTS_PER_MINUTE`, defaults.maxRequestsPerMinute, {
        min: 1,
        max: 800,
      }),
      headroomPercent: integer("LIMITS_POLL_PROVIDER_HEADROOM_PERCENT", 80, { min: 10, max: 100 }),
      unsupportedTtlMs: integer("LIMITS_POLL_UNSUPPORTED_TTL_MS", 600000, { min: 10000 }),
    };
  }

  // Picks this run's slice from the id-sorted list, wrapping around so every listed market is
  // visited in turn.
  function selectSlice(eligible, limit) {
    if (eligible.length <= limit) {
      cursor = null;
      return eligible;
    }
    const found = cursor == null ? 0 : eligible.findIndex((target) => target.mid > cursor);
    const start = found < 0 ? 0 : found;
    const slice = eligible.slice(start, start + limit);
    if (slice.length < limit) slice.push(...eligible.slice(0, limit - slice.length));
    cursor = slice.at(-1).mid;
    return slice;
  }

  async function fetchBatches(batches, concurrency, run) {
    const latencies = [];
    const responses = [];
    let next = 0;
    async function worker() {
      while (next < batches.length && !halted) {
        const batch = batches[next];
        next += 1;
        const requestedAt = Date.now();
        try {
          const response = await provider.marketSettings(
            batch.map((target) => target.mid),
            { priority: PROVIDER_PRIORITY, source, retries: 0 },
          );
          latencies.push(Date.now() - requestedAt);
          responses.push({ batch, requestedAt, items: marketSettings.responseItems(response) });
        } catch (error) {
          run.failedRequests += 1;
          run.lastRequestError = error.message;
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, batches.length) }, worker));
    return { latencies, responses };
  }

  async function applyResponse({ batch, requestedAt, items }, cfg, run) {
    const byMid = new Map(batch.map((target) => [target.mid, target]));
    const answered = new Set();
    for (const raw of items) {
      const mid = String(raw?.mid ?? "");
      const target = byMid.get(mid);
      if (!target) continue;
      const settings = raw.settings ?? raw;
      const limits = providerLimits(settings);
      if (!Object.keys(limits).length) continue;
      answered.add(mid);
      unsupported.delete(mid);
      if (sameLimit(target.minBet, limits.providerMinBet) && sameLimit(target.maxBet, limits.providerMaxBet)) {
        continue;
      }
      // A room message received after this request went out is newer than this response.
      if (marketSettings.roomUpdatedSince(mid, requestedAt)) {
        run.skippedForRoomUpdate += 1;
        continue;
      }
      run.changed += 1;
      const item = { eid: raw.eid ?? target.eid, mid, settings: { ms: settings.ms, mas: settings.mas } };
      try {
        await websocket.applyMarketSettings(item);
        run.applied += 1;
        writeMarketLimitsOutcome("POLLED", item);
      } catch (error) {
        run.applyFailed += 1;
        writeMarketLimitsOutcome("POLL_FAILED", item, error.message);
      }
    }
    const retryAt = Date.now() + cfg.unsupportedTtlMs;
    for (const mid of byMid.keys()) {
      if (answered.has(mid)) continue;
      run.omitted += 1;
      setBounded(unsupported, mid, retryAt, UNSUPPORTED_TRACK_LIMIT);
    }
  }

  async function runOnce() {
    const cfg = config();
    const startedAt = Date.now();
    state.running = true;
    state.lastStartedAt = new Date(startedAt).toISOString();
    state.lastError = null;
    try {
      const budget = provider.providerBudget(startedAt);
      if (budget.usedLastMinute >= (budget.perMinute * cfg.headroomPercent) / 100) {
        state.totals.skippedForHeadroom += 1;
        state.lastRun = { skipped: "provider-headroom", providerRequestsLastMinute: budget.usedLastMinute };
        return state.lastRun;
      }
      const { events, targets } = await collectTargets(groups);
      const eligible = targets.filter((target) => !(unsupported.get(target.mid) > startedAt));
      const requestsPerRun = Math.max(1, Math.floor((cfg.maxRequestsPerMinute * cfg.intervalMs) / 60000));
      const slice = selectSlice(eligible, requestsPerRun * cfg.batchSize);
      const batches = [];
      for (let index = 0; index < slice.length; index += cfg.batchSize) {
        batches.push(slice.slice(index, index + cfg.batchSize));
      }
      const run = {
        events,
        targets: targets.length,
        eligible: eligible.length,
        unsupported: targets.length - eligible.length,
        idsRequested: slice.length,
        requests: batches.length,
        failedRequests: 0,
        omitted: 0,
        changed: 0,
        applied: 0,
        applyFailed: 0,
        skippedForRoomUpdate: 0,
      };
      const { latencies, responses } = await fetchBatches(batches, cfg.concurrency, run);
      for (const response of responses) await applyResponse(response, cfg, run);
      const sorted = latencies.sort((a, b) => a - b).slice(-LATENCY_SAMPLE_LIMIT);
      Object.assign(run, {
        durationMs: Date.now() - startedAt,
        latencyMs: { p50: percentile(sorted, 0.5), p95: percentile(sorted, 0.95), max: sorted.at(-1) ?? null },
        // How long one pass over every eligible market takes at the current budget.
        fullCycleMs: slice.length ? Math.ceil(eligible.length / slice.length) * cfg.intervalMs : 0,
      });
      delete run.lastRequestError;
      state.lastRun = run;
      for (const key of ["requests", "failedRequests", "idsRequested", "changed", "applied", "applyFailed"]) {
        state.totals[key] += run[key];
      }
      if (run.failedRequests) state.lastError = `${run.failedRequests} settings request(s) failed`;
      if (run.changed || run.failedRequests || run.applyFailed) {
        logger.info(`[LimitsPoll:${name}] run completed`, run);
      }
      return run;
    } catch (error) {
      state.lastError = error.message;
      logger.error(`[LimitsPoll:${name}] run failed`, { error: error.message });
      throw error;
    } finally {
      state.totals.runs += 1;
      state.running = false;
      state.lastCompletedAt = new Date().toISOString();
    }
  }

  // Runs are chained, never overlapped: the next one is scheduled after the current one ends.
  function schedule(delayMs) {
    if (stopped) return;
    timer = setTimeout(() => {
      const startedAt = Date.now();
      running = runOnce()
        .catch(() => {})
        .finally(() => {
          running = null;
          schedule(Math.max(0, config().intervalMs - (Date.now() - startedAt)));
        });
    }, delayMs);
    timer.unref?.();
  }

  return {
    name,
    groups,
    runOnce,
    start({ initialDelayMs = 0 } = {}) {
      const cfg = config();
      state.enabled = cfg.enabled;
      if (!cfg.enabled) {
        logger.info(`[LimitsPoll:${name}] disabled`);
        return;
      }
      if (!stopped) return;
      stopped = false;
      halted = false;
      logger.info(`[LimitsPoll:${name}] started`, {
        intervalMs: cfg.intervalMs,
        batchSize: cfg.batchSize,
        maxRequestsPerMinute: cfg.maxRequestsPerMinute,
      });
      schedule(initialDelayMs);
    },
    async stop() {
      stopped = true;
      halted = true;
      if (timer) clearTimeout(timer);
      timer = undefined;
      if (running) await running;
    },
    getStatus() {
      return { ...state, totals: { ...state.totals }, config: config(), unsupportedTracked: unsupported.size };
    },
    __testing__: {
      reset() {
        cursor = null;
        unsupported.clear();
      },
    },
  };
}

// Benchmarked: ~70 ms per request regardless of size up to the 50-id cap, so full batches are
// always cheapest. Budgets are ceilings; ~530 listed ids need about 12 requests/min for markets
// and 70/min for fancies. Together they stay well under half of the 800/min application cap.
const marketPipeline = createPipeline({
  name: "market",
  groups: MARKET_GROUPS,
  defaults: { intervalMs: 30000, batchSize: 50, concurrency: 2, maxRequestsPerMinute: 60 },
});
const fancyPipeline = createPipeline({
  name: "fancy",
  groups: FANCY_GROUPS,
  defaults: { intervalMs: 5000, batchSize: 50, concurrency: 2, maxRequestsPerMinute: 240 },
});

function startLimitsPollPipelines() {
  // Let the provider socket subscribe and the first discovery pass settle before polling.
  const initialDelayMs = integer("LIMITS_POLL_START_DELAY_MS", 15000, { min: 0 });
  marketPipeline.start({ initialDelayMs });
  fancyPipeline.start({ initialDelayMs });
}

async function stopLimitsPollPipelines() {
  await Promise.all([marketPipeline.stop(), fancyPipeline.stop()]);
}

function getLimitsPollStatus() {
  return { market: marketPipeline.getStatus(), fancy: fancyPipeline.getStatus() };
}

module.exports = {
  MARKET_GROUPS,
  FANCY_GROUPS,
  createPipeline,
  collectTargets,
  listedEventIds,
  marketPipeline,
  fancyPipeline,
  startLimitsPollPipelines,
  stopLimitsPollPipelines,
  getLimitsPollStatus,
  __testing__: {
    resetListedEvents() {
      listed = { at: 0, ids: [], loading: null };
    },
  },
};
