const { writeProviderLog } = require("../utils/providerFileLogger");
const Bottleneck = require("bottleneck");
const { integer } = require("../config/env");
const providerMetrics = require("./providerMetrics");

const VENDOR_WINDOW_MS = 20000;
const VENDOR_WINDOW_CAP = 1000;
const VENDOR_SAFE_WINDOW_CAP = 800;
const configuredRequestsPerMinute = integer("PROVIDER_MAX_REQUESTS_PER_MINUTE", 2400, { min: 1 });
const vendorSafeRequestsPerMinute = Math.floor((VENDOR_SAFE_WINDOW_CAP * 60000) / VENDOR_WINDOW_MS);
const maxRequestsPerMinute = Math.min(configuredRequestsPerMinute, vendorSafeRequestsPerMinute);
const configuredMinTime = integer("PROVIDER_MIN_TIME_MS", 0, { min: 0 });
const rateLimitMinTime = Math.ceil(60000 / maxRequestsPerMinute);
const providerLimiter = new Bottleneck({
  maxConcurrent: integer("PROVIDER_MAX_CONCURRENT", 100, { min: 1, max: 100 }),
  minTime: Math.max(configuredMinTime, rateLimitMinTime),
  reservoir: VENDOR_SAFE_WINDOW_CAP,
  reservoirRefreshAmount: VENDOR_SAFE_WINDOW_CAP,
  reservoirRefreshInterval: VENDOR_WINDOW_MS,
});
const activeControllers = new Set();
let shuttingDown = false;
let blockedUntil = 0;
const requestAttempts = [];
const requestLifetime = {
  startedAt: new Date().toISOString(),
  attempts: 0,
  succeeded: 0,
  failed: 0,
  aborted: 0,
};

function metricRoute(pathname) {
  if (/^\/v1\/markets\/[^/]+\/runners$/.test(pathname)) return "/v1/markets/:marketId/runners";
  return pathname;
}

function pruneRequestAttempts(now = Date.now()) {
  const cutoff = now - 60_000;
  while (requestAttempts.length && requestAttempts[0].startedAt < cutoff) requestAttempts.shift();
}

function startRequestAttempt(method, url, source) {
  const entry = {
    startedAt: Date.now(),
    method,
    path: url.pathname,
    route: metricRoute(url.pathname),
    source: source || "unclassified",
    status: null,
    outcome: "running",
    durationMs: null,
  };
  requestAttempts.push(entry);
  requestLifetime.attempts += 1;
  providerMetrics.recordAttempt(entry.startedAt, method, entry.route, entry.source);
  pruneRequestAttempts(entry.startedAt);
  return entry;
}

function finishRequestAttempt(entry, { status = null, error } = {}) {
  if (!entry || entry.outcome !== "running") return;
  entry.status = status;
  entry.durationMs = Date.now() - entry.startedAt;
  if (!error && status >= 200 && status < 400) {
    entry.outcome = "succeeded";
    requestLifetime.succeeded += 1;
  } else {
    entry.outcome = error?.name === "AbortError" || /aborted/i.test(error?.message || "") ? "aborted" : "failed";
    requestLifetime.failed += 1;
    if (entry.outcome === "aborted") requestLifetime.aborted += 1;
  }
  providerMetrics.recordOutcome(
    entry.startedAt,
    entry.method,
    entry.route,
    entry.outcome,
    entry.durationMs,
    entry.source,
  );
}

function requestWindow(windowMs, now = Date.now()) {
  const rows = requestAttempts.filter((entry) => entry.startedAt >= now - windowMs);
  const byEndpoint = {};
  const bySource = {};
  for (const entry of rows) {
    const key = `${entry.method} ${entry.route}`;
    const current = byEndpoint[key] || { attempts: 0, succeeded: 0, failed: 0, aborted: 0 };
    current.attempts += 1;
    if (entry.outcome === "succeeded") current.succeeded += 1;
    else if (entry.outcome === "aborted") {
      current.failed += 1;
      current.aborted += 1;
    } else if (entry.outcome === "failed") current.failed += 1;
    byEndpoint[key] = current;
    const source = entry.source || "unclassified";
    const sourceCurrent = bySource[source] || { attempts: 0, succeeded: 0, failed: 0, aborted: 0 };
    sourceCurrent.attempts += 1;
    if (entry.outcome === "succeeded") sourceCurrent.succeeded += 1;
    else if (entry.outcome === "aborted") {
      sourceCurrent.failed += 1;
      sourceCurrent.aborted += 1;
    } else if (entry.outcome === "failed") sourceCurrent.failed += 1;
    bySource[source] = sourceCurrent;
  }
  return {
    attempts: rows.length,
    succeeded: rows.filter((entry) => entry.outcome === "succeeded").length,
    failed: rows.filter((entry) => ["failed", "aborted"].includes(entry.outcome)).length,
    aborted: rows.filter((entry) => entry.outcome === "aborted").length,
    running: rows.filter((entry) => entry.outcome === "running").length,
    byEndpoint,
    bySource,
  };
}

function isVendorRateLimitResponse(status, body) {
  const message = typeof body === "string" ? body : JSON.stringify(body || "");
  return (
    status === 429 || (status === 403 && /temporarily blocked|exceed(?:ed|ing).*requests/i.test(message))
  );
}

async function waitForVendorCooldown() {
  const delayMs = blockedUntil - Date.now();
  if (delayMs <= 0) return;
  await new Promise((resolve) => {
    const timer = setTimeout(resolve, delayMs);
    timer.unref?.();
  });
}

function getProviderRateLimitStatus() {
  const now = Date.now();
  pruneRequestAttempts(now);
  return {
    vendorWindowMs: VENDOR_WINDOW_MS,
    vendorWindowCap: VENDOR_WINDOW_CAP,
    safeWindowCap: VENDOR_SAFE_WINDOW_CAP,
    configuredRequestsPerMinute,
    effectiveRequestsPerMinute: maxRequestsPerMinute,
    minTimeMs: Math.max(configuredMinTime, rateLimitMinTime),
    configurationClamped: configuredRequestsPerMinute > maxRequestsPerMinute,
    blockedUntil: blockedUntil ? new Date(blockedUntil).toISOString() : null,
    requests: {
      last20Seconds: requestWindow(20_000, now),
      last60Seconds: requestWindow(60_000, now),
      lifetime: { ...requestLifetime },
    },
  };
}

function providerUrl(path, query) {
  const url = new URL(path, process.env.PROVIDER_BASE_URL || "https://sportexchange-test-dev.rexgames.in/");
  for (const [key, value] of Object.entries(query || {})) {
    if (value == null || value === "") continue;
    if (Array.isArray(value)) value.forEach((item) => url.searchParams.append(key, item));
    else url.searchParams.set(key, value);
  }
  return url;
}

function providerToken() {
  return process.env.PROVIDER_TOKEN || process.env.PROVIDER_X_API_KEY || "dummy_key";
}

function redact(value) {
  if (Array.isArray(value)) return value.map(redact);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      key,
      /token|password|authorization|api[-_]?key/i.test(key) ? "[REDACTED]" : redact(item),
    ]),
  );
}

function loggedPayload(value) {
  if (String(process.env.PROVIDER_LOG_PAYLOADS || "false").toLowerCase() !== "true") {
    return { omitted: true, type: Array.isArray(value) ? "array" : typeof value };
  }
  const maxLength = integer("PROVIDER_LOG_MAX_CHARS", 20000, { min: 1000 });
  const safeValue = redact(value);
  const serialized = JSON.stringify(safeValue);
  if (serialized == null || serialized.length <= maxLength) return safeValue;
  return { truncated: true, originalCharacters: serialized.length, preview: serialized.slice(0, maxLength) };
}

async function request(
  path,
  { method = "GET", query, body, retries = 2, priority = 5, timeoutMs, metricSource } = {},
) {
  if (shuttingDown) throw new Error("Provider client is shutting down");
  const url = providerUrl(path, query);
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    if (shuttingDown) throw new Error("Provider client is shutting down");
    await waitForVendorCooldown();
    const startedAt = Date.now();
    let metricEntry;
    try {
      const requestLog = {
        method,
        url: url.toString(),
        query: Object.fromEntries(url.searchParams),
        body: body === undefined ? null : loggedPayload(body),
        attempt: attempt + 1,
        source: metricSource || "unclassified",
      };
      writeProviderLog("provider.request", requestLog);
      const response = await providerLimiter.schedule({ priority }, async () => {
        metricEntry = startRequestAttempt(method, url, metricSource);
        const controller = new AbortController();
        activeControllers.add(controller);
        const timer = setTimeout(
          () => controller.abort(),
          timeoutMs ?? integer("PROVIDER_HTTP_TIMEOUT_MS", 60000, { min: 1000 }),
        );
        try {
          return await fetch(url, {
            method,
            headers: {
              Accept: "application/json",
              Authorization: `Bearer ${providerToken()}`,
              ...(body === undefined ? {} : { "Content-Type": "application/json" }),
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timer);
          activeControllers.delete(controller);
        }
      });
      const text = await response.text();
      finishRequestAttempt(metricEntry, { status: response.status });
      let data = text;
      try {
        data = text ? JSON.parse(text) : null;
      } catch {
        /* preserve provider text */
      }
      const responseLog = {
        method,
        url: url.toString(),
        status: response.status,
        ok: response.ok,
        durationMs: Date.now() - startedAt,
        body: loggedPayload(data),
      };
      writeProviderLog("provider.response", responseLog);
      if (response.ok) return data;
      if (isVendorRateLimitResponse(response.status, data)) {
        const cooldownMs = integer("PROVIDER_BLOCK_COOLDOWN_MS", 60000, { min: 20000, max: 600000 });
        blockedUntil = Math.max(blockedUntil, Date.now() + cooldownMs);
      }
      const error = new Error(
        `Provider request failed (${response.status}): ${typeof data === "string" ? data : JSON.stringify(data)}`,
      );
      error.statusCode = response.status;
      if (attempt >= retries || ![429, 500, 502, 503, 504].includes(response.status)) throw error;
      const retryAfter = Number(response.headers.get("retry-after"));
      await new Promise((resolve) =>
        setTimeout(resolve, Number.isFinite(retryAfter) ? retryAfter * 1000 : 300 * 2 ** attempt),
      );
    } catch (error) {
      finishRequestAttempt(metricEntry, { status: error.statusCode || null, error });
      const errorLog = {
        method,
        url: url.toString(),
        status: error.statusCode || null,
        durationMs: Date.now() - startedAt,
        attempt: attempt + 1,
        source: metricSource || "unclassified",
        error: error.message,
      };
      writeProviderLog("provider.error", errorLog);
      if (shuttingDown) throw error;
      if (attempt >= retries || (error.statusCode && ![429, 500, 502, 503, 504].includes(error.statusCode)))
        throw error;
      await new Promise((resolve) => setTimeout(resolve, 300 * 2 ** attempt));
    }
  }
  return null;
}

async function closeProviderRequests() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const controller of activeControllers) controller.abort();
  await providerLimiter.stop({ dropWaitingJobs: true, dropErrorMessage: "Provider client is shutting down" });
}

function postIds(path, ids) {
  if (!Array.isArray(ids) || !ids.length) return null;
  // Subscription control is latency-sensitive and must not sit behind the much
  // larger discovery queue. Bottleneck priority 1 runs before default priority 5.
  // Subscribe and unsubscribe use the common provider HTTP timeout so every vendor
  // request follows the same operational timeout policy.
  return request(path, {
    method: "POST",
    body: { data: ids },
    priority: 1,
    retries: 0,
  });
}

module.exports = {
  providerLimiter,
  getProviderRateLimitStatus,
  isVendorRateLimitResponse,
  request,
  closeProviderRequests,
  sports: () => request("/v1/sports"),
  competitions: (query) => request("/v1/competitions", { query }),
  events: (query) => request("/v1/events", { query }),
  markets: (body, { priority = 5, source } = {}) =>
    request("/v1/markets", { method: "POST", body, priority, metricSource: source }),
  runners: (marketId, { priority = 5, source, retries = 2 } = {}) => request(`/v1/markets/${encodeURIComponent(marketId)}/runners`, { priority, metricSource: source, retries }),
  // Results must not sit behind the much larger discovery queue indefinitely.
  results: (body, { priority = 2, source, retries = 2 } = {}) => request("/v1/markets/results", { method: "POST", body, priority, metricSource: source, retries }),
  subscribe: (ids) => postIds(process.env.PROVIDER_SUBSCRIPTION_URL || "/v1/subscribe", ids),
  unsubscribe: (ids) => postIds(process.env.PROVIDER_UNSUBSCRIPTION_URL || "/v1/unsubscribe", ids),
};
