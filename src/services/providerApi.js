const { writeProviderLog } = require("../utils/providerFileLogger");
const Bottleneck = require("bottleneck");

const requestedMaxPerMinute = Number(process.env.PROVIDER_MAX_REQUESTS_PER_MINUTE || 15000);
const requestedMinTime = Number(process.env.PROVIDER_MIN_TIME_MS || 200);
const maxRequestsPerMinute = Number.isFinite(requestedMaxPerMinute)
  ? Math.max(1, requestedMaxPerMinute) : 15000;
const configuredMinTime = Number.isFinite(requestedMinTime) ? Math.max(0, requestedMinTime) : 200;
const rateLimitMinTime = Math.ceil(60000 / maxRequestsPerMinute);
const providerLimiter = new Bottleneck({
  maxConcurrent: Math.max(1, Number(process.env.PROVIDER_MAX_CONCURRENT || 2)),
  minTime: Math.max(configuredMinTime, rateLimitMinTime),
});

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
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [
    key,
    /token|password|authorization|api[-_]?key/i.test(key) ? "[REDACTED]" : redact(item),
  ]));
}

function loggedPayload(value) {
  if (String(process.env.PROVIDER_LOG_PAYLOADS || "true").toLowerCase() !== "true") {
    return { omitted: true, type: Array.isArray(value) ? "array" : typeof value };
  }
  const maxLength = Math.max(1000, Number(process.env.PROVIDER_LOG_MAX_CHARS || 20000));
  const safeValue = redact(value);
  const serialized = JSON.stringify(safeValue);
  if (serialized == null || serialized.length <= maxLength) return safeValue;
  return { truncated: true, originalCharacters: serialized.length, preview: serialized.slice(0, maxLength) };
}

async function request(path, { method = "GET", query, body, retries = 2 } = {}) {
  const url = providerUrl(path, query);
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const startedAt = Date.now();
    try {
      const requestLog = {
        method,
        url: url.toString(),
        query: Object.fromEntries(url.searchParams),
        body: body === undefined ? null : loggedPayload(body),
        attempt: attempt + 1,
      };
      writeProviderLog("provider.request", requestLog);
      const response = await providerLimiter.schedule(async () => {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), Number(process.env.PROVIDER_HTTP_TIMEOUT_MS || 60000));
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
        } finally { clearTimeout(timer); }
      });
      const text = await response.text();
      let data = text;
      try { data = text ? JSON.parse(text) : null; } catch { /* preserve provider text */ }
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
      const error = new Error(`Provider request failed (${response.status}): ${typeof data === "string" ? data : JSON.stringify(data)}`);
      error.statusCode = response.status;
      if (attempt >= retries || ![429, 500, 502, 503, 504].includes(response.status)) throw error;
      const retryAfter = Number(response.headers.get("retry-after"));
      await new Promise((resolve) => setTimeout(resolve, Number.isFinite(retryAfter) ? retryAfter * 1000 : 300 * 2 ** attempt));
    } catch (error) {
      const errorLog = {
        method,
        url: url.toString(),
        status: error.statusCode || null,
        durationMs: Date.now() - startedAt,
        attempt: attempt + 1,
        error: error.message,
      };
      writeProviderLog("provider.error", errorLog);
      if (attempt >= retries || (error.statusCode && ![429, 500, 502, 503, 504].includes(error.statusCode))) throw error;
      await new Promise((resolve) => setTimeout(resolve, 300 * 2 ** attempt));
    }
  }
  return null;
}

function postIds(path, ids) {
  if (!Array.isArray(ids) || !ids.length) return null;
  return request(path, { method: "POST", body: { data: ids } });
}

module.exports = {
  providerLimiter,
  request,
  sports: () => request("/v1/sports"),
  competitions: (query) => request("/v1/competitions", { query }),
  events: (query) => request("/v1/events", { query }),
  markets: (body) => request("/v1/markets", { method: "POST", body }),
  runners: (marketId) => request(`/v1/markets/${encodeURIComponent(marketId)}/runners`),
  results: (body) => request("/v1/markets/results", { method: "POST", body }),
  subscribe: (ids) => postIds(process.env.PROVIDER_SUBSCRIPTION_URL || "/v1/subscribe", ids),
  unsubscribe: (ids) => postIds(process.env.PROVIDER_UNSUBSCRIPTION_URL || "/v1/unsubscribe", ids),
};
