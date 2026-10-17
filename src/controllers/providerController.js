const provider = require("../services/providerApi");

function handler(action) {
  return async (req, res, next) => {
    try {
      const data = await action(req);
      res.json({ status: "ok", data });
    } catch (error) {
      next(error);
    }
  };
}

function items(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.data?.data)) return payload.data.data;
  return [];
}

function same(value, expected) {
  return expected == null || expected === "" || String(value) === String(expected);
}

// Read-only vendor endpoints the admin explorer may call. Subscribe and unsubscribe are left out
// because they change the live feed the pipelines depend on.
const explorerEndpoints = {
  sports: { method: "GET", path: "/v1/sports", label: "Sports" },
  competitions: { method: "GET", path: "/v1/competitions", label: "Competitions" },
  events: { method: "GET", path: "/v1/events", label: "Events", query: { si: 4, today: 1 } },
  markets: { method: "POST", path: "/v1/markets", label: "Markets", body: { eids: [] } },
  marketSettings: {
    method: "POST",
    path: "/v1/markets/settings",
    label: "Market settings",
    body: { mids: [] },
  },
  runners: { method: "GET", path: "/v1/markets/:marketId/runners", label: "Runners", params: ["marketId"] },
  results: { method: "POST", path: "/v1/markets/results", label: "Results", body: { mids: [] } },
};

function badRequest(message) {
  const error = new Error(message);
  error.statusCode = 400;
  return error;
}

function plainObject(value) {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

// Returns the vendor's response as-is, including non-2xx bodies, so the admin sees exactly what
// the vendor sent rather than the filtered lists the other provider routes return.
async function explore(req) {
  const key = req.body?.endpoint;
  const endpoint = Object.hasOwn(explorerEndpoints, key) ? explorerEndpoints[key] : null;
  if (!endpoint) throw badRequest("Unknown vendor endpoint");
  const params = req.body.params ?? {};
  const query = req.body.query ?? {};
  if (!plainObject(params) || !plainObject(query)) throw badRequest("params and query must be objects");
  let path = endpoint.path;
  for (const name of endpoint.params || []) {
    const value = String(params[name] ?? "").trim();
    if (!value) throw badRequest(`${name} is required`);
    path = path.replace(`:${name}`, encodeURIComponent(value));
  }
  const body = endpoint.method === "POST" ? (req.body.body ?? {}) : undefined;
  const startedAt = Date.now();
  const url = provider.providerUrl(path, query).toString();
  try {
    const data = await provider.request(path, {
      method: endpoint.method,
      query,
      body,
      retries: 0,
      metricSource: "admin-explorer",
    });
    return { url, method: endpoint.method, ok: true, durationMs: Date.now() - startedAt, body: data };
  } catch (error) {
    return {
      url,
      method: endpoint.method,
      ok: false,
      status: error.statusCode || null,
      durationMs: Date.now() - startedAt,
      error: error.statusCode ? undefined : error.message,
      body: error.body ?? null,
    };
  }
}

module.exports = {
  explorerEndpoints: handler(async () => explorerEndpoints),
  explore: handler(explore),
  metrics: handler(async (req) => {
    const minutes = Number(req.query.minutes || 60);
    if (!Number.isInteger(minutes) || minutes < 1 || minutes > 1440) {
      const error = new Error("minutes must be an integer between 1 and 1440");
      error.statusCode = 400;
      throw error;
    }
    return require("../services/providerMetrics").history(minutes);
  }),
  sports: handler(async () => items(await provider.sports())),
  competitions: handler(async (req) =>
    items(await provider.competitions(req.query)).filter((item) =>
      same(item.sportId ?? item.sportid, req.query.sportId),
    ),
  ),
  events: handler(async (req) =>
    items(await provider.events(req.query))
      .filter((item) => same(item.sportId ?? item.sportid, req.query.sportId))
      .filter((item) =>
        same(item.competitionId ?? item.competitionid ?? item.leagueId, req.query.competitionId),
      )
      .filter((item) => req.query.includeCompleted === "true" || item.gameOver !== true)
      .sort(
        (left, right) =>
          Number(Boolean(right.inPlay)) - Number(Boolean(left.inPlay)) ||
          new Date(left.startTime || 0).getTime() - new Date(right.startTime || 0).getTime(),
      ),
  ),
  markets: handler(async (req) => {
    const eventIds = new Set(
      (Array.isArray(req.body?.data) ? req.body.data : req.body?.eventIds || []).map(String),
    );
    return items(await provider.markets(req.body))
      .filter((item) => !eventIds.size || eventIds.has(String(item.eventId ?? item.eventid ?? item.eid)))
      .filter((item) => item.isActive !== false && item.gameOver !== true);
  }),
  runners: handler((req) => provider.runners(req.params.marketId)),
  results: handler((req) => provider.results(req.body)),
};
