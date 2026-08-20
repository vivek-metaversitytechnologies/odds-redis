const RESULT_TYPES = new Set(["market", "fancy", "exceptional"]);

function badRequest(message) {
  const error = new Error(message);
  error.statusCode = 400;
  return error;
}

function positiveInteger(value, name) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw badRequest(`${name} must be a positive integer`);
  return parsed;
}

function dateFilter(value, name, endOfDay = false) {
  if (value == null || value === "") return null;
  const input = String(value).trim();
  const dateOnly = /^\d{4}-\d{2}-\d{2}$/.test(input);
  const parsed = new Date(dateOnly ? `${input}T00:00:00.000Z` : input);
  if (Number.isNaN(parsed.getTime())) throw badRequest(`${name} must be a valid ISO date`);
  if (dateOnly) {
    if (endOfDay) parsed.setUTCDate(parsed.getUTCDate() + 1);
    return {
      value: `${parsed.toISOString().slice(0, 10)} 00:00:00`,
      timestamp: parsed.getTime(),
      exclusive: endOfDay,
    };
  }
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    })
      .formatToParts(parsed)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
  return {
    value: `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`,
    timestamp: parsed.getTime(),
    exclusive: false,
  };
}

function resultFilters(query = {}) {
  const type = String(query.type || "")
    .trim()
    .toLowerCase();
  if (type && !RESULT_TYPES.has(type)) {
    throw badRequest("type must be market, fancy, or exceptional");
  }
  const from = dateFilter(query.from, "from");
  const to = dateFilter(query.to, "to", true);
  if (from && to && from.timestamp >= to.timestamp) throw badRequest("from must be earlier than to");
  const requestedLimit = query.limit == null || query.limit === "" ? 250 : Number(query.limit);
  return {
    type: type || null,
    sportId: positiveInteger(query.sportId, "sportId"),
    eventId: positiveInteger(query.eventId, "eventId"),
    marketId: String(query.marketId || "").trim() || null,
    status: String(query.status || "").trim() || null,
    from,
    to,
    limit: Math.max(1, Math.min(500, Number.isFinite(requestedLimit) ? Math.floor(requestedLimit) : 250)),
  };
}

function resultWhere(filters, type) {
  const columns = {
    market: {
      marketId: "marketid",
      eventId: "matchid",
      sportId: "sportid",
      status: "resultstatus",
      date: "date",
    },
    fancy: {
      marketId: "fancyid",
      eventId: "matchid",
      sportId: "sportid",
      status: "resultstatus",
      date: "updatedon",
    },
    exceptional: {
      marketId: "marketid",
      eventId: "matchid",
      sportId: "sportid",
      status: "result",
      date: "date",
    },
  }[type];
  const clauses = [];
  const params = [];
  for (const [name, value] of [
    ["sportId", filters.sportId],
    ["eventId", filters.eventId],
    ["marketId", filters.marketId],
    ["status", filters.status],
  ]) {
    if (value != null) {
      clauses.push(`${columns[name]} = ?`);
      params.push(value);
    }
  }
  if (filters.from) {
    clauses.push(`${columns.date} >= ?`);
    params.push(filters.from.value);
  }
  if (filters.to) {
    clauses.push(`${columns.date} ${filters.to.exclusive ? "<" : "<="} ?`);
    params.push(filters.to.value);
  }
  return { sql: clauses.length ? ` WHERE ${clauses.join(" AND ")}` : "", params };
}

module.exports = { RESULT_TYPES, resultFilters, resultWhere };
