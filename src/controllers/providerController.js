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

module.exports = {
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
