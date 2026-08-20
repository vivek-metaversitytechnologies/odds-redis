const { getSourcePool } = require("../config/sourceDb");
const redisStore = require("../config/redis");
const { integer } = require("../config/env");

function firstPrice(runner, side) {
  const prices = runner?.ex?.[side];
  return Array.isArray(prices) && prices.length ? Number(prices[0]?.price) || 0 : 0;
}

function openDateValue(value) {
  if (value instanceof Date) return value.toISOString().slice(0, 19);
  return value == null ? null : String(value);
}

function dashboardEntry(row, snapshot) {
  const marketId = String(row.marketid || "");
  const marketName = String(row.marketname || "");
  const common = {
    matchName: row.matchname ?? null,
    openDate: openDateValue(row.opendate),
    inPlay: Boolean(row.inplay),
    matchId: Number(row.eventid),
    marketId,
    bm: Boolean(row.isBookmaker),
    GM: Boolean(row.isGoal),
    outright: Boolean(row.isOutright),
  };

  if (marketName.toLowerCase().includes("book")) {
    const bookmaker = Array.isArray(snapshot?.Bookmaker) ? snapshot.Bookmaker : [];
    if (!bookmaker.some((item) => String(item?.mid) === marketId)) return null;
    return { ...common, team1Back: 0, team1Lay: 0, team2Back: 0, team2Lay: 0, drawBack: 0, drawLay: 0 };
  }

  const odds = (Array.isArray(snapshot?.Odds) ? snapshot.Odds : []).find(
    (item) => String(item?.marketId) === marketId,
  );
  if (!odds) return null;
  const runners = Array.isArray(odds.runners) ? odds.runners : [];
  return {
    ...common,
    inPlay: Boolean(odds.inplay),
    team1Back: firstPrice(runners[0], "availableToBack"),
    team1Lay: firstPrice(runners[0], "availableToLay"),
    team2Back: firstPrice(runners[1], "availableToBack"),
    team2Lay: firstPrice(runners[1], "availableToLay"),
    drawBack: firstPrice(runners[2], "availableToBack"),
    drawLay: firstPrice(runners[2], "availableToLay"),
    li: row.seriesid == null ? null : Number(row.seriesid),
  };
}

function compareDashboardEntries(left, right) {
  if (left.outright !== right.outright) return left.outright ? -1 : 1;
  const leftTime = Date.parse(left.openDate);
  const rightTime = Date.parse(right.openDate);
  if (!Number.isFinite(leftTime) && !Number.isFinite(rightTime)) return 0;
  if (!Number.isFinite(leftTime)) return 1;
  if (!Number.isFinite(rightTime)) return -1;
  return leftTime - rightTime;
}

function cachedDashboardRow(event, snapshot) {
  const odds = Array.isArray(snapshot?.Odds) ? snapshot.Odds : [];
  const bookmaker = Array.isArray(snapshot?.Bookmaker) ? snapshot.Bookmaker : [];
  const matchOdds = odds.find(
    (market) => String(market?.Name || "").trim().toLowerCase() === "match odds",
  );
  const bookmakerMarket = bookmaker.find((market) => /bookmaker/i.test(String(market?.t || "")));
  const winnerMarket = [...odds, ...bookmaker].find((market) =>
    /winner/i.test(String(market?.Name ?? market?.t ?? "")),
  );
  const selected = matchOdds || bookmakerMarket || winnerMarket;
  const marketId = selected?.marketId ?? selected?.mid;
  if (!marketId) return null;
  const marketName = String(selected?.Name ?? selected?.t ?? "");
  return {
    marketid: String(marketId),
    marketname: marketName,
    matchname: event.eventName,
    opendate: event.openDate,
    inplay: event.inPlay,
    eventid: event.eventId,
    sportid: event.sportId,
    seriesid: event.seriesId,
    isBookmaker: bookmaker.length > 0,
    isGoal: [...odds, ...bookmaker].some((market) =>
      /goal/i.test(String(market?.Name ?? market?.t ?? "")),
    ),
    isOutright: !matchOdds && Boolean(winnerMarket),
  };
}

function activeMatchesFromCache(events, snapshots, maxAgeHours, now = Date.now()) {
  const oldest = now - maxAgeHours * 60 * 60 * 1000;
  return (events || [])
    .filter((event) => {
      if (event?.gameOver) return false;
      const openTime = Date.parse(event?.openDate);
      return !Number.isFinite(openTime) || openTime >= oldest;
    })
    .map((event) => {
      const snapshot = snapshots.get(String(event.eventId));
      const row = cachedDashboardRow(event, snapshot);
      return row ? dashboardEntry(row, snapshot) : null;
    })
    .filter(Boolean)
    .sort(compareDashboardEntries);
}

function selectDashboardRows(rows) {
  const byEvent = new Map();
  for (const row of rows || []) {
    const eventId = String(row.eventid);
    if (!byEvent.has(eventId)) byEvent.set(eventId, []);
    byEvent.get(eventId).push(row);
  }
  const selected = [];
  for (const eventRows of byEvent.values()) {
    const activeRows = eventRows.filter((row) => !row.settled_marketid);
    const candidate = activeRows
      .filter((row) => /match odds|bookmaker|winner/i.test(String(row.marketname || "")))
      .sort((left, right) => {
        const priority = (row) => {
          const name = String(row.marketname || "").toLowerCase();
          if (name === "match odds") return 1;
          if (name.includes("bookmaker")) return 2;
          if (name.includes("winner")) return 3;
          return 99;
        };
        return priority(left) - priority(right);
      })[0];
    if (!candidate) continue;
    const marketNames = eventRows.map((row) => String(row.marketname || ""));
    selected.push({
      ...candidate,
      isBookmaker: marketNames.some((name) => /bookmaker/i.test(name)),
      isGoal: marketNames.some((name) => /goal/i.test(name)),
      isOutright:
        marketNames.some((name) => name === "Winner" || /winner bookmaker/i.test(name)) &&
        !marketNames.some((name) => name === "Match Odds"),
    });
  }
  return selected;
}

async function activeMatches(sportId) {
  const maxAgeHours = integer("ACTIVE_MATCH_MAX_AGE_HOURS", 48, { min: 1, max: 720 });
  const cachedEvents = await redisStore.getEvents(sportId);
  if (cachedEvents !== null) {
    const snapshots = await redisStore.getEventSnapshots(cachedEvents.map((event) => event.eventId));
    return activeMatchesFromCache(cachedEvents, snapshots, maxAgeHours);
  }
  const [rows] = await getSourcePool().query(
    `SELECT t.matchname, t.opendate, t.inplay, t.eventid, t.marketid, t.marketname,
      t.sportid, t.seriesid, r.marketid AS settled_marketid
    FROM t_event e
    STRAIGHT_JOIN t_market t ON t.eventid = e.eventid AND t.isactive = TRUE
    LEFT JOIN t_matchresult r ON r.marketid = t.marketid
    WHERE e.sportid = ? AND e.isactive = TRUE
      AND e.open_date >= DATE_SUB(NOW(), INTERVAL ${maxAgeHours} HOUR)
      AND (t.marketname = 'Match Odds' OR t.marketname LIKE '%Bookmaker%'
        OR t.marketname LIKE '%Winner%' OR t.marketname LIKE '%Goal%')
    ORDER BY e.eventid ASC`,
    [sportId],
  );
  const selected = selectDashboardRows(rows);
  const snapshots = await redisStore.getEventSnapshots(selected.map((row) => row.eventid));
  return selected
    .map((row) => dashboardEntry(row, snapshots.get(String(row.eventid))))
    .filter(Boolean)
    .sort(compareDashboardEntries);
}

module.exports = {
  activeMatches,
  activeMatchesFromCache,
  cachedDashboardRow,
  dashboardEntry,
  compareDashboardEntries,
  openDateValue,
  selectDashboardRows,
};
