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

async function activeMatches(sportId) {
  const maxAgeHours = integer("ACTIVE_MATCH_MAX_AGE_HOURS", 48, { min: 1, max: 720 });
  const [rows] = await getSourcePool().query(
    `SELECT t.matchname, t.opendate, t.inplay, t.eventid, t.marketid, t.marketname,
      t.sportid, t.seriesid,
      EXISTS(SELECT 1 FROM t_market bm WHERE bm.eventid = t.eventid
        AND bm.isactive = TRUE AND bm.marketname LIKE '%Bookmaker%') AS isBookmaker,
      EXISTS(SELECT 1 FROM t_market gm WHERE gm.eventid = t.eventid
        AND gm.isactive = TRUE AND gm.marketname LIKE '%Goal%') AS isGoal,
      (EXISTS(SELECT 1 FROM t_market wm WHERE wm.eventid = t.eventid
         AND wm.isactive = TRUE
         AND (wm.marketname = 'Winner' OR wm.marketname LIKE '%Winner Bookmaker%'))
       AND NOT EXISTS(SELECT 1 FROM t_market mo WHERE mo.eventid = t.eventid
         AND mo.isactive = TRUE AND mo.marketname = 'Match Odds')) AS isOutright
    FROM t_market t
    INNER JOIN t_event e ON e.eventid = t.eventid
    WHERE t.sportid = ? AND t.isactive = TRUE AND e.isactive = TRUE
      AND e.open_date >= DATE_SUB(NOW(), INTERVAL ${maxAgeHours} HOUR)
      AND NOT EXISTS (SELECT 1 FROM t_matchresult r WHERE r.marketid = t.marketid)
      AND (t.marketname = 'Match Odds' OR t.marketname LIKE '%Bookmaker%'
        OR t.marketname LIKE '%Winner%')
    ORDER BY t.eventid ASC,
      CASE WHEN t.marketname = 'Match Odds' THEN 1
        WHEN t.marketname LIKE '%Bookmaker%' THEN 2
        WHEN t.marketname LIKE '%Winner%' THEN 3 ELSE 99 END`,
    [sportId],
  );
  const selected = [];
  const seenEvents = new Set();
  for (const row of rows) {
    const eventId = String(row.eventid);
    if (seenEvents.has(eventId)) continue;
    seenEvents.add(eventId);
    selected.push(row);
  }
  const snapshots = await redisStore.getEventSnapshots(selected.map((row) => row.eventid));
  return selected
    .map((row) => dashboardEntry(row, snapshots.get(String(row.eventid))))
    .filter(Boolean)
    .sort(compareDashboardEntries);
}

module.exports = { activeMatches, dashboardEntry, compareDashboardEntries, openDateValue };
