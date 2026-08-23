const { integer } = require("../config/env");

function leadMinutes() {
  return integer("ACTIVE_EVENT_LEAD_MINUTES", 60, { min: 0, max: 1440 });
}

function nonCricketSubscriptionLeadMinutes() {
  return integer("NON_CRICKET_SUBSCRIPTION_LEAD_MINUTES", 720, { min: 0, max: 2880 });
}

function cricketSportId() {
  return integer("CRICKET_SPORT_ID", 4, { min: 1 });
}

function eventWindowSql(alias = "e", lane = "active") {
  if (lane === "all") return "1=1";
  // open_date is stored as an IST wall-clock DATETIME while production MySQL uses UTC.
  const threshold = `DATE_ADD(UTC_TIMESTAMP(), INTERVAL ${leadMinutes() + 330} MINUTE)`;
  if (lane === "future") {
    return `COALESCE(${alias}.in_play,0)=0 AND ${alias}.open_date > ${threshold}`;
  }
  return `(COALESCE(${alias}.in_play,0)=1 OR ${alias}.open_date <= ${threshold})`;
}

function subscriptionEventWindowSql(sportExpression, alias = "e", lane = "active") {
  if (lane !== "active") return eventWindowSql(alias, lane);
  const cricketThreshold = `DATE_ADD(UTC_TIMESTAMP(), INTERVAL ${leadMinutes() + 330} MINUTE)`;
  const nonCricketThreshold = `DATE_ADD(UTC_TIMESTAMP(), INTERVAL ${nonCricketSubscriptionLeadMinutes() + 330} MINUTE)`;
  return `(COALESCE(${alias}.in_play,0)=1 OR (`
    + `(${sportExpression})=${cricketSportId()} AND ${alias}.open_date <= ${cricketThreshold}`
    + `) OR ((` + sportExpression + `)<>${cricketSportId()} AND ${alias}.open_date <= ${nonCricketThreshold}))`;
}

function eventInWindow(event, lane = "active", now = Date.now()) {
  if (lane === "all") return true;
  const inPlay = Boolean(event?.inPlay ?? event?.in_play);
  const rawOpenDate = event?.openDate ?? event?.open_date;
  const normalizedOpenDate = String(rawOpenDate || "")
    .trim()
    .replace(" ", "T")
    .replace(/(\.\d{3})\d+$/, "$1");
  const openTime = Date.parse(
    normalizedOpenDate && !/(?:Z|[+-]\d\d:\d\d)$/.test(normalizedOpenDate)
      ? `${normalizedOpenDate}Z`
      : normalizedOpenDate,
  );
  if (!Number.isFinite(openTime)) return lane !== "future" && inPlay;
  // open_date is an IST wall-clock value. Parse its components as UTC and compare
  // them with the equivalent current IST wall clock used by eventWindowSql().
  const threshold = now + (leadMinutes() + 330) * 60_000;
  return lane === "future" ? !inPlay && openTime > threshold : inPlay || openTime <= threshold;
}

module.exports = {
  leadMinutes,
  nonCricketSubscriptionLeadMinutes,
  eventInWindow,
  eventWindowSql,
  subscriptionEventWindowSql,
};
