const { integer } = require("../config/env");

function leadMinutes() {
  return integer("ACTIVE_EVENT_LEAD_MINUTES", 60, { min: 0, max: 1440 });
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

module.exports = { leadMinutes, eventInWindow, eventWindowSql };
