const { integer } = require("../config/env");

function leadMinutes() {
  return integer("ACTIVE_EVENT_LEAD_MINUTES", 60, { min: 0, max: 1440 });
}

function eventWindowSql(alias = "e", lane = "active") {
  if (lane === "all") return "1=1";
  const threshold = `DATE_ADD(NOW(), INTERVAL ${leadMinutes()} MINUTE)`;
  if (lane === "future") {
    return `COALESCE(${alias}.in_play,0)=0 AND ${alias}.open_date > ${threshold}`;
  }
  return `(COALESCE(${alias}.in_play,0)=1 OR ${alias}.open_date <= ${threshold})`;
}

module.exports = { leadMinutes, eventWindowSql };
