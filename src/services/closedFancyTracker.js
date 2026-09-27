const { setBounded } = require("../utils/boundedMap");

// Session-style fancies closed by the socket (`s` explicitly false) that may be awaiting a result.
// Only the market's own `s` counts; runner `sb` is deliberately not a close signal. A later tick
// with `s` true reopens the market and removes it. In production every traced session ended this
// way, and the vendor produced its result 1.5-8 minutes after the close.
const CHASED_GROUPS = new Set(["Fancy2", "Fancy3", "OtherMarket", "OddEven", "Khado", "Meter"]);
const TRACK_LIMIT = 20000;
const closed = new Map();

function activity(value) {
  if (typeof value === "boolean") return value;
  if (value === 0 || value === 1) return value === 1;
  if (typeof value === "string" && ["true", "false", "0", "1"].includes(value.toLowerCase())) {
    return ["true", "1"].includes(value.toLowerCase());
  }
  return null;
}

function noteTick(item, group, now = Date.now()) {
  if (!CHASED_GROUPS.has(group)) return;
  const marketId = String(item?.mid ?? "");
  if (!marketId) return;
  const open = activity(item.s);
  if (open === false) {
    if (!closed.has(marketId)) setBounded(closed, marketId, { eventId: String(item.eid ?? ""), closedAt: now }, TRACK_LIMIT);
  } else if (open === true) {
    closed.delete(marketId);
  }
}

// Closed markets still worth chasing; older ones are left to the scheduled backlog sweep.
function pending(maxAgeMs, now = Date.now()) {
  const markets = [];
  for (const [marketId, entry] of closed) {
    if (now - entry.closedAt > maxAgeMs) closed.delete(marketId);
    else markets.push({ marketId, ...entry });
  }
  return markets;
}

function remove(marketIds) {
  for (const marketId of marketIds || []) closed.delete(String(marketId));
}

module.exports = {
  CHASED_GROUPS,
  noteTick,
  pending,
  remove,
  size: () => closed.size,
  __testing__: { reset: () => closed.clear() },
};
