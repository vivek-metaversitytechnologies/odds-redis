function headroomReason({ rate, counts, health, now = Date.now() }) {
  if (!Number.isFinite(Date.parse(health?.checkedAt)) || now - Date.parse(health.checkedAt) > 60000) return "health-stale";
  if (["database", "redis", "eventLoop", "memory", "socket"].some((name) => health.checks?.[name]?.status !== "healthy")) return "resources-busy";
  if (Date.parse(rate.blockedUntil) > now) return "vendor-cooldown";
  if (Number(counts.QUEUED || 0) + Number(counts.RUNNING || 0) + Number(counts.EXECUTING || 0) > 0) return "vendor-busy";
  const recent = rate.requests?.last60Seconds;
  const window = rate.requests?.last20Seconds;
  if (!recent || !window) return "metrics-unavailable";
  if (recent.failed || recent.aborted) return "recent-vendor-errors";
  if (recent.attempts >= Math.min(300, rate.effectiveRequestsPerMinute * 0.7)) return "minute-budget";
  if (window.attempts >= rate.safeWindowCap * 0.5) return "rolling-window-budget";
  return null;
}

module.exports = { headroomReason };
