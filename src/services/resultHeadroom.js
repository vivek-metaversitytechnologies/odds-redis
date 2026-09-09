function headroomReason({ rate, counts, health, now = Date.now() }) {
  if (!Number.isFinite(Date.parse(health?.checkedAt)) || now - Date.parse(health.checkedAt) > 60000) return "health-stale";
  if (["database", "redis", "eventLoop", "memory", "socket"].some((name) => health.checks?.[name]?.status !== "healthy")) return "resources-busy";
  if (Date.parse(rate.blockedUntil) > now) return "vendor-cooldown";
  if (Number(counts.QUEUED || 0) > 0) return "vendor-busy";
  const recent = rate.requests?.last60Seconds;
  const window = rate.requests?.last20Seconds;
  if (!recent || !window) return "metrics-unavailable";
  if (recent.failed || recent.aborted) return "recent-vendor-errors";
  const background = ["result-headroom", "result-runner-repair"]
    .reduce((total, source) => total + Number(recent.bySource?.[source]?.attempts || 0), 0);
  if (background >= 400) return "background-budget";
  if (recent.attempts >= rate.effectiveRequestsPerMinute - 1) return "minute-budget";
  if (window.attempts >= rate.safeWindowCap * 0.5) return "rolling-window-budget";
  return null;
}

module.exports = { headroomReason };
