const $ = (selector) => document.querySelector(selector);
function headers() {
  const key = $("#adminKey").value.trim();
  if (key) sessionStorage.setItem("se-admin-key", key);
  return key ? { "X-Internal-API-Key": key } : {};
}
async function request(path) {
  const response = await fetch(path, { headers: headers() });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.message || `Request failed (${response.status})`);
  return body?.data ?? body;
}
function relative(value) {
  if (!value) return "Never";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 5) return "Just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return new Date(value).toLocaleString();
}
function toast(message) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.add("show");
  setTimeout(() => node.classList.remove("show"), 2800);
}
function bytes(value) {
  const amount = Number(value) || 0;
  if (amount < 1024) return `${amount} B`;
  if (amount < 1024 ** 2) return `${(amount / 1024).toFixed(1)} KB`;
  return `${(amount / 1024 ** 2).toFixed(1)} MB`;
}
function pipelineErrors(pipelines = {}) {
  const labels = {
    competition: "Competition sync",
    events: "Event sync",
    discovery: "Market discovery",
    subscriptions: "Subscription sync",
    results: "Result sync",
    redisEventCleanup: "Redis event cleanup",
  };
  return Object.entries(labels).flatMap(([key, label]) => {
    const error = pipelines[key]?.lastError;
    return error ? [`${label}: ${error}`] : [];
  });
}
const vendorSeries = [
  { key: "POST /v1/markets", label: "Markets", color: "#2563eb" },
  { key: "GET /v1/markets/:marketId/runners", label: "Runners", color: "#7c3aed" },
  { key: "POST /v1/unsubscribe", label: "Unsubscribe", color: "#d97706" },
  { key: "POST /v1/subscribe", label: "Subscribe", color: "#059669" },
  { key: "other", label: "Other", color: "#94a3b8" },
];
let lastVendorMetrics;
function vendorMinuteLabel(value) {
  return `${value.slice(8, 10)}:${value.slice(10, 12)}`;
}
function renderVendorLegend() {
  const legend = $("#vendorChartLegend");
  if (legend.children.length) return;
  [...vendorSeries, { label: "Aborted", color: "#dc2626", line: true }].forEach((series) => {
    const item = document.createElement("span");
    item.innerHTML = `<i style="background:${series.color};height:${series.line ? "2px" : "10px"}"></i>${series.label}`;
    legend.appendChild(item);
  });
}
function vendorChartRows(metrics) {
  const known = new Set(vendorSeries.filter((series) => series.key !== "other").map((series) => series.key));
  return (metrics.buckets || []).map((bucket) => {
    const row = { minute: bucket.minute, aborted: 0, other: 0 };
    vendorSeries.forEach((series) => {
      if (series.key !== "other") row[series.key] = bucket.endpoints?.[series.key]?.attempts || 0;
    });
    Object.entries(bucket.endpoints || {}).forEach(([endpoint, values]) => {
      row.aborted += values.aborted || 0;
      if (!known.has(endpoint)) row.other += values.attempts || 0;
    });
    row.total = vendorSeries.reduce((sum, series) => sum + (row[series.key] || 0), 0);
    return row;
  });
}
function drawVendorChart(metrics) {
  lastVendorMetrics = metrics;
  renderVendorLegend();
  const canvas = $("#vendorChart");
  const rows = vendorChartRows(metrics);
  const hasData = rows.some((row) => row.total || row.aborted);
  const context = canvas.getContext("2d");
  const width = Math.max(320, canvas.parentElement.clientWidth);
  const height = 320;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  context.scale(ratio, ratio);
  context.clearRect(0, 0, width, height);
  if (!hasData) {
    context.fillStyle = "#64748b";
    context.font = "13px sans-serif";
    context.fillText("No vendor request history recorded yet.", 16, 32);
    $("#vendorChartStatus").textContent = "Waiting for the first Redis metrics bucket.";
    return;
  }
  const margin = { top: 15, right: 18, bottom: 38, left: 52 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const maximum = Math.max(10, ...rows.map((row) => row.total));
  const ceiling = Math.ceil(maximum / 50) * 50;
  const xStep = plotWidth / rows.length;
  const barWidth = Math.max(2, xStep * 0.72);
  context.font = "11px sans-serif";
  context.textAlign = "right";
  context.textBaseline = "middle";
  for (let index = 0; index <= 4; index += 1) {
    const value = (ceiling / 4) * index;
    const y = margin.top + plotHeight - (value / ceiling) * plotHeight;
    context.strokeStyle = "#e2e8f0";
    context.beginPath();
    context.moveTo(margin.left, y);
    context.lineTo(width - margin.right, y);
    context.stroke();
    context.fillStyle = "#64748b";
    context.fillText(String(Math.round(value)), margin.left - 8, y);
  }
  rows.forEach((row, index) => {
    const x = margin.left + index * xStep + (xStep - barWidth) / 2;
    let bottom = margin.top + plotHeight;
    vendorSeries.forEach((series) => {
      const value = row[series.key] || 0;
      const segmentHeight = (value / ceiling) * plotHeight;
      context.fillStyle = series.color;
      context.fillRect(x, bottom - segmentHeight, barWidth, segmentHeight);
      bottom -= segmentHeight;
    });
  });
  context.strokeStyle = "#dc2626";
  context.lineWidth = 2;
  context.beginPath();
  rows.forEach((row, index) => {
    const x = margin.left + index * xStep + xStep / 2;
    const y = margin.top + plotHeight - (row.aborted / ceiling) * plotHeight;
    if (index === 0) context.moveTo(x, y);
    else context.lineTo(x, y);
  });
  context.stroke();
  context.fillStyle = "#64748b";
  context.textAlign = "center";
  context.textBaseline = "top";
  const tickEvery = Math.max(1, Math.ceil(rows.length / (width < 600 ? 5 : 10)));
  rows.forEach((row, index) => {
    if (index % tickEvery !== 0 && index !== rows.length - 1) return;
    context.fillText(vendorMinuteLabel(row.minute), margin.left + index * xStep + xStep / 2, height - 27);
  });
  const attempts = rows.reduce((sum, row) => sum + row.total, 0);
  const aborted = rows.reduce((sum, row) => sum + row.aborted, 0);
  $("#vendorChartStatus").textContent =
    `${attempts.toLocaleString()} attempts · ${aborted.toLocaleString()} aborted · UTC minute buckets`;
}
async function refresh() {
  const button = $("#refresh");
  button.disabled = true;
  button.textContent = "Refreshing…";
  try {
    const [health, counts, redis, vendorMetrics] = await Promise.all([
      request("/health"),
      request("/api/source/overview"),
      request("/api/redis/ticks?limit=250"),
      request("/api/provider/metrics?minutes=60"),
    ]);
    const ws = health.websocket || {};
    const ready = health.sourceDatabase === "connected" && health.redis?.connected && ws.connected;
    const receiving = Boolean(ws.lastTickAt) && Date.now() - new Date(ws.lastTickAt).getTime() < 120000;
    const errors = pipelineErrors(health.pipelines);
    const healthy = ready && receiving && !errors.length;
    $("#overallState").className =
      `dashboard-alert ${errors.length || !ready ? "error" : healthy ? "healthy" : "warning"}`;
    $("#overallTitle").textContent = errors.length
      ? "Background worker failure"
      : healthy
        ? "All systems healthy"
        : ready
          ? "Connected, waiting for data"
          : "A service needs attention";
    $("#overallDetail").textContent = errors.length
      ? errors.join(" · ")
      : ready
        ? "MySQL, Redis and the provider stream are connected."
        : "Check the local database, Redis and provider socket.";
    $("#sideStatus").className = healthy ? "online" : "offline";
    $("#sideStatusText").textContent = errors.length
      ? "Worker failure"
      : healthy
        ? "Services online"
        : "Service issue";
    $("#competitionCount").textContent = counts.competitions ?? 0;
    $("#providerEventCount").textContent = counts.events ?? 0;
    $("#marketCount").textContent = counts.activeMarkets ?? 0;
    $("#subscriptionCount").textContent = ws.subscribedCount ?? 0;
    $("#eventCount").textContent = new Set((redis.items || []).map((item) => item.eventId)).size;
    $("#lastTick").textContent = relative(ws.lastTickAt);
    const traffic = ws.traffic || {};
    const providerQueue = health.pipelines?.providerQueue || {};
    $("#ingestedRate").textContent = `${traffic.ingestedTicksPerSecond || 0}/s`;
    $("#ingestedBytes").textContent = `${traffic.ingestedTicks || 0} ticks · ${bytes(traffic.ingestedBytes)}`;
    $("#pendingTicks").textContent = ws.pendingTickCount || 0;
    $("#pendingEvents").textContent = `${ws.pendingEventCount || 0} queued events`;
    $("#processingEvents").textContent = ws.activeEventWriteCount || 0;
    $("#persistedRate").textContent = `${traffic.persistedTicksPerSecond || 0}/s`;
    $("#persistedBytes").textContent =
      `${traffic.persistedTicks || 0} ticks · ${bytes(traffic.persistedBytes)}`;
    $("#forwardedRate").textContent = `${traffic.forwardedEventsPerSecond || 0}/s`;
    $("#forwardedBytes").textContent =
      `${traffic.forwardedEvents || 0} events · ${bytes(traffic.forwardedBytes)}`;
    $("#providerQueued").textContent = providerQueue.QUEUED || 0;
    $("#providerRunning").textContent =
      `${providerQueue.RUNNING || 0} running · ${providerQueue.EXECUTING || 0} executing`;
    drawVendorChart(vendorMetrics);
    $("#updatedAt").textContent = `Updated ${new Date().toLocaleTimeString()} · Refreshes every 15 seconds`;
  } catch (error) {
    $("#overallState").className = "dashboard-alert error";
    $("#overallTitle").textContent = "Unable to load health";
    $("#overallDetail").textContent = error.message;
    toast(error.message);
  } finally {
    button.disabled = false;
    button.textContent = "Refresh";
  }
}
$("#adminKey").value = sessionStorage.getItem("se-admin-key") || "";
$("#refresh").addEventListener("click", refresh);
void refresh();
setInterval(refresh, 15000);
window.addEventListener("resize", () => {
  clearTimeout(window.vendorChartResizeTimer);
  window.vendorChartResizeTimer = setTimeout(() => {
    if (lastVendorMetrics) drawVendorChart(lastVendorMetrics);
  }, 150);
});
