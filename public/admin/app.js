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
async function refresh() {
  const button = $("#refresh");
  button.disabled = true;
  button.textContent = "Refreshing…";
  try {
    const [health, counts, redis] = await Promise.all([
      request("/health"),
      request("/api/source/overview"),
      request("/api/redis/ticks?limit=250"),
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
