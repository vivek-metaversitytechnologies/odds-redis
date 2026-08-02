const state = { markets: [] };
const $ = (selector) => document.querySelector(selector);

function headers() {
  const key = $("#adminKey").value.trim();
  if (key) sessionStorage.setItem("se-admin-key", key);
  return key ? { "X-Internal-API-Key": key } : {};
}

function value(item, keys, fallback = "—") {
  for (const key of keys) if (item?.[key] != null && item[key] !== "") return item[key];
  return fallback;
}

function toast(message) {
  const node = $("#toast"); node.textContent = message; node.classList.add("show");
  setTimeout(() => node.classList.remove("show"), 2800);
}

function render() {
  const query = $("#search").value.trim().toLowerCase();
  const grouped = new Map();
  state.markets.forEach((market) => {
    const eventId = String(value(market, ["eventid"], "unknown"));
    if (!grouped.has(eventId)) grouped.set(eventId, []);
    grouped.get(eventId).push(market);
  });
  const groups = [...grouped.entries()].flatMap(([eventId, markets]) => {
    const eventName = String(value(markets[0], ["matchname"], `Event ${eventId}`));
    const eventMatches = !query || `${eventId} ${eventName}`.toLowerCase().includes(query);
    const visibleMarkets = eventMatches ? markets : markets.filter((market) => ["marketid", "marketname"]
      .some((key) => String(market[key] ?? "").toLowerCase().includes(query)));
    return visibleMarkets.length ? [{ eventId, eventName, markets: visibleMarkets, total: markets.length }] : [];
  });
  const container = $("#eventGroups"); container.innerHTML = "";
  groups.forEach((group, index) => {
    const details = document.createElement("details"); details.className = "event-group";
    details.open = Boolean(query) || index === 0;
    details.innerHTML = `<summary><span class="event-title"><strong></strong><small></small></span><span class="event-count"></span></summary><div class="table-wrap"><table><thead><tr><th>Market</th><th>Market ID</th><th>Status</th><th>Bet limits</th><th>Delay</th></tr></thead><tbody></tbody></table></div>`;
    details.querySelector(".event-title strong").textContent = group.eventName;
    details.querySelector(".event-title small").textContent = `Event ID · ${group.eventId}`;
    details.querySelector(".event-count").textContent = `${group.markets.length}${group.markets.length !== group.total ? ` of ${group.total}` : ""} markets`;
    const body = details.querySelector("tbody");
    group.markets.forEach((market) => {
      const row = document.createElement("tr");
      row.innerHTML = "<td class=\"cell-main\"><strong></strong><small></small></td><td class=\"market-id\"></td><td><span class=\"pill\">Active</span></td><td></td><td></td>";
      row.children[0].querySelector("strong").textContent = value(market, ["marketname"], "Unnamed market");
      row.children[0].querySelector("small").textContent = `Record #${value(market, ["id"])}`;
      row.children[1].textContent = value(market, ["marketid"]);
      row.children[3].textContent = `${value(market, ["minbet"], "—")} – ${value(market, ["maxbet"], "—")}`;
      row.children[4].textContent = value(market, ["betdelay"], "0");
      body.appendChild(row);
    });
    container.appendChild(details);
  });
  const shown = groups.reduce((total, group) => total + group.markets.length, 0);
  $("#tableStatus").textContent = `${groups.length} events · ${shown} of ${state.markets.length} markets shown`;
}

async function load() {
  const button = $("#refresh"); button.disabled = true; button.textContent = "Loading…";
  try {
    const response = await fetch("/api/source/markets", { headers: headers() });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new Error(payload?.message || `Request failed (${response.status})`);
    state.markets = Array.isArray(payload?.data) ? payload.data : [];
    $("#connection").textContent = "Connected"; $("#connection").className = "good";
    $("#marketTotal").textContent = state.markets.length;
    $("#eventTotal").textContent = new Set(state.markets.map((item) => item.eventid).filter(Boolean)).size;
    $("#bookmakerTotal").textContent = state.markets.filter((item) => String(item.marketid || "").toUpperCase().includes("BM")).length;
    render();
  } catch (error) {
    $("#connection").textContent = "Unavailable"; $("#connection").className = "bad";
    $("#tableStatus").textContent = error.message; toast(error.message);
  } finally { button.disabled = false; button.textContent = "Refresh data"; }
}

function syncMessage(entry) {
  const details = Object.entries(entry).filter(([key]) => !["at", "type"].includes(key))
    .map(([key, item]) => `${key}=${item}`).join(" · ");
  return `${entry.type}${details ? ` · ${details}` : ""}`;
}

function renderSync(sync) {
  $("#syncStatus").textContent = sync.running ? "Running" : sync.lastError ? "Failed" : "Idle";
  $("#syncStatus").className = sync.lastError ? "bad" : "good";
  $("#syncSubscribed").textContent = sync.activeMarketCount || 0;
  $("#syncCompleted").textContent = sync.lastCompletedAt ? new Date(sync.lastCompletedAt).toLocaleString() : "Never";
  $("#syncError").textContent = sync.lastError || "None";
  const log = $("#syncLog"); log.innerHTML = "";
  const entries = Array.isArray(sync.recent) ? sync.recent : [];
  if (!entries.length) {
    log.innerHTML = '<div class="log-line muted"><time>—</time><span>No sync activity recorded.</span></div>';
  } else entries.forEach((entry) => {
    const line = document.createElement("div"); line.className = `log-line ${entry.type.includes("failed") ? "error" : entry.type.includes("completed") ? "success" : ""}`;
    line.innerHTML = "<time></time><span></span>";
    line.querySelector("time").textContent = new Date(entry.at).toLocaleTimeString();
    line.querySelector("span").textContent = syncMessage(entry); log.appendChild(line);
  });
  $("#runSync").disabled = Boolean(sync.running);
  $("#runSync").textContent = sync.running ? "Sync running…" : "Run sync now";
}

async function loadSync() {
  try {
    const response = await fetch("/api/source/sync", { headers: headers() });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new Error(payload?.message || `Sync status failed (${response.status})`);
    renderSync(payload.data || {});
  } catch (error) { $("#syncError").textContent = error.message; }
}

async function runSync() {
  const button = $("#runSync"); button.disabled = true; button.textContent = "Sync running…";
  try {
    const response = await fetch("/api/source/sync", { method: "POST", headers: headers() });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new Error(payload?.message || `Sync failed (${response.status})`);
    renderSync(payload.data?.sync || {}); toast("Market subscription sync completed.");
  } catch (error) { toast(error.message); await loadSync(); }
}

async function subscribeManual() {
  const marketId = $("#manualMarketId").value.trim();
  if (!marketId) return toast("Enter a market ID.");
  const button = $("#subscribeManual"); button.disabled = true; button.textContent = "Subscribing…";
  try {
    const response = await fetch("/api/source/subscribe", { method: "POST",
      headers: { "Content-Type": "application/json", ...headers() },
      body: JSON.stringify({ marketIds: [marketId] }) });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new Error(payload?.message || `Subscription failed (${response.status})`);
    const subscribed = Array.isArray(payload?.data?.subscribed) ? payload.data.subscribed : [];
    const skipped = Array.isArray(payload?.data?.skipped) ? payload.data.skipped : [];
    toast(subscribed.length ? `${marketId} subscribed successfully.` : `${marketId} skipped by provider.`);
    await loadSync();
    if (skipped.length) $("#syncError").textContent = `Provider skipped: ${skipped.join(", ")}`;
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = "Subscribe ID"; }
}

$("#adminKey").value = sessionStorage.getItem("se-admin-key") || "";
$("#refresh").addEventListener("click", load);
$("#runSync").addEventListener("click", runSync);
$("#subscribeManual").addEventListener("click", subscribeManual);
$("#search").addEventListener("input", render);
void load(); void loadSync(); setInterval(loadSync, 10000);
