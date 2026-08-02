const state = { items: [], socket: {} };
const $ = (selector) => document.querySelector(selector);

function headers() { const key = $("#adminKey").value.trim(); if (key) sessionStorage.setItem("se-admin-key", key); return key ? { "X-Internal-API-Key": key } : {}; }
function toast(message) { const node = $("#toast"); node.textContent = message; node.classList.add("show"); setTimeout(() => node.classList.remove("show"), 2800); }
function relative(value) { if (!value) return "Never"; const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000)); if (seconds < 5) return "Just now"; if (seconds < 60) return `${seconds}s ago`; if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`; return new Date(value).toLocaleString(); }

function render() {
  const search = $("#search").value.trim().toLowerCase(); const provider = $("#providerFilter").value; const freshness = $("#freshnessFilter").value;
  const items = state.items.filter((item) => (provider === "all" || item.providerState === provider) && (freshness === "all" || item.freshness === freshness)
    && (!search || `${item.eventName} ${item.eventId} ${item.marketName} ${item.marketId}`.toLowerCase().includes(search)));
  const rows = $("#rows"); rows.innerHTML = "";
  items.forEach((item) => {
    const row = document.createElement("tr");
    row.innerHTML = `<td class="cell-copy"><strong></strong><small></small></td><td class="cell-copy"><strong></strong><small></small></td><td><span class="state-pill"></span></td><td><span class="state-pill"></span></td><td></td><td></td><td><button class="market-link">Inspect Redis</button></td>`;
    row.children[0].querySelector("strong").textContent = item.eventName || `Event ${item.eventId}`; row.children[0].querySelector("small").textContent = item.eventId;
    row.children[1].querySelector("strong").textContent = item.marketName || "Unnamed market"; row.children[1].querySelector("small").textContent = item.marketId;
    const providerPill = row.children[2].querySelector("span"); providerPill.textContent = item.providerState; providerPill.classList.add(item.providerState);
    const feedPill = row.children[3].querySelector("span"); feedPill.textContent = item.freshness; feedPill.classList.add(item.freshness);
    row.children[4].textContent = relative(item.lastTickAt); row.children[5].textContent = item.tickCount || 0;
    row.querySelector("button").addEventListener("click", () => { location.href = `/admin/redis/?eventId=${encodeURIComponent(item.eventId)}`; }); rows.appendChild(row);
  });
  $("#summary").textContent = `${items.length} of ${state.items.length} markets shown`;
  $("#total").textContent = state.items.length; $("#subscribed").textContent = state.items.filter((item) => item.providerState === "subscribed").length;
  $("#receiving").textContent = state.items.filter((item) => item.receiving).length;
  $("#attention").textContent = state.items.filter((item) => ["skipped", "stale", "delayed"].includes(item.providerState) || ["stale", "delayed"].includes(item.freshness)).length;
  $("#socketSummary").textContent = state.socket.connected ? `Vendor socket connected · ${state.socket.subscribedCount || 0} active subscriptions`
    : state.socket.connectionRequested ? "Vendor socket reconnecting" : "Vendor socket not started";
}

async function load() {
  const button = $("#refresh"); button.disabled = true;
  try { const response = await fetch("/api/events/subscriptions", { headers: headers() }); const payload = await response.json().catch(() => null); if (!response.ok) throw new Error(payload?.message || `Request failed (${response.status})`); state.items = payload.data || []; state.socket = payload.meta?.socket || {}; render(); }
  catch (error) { toast(error.message); $("#summary").textContent = error.message; }
  finally { button.disabled = false; }
}

$("#adminKey").value = sessionStorage.getItem("se-admin-key") || "";
$("#refresh").addEventListener("click", load); ["#search", "#providerFilter", "#freshnessFilter"].forEach((selector) => $(selector).addEventListener(selector === "#search" ? "input" : "change", render));
setInterval(() => { if ($("#autoRefresh").checked) void load(); }, 5000); void load();
