const state = { regular: [], fancy: [], kind: "regular", sport: "all" };
const $ = (selector) => document.querySelector(selector);

function headers() {
  const key = $("#adminKey").value.trim();
  if (key) sessionStorage.setItem("se-admin-key", key);
  return key ? { "X-Internal-API-Key": key } : {};
}

async function request(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { ...headers(), ...(options.headers || {}) } });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.message || `Request failed (${response.status})`);
  return body?.data ?? body;
}

function toast(message) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.add("show");
  setTimeout(() => node.classList.remove("show"), 2800);
}

function sport(id) { return ({ 1: "Football", 2: "Tennis", 4: "Cricket" })[Number(id)] || id || "—"; }
function date(value) { return value ? new Date(value).toLocaleString() : "—"; }
function yes(value) {
  return value === true || value === 1 || value === "1"
    || (Array.isArray(value?.data) && value.data[0] === 1);
}
function limits(row) {
  const min = Number(row.minbet);
  const max = Number(row.maxbet);
  if (!Number.isFinite(min) && !Number.isFinite(max)) return "—";
  return `${Number.isFinite(min) ? min.toLocaleString() : "—"} – ${Number.isFinite(max) ? max.toLocaleString() : "—"}`;
}

function cell(text, className) {
  const node = document.createElement("td");
  if (className) node.className = className;
  node.textContent = text ?? "—";
  return node;
}

function renderRegular(row) {
  const tr = document.createElement("tr");
  const name = cell("", "primary-cell");
  const strong = document.createElement("strong"); strong.textContent = row.marketname || "Unnamed market"; name.appendChild(strong);
  const marketId = cell(""); const code = document.createElement("code"); code.textContent = row.marketid; marketId.appendChild(code);
  tr.append(name, marketId, cell(row.matchname || row.eventid), cell(`${sport(row.sportid)} · ${row.sportid}`),
    cell(limits(row)), cell(Number(row.betdelay ?? 0)), statusCell(yes(row.isactive) && yes(row.status)));
  return tr;
}

function renderFancy(row) {
  const tr = document.createElement("tr");
  const name = cell("", "primary-cell");
  const strong = document.createElement("strong"); strong.textContent = row.name || "Unnamed session"; name.appendChild(strong);
  const marketId = cell(""); const code = document.createElement("code"); code.textContent = row.fancyid; marketId.appendChild(code);
  tr.append(name, marketId, cell(row.matchname || row.eventid), cell(`${sport(row.sportid)} · ${row.sportid}`),
    cell(row.oddstype || "—"), cell(limits(row)), statusCell(yes(row.isactive) && yes(row.isshow)));
  return tr;
}

function statusCell(active) {
  const td = cell(""); const badge = document.createElement("span");
  badge.className = `status-badge${active ? "" : " inactive"}`;
  badge.textContent = active ? "Active" : "Inactive";
  td.appendChild(badge); return td;
}

function render() {
  const isRegular = state.kind === "regular";
  const source = isRegular ? state.regular : state.fancy;
  const q = $("#search").value.trim().toLowerCase();
  const matches = source.filter((row) => {
    if (state.sport !== "all" && String(row.sportid) !== state.sport) return false;
    const text = isRegular
      ? `${row.marketname || ""} ${row.marketid || ""} ${row.matchname || ""} ${row.eventid || ""}`
      : `${row.name || ""} ${row.fancyid || ""} ${row.matchname || ""} ${row.eventid || ""}`;
    return !q || text.toLowerCase().includes(q);
  });
  const shown = matches.slice(0, 300);
  $("#tableTitle").textContent = isRegular ? "Regular markets" : "Session / Fancy markets";
  $("#tableHead").innerHTML = isRegular
    ? "<tr><th>Market</th><th>Market ID</th><th>Event</th><th>Sport</th><th>Min – Max</th><th>Delay</th><th>State</th></tr>"
    : "<tr><th>Session</th><th>Fancy ID</th><th>Event</th><th>Sport</th><th>Odds type</th><th>Min – Max</th><th>State</th></tr>";
  const body = $("#rows"); body.innerHTML = "";
  if (!shown.length) body.innerHTML = '<tr><td colspan="7" class="empty-row">No markets found.</td></tr>';
  shown.forEach((row) => body.appendChild(isRegular ? renderRegular(row) : renderFancy(row)));
  $("#summary").textContent = matches.length > 300
    ? `Showing 300 of ${matches.length}; use search to narrow results`
    : `${matches.length} of ${source.length} markets shown`;
}

function renderHealth(discovery, subscription) {
  const alert = $("#discoveryHealth");
  alert.className = "dashboard-alert";
  if (discovery.running) {
    alert.classList.add("warning");
    $("#discoveryTitle").textContent = "Market discovery is running";
    $("#discoveryDetail").textContent = "Markets, runners and subscriptions are being synchronized.";
  } else if (discovery.lastError) {
    alert.classList.add("error");
    $("#discoveryTitle").textContent = "Last market discovery failed";
    $("#discoveryDetail").textContent = discovery.lastError;
  } else if (discovery.lastCompletedAt) {
    alert.classList.add("healthy");
    $("#discoveryTitle").textContent = "Market discovery is healthy";
    $("#discoveryDetail").textContent = `Last completed ${date(discovery.lastCompletedAt)}.`;
  } else {
    alert.classList.add("warning");
    $("#discoveryTitle").textContent = "Waiting for first market discovery";
    $("#discoveryDetail").textContent = "Run discovery to populate markets and subscriptions.";
  }
  $("#syncStatus").textContent = discovery.lastCompletedAt
    ? `Last discovered ${date(discovery.lastCompletedAt)} · Runs after event sync`
    : "Waiting for first discovery";
  const active = subscription?.lastResult?.activeMarketIds?.length
    ?? discovery?.lastResult?.subscription?.active ?? 0;
  $("#subscriptionCount").textContent = Number(active).toLocaleString();
}

async function load() {
  const button = $("#refresh"); button.disabled = true;
  try {
    const [regular, fancy, discovery, subscription] = await Promise.all([
      request("/api/source/markets"), request("/api/source/fancies"),
      request("/api/source/markets/discovery"), request("/api/source/sync"),
    ]);
    state.regular = regular; state.fancy = fancy;
    $("#regularCount").textContent = regular.length.toLocaleString();
    $("#fancyCount").textContent = fancy.length.toLocaleString();
    $("#regularTabCount").textContent = regular.length.toLocaleString();
    $("#fancyTabCount").textContent = fancy.length.toLocaleString();
    renderHealth(discovery, subscription); render();
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; }
}

async function sync() {
  const button = $("#syncNow"); button.disabled = true; button.textContent = "Discovering…";
  try {
    const result = await request("/api/source/events/sync", { method: "POST" });
    const markets = result?.result?.marketDiscovery?.markets ?? result?.result?.markets ?? 0;
    toast(`Discovery complete · ${markets} regular markets`); await load();
  } catch (error) { toast(error.message); }
  finally { button.disabled = false; button.textContent = "Run discovery"; }
}

$("#adminKey").value = sessionStorage.getItem("se-admin-key") || "";
$("#refresh").addEventListener("click", load);
$("#syncNow").addEventListener("click", sync);
$("#search").addEventListener("input", render);
document.querySelectorAll("[data-kind]").forEach((button) => button.addEventListener("click", () => {
  document.querySelectorAll("[data-kind]").forEach((item) => { item.classList.remove("active"); item.setAttribute("aria-selected", "false"); });
  button.classList.add("active"); button.setAttribute("aria-selected", "true"); state.kind = button.dataset.kind; render();
}));
document.querySelectorAll("[data-sport]").forEach((button) => button.addEventListener("click", () => {
  document.querySelectorAll("[data-sport]").forEach((item) => item.classList.remove("active"));
  button.classList.add("active"); state.sport = button.dataset.sport; render();
}));
void load();
