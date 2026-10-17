const state = { endpoints: {}, active: null, drafts: {}, history: [], lastBody: null };
const $ = (selector) => document.querySelector(selector);

function headers() {
  const key = $("#adminKey").value.trim();
  if (key) sessionStorage.setItem("se-admin-key", key);
  return key ? { "X-Internal-API-Key": key, "Content-Type": "application/json" } : { "Content-Type": "application/json" };
}
function toast(message) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.add("show");
  setTimeout(() => node.classList.remove("show"), 2800);
}
async function api(path, options = {}) {
  const response = await fetch(path, { ...options, headers: headers() });
  const payload = await response.json().catch(() => null);
  if (!response.ok) throw new Error(payload?.message || `Request failed (${response.status})`);
  return payload.data;
}
function bytes(count) {
  if (count < 1024) return `${count} B`;
  if (count < 1024 * 1024) return `${(count / 1024).toFixed(1)} KB`;
  return `${(count / 1024 / 1024).toFixed(2)} MB`;
}
function rowCount(body) {
  if (Array.isArray(body)) return body.length;
  if (Array.isArray(body?.data)) return body.data.length;
  if (Array.isArray(body?.data?.data)) return body.data.data.length;
  return null;
}

function current() {
  return state.endpoints[state.active];
}
function defaultPayload(endpoint) {
  return JSON.stringify(endpoint.method === "POST" ? endpoint.body || {} : endpoint.query || {}, null, 2);
}
// Each endpoint keeps its own draft so switching back and forth does not lose edits.
function saveDraft() {
  const key = state.active;
  if (!key) return;
  const params = {};
  document.querySelectorAll("#paramFields input").forEach((input) => {
    params[input.name] = input.value;
  });
  state.drafts[key] = { params, payload: $("#payload").value };
}
function selectEndpoint(key, draft) {
  saveDraft();
  state.active = key;
  $("#endpoint").value = key;
  const endpoint = current();
  const saved = draft || state.drafts[key] || {};
  const fields = $("#paramFields");
  fields.innerHTML = "";
  fields.hidden = !endpoint.params?.length;
  for (const name of endpoint.params || []) {
    const label = document.createElement("label");
    label.textContent = name;
    const input = document.createElement("input");
    input.name = name;
    input.required = true;
    input.autocomplete = "off";
    input.value = saved.params?.[name] || "";
    input.addEventListener("input", renderPreview);
    label.appendChild(input);
    fields.appendChild(label);
  }
  $("#payloadLabel").textContent = endpoint.method === "POST" ? "Request body (JSON)" : "Query parameters (JSON)";
  $("#payload").value = saved.payload ?? defaultPayload(endpoint);
  renderPreview();
}
function parsedPayload() {
  const text = $("#payload").value.trim();
  if (!text) return {};
  return JSON.parse(text);
}
function renderPreview() {
  const endpoint = current();
  let path = endpoint.path;
  document.querySelectorAll("#paramFields input").forEach((input) => {
    path = path.replace(`:${input.name}`, encodeURIComponent(input.value.trim()) || `:${input.name}`);
  });
  let suffix = "";
  if (endpoint.method === "GET") {
    try {
      const query = new URLSearchParams();
      for (const [key, value] of Object.entries(parsedPayload())) {
        if (value == null || value === "") continue;
        [].concat(value).forEach((item) => query.append(key, item));
      }
      suffix = query.size ? `?${query}` : "";
    } catch {
      suffix = " (invalid JSON)";
    }
  }
  $("#preview").textContent = `${endpoint.method} ${path}${suffix}`;
}

function renderResponse(result) {
  $("#responseCard").hidden = false;
  const status = $("#responseStatus");
  status.className = `status-badge${result.ok ? "" : " failed"}`;
  status.textContent = result.ok ? "OK" : result.status ? `HTTP ${result.status}` : "No response";
  $("#responseTitle").textContent = `${result.method} ${result.url}`;
  const body = result.ok || result.body != null ? result.body : { error: result.error };
  const text = typeof body === "string" ? body : JSON.stringify(body, null, 2);
  const rows = rowCount(body);
  $("#responseMeta").textContent = [
    `${result.durationMs} ms`,
    bytes(new Blob([text ?? ""]).size),
    rows == null ? null : `${rows.toLocaleString()} rows`,
  ]
    .filter(Boolean)
    .join(" · ");
  $("#responseBody").textContent = text ?? "null";
  state.lastBody = text;
}

function renderHistory() {
  const list = $("#history");
  list.innerHTML = "";
  $("#historyCard").hidden = !state.history.length;
  state.history.forEach((entry) => {
    const item = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.innerHTML = '<span class="status-badge"></span><code></code><span></span>';
    button.children[0].className = `status-badge${entry.ok ? "" : " failed"}`;
    button.children[0].textContent = entry.ok ? "OK" : entry.status || "ERR";
    button.children[1].textContent = entry.preview;
    button.children[2].textContent = entry.at.toLocaleTimeString();
    button.addEventListener("click", () => {
      selectEndpoint(entry.endpoint, entry.draft);
      renderResponse(entry.result);
    });
    item.appendChild(button);
    list.appendChild(item);
  });
}

async function send(event) {
  event?.preventDefault();
  const endpoint = current();
  let payload;
  try {
    payload = parsedPayload();
  } catch (error) {
    toast(`Invalid JSON: ${error.message}`);
    return;
  }
  if (endpoint.method === "GET" && (payload == null || typeof payload !== "object" || Array.isArray(payload))) {
    toast("Query parameters must be a JSON object");
    return;
  }
  saveDraft();
  const key = state.active;
  const draft = structuredClone(state.drafts[key]);
  const preview = $("#preview").textContent;
  $("#send").disabled = true;
  $("#send").textContent = "Sending…";
  try {
    const result = await api("/api/provider/explorer", {
      method: "POST",
      body: JSON.stringify({
        endpoint: key,
        params: draft.params,
        ...(endpoint.method === "POST" ? { body: payload } : { query: payload }),
      }),
    });
    renderResponse(result);
    state.history.unshift({ endpoint: key, draft, preview, result, ok: result.ok, status: result.status, at: new Date() });
    state.history.length = Math.min(state.history.length, 15);
    renderHistory();
    $("#responseCard").scrollIntoView({ behavior: "smooth", block: "nearest" });
  } catch (error) {
    toast(error.message);
  } finally {
    $("#send").disabled = false;
    $("#send").textContent = "Send request";
  }
}

async function init() {
  $("#adminKey").value = sessionStorage.getItem("se-admin-key") || "";
  try {
    state.endpoints = await api("/api/provider/explorer");
  } catch (error) {
    toast(error.message);
    return;
  }
  const select = $("#endpoint");
  for (const [key, endpoint] of Object.entries(state.endpoints)) {
    const option = document.createElement("option");
    option.value = key;
    option.textContent = `${endpoint.label} · ${endpoint.method} ${endpoint.path}`;
    select.appendChild(option);
  }
  select.addEventListener("change", () => selectEndpoint(select.value));
  selectEndpoint(select.value);
}

$("#requestForm").addEventListener("submit", send);
$("#payload").addEventListener("input", renderPreview);
$("#payload").addEventListener("keydown", (event) => {
  if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) void send(event);
});
$("#copy").addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(state.lastBody ?? "");
    toast("Response copied");
  } catch {
    toast("Clipboard is not available");
  }
});
void init();
