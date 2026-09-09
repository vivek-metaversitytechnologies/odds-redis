const state={items:[],type:"all"};const $=s=>document.querySelector(s);function headers(){const k=$("#adminKey").value.trim();if(k)sessionStorage.setItem("se-admin-key",k);return k?{"X-Internal-API-Key":k}:{}}async function request(path,options={}){const r=await fetch(path,{...options,headers:{...headers(),...(options.headers||{})}}),b=await r.json().catch(()=>null);if(!r.ok)throw new Error(b?.message||`Request failed (${r.status})`);return {data:b?.data??b,meta:b?.meta||{}}}function toast(m){const n=$("#toast");n.textContent=m;n.classList.add("show");setTimeout(()=>n.classList.remove("show"),2800)}function date(v){return v?new Date(v).toLocaleString():"—"}function render(){const q=$("#search").value.trim().toLowerCase(),items=state.items.filter(x=>(state.type==="all"||x.resulttype===state.type)&&(!q||`${x.marketname||""} ${x.marketid||""} ${x.matchname||""} ${x.selectionname||""} ${x.result||""}`.toLowerCase().includes(q))),body=$("#rows");body.innerHTML="";if(!items.length)body.innerHTML='<tr><td colspan="6" class="empty-row">No declared results found.</td></tr>';items.forEach(x=>{const r=document.createElement("tr");r.innerHTML='<td><span class="sport-badge"></span></td><td class="primary-cell"><strong></strong></td><td><code></code></td><td></td><td></td><td></td>';r.querySelector(".sport-badge").textContent=x.resulttype;r.querySelector("strong").textContent=x.marketname||"—";r.querySelector("code").textContent=x.marketid;r.children[3].textContent=x.matchname||x.matchid||"—";r.children[4].textContent=x.selectionname||x.result||"—";r.children[5].textContent=date(x.declaredat);body.appendChild(r)});$("#summary").textContent=`${items.length} of ${state.items.length} results shown`}
function health(s){const a=$("#resultHealth");a.className="dashboard-alert";if(s.running){a.classList.add("warning");$("#healthTitle").textContent="Result reconciliation is running";$("#healthDetail").textContent="Checking provider results and settlement tables."}else if(s.lastError){a.classList.add("error");$("#healthTitle").textContent="Last result sync failed";$("#healthDetail").textContent=s.lastError}else if(s.lastCompletedAt){a.classList.add("healthy");$("#healthTitle").textContent="Result reconciliation is healthy";$("#healthDetail").textContent=`Last completed ${date(s.lastCompletedAt)} · ${s.lastResult?.settled||0} settled`}else{a.classList.add("warning");$("#healthTitle").textContent="Waiting for first result sync";$("#healthDetail").textContent="The worker runs every minute."}$("#syncStatus").textContent=s.lastCompletedAt?`Last checked ${date(s.lastCompletedAt)} · Every minute`:"Runs every minute"}
async function load(){const b=$("#refresh");b.disabled=true;try{const[results,sync]=await Promise.all([request("/api/source/results?limit=500"),request("/api/source/results/sync")]);state.items=results.data;$("#marketCount").textContent=results.meta.market??0;$("#fancyCount").textContent=results.meta.fancy??0;$("#exceptionalCount").textContent=results.meta.exceptional??0;health(sync.data);render()}catch(e){toast(e.message)}finally{b.disabled=false}}async function sync(){const b=$("#syncNow");b.disabled=true;b.textContent="Reconciling…";try{const x=await request("/api/source/results/sync",{method:"POST"});toast(`${x.data.result.settled} markets settled`);await load()}catch(e){toast(e.message)}finally{b.disabled=false;b.textContent="Run result sync"}}
$("#adminKey").value=sessionStorage.getItem("se-admin-key")||"";$("#refresh").addEventListener("click",load);$("#syncNow").addEventListener("click",sync);$("#search").addEventListener("input",render);document.querySelectorAll("[data-type]").forEach(b=>b.addEventListener("click",()=>{document.querySelectorAll("[data-type]").forEach(x=>x.classList.remove("active"));b.classList.add("active");state.type=b.dataset.type;render()}));void load();

const reviewPanel = document.createElement("section");
reviewPanel.className = "dashboard-card";
reviewPanel.innerHTML = `<div class="card-head"><div><h2>Pending result review</h2><p>Unresolved after 24 hours. Automatic retries have stopped.</p><span id="reviewCount"></span></div><button class="button ghost" id="reviewRefresh">Refresh review</button></div>
<div class="table-tools"><button class="button primary" id="reviewRun">Check selected with vendor (max 20)</button><span id="reviewMessage" role="status"></span></div>
<div class="dashboard-table-wrap bounded-table"><table class="dashboard-table"><thead><tr><th>Select</th><th>Market ID</th><th>First queued</th><th>Review since</th><th>Attempts</th><th>Reason</th></tr></thead><tbody id="reviewRows"></tbody></table></div><div class="table-footer"><button class="button ghost" id="reviewNext">Next page</button></div>`;
$(".dashboard-main").appendChild(reviewPanel);
let reviewCursor = "0";
let reviewBusy = false;
async function loadReview(cursor = "0") {
  try {
    const { data } = await request(`/api/source/results/review?cursor=${encodeURIComponent(cursor)}`);
    reviewCursor = String(data.cursor);
    $("#reviewCount").textContent = `${data.total} markets awaiting review`;
    $("#reviewNext").disabled = reviewCursor === "0";
    $("#reviewRows").replaceChildren();
    for (const item of data.entries) {
      const row = document.createElement("tr");
      const select = document.createElement("td");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox"; checkbox.value = item.marketId;
      checkbox.setAttribute("aria-label", `Select ${item.marketId}`);
      checkbox.addEventListener("change", () => {
        if ($("#reviewRows").querySelectorAll("input:checked").length > 20) {
          checkbox.checked = false; toast("Maximum 20 markets per manual request");
        }
      });
      select.appendChild(checkbox); row.appendChild(select);
      for (const value of [item.marketId, date(item.firstQueuedAt), date(item.reviewAt), item.attempts, item.reason]) {
        const cell = document.createElement("td"); cell.textContent = value; row.appendChild(cell);
      }
      $("#reviewRows").appendChild(row);
    }
    if (!data.entries.length) $("#reviewRows").innerHTML = '<tr><td colspan="6">No markets on this page.</td></tr>';
  } catch (error) { $("#reviewMessage").textContent = error.message; }
}
$("#reviewRefresh").addEventListener("click", () => loadReview());
$("#refresh").addEventListener("click", () => loadReview());
$("#reviewNext").addEventListener("click", () => loadReview(reviewCursor));
$("#reviewRun").addEventListener("click", async () => {
  if (reviewBusy) return;
  const marketIds = [...$("#reviewRows").querySelectorAll("input:checked")].map((input) => input.value);
  if (!marketIds.length) return toast("Select up to 20 markets to check");
  reviewBusy = true; $("#reviewRun").disabled = true;
  $("#reviewMessage").textContent = "Checking vendor and saving available results…";
  try {
    const { data } = await request("/api/source/results/review/reconcile", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ marketIds }),
    });
    $("#reviewMessage").textContent = `${data.settled.length} saved; ${data.remaining.length} remain for review. ${data.persistenceFailures} persistence failures.`;
    await loadReview(); await load();
  } catch (error) { $("#reviewMessage").textContent = error.message; }
  finally { reviewBusy = false; $("#reviewRun").disabled = false; }
});
void loadReview();
