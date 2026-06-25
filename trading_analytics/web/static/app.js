/* Trading Analytics dashboard — SSE driven, no build step. */
"use strict";

const PLOT_LAYOUT = {
  paper_bgcolor: "transparent",
  plot_bgcolor: "transparent",
  font: { color: "#cbd5e1", size: 11 },
  margin: { l: 48, r: 16, t: 34, b: 36 },
  legend: { orientation: "h", y: 1.12, x: 0 },
  xaxis: { gridcolor: "#1f2937", zeroline: false },
  yaxis: { gridcolor: "#1f2937", zeroline: false },
};
const CFG = { displayModeBar: false, responsive: true };
const fmt = (n, d = 0) =>
  n == null || isNaN(n) ? "—" :
  Number(n).toLocaleString("en-IN", { maximumFractionDigits: d, minimumFractionDigits: d });

let chainGrid = null;

/* ---------------- Summary cards ---------------- */
function toneClass(t) {
  return t === "bullish" || t === "Bullish" ? "bull"
    : t === "bearish" || t === "Bearish" ? "bear" : "neutral";
}
function badgeClass(t) {
  return t === "Bullish" ? "badge-bull" : t === "Bearish" ? "badge-bear" : "badge-neutral";
}

function renderCards(s) {
  if (!s || !Object.keys(s).length) return;
  const cards = [
    { label: "Spot Price", value: fmt(s.spot_price, 2) },
    { label: "PCR", value: fmt(s.pcr, 3) },
    { label: "Max Pain", value: fmt(s.max_pain, 0) },
    { label: "Dist from Max Pain", value: `${fmt(s.distance_from_max_pain, 1)} (${fmt(s.distance_pct, 2)}%)` },
    { label: "Open Interest", value: fmt(s.oi, 0) },
    { label: "Change OI", value: fmt(s.change_oi, 0) },
    { label: "Volume", value: fmt(s.volume, 0) },
    { label: "Last Update", value: s.time || "—" },
  ];
  const el = document.getElementById("cards");
  el.innerHTML = cards.map(c => `
    <div class="card p-3">
      <div class="text-xs text-gray-400">${c.label}</div>
      <div class="text-lg font-semibold mt-1">${c.value}</div>
    </div>`).join("") + `
    <div class="card p-3">
      <div class="text-xs text-gray-400">Market Bias</div>
      <div class="text-lg font-semibold mt-1"><span class="px-2 py-0.5 rounded-full text-sm ${badgeClass(s.market_bias)}">${s.market_bias || "—"}</span></div>
    </div>
    <div class="card p-3">
      <div class="text-xs text-gray-400">Confidence</div>
      <div class="text-lg font-semibold mt-1">${fmt(s.confidence, 0)}<span class="text-gray-500 text-sm">/100</span></div>
      <div class="h-1.5 mt-2 rounded bg-gray-700 overflow-hidden">
        <div class="h-full ${s.market_bias === 'Bearish' ? 'bg-red-500' : 'bg-green-500'}" style="width:${s.confidence || 0}%"></div>
      </div>
    </div>`;
}

/* ---------------- Understanding points ---------------- */
function renderUnderstanding(points) {
  const el = document.getElementById("understanding");
  if (!points || !points.length) { el.innerHTML = '<p class="text-gray-500 text-sm">Awaiting data…</p>'; return; }
  el.innerHTML = points.map(p => {
    const tc = toneClass(p.tone);
    const bar = p.tone === "bearish" ? "bg-red-500" : p.tone === "bullish" ? "bg-green-500" : "bg-gray-500";
    return `
      <div class="border border-gray-700 rounded-lg p-3">
        <div class="flex items-center justify-between">
          <span class="text-[11px] uppercase tracking-wide text-gray-500">${p.category}</span>
          <span class="text-xs ${tc} font-semibold">${fmt(p.conviction, 0)}</span>
        </div>
        <div class="font-semibold ${tc} mt-0.5">${p.headline}</div>
        <div class="text-xs text-gray-400 mt-1">${p.evidence}</div>
        <div class="text-sm text-gray-200 mt-1">${p.implication}</div>
        <div class="h-1 mt-2 rounded bg-gray-800 overflow-hidden"><div class="h-full ${bar}" style="width:${p.conviction}%"></div></div>
      </div>`;
  }).join("");
}

/* ---------------- Charts ---------------- */
function dualAxis(div, x, y1, y2, name1, name2, title, c2 = "#f59e0b") {
  Plotly.react(div, [
    { x, y: y1, name: name1, type: "scatter", mode: "lines", line: { color: "#3b82f6", width: 2 } },
    { x, y: y2, name: name2, type: "scatter", mode: "lines", yaxis: "y2", line: { color: c2, width: 2 } },
  ], {
    ...PLOT_LAYOUT, title: { text: title, font: { size: 13 } },
    yaxis2: { overlaying: "y", side: "right", gridcolor: "transparent", zeroline: false },
  }, CFG);
}

function renderCharts(master, analytics, chain) {
  if (!master || !master.length) return;
  const t = master.map(r => r.time);
  const spot = master.map(r => r.spot_price);

  dualAxis("chart_spot_oi", t, spot, master.map(r => r.oi), "Spot", "OI", "Spot vs OI");
  dualAxis("chart_spot_pcr", t, spot, master.map(r => r.pcr), "Spot", "PCR", "Spot vs PCR", "#a78bfa");
  dualAxis("chart_spot_maxpain", t, spot, master.map(r => r.max_pain), "Spot", "Max Pain", "Spot vs Max Pain", "#ef4444");
  dualAxis("chart_spot_changeoi", t, spot, master.map(r => r.change_oi), "Spot", "ΔOI", "Spot vs Change in OI", "#10b981");

  Plotly.react("chart_oi_growth", [
    { x: t, y: master.map(r => r.oi), type: "scatter", mode: "lines", fill: "tozeroy",
      line: { color: "#3b82f6" }, name: "OI" }],
    { ...PLOT_LAYOUT, title: { text: "OI Growth Curve", font: { size: 13 } } }, CFG);

  Plotly.react("chart_vol_growth", [
    { x: t, y: master.map(r => r.volume), type: "bar", marker: { color: "#6366f1" }, name: "Volume" }],
    { ...PLOT_LAYOUT, title: { text: "Volume Curve", font: { size: 13 } } }, CFG);

  Plotly.react("chart_pcr_trend", [
    { x: t, y: master.map(r => r.pcr), type: "scatter", mode: "lines+markers",
      line: { color: "#a78bfa", width: 2 }, name: "PCR" },
    { x: t, y: t.map(() => 1.3), type: "scatter", mode: "lines", line: { color: "#22c55e", dash: "dot", width: 1 }, name: "Bull ext" },
    { x: t, y: t.map(() => 0.7), type: "scatter", mode: "lines", line: { color: "#ef4444", dash: "dot", width: 1 }, name: "Bear ext" }],
    { ...PLOT_LAYOUT, title: { text: "PCR Trend", font: { size: 13 } } }, CFG);

  // OI heatmap (call/put OI by strike)
  if (chain && chain.length) {
    const strikes = chain.map(r => r.strike_price);
    Plotly.react("chart_oi_heatmap", [
      { z: [chain.map(r => r.call_oi), chain.map(r => r.put_oi)],
        x: strikes, y: ["Call OI", "Put OI"], type: "heatmap", colorscale: "Viridis" }],
      { ...PLOT_LAYOUT, title: { text: "OI Heatmap by Strike", font: { size: 13 } } }, CFG);
  }

  // Institutional timeline: net OI pressure proxy = ΔOI * sign(ΔPrice)
  const instY = master.map((r, i) => {
    if (i === 0) return 0;
    const dp = Math.sign(r.close - master[i - 1].close);
    return dp * (r.oi - master[i - 1].oi);
  });
  Plotly.react("chart_inst_timeline", [
    { x: t, y: instY, type: "bar",
      marker: { color: instY.map(v => v >= 0 ? "#22c55e" : "#ef4444") }, name: "Net OI pressure" }],
    { ...PLOT_LAYOUT, title: { text: "Institutional Activity Timeline", font: { size: 13 } } }, CFG);

  // Trend dashboard: price with EMAs + VWAP
  const tr = analytics && analytics.trend ? analytics.trend : {};
  const ema = (span) => {
    const k = 2 / (span + 1); let prev = master[0].close;
    return master.map((r, i) => (prev = i === 0 ? r.close : r.close * k + prev * (1 - k)));
  };
  Plotly.react("chart_trend_dash", [
    { x: t, y: spot, name: "Price", type: "scatter", mode: "lines", line: { color: "#e5e7eb", width: 2 } },
    { x: t, y: ema(9), name: "EMA9", type: "scatter", mode: "lines", line: { color: "#3b82f6", width: 1 } },
    { x: t, y: ema(20), name: "EMA20", type: "scatter", mode: "lines", line: { color: "#f59e0b", width: 1 } },
    { x: t, y: ema(50), name: "EMA50", type: "scatter", mode: "lines", line: { color: "#ef4444", width: 1 } }],
    { ...PLOT_LAYOUT, title: { text: `Trend Dashboard — ${tr.direction || ""}`, font: { size: 13 } } }, CFG);
}

/* ---------------- Option chain grid ---------------- */
function renderChain(chain) {
  if (!chain) return;
  const columnDefs = [
    { field: "strike_price", headerName: "Strike", valueFormatter: p => fmt(p.value, 0) },
    { field: "call_oi", headerName: "Call OI", valueFormatter: p => fmt(p.value, 0) },
    { field: "call_chg_oi", headerName: "Call ΔOI", valueFormatter: p => fmt(p.value, 0) },
    { field: "put_oi", headerName: "Put OI", valueFormatter: p => fmt(p.value, 0) },
    { field: "put_chg_oi", headerName: "Put ΔOI", valueFormatter: p => fmt(p.value, 0) },
  ];
  if (!chainGrid) {
    chainGrid = agGrid.createGrid(document.getElementById("chainGrid"), {
      columnDefs, rowData: chain,
      defaultColDef: { flex: 1, sortable: true, resizable: true },
    });
  } else {
    chainGrid.setGridOption("rowData", chain);
  }
}

/* ---------------- Render orchestration ---------------- */
function renderAll(d) {
  renderCards(d.summary);
  renderUnderstanding(d.understanding);
  renderCharts(d.master, d.analytics, d.chain);
  renderChain(d.chain);
  document.getElementById("clock").textContent = new Date().toLocaleTimeString();
  const cards = document.getElementById("cards");
  cards.classList.remove("pulse"); void cards.offsetWidth; cards.classList.add("pulse");
}

function setConn(state) {
  const el = document.getElementById("conn");
  const map = { live: ["LIVE", "badge-bull"], off: ["disconnected", "badge-bear"], conn: ["connecting…", "badge-neutral"] };
  const [txt, cls] = map[state];
  el.textContent = txt; el.className = `px-3 py-1 rounded-full ${cls}`;
}

/* ---------------- SSE wiring ---------------- */
function connect() {
  setConn("conn");
  const es = new EventSource("/stream");
  es.onopen = () => setConn("live");
  es.onmessage = (e) => { if (e.data) { try { renderAll(JSON.parse(e.data)); } catch (_) {} } };
  es.onerror = () => { setConn("off"); es.close(); setTimeout(connect, 3000); };
}

document.getElementById("refreshBtn").addEventListener("click", async () => {
  await fetch("/refresh", { method: "POST" });
});

connect();
