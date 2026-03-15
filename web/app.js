/* global window, document */

const $ = (id) => document.getElementById(id);
let histWatchSymbol = null;
let histWatchEnabled = false;
let historyReqSeq = 0;
let histSymbolsCache = { key: "", ts: 0, symbols: [] };
let tableReqSeq = 0;

function nowCN() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`;
}

function fmtNum(x, digits = 2) {
  if (x === null || x === undefined || Number.isNaN(x)) return "-";
  const n = Number(x);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(digits);
}

function fmtMaybe(x, digits = 2) {
  if (x === null || x === undefined) return "-";
  return fmtNum(x, digits);
}

function fmtAgeMs(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(Number(ms))) return "未知";
  const n = Number(ms);
  if (n < 1000) return `${Math.round(n)} ms`;
  return `${(n / 1000).toFixed(1)} s`;
}

function renderLatencyNote(prefix, count, payload) {
  const parts = [`展示 ${count} 条`];
  const source = String(payload?.source || "");
  if (source === "snapshot") {
    parts.push(`来源：快照${payload?.snapshot_stale ? "（过期兜底）" : ""}`);
  } else if (source === "live") {
    parts.push("来源：实时");
  }
  if (payload?.snapshot_age_ms !== undefined && payload?.snapshot_age_ms !== null) {
    parts.push(`快照年龄：${fmtAgeMs(payload.snapshot_age_ms)}`);
  }
  if (payload?.fetch_ms !== undefined && payload?.fetch_ms !== null) {
    parts.push(`最近计算：${payload.fetch_ms} ms`);
  }
  if (payload?.lighter_ws_subscribed !== undefined) {
    const covered = Number(payload?.lighter_ws_covered || 0);
    const subscribed = Number(payload?.lighter_ws_subscribed || 0);
    parts.push(`WS覆盖：${covered}/${subscribed}`);
    if (payload?.lighter_ws_age_ms !== undefined && payload?.lighter_ws_age_ms !== null) {
      parts.push(`WS年龄：${fmtAgeMs(payload.lighter_ws_age_ms)}`);
    }
    if (payload?.lighter_ws_fallback_count !== undefined && payload?.lighter_ws_fallback_count !== null) {
      parts.push(`REST回退：${payload.lighter_ws_fallback_count}`);
    }
  }
  prefix.textContent = `${parts.join("。")}。`;
}

function latencyBadgeText(label, payload) {
  if (!payload) return `${label}: 等待中`;
  const source = payload.source === "live" ? "实时" : "快照";
  const age = payload.snapshot_age_ms !== undefined ? fmtAgeMs(payload.snapshot_age_ms) : "未知";
  const parts = [`${label}: ${source} ${age}`];
  if (payload?.lighter_ws_subscribed !== undefined) {
    parts.push(`WS ${Number(payload?.lighter_ws_covered || 0)}/${Number(payload?.lighter_ws_subscribed || 0)}`);
    if (payload?.lighter_ws_age_ms !== undefined && payload?.lighter_ws_age_ms !== null) {
      parts.push(fmtAgeMs(payload.lighter_ws_age_ms));
    }
  }
  return parts.join(" | ");
}

function setLatencyBadges(prefix, fastPayload, detailPayload) {
  $(`${prefix}FastMeta`).textContent = latencyBadgeText("主", fastPayload);
  if (!detailPayload) {
    $(`${prefix}DetailMeta`).textContent = "明细: 补充中";
    return;
  }
  const stale = detailPayload.snapshot_stale ? " 兜底" : "";
  $(`${prefix}DetailMeta`).textContent = `${latencyBadgeText("明细", detailPayload)}${stale}`;
}

function mergeValue(base, detail) {
  if (Array.isArray(base) || Array.isArray(detail)) return detail !== undefined ? detail : base;
  if (detail === null || detail === undefined) return base;
  if (
    base &&
    detail &&
    typeof base === "object" &&
    typeof detail === "object" &&
    !Array.isArray(base) &&
    !Array.isArray(detail)
  ) {
    const out = { ...base };
    Object.keys(detail).forEach((key) => {
      out[key] = mergeValue(base[key], detail[key]);
    });
    return out;
  }
  return detail;
}

function mergeItemsBySymbol(fastPayload, detailPayload) {
  if (!fastPayload) return detailPayload;
  if (!detailPayload || !Array.isArray(detailPayload.items)) return fastPayload;
  const detailMap = new Map(detailPayload.items.map((it) => [String(it.symbol || ""), it]));
  const mergedItems = (fastPayload.items || []).map((it) => {
    const sym = String(it.symbol || "");
    const detail = detailMap.get(sym);
    return detail ? mergeValue(it, detail) : it;
  });
  return { ...fastPayload, items: mergedItems };
}

function badgeForValue(v) {
  // v: net profit or net/day
  if (v === null || v === undefined) return "warn";
  const n = Number(v);
  if (!Number.isFinite(n)) return "warn";
  if (n > 0) return "good";
  if (n < 0) return "bad";
  return "warn";
}

function mkBadge(label, cls) {
  const span = document.createElement("span");
  span.className = `badge ${cls}`;
  const dot = document.createElement("i");
  span.appendChild(dot);
  const text = document.createElement("span");
  text.textContent = label;
  span.appendChild(text);
  return span;
}

function clearTbody(tbody) {
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
}

async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`${r.status} ${r.statusText}${t ? `: ${t}` : ""}`);
  }
  return r.json();
}

function buildParams() {
  const notional = Number(($("notional").value || "1000").trim());
  const top = Number(($("top").value || "30").trim());
  const lighterSpread = Number(($("lighterSpread").value || "5").trim());

  return {
    notional: Number.isFinite(notional) ? notional : 1000,
    top: Number.isFinite(top) ? Math.max(1, Math.min(200, top)) : 30,
    lighter_spread_bps: Number.isFinite(lighterSpread) ? lighterSpread : 5,
  };
}

function renderFunding(j, detailPayload = null) {
  const tbody = $("fundingBody");
  clearTbody(tbody);

  const items = (j && j.items) || [];
  if (!Array.isArray(items) || items.length === 0) {
    const tr = document.createElement("tr");
    tr.className = "skeleton";
    const td = document.createElement("td");
    td.colSpan = 10;
    td.textContent = "没有数据（或接口返回为空）";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  items.forEach((it, idx) => {
    const tr = document.createElement("tr");

    const rank = document.createElement("td");
    rank.className = "num";
    rank.textContent = String(idx + 1);
    tr.appendChild(rank);

    const sym = document.createElement("td");
    sym.textContent = it.symbol ?? "-";
    tr.appendChild(sym);

    const strategy = document.createElement("td");
    const s = String(it.strategy || "");
    // Translate key phrases
    let zh = s
      .replace("Long VAR / Short Lighter", "做多VAR / 做空Lighter")
      .replace("Short VAR / Long Lighter", "做空VAR / 做多Lighter");
    if (!zh) zh = "-";
    strategy.appendChild(mkBadge(zh, "warn"));
    tr.appendChild(strategy);

    const net = document.createElement("td");
    net.className = "num";
    net.appendChild(mkBadge(fmtMaybe(it.net_1d_usd, 2), badgeForValue(it.net_1d_usd)));
    tr.appendChild(net);

    const fund = document.createElement("td");
    fund.className = "num";
    fund.textContent = fmtMaybe(it.funding_pnl_1d_usd, 2);
    tr.appendChild(fund);

    const cost = document.createElement("td");
    cost.className = "num";
    cost.textContent = fmtMaybe(it.round_trip_cost_usd, 2);
    tr.appendChild(cost);

    const be = document.createElement("td");
    be.className = "num";
    be.textContent = it.breakeven_days === null ? "-" : fmtMaybe(it.breakeven_days, 2);
    tr.appendChild(be);

    const vbp = document.createElement("td");
    vbp.className = "num";
    vbp.textContent = fmtMaybe(it.var?.round_trip_bps, 2);
    tr.appendChild(vbp);

    const lbp = document.createElement("td");
    lbp.className = "num";
    lbp.textContent = fmtMaybe(it.lighter?.round_trip_bps, 2);
    tr.appendChild(lbp);

    const basis = document.createElement("td");
    basis.className = "num";
    basis.textContent = fmtMaybe(it.basis_bps_indicative, 2);
    tr.appendChild(basis);

    tbody.appendChild(tr);
  });

  const note = $("fundingNote");
  setLatencyBadges("funding", j, detailPayload);
  renderLatencyNote(note, items.length, detailPayload || j);
}

function renderPrice(j, detailPayload = null) {
  const tbody = $("priceBody");
  clearTbody(tbody);

  const items = (j && j.items) || [];
  if (!Array.isArray(items) || items.length === 0) {
    const tr = document.createElement("tr");
    tr.className = "skeleton";
    const td = document.createElement("td");
    td.colSpan = 11;
    td.textContent = "没有数据（或接口返回为空）";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  items.forEach((it, idx) => {
    const tr = document.createElement("tr");

    const rank = document.createElement("td");
    rank.className = "num";
    rank.textContent = String(idx + 1);
    tr.appendChild(rank);

    const sym = document.createElement("td");
    sym.textContent = it.symbol ?? "-";
    tr.appendChild(sym);

    const dir = document.createElement("td");
    dir.appendChild(mkBadge(it.direction_hint || "-", "warn"));
    tr.appendChild(dir);

    const diff = document.createElement("td");
    diff.className = "num";
    diff.textContent = fmtMaybe(it.diff_bps, 2);
    tr.appendChild(diff);

    const gross = document.createElement("td");
    gross.className = "num";
    gross.textContent = fmtMaybe(it.gross_u, 4);
    tr.appendChild(gross);

    const rt = document.createElement("td");
    rt.className = "num";
    rt.textContent = fmtMaybe(it.round_trip_bps, 2);
    tr.appendChild(rt);

    const net = document.createElement("td");
    net.className = "num";
    net.appendChild(mkBadge(fmtMaybe(it.net_u_round_trip, 4), badgeForValue(it.net_u_round_trip)));
    tr.appendChild(net);

    const varS = document.createElement("td");
    varS.className = "num";
    varS.textContent = fmtMaybe(it.var?.spread_bps, 2);
    tr.appendChild(varS);

    const lt = document.createElement("td");
    lt.className = "num";
    lt.textContent = fmtMaybe(it.lighter?.taker_fee_bps, 2);
    tr.appendChild(lt);

    const vm = document.createElement("td");
    vm.className = "num";
    vm.textContent = fmtMaybe(it.var?.mid, 4);
    tr.appendChild(vm);

    const ll = document.createElement("td");
    ll.className = "num";
    ll.textContent = fmtMaybe(it.lighter?.last_trade, 4);
    tr.appendChild(ll);

    tbody.appendChild(tr);
  });

  const note = $("priceNote");
  setLatencyBadges("price", j, detailPayload);
  renderLatencyNote(note, items.length, detailPayload || j);
}

async function loadAll({ force = false } = {}) {
  const reqSeq = ++tableReqSeq;
  const p = buildParams();
  const fundingFetchLimit = Math.max(1, Math.min(p.top, 8));
  const priceMaxMarkets = Math.max(20, Math.min(40, p.top * 2));
  const updatedAt = $("updatedAt");
  updatedAt.textContent = "刷新中…";

  const fundingFastQs = new URLSearchParams({
    notional: String(p.notional),
    top: String(p.top),
    lighter_spread_bps: String(p.lighter_spread_bps),
    var_fee_bps: "0",
    detail_level: "fast",
    snapshot_max_age_s: "5",
    force: force ? "1" : "0",
  });

  const fundingFullQs = new URLSearchParams({
    notional: String(p.notional),
    top: String(p.top),
    lighter_spread_bps: String(p.lighter_spread_bps),
    var_fee_bps: "0",
    detail_level: "full",
    fetch_lighter_last: "1",
    fetch_lighter_last_limit: String(fundingFetchLimit),
    fetch_lighter_last_workers: "8",
    snapshot_max_age_s: "15",
    force: "0",
  });

  const priceFastQs = new URLSearchParams({
    notional: String(p.notional),
    top: String(p.top),
    lighter_spread_bps: String(p.lighter_spread_bps),
    var_fee_bps: "0",
    detail_level: "fast",
    max_markets: String(priceMaxMarkets),
    concurrency: "16",
    timeout_s: "25",
    orderbook_cache_s: "1",
    snapshot_max_age_s: "5",
    force: force ? "1" : "0",
  });

  const priceFullQs = new URLSearchParams({
    notional: String(p.notional),
    top: String(p.top),
    lighter_spread_bps: String(p.lighter_spread_bps),
    var_fee_bps: "0",
    detail_level: "full",
    max_markets: String(priceMaxMarkets),
    concurrency: "16",
    timeout_s: "25",
    orderbook_cache_s: "30",
    snapshot_max_age_s: "15",
    force: "0",
  });

  try {
    const [fundingFast, priceFast] = await Promise.all([
      fetchJSON(`/api/funding?${fundingFastQs.toString()}`),
      fetchJSON(`/api/price?${priceFastQs.toString()}`),
    ]);
    if (reqSeq !== tableReqSeq) return;
    renderFunding(fundingFast, null);
    renderPrice(priceFast, null);
    refreshHistSymbolOptions({ force: false });
    updatedAt.textContent = nowCN();

    Promise.allSettled([
      fetchJSON(`/api/funding?${fundingFullQs.toString()}`),
      fetchJSON(`/api/price?${priceFullQs.toString()}`),
    ]).then((results) => {
      if (reqSeq !== tableReqSeq) return;
      const fundingFull = results[0].status === "fulfilled" ? results[0].value : null;
      const priceFull = results[1].status === "fulfilled" ? results[1].value : null;
      renderFunding(mergeItemsBySymbol(fundingFast, fundingFull), fundingFull);
      renderPrice(mergeItemsBySymbol(priceFast, priceFull), priceFull);
    });
  } catch (e) {
    updatedAt.textContent = "失败";
    $("fundingFastMeta").textContent = "主: 失败";
    $("fundingDetailMeta").textContent = "明细: 失败";
    $("priceFastMeta").textContent = "主: 失败";
    $("priceDetailMeta").textContent = "明细: 失败";
    $("fundingNote").textContent = `加载失败：${String(e.message || e)}`;
    $("priceNote").textContent = `加载失败：${String(e.message || e)}`;
  }
}

function init() {
  $("refresh").addEventListener("click", () => loadAll({ force: true }));

  const onEnter = (ev) => {
    if (ev.key === "Enter") loadAll({ force: true });
  };
  $("notional").addEventListener("keydown", onEnter);
  $("top").addEventListener("keydown", onEnter);
  $("lighterSpread").addEventListener("keydown", onEnter);

  loadAll({ force: true });

  // 1秒自动刷新，优先盯住主排名变化。
  window.setInterval(() => {
    loadAll({ force: false });
  }, 1 * 1000);

  initHistory();
}

init();

// -------------------------------
// 历史价差图
// -------------------------------

function pickSymbolFromTables() {
  // Prefer price table symbols; fallback to funding.
  const priceSyms = Array.from(document.querySelectorAll("#priceBody tr td:nth-child(2)"))
    .map((td) => td.textContent)
    .filter((s) => s && s !== "-");
  const fundingSyms = Array.from(document.querySelectorAll("#fundingBody tr td:nth-child(2)"))
    .map((td) => td.textContent)
    .filter((s) => s && s !== "-");
  const set = new Set([...priceSyms, ...fundingSyms]);
  return Array.from(set).slice(0, 200);
}

async function fetchHistSymbols(source, { force = false } = {}) {
  const key = `${source || "all"}|q2`;
  const now = Date.now();
  if (!force && histSymbolsCache.key === key && now - histSymbolsCache.ts < 15000) {
    return histSymbolsCache.symbols;
  }
  const fetchOne = async (minQuotePoints) => {
    const qs = new URLSearchParams({
      source: source || "",
      range_s: String(30 * 24 * 3600),
      limit: "300",
      min_points: "2",
      min_quote_points: String(minQuotePoints),
    });
    const j = await fetchJSON(`/api/history_symbols?${qs.toString()}`);
    return ((j && j.symbols) || []).map((x) => String(x.symbol || "").trim()).filter(Boolean);
  };
  let syms = await fetchOne(2);
  if (!syms.length) syms = await fetchOne(0);
  histSymbolsCache = { key, ts: now, symbols: syms };
  return syms;
}

function getHistRangeS() {
  const active = document.querySelector(".chip.active");
  const v = active ? Number(active.dataset.range || "86400") : 86400;
  return Number.isFinite(v) ? v : 86400;
}

function getHistSource() {
  const el = document.querySelector("input[name='histSource']:checked");
  return el ? el.value : "basis";
}

async function histWatch(symbol, on) {
  const qs = new URLSearchParams({
    symbol,
    on: on ? "1" : "0",
    interval_s: "10",
    replace: on ? "1" : "0",
  });
  await fetchJSON(`/api/watch?${qs.toString()}`);
}

async function syncHistWatch(symbol, on) {
  if (!symbol) return;
  if (on) {
    if (histWatchEnabled && histWatchSymbol === symbol) return;
    await histWatch(symbol, true);
    histWatchEnabled = true;
    histWatchSymbol = symbol;
    return;
  }
  if (histWatchEnabled && histWatchSymbol) {
    await histWatch(histWatchSymbol, false);
  } else {
    await histWatch(symbol, false);
  }
  histWatchEnabled = false;
  histWatchSymbol = null;
}

function toPositive(x) {
  const n = Number(x);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function crossBps(a, b) {
  const x = toPositive(a);
  const y = toPositive(b);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  const mid = 0.5 * (x + y);
  if (!Number.isFinite(mid) || mid <= 0) return null;
  return ((x - y) / mid) * 1e4;
}

function buildDirectionalSeries(points, makerKey, takerKey, baselineBp) {
  const vals = points.map((p) => {
    const v = crossBps(p[makerKey], p[takerKey]);
    return Number.isFinite(v) ? v - baselineBp : null;
  });
  const validCount = vals.filter((v) => Number.isFinite(v)).length;
  return { vals, validCount };
}

function drawChart(canvas, points, baselineBp, opts = {}) {
  const showBlue = opts.showBlue !== false;
  const showGreen = opts.showGreen !== false;
  const ctx = canvas.getContext("2d");
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  // Theme
  const bg = "rgba(0,0,0,0.12)";
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, w, h);

  const padL = 70;
  const padR = 24;
  const padT = 28;
  const padB = 42;

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  // Extract series
  const xs = points.map((p) => p.ts);
  if (xs.length < 1) {
    ctx.fillStyle = "rgba(246,240,227,0.8)";
    ctx.font = "14px IBM Plex Mono";
    ctx.fillText("暂无足够历史数据（建议开启采样并等待几分钟）", padL, padT + 30);
    return { xScale: null, yScale: null, padL, padT, plotW, plotH, active: [] };
  }

  // 蓝线: OM Bid - LG Ask（买LG卖OM）
  const blueSeries = buildDirectionalSeries(points, "var_bid", "lighter_ask", baselineBp);
  // 绿线: LG Bid - OM Ask（买OM卖LG）
  const greenSeries = buildDirectionalSeries(points, "lighter_bid", "var_ask", baselineBp);

  const active = [];
  if (showBlue && blueSeries.validCount >= 1) {
    active.push({
      key: "blue",
      label: "买LG卖OM",
      formula: "OM Bid - LG Ask",
      color: "rgba(64,113,255,0.95)",
      vals: blueSeries.vals,
      validCount: blueSeries.validCount,
    });
  }
  if (showGreen && greenSeries.validCount >= 1) {
    active.push({
      key: "green",
      label: "买OM卖LG",
      formula: "LG Bid - OM Ask",
      color: "rgba(54,209,163,0.95)",
      vals: greenSeries.vals,
      validCount: greenSeries.validCount,
    });
  }
  if (!active.length) {
    ctx.fillStyle = "rgba(246,240,227,0.8)";
    ctx.font = "14px IBM Plex Mono";
    ctx.fillText("该区间缺少双边 bid/ask 历史，无法绘制双向bps（可刷新后等待采样）", padL, padT + 30);
    return { xScale: null, yScale: null, padL, padT, plotW, plotH, active: [] };
  }

  const xmin = Math.min(...xs);
  const xmax = Math.max(...xs);
  const spanS = Math.max(1, xmax - xmin);
  const allVals = active.flatMap((s) => s.vals).filter((v) => Number.isFinite(v));
  let ymin = Math.min(...allVals);
  let ymax = Math.max(...allVals);
  ymin = Math.min(ymin, 0);
  ymax = Math.max(ymax, 0);

  // Expand range a bit for aesthetics
  const span = Math.max(1e-6, ymax - ymin);
  ymin -= span * 0.08;
  ymax += span * 0.08;

  const xScale = (t) => padL + ((t - xmin) / (xmax - xmin || 1)) * plotW;
  const yScale = (v) => padT + (1 - (v - ymin) / (ymax - ymin || 1)) * plotH;

  // Grid
  ctx.strokeStyle = "rgba(246,240,227,0.08)";
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 6]);
  const gy = 6;
  for (let i = 0; i <= gy; i++) {
    const y = padT + (i / gy) * plotH;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
  }
  ctx.setLineDash([]);

  // Zero line (relative baseline)
  const y0 = yScale(0);
  ctx.strokeStyle = "rgba(246,240,227,0.18)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, y0);
  ctx.lineTo(padL + plotW, y0);
  ctx.stroke();

  // Lines
  const drawSeries = (vals, color) => {
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.2;
    ctx.beginPath();
    let started = false;
    let cnt = 0;
    let lastX = 0;
    let lastY = 0;
    for (let i = 0; i < xs.length; i++) {
      const v = vals[i];
      if (!Number.isFinite(v)) {
        started = false;
        continue;
      }
      const x = xScale(xs[i]);
      const y = yScale(v);
      lastX = x;
      lastY = y;
      cnt += 1;
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    }
    ctx.stroke();
    if (cnt === 1) {
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(lastX, lastY, 3.2, 0, Math.PI * 2);
      ctx.fill();
    }
  };
  active.forEach((s) => drawSeries(s.vals, s.color));

  // Axes labels
  ctx.fillStyle = "rgba(246,240,227,0.78)";
  ctx.font = "12px IBM Plex Mono";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= gy; i++) {
    const v = ymax - (i / gy) * (ymax - ymin);
    const y = padT + (i / gy) * plotH;
    ctx.fillText(`${v.toFixed(1)}bp`, padL - 10, y);
  }

  // Time labels
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  const gx = 6;

  const fmtAxisTs = (tsSec) => {
    const d = new Date(tsSec * 1000);
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const mo = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    if (spanS >= 7 * 24 * 3600) return `${mo}-${dd}`;
    if (spanS >= 24 * 3600) return `${mo}-${dd} ${hh}:${mm}`;
    return `${hh}:${mm}`;
  };

  for (let i = 0; i <= gx; i++) {
    const t = xmin + (i / gx) * (xmax - xmin);
    const x = xScale(t);
    const lab = fmtAxisTs(t);
    ctx.fillText(lab, x, padT + plotH + 14);
  }

  // Inline legend for active series
  ctx.textAlign = "left";
  ctx.textBaseline = "middle";
  ctx.font = "12px IBM Plex Mono";
  let lx = padL + 10;
  const ly = padT + 12;
  active.forEach((s) => {
    ctx.fillStyle = s.color;
    ctx.fillRect(lx, ly - 4, 10, 10);
    lx += 14;
    ctx.fillStyle = "rgba(246,240,227,0.85)";
    ctx.fillText(s.label, lx, ly + 1);
    lx += ctx.measureText(s.label).width + 16;
  });

  return {
    xScale,
    yScale,
    padL,
    padT,
    plotW,
    plotH,
    xmin,
    xmax,
    ymin,
    ymax,
    spanS,
    active,
    coverage: {
      blue: blueSeries.validCount,
      green: greenSeries.validCount,
      total: points.length,
    },
  };
}

async function loadHistory({ force = false } = {}) {
  const reqSeq = ++historyReqSeq;
  const symbol = $("histSymbol").value || "BTC";
  const rangeS = getHistRangeS();
  const source = getHistSource();
  const baseline = Number(($("histBaseline").value || "0").trim());
  const baselineBp = Number.isFinite(baseline) ? baseline : 0;
  const targetPoints = Math.min(5000, Math.max(1200, Math.floor(rangeS / 15)));
  const showBlue = $("histShowVar").checked;
  const showGreen = $("histShowLighter").checked;

  const watchOn = $("histWatch").checked;
  try {
    await syncHistWatch(symbol, watchOn);
    if (watchOn) {
      const sqs = new URLSearchParams({ symbol, source });
      await fetchJSON(`/api/sample_symbol?${sqs.toString()}`);
    }
  } catch (_) {
    // ignore
  }

  const qs = new URLSearchParams({
    symbol,
    source,
    range_s: String(rangeS),
    limit: String(targetPoints),
  });

  try {
    const j = await fetchJSON(`/api/basis_history?${qs.toString()}`);
    if (reqSeq !== historyReqSeq) return;
    const pts = (j && j.points) || [];
    const canvas = $("histCanvas");
    const meta = drawChart(canvas, pts, baselineBp, { showBlue, showGreen });

    const latestFinite = (vals) => {
      if (!Array.isArray(vals)) return null;
      for (let i = vals.length - 1; i >= 0; i--) {
        if (Number.isFinite(vals[i])) return vals[i];
      }
      return null;
    };
    const seriesMap = {};
    (meta.active || []).forEach((s) => {
      seriesMap[s.key] = s.vals;
    });
    const blueLast = latestFinite(seriesMap.blue);
    const greenLast = latestFinite(seriesMap.green);
    const statParts = [];
    if (Number.isFinite(blueLast)) statParts.push(`买LG卖OM ${blueLast.toFixed(2)}bp`);
    if (Number.isFinite(greenLast)) statParts.push(`买OM卖LG ${greenLast.toFixed(2)}bp`);
    $("histStat").textContent = statParts.length ? `${symbol} 最新 · ${statParts.join(" · ")}` : `${symbol} · 无数据`;
    const sampled = j.downsampled ? "（已下采样）" : "";
    const cov = meta.coverage || { blue: 0, green: 0, total: pts.length };
    const rawQ = Number(j.raw_quote_points || 0);
    $("histNote").textContent = `点数：${pts.length}${sampled} · 原始双边报价点：${rawQ} · 有效报价(蓝/绿)：${
      cov.blue || 0
    }/${pts.length} · ${cov.green || 0}/${pts.length} · 数据源：${source} · 时间范围：${Math.round(rangeS / 60)} 分钟`;

    // Mouse hover for tip
    if (meta && meta.xScale) {
      canvas.onmousemove = (ev) => {
        const rect = canvas.getBoundingClientRect();
        const x = ((ev.clientX - rect.left) / rect.width) * canvas.width;
        // find nearest point by x
        let bestIdx = -1;
        let bestDx = Infinity;
        for (let i = 0; i < pts.length; i++) {
          const hasValue = (meta.active || []).some((s) => Number.isFinite(s.vals[i]));
          if (!hasValue) continue;
          const px = meta.xScale(pts[i].ts);
          const dx = Math.abs(px - x);
          if (dx < bestDx) {
            bestDx = dx;
            bestIdx = i;
          }
        }
        if (bestIdx >= 0) {
          const best = pts[bestIdx];
          const d = new Date(best.ts * 1000);
          const ts = `${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(
            2,
            "0"
          )} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(
            2,
            "0"
          )}:${String(d.getSeconds()).padStart(2, "0")}`;
          const parts = [ts];
          (meta.active || []).forEach((s) => {
            const v = s.vals[bestIdx];
            if (Number.isFinite(v)) {
              const sign = v > 0 ? "+" : "";
              parts.push(`${s.label} ${sign}${v.toFixed(2)}bp`);
            }
          });
          $("histTip").textContent = parts.join(" · ");
        }
      };
    } else {
      canvas.onmousemove = null;
      $("histTip").textContent = "暂无可悬浮查看的数据";
    }
  } catch (e) {
    if (reqSeq !== historyReqSeq) return;
    $("histStat").textContent = `${symbol} · 加载失败`;
    $("histNote").textContent = `加载失败：${String(e.message || e)}`;
  }
}

async function refreshHistSymbolOptions({ force = false } = {}) {
  const sel = $("histSymbol");
  const source = getHistSource();
  let syms = [];
  try {
    syms = await fetchHistSymbols(source, { force });
  } catch (_) {
    // fallback to table-derived symbols
    syms = [];
  }
  if (!syms.length) syms = pickSymbolFromTables();
  if (!syms.length) return false;
  const sig = syms.join(",");
  const prevSig = sel.dataset.sig || "";
  const current = sel.value;
  if (sig === prevSig && current) return true;
  sel.dataset.sig = sig;
  sel.innerHTML = "";
  syms.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    sel.appendChild(opt);
  });
  if (current && syms.includes(current)) {
    sel.value = current;
  } else if (histWatchSymbol && syms.includes(histWatchSymbol)) {
    sel.value = histWatchSymbol;
  } else {
    sel.value = syms[0];
  }
  return true;
}

function initHistory() {
  const sel = $("histSymbol");
  // Keep symbol options in sync with the main tables.
  window.setInterval(() => {
    refreshHistSymbolOptions({ force: false });
  }, 2000);

  document.querySelectorAll(".chip").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".chip").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      loadHistory({ force: true });
    });
  });

  sel.addEventListener("change", () => loadHistory({ force: true }));
  $("histRefresh").addEventListener("click", () => loadHistory({ force: true }));
  $("histBaseline").addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") loadHistory({ force: true });
  });
  document.querySelectorAll("input[name='histSource']").forEach((el) => {
    el.addEventListener("change", async () => {
      await refreshHistSymbolOptions({ force: true });
      loadHistory({ force: true });
    });
  });
  $("histShowVar").addEventListener("change", () => loadHistory({ force: true }));
  $("histShowLighter").addEventListener("change", () => loadHistory({ force: true }));
  $("histWatch").addEventListener("change", () => loadHistory({ force: true }));

  // periodic refresh (chart) every 10s; sampling runs server-side if enabled.
  window.setInterval(() => {
    loadHistory({ force: false });
  }, 10 * 1000);

  // initial
  window.setTimeout(() => refreshHistSymbolOptions({ force: true }), 300);
  window.setTimeout(() => loadHistory({ force: true }), 1200);
}
