/* global window, document */

const $ = (id) => document.getElementById(id);

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

function renderFunding(j) {
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
  const n = items.length;
  const ms = j.fetch_ms ?? null;
  note.textContent = `展示 ${n} 条。抓取耗时：${ms !== null ? `${ms} ms` : "未知"}。`;
}

function renderPrice(j) {
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
  const n = items.length;
  const ms = j.fetch_ms ?? null;
  note.textContent = `展示 ${n} 条。抓取耗时：${ms !== null ? `${ms} ms` : "未知"}。`;
}

async function loadAll({ force = false } = {}) {
  const p = buildParams();
  const fundingFetchLimit = Math.max(1, Math.min(p.top, 8));
  const priceMaxMarkets = Math.max(20, Math.min(40, p.top * 2));
  const updatedAt = $("updatedAt");
  updatedAt.textContent = "刷新中…";

  const qs = new URLSearchParams({
    notional: String(p.notional),
    top: String(p.top),
    lighter_spread_bps: String(p.lighter_spread_bps),
    var_fee_bps: "0",
    fetch_lighter_last: "1",
    fetch_lighter_last_limit: String(fundingFetchLimit),
    fetch_lighter_last_workers: "8",
    cache_s: "300",
    force: force ? "1" : "0",
  });

  const qs2 = new URLSearchParams({
    notional: String(p.notional),
    top: String(p.top),
    lighter_spread_bps: String(p.lighter_spread_bps),
    var_fee_bps: "0",
    max_markets: String(priceMaxMarkets),
    concurrency: "16",
    timeout_s: "25",
    orderbook_cache_s: "30",
    cache_s: "300",
    force: force ? "1" : "0",
  });

  try {
    const funding = await fetchJSON(`/api/funding?${qs.toString()}`);
    const price = await fetchJSON(`/api/price?${qs2.toString()}`);
    renderFunding(funding);
    renderPrice(price);
    updatedAt.textContent = nowCN();
  } catch (e) {
    updatedAt.textContent = "失败";
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

  // 10秒自动刷新
  window.setInterval(() => {
    loadAll({ force: false });
  }, 10 * 1000);

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
  });
  await fetchJSON(`/api/watch?${qs.toString()}`);
}

function drawChart(canvas, points, baselineBp) {
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
  const ys = points.map((p) => (p.bps === null || p.bps === undefined ? null : Number(p.bps) - baselineBp));
  const valid = ys.filter((v) => Number.isFinite(v));
  if (valid.length < 2) {
    ctx.fillStyle = "rgba(246,240,227,0.8)";
    ctx.font = "14px IBM Plex Mono";
    ctx.fillText("暂无足够历史数据（建议开启采样并等待几分钟）", padL, padT + 30);
    return { xScale: null, yScale: null, padL, padT, plotW, plotH };
  }

  const xmin = Math.min(...xs);
  const xmax = Math.max(...xs);
  let ymin = Math.min(...valid);
  let ymax = Math.max(...valid);

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

  // Line
  ctx.strokeStyle = "rgba(0,209,193,0.95)";
  ctx.lineWidth = 2.2;
  ctx.beginPath();
  let started = false;
  for (let i = 0; i < xs.length; i++) {
    const v = ys[i];
    if (!Number.isFinite(v)) continue;
    const x = xScale(xs[i]);
    const y = yScale(v);
    if (!started) {
      ctx.moveTo(x, y);
      started = true;
    } else {
      ctx.lineTo(x, y);
    }
  }
  ctx.stroke();

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
  for (let i = 0; i <= gx; i++) {
    const t = xmin + (i / gx) * (xmax - xmin);
    const x = xScale(t);
    const d = new Date(t * 1000);
    const lab = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
    ctx.fillText(lab, x, padT + plotH + 14);
  }

  return { xScale, yScale, padL, padT, plotW, plotH, xmin, xmax, ymin, ymax };
}

async function loadHistory({ force = false } = {}) {
  const symbol = $("histSymbol").value || "BTC";
  const rangeS = getHistRangeS();
  const source = getHistSource();
  const baseline = Number(($("histBaseline").value || "0").trim());
  const baselineBp = Number.isFinite(baseline) ? baseline : 0;

  const watchOn = $("histWatch").checked;
  try {
    await histWatch(symbol, watchOn);
  } catch (_) {
    // ignore
  }

  const qs = new URLSearchParams({
    symbol,
    source,
    range_s: String(rangeS),
    limit: "2500",
  });

  try {
    const j = await fetchJSON(`/api/basis_history?${qs.toString()}`);
    const pts = (j && j.points) || [];
    const canvas = $("histCanvas");
    const meta = drawChart(canvas, pts, baselineBp);

    // Stats
    const vals = pts
      .map((p) => (p.bps === null || p.bps === undefined ? null : Number(p.bps) - baselineBp))
      .filter((v) => Number.isFinite(v));
    const last = vals.length ? vals[vals.length - 1] : null;
    const min = vals.length ? Math.min(...vals) : null;
    const max = vals.length ? Math.max(...vals) : null;
    $("histStat").textContent =
      last === null
        ? `${symbol} · 无数据`
        : `${symbol} · 最新 ${last.toFixed(2)}bp · 区间[${min.toFixed(2)}, ${max.toFixed(2)}]`;
    $("histNote").textContent = `点数：${pts.length} · 数据源：${source} · 时间范围：${Math.round(rangeS / 60)} 分钟`;

    // Mouse hover for tip
    if (meta && meta.xScale) {
      canvas.onmousemove = (ev) => {
        const rect = canvas.getBoundingClientRect();
        const x = ((ev.clientX - rect.left) / rect.width) * canvas.width;
        // find nearest point by x
        let best = null;
        let bestDx = Infinity;
        for (const p of pts) {
          if (p.bps === null || p.bps === undefined) continue;
          const px = meta.xScale(p.ts);
          const dx = Math.abs(px - x);
          if (dx < bestDx) {
            bestDx = dx;
            best = p;
          }
        }
        if (best) {
          const d = new Date(best.ts * 1000);
          const ts = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(
            2,
            "0"
          )}:${String(d.getSeconds()).padStart(2, "0")}`;
          const v = Number(best.bps) - baselineBp;
          $("histTip").textContent = `${ts} · ${v.toFixed(2)}bp`;
        }
      };
    }
  } catch (e) {
    $("histNote").textContent = `加载失败：${String(e.message || e)}`;
  }
}

function initHistory() {
  // Populate symbols after initial load; retry a few times.
  const sel = $("histSymbol");
  const fill = () => {
    const syms = pickSymbolFromTables();
    if (!syms.length) return false;
    const current = sel.value;
    sel.innerHTML = "";
    syms.forEach((s) => {
      const opt = document.createElement("option");
      opt.value = s;
      opt.textContent = s;
      sel.appendChild(opt);
    });
    if (current) sel.value = current;
    return true;
  };

  let tries = 0;
  const timer = window.setInterval(() => {
    tries += 1;
    if (fill() || tries > 10) window.clearInterval(timer);
  }, 600);

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
    el.addEventListener("change", () => loadHistory({ force: true }));
  });
  $("histWatch").addEventListener("change", () => loadHistory({ force: true }));

  // periodic refresh (chart) every 10s; sampling runs server-side if enabled.
  window.setInterval(() => {
    loadHistory({ force: false });
  }, 10 * 1000);

  // initial
  window.setTimeout(() => loadHistory({ force: true }), 1200);
}
