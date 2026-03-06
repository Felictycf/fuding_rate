#!/usr/bin/env python3
"""
Local web dashboard for:
  - Funding套利 (arb_rank.py)
  - 差价套利 (price_arb_cn.py)

No third-party dependencies.
Start:
  python3 /Users/felicity/crypto/fuding_rate/server.py
Then open:
  http://127.0.0.1:8088
"""

from __future__ import annotations

import json
import io
import os
import time
import threading
import sqlite3
from contextlib import redirect_stderr, redirect_stdout
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import arb_rank
import price_arb_cn


ROOT = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(ROOT, "web")
DB_PATH = os.path.join(ROOT, "history.db")

VARIATIONAL_STATS_URL = (
    "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
)
LIGHTER_ORDERBOOKS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
LIGHTER_ORDERBOOK_DETAILS_URL = (
    "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails?market_id={market_id}"
)
LIGHTER_ORDERBOOK_ORDERS_URL = (
    "https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id={market_id}&limit={limit}"
)


def _qget(q: dict, name: str, default: str) -> str:
    v = q.get(name)
    if not v:
        return default
    return v[0]


def _qget_int(q: dict, name: str, default: int) -> int:
    try:
        return int(_qget(q, name, str(default)))
    except ValueError:
        return default


def _qget_float(q: dict, name: str, default: float) -> float:
    try:
        return float(_qget(q, name, str(default)))
    except ValueError:
        return default


def _qget_bool(q: dict, name: str, default: bool) -> bool:
    raw = _qget(q, name, "1" if default else "0").strip().lower()
    if raw in ("1", "true", "yes", "y", "on"):
        return True
    if raw in ("0", "false", "no", "n", "off"):
        return False
    return default


def _run_module_json(main_fn, argv: list[str], timeout_s: float) -> dict:
    out = io.StringIO()
    err = io.StringIO()

    def _runner() -> int:
        with MODULE_RUN_LOCK:
            with redirect_stdout(out), redirect_stderr(err):
                rc = main_fn(argv)
                return int(rc or 0)

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_runner)
        try:
            wait_s = max(10.0, float(timeout_s)) + 35.0
            rc = fut.result(timeout=wait_s)
        except FuturesTimeoutError as e:
            raise RuntimeError(f"command timeout after {wait_s:.1f}s: {argv}") from e
    if rc != 0:
        raise RuntimeError(err.getvalue().strip() or f"command failed: {argv}")
    raw = out.getvalue()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON from module argv={argv}: {e}") from e


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS basis_samples (
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          source TEXT NOT NULL,
          var_mid REAL,
          lighter_last REAL,
          bps REAL,
          PRIMARY KEY (ts, symbol, source)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_basis_symbol_source_ts ON basis_samples(symbol, source, ts)"
    )
    return conn


DB = _db()
DB_LOCK = threading.Lock()


def _db_insert_many(rows: list[tuple[int, str, str, float | None, float | None, float | None]]) -> None:
    if not rows:
        return
    with DB_LOCK:
        DB.executemany(
            "INSERT OR IGNORE INTO basis_samples(ts,symbol,source,var_mid,lighter_last,bps) VALUES (?,?,?,?,?,?)",
            rows,
        )
        DB.commit()


class _Cache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, dict]] = {}

    def get(self, key: str, ttl_s: float) -> dict | None:
        it = self._store.get(key)
        if not it:
            return None
        ts, data = it
        if (time.time() - ts) <= ttl_s:
            return data
        return None

    def set(self, key: str, data: dict) -> None:
        self._store[key] = (time.time(), data)


CACHE = _Cache()
MODULE_RUN_LOCK = threading.Lock()

WATCH: dict[str, dict] = {}
WATCH_LOCK = threading.Lock()


def _http_get_json(url: str, timeout_s: float) -> dict:
    import urllib.request

    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "arb-dashboard/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _bps(var_mid: float | None, lighter_last: float | None) -> float | None:
    if not var_mid or not lighter_last or var_mid <= 0 or lighter_last <= 0:
        return None
    mid = 0.5 * (var_mid + lighter_last)
    if mid <= 0:
        return None
    return (var_mid - lighter_last) / mid * 1e4


_VAR_CACHE: tuple[float, dict] | None = None
_LIGHTER_MAP_CACHE: tuple[float, dict] | None = None


def _get_var_mid(symbol: str, timeout_s: float = 10.0) -> float | None:
    global _VAR_CACHE
    now = time.time()
    if _VAR_CACHE and (now - _VAR_CACHE[0]) < 5.0:
        j = _VAR_CACHE[1]
    else:
        j = _http_get_json(VARIATIONAL_STATS_URL, timeout_s=timeout_s)
        _VAR_CACHE = (now, j)
    sym = symbol.upper()
    for it in j.get("listings", []) or []:
        if (it.get("ticker") or "").upper() == sym:
            mp = it.get("mark_price")
            try:
                v = float(mp)
            except Exception:
                v = None
            if v and v > 0:
                return v
            # fallback: use quote mid if present
            q = (it.get("quotes") or {}).get("size_1k") or (it.get("quotes") or {}).get("base") or {}
            try:
                bid = float(q.get("bid"))
                ask = float(q.get("ask"))
                if bid > 0 and ask > 0 and ask >= bid:
                    return 0.5 * (bid + ask)
            except Exception:
                return None
    return None


def _get_lighter_market_map(timeout_s: float = 10.0) -> dict:
    global _LIGHTER_MAP_CACHE
    now = time.time()
    if _LIGHTER_MAP_CACHE and (now - _LIGHTER_MAP_CACHE[0]) < 60.0:
        return _LIGHTER_MAP_CACHE[1]
    j = _http_get_json(LIGHTER_ORDERBOOKS_URL, timeout_s=timeout_s)
    m: dict[str, int] = {}
    if int(j.get("code") or 0) == 200:
        for it in j.get("order_books", []) or []:
            if (it.get("market_type") or "").lower() != "perp":
                continue
            sym = (it.get("symbol") or "").upper()
            try:
                mid = int(it.get("market_id") or 0)
            except Exception:
                mid = 0
            if sym and mid > 0:
                m[sym] = mid
    _LIGHTER_MAP_CACHE = (now, m)
    return m


def _extract_best_bid_ask(orders_j: dict) -> tuple[float | None, float | None]:
    if int(orders_j.get("code") or 0) != 200:
        return (None, None)
    best_bid: float | None = None
    best_ask: float | None = None
    for it in (orders_j.get("bids") or []):
        try:
            p = float((it or {}).get("price"))
        except Exception:
            p = None
        if p and p > 0:
            best_bid = p if best_bid is None else max(best_bid, p)
    for it in (orders_j.get("asks") or []):
        try:
            p = float((it or {}).get("price"))
        except Exception:
            p = None
        if p and p > 0:
            best_ask = p if best_ask is None else min(best_ask, p)
    if best_bid is not None and best_ask is not None and best_ask < best_bid:
        return (None, None)
    return (best_bid, best_ask)


def _get_lighter_last(symbol: str, timeout_s: float = 10.0) -> float | None:
    m = _get_lighter_market_map(timeout_s=timeout_s)
    mid = m.get(symbol.upper())
    if not mid:
        return None
    orders_url = LIGHTER_ORDERBOOK_ORDERS_URL.format(market_id=mid, limit=50)
    oj = _http_get_json(orders_url, timeout_s=timeout_s)
    bid, ask = _extract_best_bid_ask(oj)
    if bid is not None and ask is not None and ask >= bid:
        return 0.5 * (bid + ask)

    url = LIGHTER_ORDERBOOK_DETAILS_URL.format(market_id=mid)
    j = _http_get_json(url, timeout_s=timeout_s)
    if int(j.get("code") or 0) != 200:
        return None
    for k in ("order_book_details", "spot_order_book_details"):
        arr = j.get(k)
        if isinstance(arr, list) and arr:
            try:
                p = float(arr[0].get("last_trade_price"))
            except Exception:
                p = None
            if p and p > 0:
                return p
    return None


def _sampler_loop() -> None:
    # Background sampler: periodically records basis history for watched symbols.
    while True:
        time.sleep(0.5)
        with WATCH_LOCK:
            items = list(WATCH.items())
        if not items:
            continue
        now = time.time()
        rows: list[tuple[int, str, str, float | None, float | None, float | None]] = []
        for sym, cfg in items:
            interval_s = float(cfg.get("interval_s") or 30.0)
            next_ts = float(cfg.get("next_ts") or 0.0)
            if now < next_ts:
                continue
            try:
                var_mid = _get_var_mid(sym, timeout_s=10.0)
                lighter_last = _get_lighter_last(sym, timeout_s=10.0)
                b = _bps(var_mid, lighter_last)
                ts = int(now)
                rows.append((ts, sym, "basis", var_mid, lighter_last, b))
            except Exception:
                # Ignore and retry next tick
                pass
            cfg["next_ts"] = now + interval_s
            with WATCH_LOCK:
                WATCH[sym] = cfg
        _db_insert_many(rows)


class Handler(BaseHTTPRequestHandler):
    server_version = "arb-dashboard/1.0"

    def _send_json(self, code: int, obj: dict) -> None:
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _send_bytes(self, code: int, raw: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args) -> None:
        # Keep server logs quiet; frontend shows errors directly.
        return

    def do_GET(self) -> None:
        u = urlparse(self.path)
        if u.path.startswith("/api/"):
            self._handle_api(u)
            return
        self._handle_static(u.path)

    def _handle_api(self, u) -> None:
        q = parse_qs(u.query)
        ttl_s = _qget_float(q, "cache_s", 300.0)
        force = _qget_bool(q, "force", False)
        timeout_s = _qget_float(q, "timeout_s", 45.0)

        try:
            if u.path == "/api/funding":
                notional = _qget_float(q, "notional", 1000.0)
                top = _qget_int(q, "top", 30)
                lighter_spread_bps = _qget_float(q, "lighter_spread_bps", 5.0)
                var_fee_bps = _qget_float(q, "var_fee_bps", 0.0)
                fetch_last = _qget_bool(q, "fetch_lighter_last", True)
                fetch_limit = _qget_int(q, "fetch_lighter_last_limit", top)
                funding_sign = _qget(q, "funding_sign", "longs_pay")
                key = (
                    f"funding|n={notional}|top={top}|ls={lighter_spread_bps}|vf={var_fee_bps}"
                    f"|fl={int(fetch_last)}|fll={fetch_limit}|fs={funding_sign}"
                )
                if not force:
                    cached = CACHE.get(key, ttl_s=ttl_s)
                    if cached is not None:
                        self._send_json(200, cached)
                        return

                argv = [
                    "--json",
                    "--notional",
                    str(notional),
                    "--top",
                    str(top),
                    "--lighter-spread-bps",
                    str(lighter_spread_bps),
                    "--var-fee-bps",
                    str(var_fee_bps),
                    "--funding-sign",
                    funding_sign,
                ]
                if fetch_last:
                    argv += ["--fetch-lighter-last", "--fetch-lighter-last-limit", str(fetch_limit)]

                data = _run_module_json(arb_rank.main, argv, timeout_s=timeout_s)
                # Persist basis history for these items (if available).
                ts = int(time.time())
                rows = []
                for it in data.get("items", []) or []:
                    sym = it.get("symbol")
                    b = it.get("basis_bps_indicative")
                    prices = it.get("prices") or {}
                    var_mid = prices.get("var_mid")
                    ll = prices.get("lighter_last_trade")
                    if sym:
                        rows.append((ts, sym, "funding_basis", var_mid, ll, b))
                _db_insert_many(rows)
                CACHE.set(key, data)
                self._send_json(200, data)
                return

            if u.path == "/api/price":
                notional = _qget_float(q, "notional", 1000.0)
                top = _qget_int(q, "top", 30)
                lighter_spread_bps = _qget_float(q, "lighter_spread_bps", 5.0)
                var_fee_bps = _qget_float(q, "var_fee_bps", 0.0)
                max_markets = _qget_int(q, "max_markets", 120)
                concurrency = _qget_int(q, "concurrency", 16)
                cache_seconds = _qget_float(q, "orderbook_cache_s", 30.0)

                key = (
                    f"price|n={notional}|top={top}|ls={lighter_spread_bps}|vf={var_fee_bps}"
                    f"|mm={max_markets}|ccy={concurrency}|ocs={cache_seconds}"
                )
                if not force:
                    cached = CACHE.get(key, ttl_s=ttl_s)
                    if cached is not None:
                        self._send_json(200, cached)
                        return

                argv = [
                    "--json",
                    "--名义本金",
                    str(notional),
                    "--显示前N",
                    str(top),
                    "--Lighter点差bps",
                    str(lighter_spread_bps),
                    "--VAR手续费bps",
                    str(var_fee_bps),
                    "--最多市场数",
                    str(max_markets),
                    "--并发",
                    str(concurrency),
                    "--缓存秒",
                    str(cache_seconds),
                ]
                data = _run_module_json(price_arb_cn.main, argv, timeout_s=timeout_s)
                # Persist diff history for these items.
                ts = int(time.time())
                rows = []
                for it in data.get("items", []) or []:
                    sym = it.get("symbol")
                    b = it.get("diff_bps")
                    var_mid = (it.get("var") or {}).get("mid")
                    ll = (it.get("lighter") or {}).get("last_trade")
                    if sym:
                        rows.append((ts, sym, "price_diff", var_mid, ll, b))
                _db_insert_many(rows)
                CACHE.set(key, data)
                self._send_json(200, data)
                return

            if u.path == "/api/basis_history":
                symbol = _qget(q, "symbol", "BTC").upper()
                source = _qget(q, "source", "basis")
                range_s = _qget_int(q, "range_s", 3600)
                limit = _qget_int(q, "limit", 2000)
                since = int(time.time()) - max(1, range_s)
                with DB_LOCK:
                    cur = DB.execute(
                        """
                        SELECT ts, bps, var_mid, lighter_last
                        FROM basis_samples
                        WHERE symbol=? AND source=? AND ts>=?
                        ORDER BY ts ASC
                        LIMIT ?
                        """,
                        (symbol, source, since, limit),
                    )
                    rows = cur.fetchall()
                points = [
                    {"ts": int(r[0]), "bps": r[1], "var_mid": r[2], "lighter_last": r[3]}
                    for r in rows
                ]
                self._send_json(
                    200,
                    {
                        "symbol": symbol,
                        "source": source,
                        "range_s": range_s,
                        "points": points,
                        "asof_unix": int(time.time()),
                    },
                )
                return

            if u.path == "/api/watch":
                symbol = _qget(q, "symbol", "").upper()
                on = _qget_bool(q, "on", True)
                interval_s = _qget_float(q, "interval_s", 10.0)
                if not symbol:
                    self._send_json(400, {"error": "symbol required"})
                    return
                with WATCH_LOCK:
                    if on:
                        WATCH[symbol] = {"interval_s": max(2.0, interval_s), "next_ts": 0.0}
                    else:
                        WATCH.pop(symbol, None)
                    active = sorted(WATCH.keys())
                self._send_json(200, {"ok": True, "watching": active})
                return

            if u.path == "/api/health":
                self._send_json(200, {"ok": True, "ts": int(time.time())})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown endpoint"})
        except Exception as e:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})

    def _handle_static(self, path: str) -> None:
        if path in ("", "/"):
            path = "/index.html"
        # Basic traversal protection
        safe = os.path.normpath(path).lstrip(os.sep)
        full = os.path.join(WEB_DIR, safe)
        if not full.startswith(WEB_DIR + os.sep):
            self._send_bytes(HTTPStatus.FORBIDDEN, b"forbidden", "text/plain; charset=utf-8")
            return

        if not os.path.exists(full) or not os.path.isfile(full):
            self._send_bytes(HTTPStatus.NOT_FOUND, b"not found", "text/plain; charset=utf-8")
            return

        ext = os.path.splitext(full)[1].lower()
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }.get(ext, "application/octet-stream")

        with open(full, "rb") as f:
            raw = f.read()
        self._send_bytes(HTTPStatus.OK, raw, ctype)


def main() -> int:
    host = "127.0.0.1"
    port = int(os.environ.get("PORT", "8099"))
    if not os.path.isdir(WEB_DIR):
        raise SystemExit(f"Missing web dir: {WEB_DIR}")
    th = threading.Thread(target=_sampler_loop, name="basis-sampler", daemon=True)
    th.start()
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}")
    print("API: /api/funding , /api/price")
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
