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
from http_json import get_json
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
          var_bid REAL,
          var_ask REAL,
          lighter_bid REAL,
          lighter_ask REAL,
          PRIMARY KEY (ts, symbol, source)
        )
        """
    )
    cur = conn.execute("PRAGMA table_info(basis_samples)")
    cols = {str(r[1]) for r in cur.fetchall()}
    for name, decl in (
        ("var_bid", "REAL"),
        ("var_ask", "REAL"),
        ("lighter_bid", "REAL"),
        ("lighter_ask", "REAL"),
    ):
        if name not in cols:
            conn.execute(f"ALTER TABLE basis_samples ADD COLUMN {name} {decl}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_basis_symbol_source_ts ON basis_samples(symbol, source, ts)"
    )
    return conn


DB = _db()
DB_LOCK = threading.Lock()


def _db_insert_many(
    rows: list[
        tuple[
            int,
            str,
            str,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
            float | None,
        ]
    ]
) -> None:
    if not rows:
        return
    with DB_LOCK:
        DB.executemany(
            (
                "INSERT OR IGNORE INTO basis_samples("
                "ts,symbol,source,var_mid,lighter_last,bps,var_bid,var_ask,lighter_bid,lighter_ask"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)"
            ),
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
COLLECTOR_INTERVAL_S = max(5.0, float(os.environ.get("COLLECTOR_INTERVAL_S", "10")))
COLLECTOR_BATCH_SIZE = max(1, int(os.environ.get("COLLECTOR_BATCH_SIZE", "12")))
COLLECTOR_SYMBOL_REFRESH_S = max(30.0, float(os.environ.get("COLLECTOR_SYMBOL_REFRESH_S", "300")))
COLLECTOR_WORKERS = max(1, int(os.environ.get("COLLECTOR_WORKERS", "8")))

HISTORY_RANGE_MIN_S = 60
HISTORY_RANGE_MAX_S = 7 * 24 * 3600
HISTORY_LIMIT_MIN = 200
HISTORY_LIMIT_MAX = 5000
HISTORY_SOURCES = {"basis", "price_diff", "funding_basis"}
HIST_SYMBOLS_RANGE_MIN_S = 3600
HIST_SYMBOLS_RANGE_MAX_S = 90 * 24 * 3600
HIST_SYMBOLS_LIMIT_MIN = 20
HIST_SYMBOLS_LIMIT_MAX = 500
HIST_SYMBOLS_MIN_POINTS_MIN = 1
HIST_SYMBOLS_MIN_POINTS_MAX = 1000


def _http_get_json(url: str, timeout_s: float) -> dict:
    return get_json(url, timeout_s=timeout_s, user_agent="arb-dashboard/1.0")


def normalize_history_query(range_s: int, limit: int) -> tuple[int, int]:
    r = int(range_s)
    l = int(limit)
    if r < HISTORY_RANGE_MIN_S:
        r = HISTORY_RANGE_MIN_S
    if r > HISTORY_RANGE_MAX_S:
        r = HISTORY_RANGE_MAX_S
    if l < HISTORY_LIMIT_MIN:
        l = HISTORY_LIMIT_MIN
    if l > HISTORY_LIMIT_MAX:
        l = HISTORY_LIMIT_MAX
    return (r, l)


def downsample_points(points: list[dict], max_points: int) -> list[dict]:
    n = len(points)
    if max_points <= 0 or n <= max_points:
        return points
    if max_points == 1:
        return [points[-1]]
    # Evenly sample across the whole window while preserving chronological order.
    idxs = [(i * (n - 1)) // (max_points - 1) for i in range(max_points)]
    return [points[i] for i in idxs]


def normalize_history_symbols_query(range_s: int, limit: int, min_points: int) -> tuple[int, int, int]:
    r = int(range_s)
    l = int(limit)
    m = int(min_points)
    if r < HIST_SYMBOLS_RANGE_MIN_S:
        r = HIST_SYMBOLS_RANGE_MIN_S
    if r > HIST_SYMBOLS_RANGE_MAX_S:
        r = HIST_SYMBOLS_RANGE_MAX_S
    if l < HIST_SYMBOLS_LIMIT_MIN:
        l = HIST_SYMBOLS_LIMIT_MIN
    if l > HIST_SYMBOLS_LIMIT_MAX:
        l = HIST_SYMBOLS_LIMIT_MAX
    if m < HIST_SYMBOLS_MIN_POINTS_MIN:
        m = HIST_SYMBOLS_MIN_POINTS_MIN
    if m > HIST_SYMBOLS_MIN_POINTS_MAX:
        m = HIST_SYMBOLS_MIN_POINTS_MAX
    return (r, l, m)


def normalize_min_quote_points(min_quote_points: int) -> int:
    m = int(min_quote_points)
    if m < 0:
        m = 0
    if m > HIST_SYMBOLS_MIN_POINTS_MAX:
        m = HIST_SYMBOLS_MIN_POINTS_MAX
    return m


def take_rotating_batch(symbols: list[str], cursor: int, batch_size: int) -> tuple[list[str], int]:
    if not symbols:
        return ([], 0)
    n = len(symbols)
    size = max(1, min(int(batch_size), n))
    start = int(cursor) % n
    out = [symbols[(start + i) % n] for i in range(size)]
    return (out, (start + size) % n)


def intersect_collector_symbols(var_symbols: list[str], lighter_map: dict[str, int]) -> list[str]:
    lighter_symbols = {str(sym).upper() for sym, mid in lighter_map.items() if int(mid or 0) > 0}
    common = {str(sym).upper() for sym in var_symbols if str(sym).upper() in lighter_symbols}
    return sorted(common)


def _bps(var_mid: float | None, lighter_last: float | None) -> float | None:
    if not var_mid or not lighter_last or var_mid <= 0 or lighter_last <= 0:
        return None
    mid = 0.5 * (var_mid + lighter_last)
    if mid <= 0:
        return None
    return (var_mid - lighter_last) / mid * 1e4


_VAR_CACHE: tuple[float, dict] | None = None
_LIGHTER_MAP_CACHE: tuple[float, dict] | None = None


def _get_var_quote(symbol: str, timeout_s: float = 10.0) -> tuple[float | None, float | None, float | None]:
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
            bid: float | None = None
            ask: float | None = None
            quotes = it.get("quotes") or {}
            q = quotes.get("size_1k") or quotes.get("base") or {}
            try:
                bid_v = float((q or {}).get("bid"))
                ask_v = float((q or {}).get("ask"))
                if bid_v > 0 and ask_v > 0 and ask_v >= bid_v:
                    bid = bid_v
                    ask = ask_v
            except Exception:
                bid = None
                ask = None
            if bid is not None and ask is not None:
                return (bid, ask, 0.5 * (bid + ask))
            try:
                v = float(mp)
            except Exception:
                v = None
            if v and v > 0:
                return (bid, ask, v)
            return (bid, ask, None)
    return (None, None, None)


def _get_var_symbols(timeout_s: float = 10.0) -> list[str]:
    global _VAR_CACHE
    now = time.time()
    if _VAR_CACHE and (now - _VAR_CACHE[0]) < 5.0:
        j = _VAR_CACHE[1]
    else:
        j = _http_get_json(VARIATIONAL_STATS_URL, timeout_s=timeout_s)
        _VAR_CACHE = (now, j)
    out: list[str] = []
    for it in j.get("listings", []) or []:
        sym = str(it.get("ticker") or "").upper()
        if sym:
            out.append(sym)
    return out


def _get_var_mid(symbol: str, timeout_s: float = 10.0) -> float | None:
    _bid, _ask, mid = _get_var_quote(symbol, timeout_s=timeout_s)
    return mid


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


def _get_lighter_quote(
    symbol: str, timeout_s: float = 10.0
) -> tuple[float | None, float | None, float | None]:
    m = _get_lighter_market_map(timeout_s=timeout_s)
    mid = m.get(symbol.upper())
    if not mid:
        return (None, None, None)
    orders_url = LIGHTER_ORDERBOOK_ORDERS_URL.format(market_id=mid, limit=50)
    oj = _http_get_json(orders_url, timeout_s=timeout_s)
    bid, ask = _extract_best_bid_ask(oj)
    if bid is not None and ask is not None and ask >= bid:
        return (bid, ask, 0.5 * (bid + ask))

    url = LIGHTER_ORDERBOOK_DETAILS_URL.format(market_id=mid)
    j = _http_get_json(url, timeout_s=timeout_s)
    if int(j.get("code") or 0) != 200:
        return (bid, ask, None)
    for k in ("order_book_details", "spot_order_book_details"):
        arr = j.get(k)
        if isinstance(arr, list) and arr:
            try:
                p = float(arr[0].get("last_trade_price"))
            except Exception:
                p = None
            if p and p > 0:
                return (bid, ask, p)
    return (bid, ask, None)


def _get_lighter_last(symbol: str, timeout_s: float = 10.0) -> float | None:
    _bid, _ask, last = _get_lighter_quote(symbol, timeout_s=timeout_s)
    return last


def _sampler_loop() -> None:
    # Background sampler: periodically records basis history for watched symbols.
    while True:
        time.sleep(0.5)
        with WATCH_LOCK:
            items = list(WATCH.items())
        if not items:
            continue
        now = time.time()
        rows: list[
            tuple[
                int,
                str,
                str,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
            ]
        ] = []
        for sym, cfg in items:
            interval_s = float(cfg.get("interval_s") or 30.0)
            next_ts = float(cfg.get("next_ts") or 0.0)
            if now < next_ts:
                continue
            try:
                var_bid, var_ask, var_mid = _get_var_quote(sym, timeout_s=10.0)
                lighter_bid, lighter_ask, lighter_last = _get_lighter_quote(sym, timeout_s=10.0)
                b = _bps(var_mid, lighter_last)
                ts = int(now)
                rows.append((ts, sym, "basis", var_mid, lighter_last, b, var_bid, var_ask, lighter_bid, lighter_ask))
                rows.append((ts, sym, "price_diff", var_mid, lighter_last, b, var_bid, var_ask, lighter_bid, lighter_ask))
            except Exception:
                # Ignore and retry next tick
                pass
            cfg["next_ts"] = now + interval_s
            with WATCH_LOCK:
                WATCH[sym] = cfg
        _db_insert_many(rows)


def _collector_rows_for_symbol(symbol: str, ts: int) -> list[tuple[int, str, str, float | None, float | None, float | None, float | None, float | None, float | None, float | None]]:
    var_bid, var_ask, var_mid = _get_var_quote(symbol, timeout_s=10.0)
    lighter_bid, lighter_ask, lighter_last = _get_lighter_quote(symbol, timeout_s=10.0)
    if any(x is None for x in (var_bid, var_ask, lighter_bid, lighter_ask)):
        return []
    basis_bps = _bps(var_mid, lighter_last)
    return [
        (ts, symbol, "basis", var_mid, lighter_last, basis_bps, var_bid, var_ask, lighter_bid, lighter_ask),
        (ts, symbol, "price_diff", var_mid, lighter_last, basis_bps, var_bid, var_ask, lighter_bid, lighter_ask),
    ]


def _collector_loop() -> None:
    # Long-running real quote collector for common VAR/Lighter symbols.
    symbols: list[str] = []
    cursor = 0
    last_refresh = 0.0
    while True:
        now = time.time()
        if not symbols or (now - last_refresh) >= COLLECTOR_SYMBOL_REFRESH_S:
            try:
                symbols = intersect_collector_symbols(_get_var_symbols(timeout_s=10.0), _get_lighter_market_map(timeout_s=10.0))
                last_refresh = now
                cursor = 0 if cursor >= len(symbols) else cursor
            except Exception:
                time.sleep(5.0)
                continue
        batch, cursor = take_rotating_batch(symbols, cursor, COLLECTOR_BATCH_SIZE)
        if not batch:
            time.sleep(5.0)
            continue
        ts = int(now)
        rows: list[tuple[int, str, str, float | None, float | None, float | None, float | None, float | None, float | None, float | None]] = []
        with ThreadPoolExecutor(max_workers=min(COLLECTOR_WORKERS, len(batch))) as ex:
            futs = [ex.submit(_collector_rows_for_symbol, sym, ts) for sym in batch]
            for fut in futs:
                try:
                    rows.extend(fut.result())
                except Exception:
                    continue
        _db_insert_many(rows)
        time.sleep(COLLECTOR_INTERVAL_S)


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

    def _history_row_to_point(
        self,
        r: tuple,
    ) -> dict:
        ts = int(r[0])
        bps = r[1]
        var_mid = r[2]
        lighter_last = r[3]
        var_bid = r[4]
        var_ask = r[5]
        lighter_bid = r[6]
        lighter_ask = r[7]
        has_dual = all(x is not None for x in (var_bid, var_ask, lighter_bid, lighter_ask))
        return {
            "ts": ts,
            "bps": bps,
            "var_mid": var_mid,
            "lighter_last": lighter_last,
            "var_bid": var_bid,
            "var_ask": var_ask,
            "lighter_bid": lighter_bid,
            "lighter_ask": lighter_ask,
            "has_dual_quotes": has_dual,
            "quote_mode": "raw" if has_dual else "none",
        }

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
                    var_obj = it.get("var") or {}
                    var_bid = var_obj.get("bid")
                    var_ask = var_obj.get("ask")
                    lighter_bid = prices.get("lighter_best_bid")
                    lighter_ask = prices.get("lighter_best_ask")
                    if sym:
                        rows.append(
                            (
                                ts,
                                sym,
                                "funding_basis",
                                var_mid,
                                ll,
                                b,
                                var_bid,
                                var_ask,
                                lighter_bid,
                                lighter_ask,
                            )
                        )
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
                    var_obj = it.get("var") or {}
                    lighter_obj = it.get("lighter") or {}
                    var_mid = var_obj.get("mid")
                    ll = lighter_obj.get("last_trade")
                    var_bid = var_obj.get("bid")
                    var_ask = var_obj.get("ask")
                    lighter_bid = lighter_obj.get("best_bid")
                    lighter_ask = lighter_obj.get("best_ask")
                    if sym:
                        rows.append(
                            (
                                ts,
                                sym,
                                "price_diff",
                                var_mid,
                                ll,
                                b,
                                var_bid,
                                var_ask,
                                lighter_bid,
                                lighter_ask,
                            )
                        )
                _db_insert_many(rows)
                CACHE.set(key, data)
                self._send_json(200, data)
                return

            if u.path == "/api/basis_history":
                symbol = _qget(q, "symbol", "BTC").upper()
                source = _qget(q, "source", "basis")
                range_s_raw = _qget_int(q, "range_s", 3600)
                limit_raw = _qget_int(q, "limit", 2000)
                range_s, limit = normalize_history_query(range_s_raw, limit_raw)
                if source not in HISTORY_SOURCES:
                    self._send_json(400, {"error": f"invalid source: {source}"})
                    return
                since = int(time.time()) - range_s
                # Pull latest window first, then downsample to keep chart lightweight.
                raw_limit = min(120000, max(limit * 20, 5000))
                with DB_LOCK:
                    cur = DB.execute(
                        """
                        SELECT ts, bps, var_mid, lighter_last, var_bid, var_ask, lighter_bid, lighter_ask
                        FROM basis_samples
                        WHERE symbol=? AND source=? AND ts>=?
                        ORDER BY ts DESC
                        LIMIT ?
                        """,
                        (symbol, source, since, raw_limit),
                    )
                    rows = cur.fetchall()
                rows.reverse()
                raw_quote_points = 0
                points_raw = [
                    self._history_row_to_point(r=r)
                    for r in rows
                ]
                for p in points_raw:
                    if p.get("quote_mode") == "raw":
                        raw_quote_points += 1
                points = downsample_points(points_raw, limit)
                self._send_json(
                    200,
                    {
                        "symbol": symbol,
                        "source": source,
                        "range_s": range_s,
                        "raw_points": len(points_raw),
                        "points_returned": len(points),
                        "downsampled": len(points_raw) > len(points),
                        "raw_quote_points": raw_quote_points,
                        "points": points,
                        "asof_unix": int(time.time()),
                    },
                )
                return

            if u.path == "/api/watch":
                symbol = _qget(q, "symbol", "").upper()
                on = _qget_bool(q, "on", True)
                interval_s = _qget_float(q, "interval_s", 10.0)
                replace = _qget_bool(q, "replace", False)
                if not symbol:
                    self._send_json(400, {"error": "symbol required"})
                    return
                with WATCH_LOCK:
                    if on:
                        if replace:
                            WATCH.clear()
                        WATCH[symbol] = {"interval_s": max(2.0, interval_s), "next_ts": 0.0}
                    else:
                        WATCH.pop(symbol, None)
                    active = sorted(WATCH.keys())
                self._send_json(200, {"ok": True, "watching": active, "count": len(active)})
                return

            if u.path == "/api/sample_symbol":
                symbol = _qget(q, "symbol", "").upper()
                source = _qget(q, "source", "price_diff")
                if not symbol:
                    self._send_json(400, {"error": "symbol required"})
                    return
                if source not in HISTORY_SOURCES:
                    self._send_json(400, {"error": f"invalid source: {source}"})
                    return
                var_bid, var_ask, var_mid = _get_var_quote(symbol, timeout_s=max(5.0, min(15.0, timeout_s)))
                lighter_bid, lighter_ask, lighter_last = _get_lighter_quote(
                    symbol, timeout_s=max(5.0, min(15.0, timeout_s))
                )
                b = _bps(var_mid, lighter_last)
                ts = int(time.time())
                _db_insert_many(
                    [
                        (
                            ts,
                            symbol,
                            source,
                            var_mid,
                            lighter_last,
                            b,
                            var_bid,
                            var_ask,
                            lighter_bid,
                            lighter_ask,
                        )
                    ]
                )
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "symbol": symbol,
                        "source": source,
                        "ts": ts,
                        "bps": b,
                        "var_mid": var_mid,
                        "lighter_last": lighter_last,
                        "var_bid": var_bid,
                        "var_ask": var_ask,
                        "lighter_bid": lighter_bid,
                        "lighter_ask": lighter_ask,
                        "has_dual_quotes": all(
                            x is not None for x in (var_bid, var_ask, lighter_bid, lighter_ask)
                        ),
                    },
                )
                return

            if u.path == "/api/history_symbols":
                source = _qget(q, "source", "")
                range_s_raw = _qget_int(q, "range_s", 30 * 24 * 3600)
                limit_raw = _qget_int(q, "limit", 300)
                min_points_raw = _qget_int(q, "min_points", 2)
                min_quote_points_raw = _qget_int(q, "min_quote_points", 0)
                range_s, limit, min_points = normalize_history_symbols_query(
                    range_s=range_s_raw, limit=limit_raw, min_points=min_points_raw
                )
                min_quote_points = normalize_min_quote_points(min_quote_points_raw)
                if source and source not in HISTORY_SOURCES:
                    self._send_json(400, {"error": f"invalid source: {source}"})
                    return
                since = int(time.time()) - range_s
                params: list[object] = [since]
                source_sql = ""
                if source:
                    source_sql = " AND source=? "
                    params.append(source)
                params.extend([min_points, min_quote_points, limit])
                with DB_LOCK:
                    cur = DB.execute(
                        f"""
                        SELECT
                          symbol,
                          COUNT(*) AS c,
                          SUM(
                            CASE
                              WHEN var_bid IS NOT NULL AND var_ask IS NOT NULL
                               AND lighter_bid IS NOT NULL AND lighter_ask IS NOT NULL
                              THEN 1 ELSE 0 END
                          ) AS q,
                          MAX(ts) AS last_ts
                        FROM basis_samples
                        WHERE ts>=? {source_sql}
                        GROUP BY symbol
                        HAVING COUNT(*)>=? AND q>=?
                        ORDER BY q DESC, c DESC, last_ts DESC, symbol ASC
                        LIMIT ?
                        """,
                        tuple(params),
                    )
                    rows = cur.fetchall()
                symbols = [
                    {"symbol": str(r[0]), "points": int(r[1]), "quote_points": int(r[2] or 0), "last_ts": int(r[3] or 0)}
                    for r in rows
                ]
                self._send_json(
                    200,
                    {
                        "source": source or "all",
                        "range_s": range_s,
                        "min_points": min_points,
                        "min_quote_points": min_quote_points,
                        "symbols": symbols,
                        "count": len(symbols),
                        "asof_unix": int(time.time()),
                    },
                )
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
    collector = threading.Thread(target=_collector_loop, name="history-collector", daemon=True)
    collector.start()
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}")
    print("API: /api/funding , /api/price")
    print(
        f"Collector: enabled interval={COLLECTOR_INTERVAL_S:.1f}s batch={COLLECTOR_BATCH_SIZE} workers={COLLECTOR_WORKERS}"
    )
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
