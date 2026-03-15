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

import asyncio
import json
import io
import math
import os
import ssl
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

try:
    import websockets
except ImportError:  # pragma: no cover - optional runtime dependency
    websockets = None


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
LIGHTER_STREAM_URL = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"


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
FAST_SNAPSHOT_REFRESH_INTERVAL_S = max(0.5, float(os.environ.get("FAST_SNAPSHOT_REFRESH_INTERVAL_S", "1")))
FULL_SNAPSHOT_REFRESH_INTERVAL_S = max(2.0, float(os.environ.get("FULL_SNAPSHOT_REFRESH_INTERVAL_S", "6")))
SNAPSHOT_ACTIVE_TTL_S = max(60.0, float(os.environ.get("SNAPSHOT_ACTIVE_TTL_S", "900")))
FAST_SNAPSHOT_DEFAULT_MAX_AGE_S = max(
    FAST_SNAPSHOT_REFRESH_INTERVAL_S * 2.5,
    float(os.environ.get("FAST_SNAPSHOT_DEFAULT_MAX_AGE_S", "5")),
)
FULL_SNAPSHOT_DEFAULT_MAX_AGE_S = max(
    FULL_SNAPSHOT_REFRESH_INTERVAL_S * 2.5,
    float(os.environ.get("FULL_SNAPSHOT_DEFAULT_MAX_AGE_S", "15")),
)
LIGHTER_WS_MARKET_REFRESH_S = max(30.0, float(os.environ.get("LIGHTER_WS_MARKET_REFRESH_S", "60")))
LIGHTER_WS_STALE_AFTER_S = max(1.0, float(os.environ.get("LIGHTER_WS_STALE_AFTER_S", "10")))
VAR_STATS_FEED_REFRESH_S = max(0.5, float(os.environ.get("VAR_STATS_FEED_REFRESH_S", "1")))
LIGHTER_FUNDING_FEED_REFRESH_S = max(0.5, float(os.environ.get("LIGHTER_FUNDING_FEED_REFRESH_S", "1")))
LIGHTER_ORDERBOOKS_FEED_REFRESH_S = max(10.0, float(os.environ.get("LIGHTER_ORDERBOOKS_FEED_REFRESH_S", "60")))

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

SNAPSHOTS: dict[str, dict[str, dict]] = {"funding": {}, "price": {}}
SNAPSHOT_LOCK = threading.Lock()
FEED_CACHE: dict[str, dict[str, object]] = {}
FEED_CACHE_LOCK = threading.Lock()


class LighterOrderBookState:
    def __init__(self) -> None:
        self._asks: dict[float, float] = {}
        self._bids: dict[float, float] = {}
        self.updated_at = 0.0

    def apply_levels(
        self, *, asks: list[dict] | None = None, bids: list[dict] | None = None, updated_at: float | None = None
    ) -> None:
        for book_side, levels in ((self._asks, asks or []), (self._bids, bids or [])):
            for it in levels:
                price = price_arb_cn.to_float((it or {}).get("price"))
                size = price_arb_cn.to_float((it or {}).get("size"))
                if price is None or price <= 0:
                    continue
                if size is None or size <= 0:
                    book_side.pop(price, None)
                    continue
                book_side[price] = size
        if updated_at is not None and updated_at > 0:
            self.updated_at = updated_at

    def snapshot(self, *, now: float | None = None) -> dict[str, float | None]:
        best_ask = min(self._asks) if self._asks else None
        best_bid = max(self._bids) if self._bids else None
        mid = None
        if best_bid is not None and best_ask is not None and best_ask >= best_bid:
            mid = 0.5 * (best_bid + best_ask)
        ts_now = time.time() if now is None else now
        age_ms = int(max(0.0, ts_now - self.updated_at) * 1000) if self.updated_at > 0 else None
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "updated_at": self.updated_at or None,
            "age_ms": age_ms,
        }


class LighterOrderBookStream:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._started = False
        self._states: dict[int, LighterOrderBookState] = {}
        self._symbol_to_market: dict[str, int] = {}
        self._market_to_symbol: dict[int, str] = {}
        self._subscribed_markets: set[int] = set()
        self._connected = False
        self._last_error: str | None = None
        self._last_message_at = 0.0
        self._last_market_refresh_at = 0.0

    def ensure_started(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._thread = threading.Thread(target=self._run, name="lighter-orderbook-ws", daemon=True)
            self._thread.start()

    def snapshot_quotes(self, symbols: list[str]) -> tuple[dict[str, dict[str, float | str | int | bool | None]], dict]:
        now = time.time()
        out: dict[str, dict[str, float | str | int | bool | None]] = {}
        max_age_ms: int | None = None
        with self._lock:
            for raw_sym in symbols:
                sym = str(raw_sym or "").upper()
                market_id = self._symbol_to_market.get(sym)
                if not market_id:
                    continue
                state = self._states.get(market_id)
                if not state:
                    continue
                snap = state.snapshot(now=now)
                if snap["mid"] is None:
                    continue
                age_ms = snap["age_ms"]
                if age_ms is not None and age_ms > int(LIGHTER_WS_STALE_AFTER_S * 1000):
                    continue
                if age_ms is not None:
                    max_age_ms = age_ms if max_age_ms is None else max(max_age_ms, age_ms)
                out[sym] = {
                    "best_bid": snap["best_bid"],
                    "best_ask": snap["best_ask"],
                    "mid": snap["mid"],
                    "age_ms": age_ms,
                    "source": "ws_orderbook",
                    "market_id": market_id,
                }
            meta = {
                "connected": self._connected,
                "subscribed": len(self._subscribed_markets),
                "covered": len(out),
                "age_ms": max_age_ms,
                "last_error": self._last_error,
                "last_message_age_ms": int(max(0.0, now - self._last_message_at) * 1000)
                if self._last_message_at > 0
                else None,
            }
        return out, meta

    def _run(self) -> None:
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        while True:
            try:
                await self._refresh_market_targets(force=True)
                await self._connect_and_consume()
            except Exception as exc:  # pragma: no cover - network runtime behavior
                with self._lock:
                    self._connected = False
                    self._last_error = str(exc)
                await asyncio.sleep(1.0)

    async def _connect_and_consume(self) -> None:
        if websockets is None:  # pragma: no cover - dependency guard
            raise RuntimeError("websockets package is unavailable")
        try:
            await self._connect_and_consume_with_ssl(ssl.create_default_context())
        except ssl.SSLCertVerificationError:
            insecure = ssl.create_default_context()
            insecure.check_hostname = False
            insecure.verify_mode = ssl.CERT_NONE
            print("warning: SSL verification failed for Lighter WS; retrying without certificate verification")
            await self._connect_and_consume_with_ssl(insecure)

    async def _connect_and_consume_with_ssl(self, ssl_context: ssl.SSLContext) -> None:
        async with websockets.connect(
            LIGHTER_STREAM_URL,
            ssl=ssl_context,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=2048,
        ) as ws:
            with self._lock:
                self._connected = False
                self._subscribed_markets.clear()
            await self._subscribe_all(ws)
            with self._lock:
                self._connected = True
                self._last_error = None
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                self._handle_ws_message(raw)
                await self._refresh_market_targets()
                await self._subscribe_all(ws)

    async def _refresh_market_targets(self, *, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            if not force and (now - self._last_market_refresh_at) < LIGHTER_WS_MARKET_REFRESH_S:
                return
        lighter_map = _get_lighter_market_map(timeout_s=10.0)
        var_symbols = _get_var_symbols(timeout_s=10.0)
        common_symbols = intersect_collector_symbols(var_symbols, lighter_map)
        symbol_to_market = {sym: int(lighter_map[sym]) for sym in common_symbols if int(lighter_map.get(sym) or 0) > 0}
        with self._lock:
            self._symbol_to_market = symbol_to_market
            self._market_to_symbol = {market_id: sym for sym, market_id in symbol_to_market.items()}
            self._last_market_refresh_at = now

    async def _subscribe_all(self, ws) -> None:
        with self._lock:
            targets = [
                (sym, market_id)
                for sym, market_id in self._symbol_to_market.items()
                if market_id not in self._subscribed_markets
            ]
        for _sym, market_id in targets:
            await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
            with self._lock:
                self._subscribed_markets.add(market_id)

    def _handle_ws_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        book = payload.get("order_book") or {}
        if not isinstance(book, dict):
            return
        channel = str(payload.get("channel") or "")
        if not channel.startswith("order_book:"):
            return
        try:
            market_id = int(channel.split(":", 1)[1])
        except Exception:
            return
        updated_at = price_arb_cn.to_float(payload.get("timestamp"))
        if updated_at:
            updated_at = updated_at / 1000.0
        else:
            updated_at = time.time()
        with self._lock:
            state = self._states.setdefault(market_id, LighterOrderBookState())
            state.apply_levels(
                asks=book.get("asks") if isinstance(book.get("asks"), list) else [],
                bids=book.get("bids") if isinstance(book.get("bids"), list) else [],
                updated_at=updated_at,
            )
            self._last_message_at = updated_at


LIGHTER_ORDERBOOK_STREAM = LighterOrderBookStream()


def _http_get_json(url: str, timeout_s: float) -> dict:
    return get_json(url, timeout_s=timeout_s, user_agent="arb-dashboard/1.0")


def _feed_cache_set(name: str, data: dict, *, updated_at: float | None = None, last_error: str | None = None) -> None:
    ts = time.time() if updated_at is None else float(updated_at)
    with FEED_CACHE_LOCK:
        FEED_CACHE[name] = {
            "data": data,
            "updated_at": ts,
            "last_error": last_error,
        }


def _feed_cache_get(name: str, *, max_age_s: float | None = None) -> tuple[dict | None, int | None]:
    with FEED_CACHE_LOCK:
        entry = FEED_CACHE.get(name) or {}
        data = entry.get("data")
        updated_at = float(entry.get("updated_at") or 0.0)
    if not isinstance(data, dict) or updated_at <= 0:
        return (None, None)
    age_ms = int(max(0.0, time.time() - updated_at) * 1000)
    if max_age_s is not None and (age_ms / 1000.0) > max_age_s:
        return (None, age_ms)
    return (data, age_ms)


def _feed_cache_get_or_fetch(
    name: str,
    *,
    max_age_s: float,
    fetcher,
) -> tuple[dict, int | None]:
    cached, age_ms = _feed_cache_get(name, max_age_s=max_age_s)
    if cached is not None:
        return (cached, age_ms)
    data = fetcher()
    _feed_cache_set(name, data)
    return (data, 0)


def refresh_fast_feed_cache_once(timeout_s: float = 10.0) -> None:
    _feed_cache_set("var_stats", _http_get_json(VARIATIONAL_STATS_URL, timeout_s=timeout_s))
    _feed_cache_set("lighter_funding", _http_get_json(arb_rank.LIGHTER_FUNDING_URL, timeout_s=timeout_s))
    _feed_cache_set("lighter_orderbooks", _http_get_json(LIGHTER_ORDERBOOKS_URL, timeout_s=timeout_s))


def _feed_refresh_loop(name: str, url: str, refresh_interval_s: float, timeout_s: float) -> None:
    while True:
        try:
            _feed_cache_set(name, _http_get_json(url, timeout_s=timeout_s))
        except Exception as exc:
            with FEED_CACHE_LOCK:
                entry = FEED_CACHE.setdefault(name, {})
                entry["last_error"] = str(exc)
        time.sleep(max(0.1, refresh_interval_s))


def default_snapshot_max_age(detail_level: str) -> float:
    return FAST_SNAPSHOT_DEFAULT_MAX_AGE_S if detail_level == "fast" else FULL_SNAPSHOT_DEFAULT_MAX_AGE_S


def default_funding_snapshot_params(detail_level: str = "fast") -> dict[str, object]:
    fast = detail_level == "fast"
    return {
        "detail_level": detail_level,
        "notional": 1000.0,
        "top": 30,
        "lighter_spread_bps": 5.0,
        "var_fee_bps": 0.0,
        "fetch_last": not fast,
        "fetch_limit": 0 if fast else 8,
        "funding_sign": "longs_pay",
        "timeout_s": 25.0,
        "refresh_interval_s": FAST_SNAPSHOT_REFRESH_INTERVAL_S if fast else FULL_SNAPSHOT_REFRESH_INTERVAL_S,
    }


def default_price_snapshot_params(detail_level: str = "fast") -> dict[str, object]:
    fast = detail_level == "fast"
    return {
        "detail_level": detail_level,
        "notional": 1000.0,
        "top": 30,
        "lighter_spread_bps": 5.0,
        "var_fee_bps": 0.0,
        "max_markets": 40,
        "concurrency": 16,
        "cache_seconds": 1.0 if fast else 30.0,
        "timeout_s": 25.0,
        "fast_mode": fast,
        "refresh_interval_s": FAST_SNAPSHOT_REFRESH_INTERVAL_S if fast else FULL_SNAPSHOT_REFRESH_INTERVAL_S,
    }


def funding_snapshot_key(params: dict[str, object]) -> str:
    return (
        "funding"
        f"|detail={params['detail_level']}"
        f"|n={params['notional']}"
        f"|top={params['top']}"
        f"|ls={params['lighter_spread_bps']}"
        f"|vf={params['var_fee_bps']}"
        f"|fl={int(bool(params['fetch_last']))}"
        f"|fll={params['fetch_limit']}"
        f"|fs={params['funding_sign']}"
    )


def price_snapshot_key(params: dict[str, object]) -> str:
    return (
        "price"
        f"|detail={params['detail_level']}"
        f"|n={params['notional']}"
        f"|top={params['top']}"
        f"|ls={params['lighter_spread_bps']}"
        f"|vf={params['var_fee_bps']}"
        f"|mm={params['max_markets']}"
        f"|ccy={params['concurrency']}"
        f"|ocs={params['cache_seconds']}"
    )


def seed_snapshot_targets() -> None:
    now = time.time()
    with SNAPSHOT_LOCK:
        for detail_level in ("fast", "full"):
            funding_params = default_funding_snapshot_params(detail_level)
            SNAPSHOTS.setdefault("funding", {}).setdefault(
                funding_snapshot_key(funding_params),
                {
                    "params": funding_params,
                    "data": None,
                    "updated_at": 0.0,
                    "last_access": now,
                    "last_error": None,
                    "refresh_interval_s": funding_params["refresh_interval_s"],
                },
            )
            price_params = default_price_snapshot_params(detail_level)
            SNAPSHOTS.setdefault("price", {}).setdefault(
                price_snapshot_key(price_params),
                {
                    "params": price_params,
                    "data": None,
                    "updated_at": 0.0,
                    "last_access": now,
                    "last_error": None,
                    "refresh_interval_s": price_params["refresh_interval_s"],
                },
            )


def _snapshot_payload(
    data: dict, source: str, updated_at: float, last_error: str | None, detail_level: str
) -> dict:
    payload = dict(data)
    age_ms = max(0, int((time.time() - updated_at) * 1000)) if updated_at > 0 else None
    payload["source"] = source
    payload["detail_level"] = detail_level
    payload["snapshot_asof_unix"] = int(updated_at) if updated_at > 0 else None
    payload["snapshot_age_ms"] = age_ms
    if last_error:
        payload["snapshot_last_error"] = last_error
    return payload


def resolve_snapshot_payload(
    kind: str,
    key: str,
    params: dict[str, object],
    *,
    force: bool,
    snapshot_max_age_s: float,
    live_fetcher,
) -> dict:
    now = time.time()
    stale_data: dict | None = None
    stale_updated_at = 0.0
    stale_error: str | None = None
    with SNAPSHOT_LOCK:
        bucket = SNAPSHOTS.setdefault(kind, {})
        entry = bucket.setdefault(
            key,
            {
                "params": dict(params),
                "data": None,
                "updated_at": 0.0,
                "last_access": now,
                "last_error": None,
            },
        )
        entry["params"] = dict(params)
        entry["last_access"] = now
        stale_data = entry.get("data")
        stale_updated_at = float(entry.get("updated_at") or 0.0)
        stale_error = entry.get("last_error")
        if (
            not force
            and stale_data is not None
            and stale_updated_at > 0
            and (now - stale_updated_at) <= max(0.0, float(snapshot_max_age_s))
        ):
            return _snapshot_payload(
                stale_data,
                "snapshot",
                stale_updated_at,
                stale_error,
                str(params.get("detail_level") or "fast"),
            )

    try:
        data = live_fetcher()
    except Exception as exc:
        with SNAPSHOT_LOCK:
            entry = SNAPSHOTS.setdefault(kind, {}).setdefault(
                key,
                {
                    "params": dict(params),
                    "data": stale_data,
                    "updated_at": stale_updated_at,
                    "last_access": now,
                    "last_error": None,
                },
            )
            entry["last_access"] = now
            entry["last_error"] = str(exc)
            stale_data = entry.get("data")
            stale_updated_at = float(entry.get("updated_at") or 0.0)
            stale_error = entry.get("last_error")
        if stale_data is not None and not force:
            payload = _snapshot_payload(
                stale_data,
                "snapshot",
                stale_updated_at,
                stale_error,
                str(params.get("detail_level") or "fast"),
            )
            payload["snapshot_stale"] = True
            return payload
        raise

    updated_at = time.time()
    with SNAPSHOT_LOCK:
        entry = SNAPSHOTS.setdefault(kind, {}).setdefault(
            key,
            {
                "params": dict(params),
                "data": None,
                "updated_at": 0.0,
                "last_access": updated_at,
                "last_error": None,
            },
        )
        entry["params"] = dict(params)
        entry["data"] = data
        entry["updated_at"] = updated_at
        entry["last_access"] = updated_at
        entry["last_error"] = None
        entry["refresh_interval_s"] = float(params.get("refresh_interval_s") or entry.get("refresh_interval_s") or 0)
    return _snapshot_payload(data, "live", updated_at, None, str(params.get("detail_level") or "fast"))


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


def _get_lighter_ws_quotes_for_symbols(symbols: list[str]) -> tuple[dict[str, dict], dict]:
    LIGHTER_ORDERBOOK_STREAM.ensure_started()
    return LIGHTER_ORDERBOOK_STREAM.snapshot_quotes(symbols)


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
    ws_quotes, _ws_meta = _get_lighter_ws_quotes_for_symbols([symbol])
    ws_quote = ws_quotes.get(symbol.upper())
    if ws_quote and ws_quote.get("mid") is not None:
        return (
            price_arb_cn.to_float(ws_quote.get("best_bid")),
            price_arb_cn.to_float(ws_quote.get("best_ask")),
            price_arb_cn.to_float(ws_quote.get("mid")),
        )
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


def build_funding_payload_fast(params: dict[str, object]) -> dict:
    t0 = time.time()
    pos_means_longs_pay = str(params["funding_sign"]) == "longs_pay"
    timeout_s = float(params["timeout_s"])
    var_j, var_age_ms = _feed_cache_get_or_fetch(
        "var_stats",
        max_age_s=3.0,
        fetcher=lambda: arb_rank.http_get_json(arb_rank.VARIATIONAL_STATS_URL, timeout_s=timeout_s),
    )
    lighter_funding_j, lighter_funding_age_ms = _feed_cache_get_or_fetch(
        "lighter_funding",
        max_age_s=3.0,
        fetcher=lambda: arb_rank.http_get_json(arb_rank.LIGHTER_FUNDING_URL, timeout_s=timeout_s),
    )
    lighter_meta_j, lighter_meta_age_ms = _feed_cache_get_or_fetch(
        "lighter_orderbooks",
        max_age_s=60.0,
        fetcher=lambda: arb_rank.http_get_json(arb_rank.LIGHTER_ORDERBOOKS_URL, timeout_s=timeout_s),
    )

    aliases = arb_rank.parse_symbol_aliases(str(params.get("symbol_aliases_json") or ""))
    var = arb_rank.parse_variational_markets(
        var_j,
        notional_bucket="size_1k",
        rate_is_percent=True,
        pos_means_longs_pay=pos_means_longs_pay,
        default_fee_bps=float(params["var_fee_bps"]),
    )
    lighter_rates = arb_rank.parse_lighter_funding(
        lighter_funding_j,
        rate_is_percent=False,
        interval_s=28800,
        pos_means_longs_pay=pos_means_longs_pay,
    )
    lighter_meta = arb_rank.parse_lighter_orderbooks_meta(lighter_meta_j)
    if aliases:
        var = {arb_rank.canonical_symbol(k, aliases): v for k, v in var.items()}
        lighter_rates = {arb_rank.canonical_symbol(k, aliases): v for k, v in lighter_rates.items()}
        lighter_meta = {arb_rank.canonical_symbol(k, aliases): v for k, v in lighter_meta.items()}
    symbol_whitelist = arb_rank.build_symbol_whitelist(var.keys(), lighter_rates.keys(), aliases=aliases)
    rows = arb_rank.build_rows(
        var=var,
        lighter_rates=lighter_rates,
        lighter_meta=lighter_meta,
        lighter_spread_bps_assumed=float(params["lighter_spread_bps"]),
        symbol_whitelist=symbol_whitelist,
    )

    ranked: list[tuple[float, arb_rank.MarketRow, str, float, float, float]] = []
    notional = float(params["notional"])
    for r in rows:
        strat, funding_1d, _funding_interval = r.best_funding_trade(notional)
        cost = r.round_trip_cost_usd(notional)
        net_1d = funding_1d - cost
        breakeven_days = (cost / funding_1d) if funding_1d > 0 else float("inf")
        ranked.append((net_1d, r, strat, funding_1d, cost, breakeven_days))
    ranked.sort(key=lambda x: x[0], reverse=True)

    payload = {
        "asof_unix": int(time.time()),
        "notional_usd": notional,
        "detail_level": "fast",
        "assumptions": {
            "funding_sign": str(params["funding_sign"]),
            "var_fee_bps_per_trade": float(params["var_fee_bps"]),
            "lighter_spread_bps_assumed_round_trip_excl_fees": float(params["lighter_spread_bps"]),
            "symbol_whitelist_size": len(symbol_whitelist),
        },
        "items": [],
        "fetch_ms": int((time.time() - t0) * 1000),
        "market_feed_age_ms": {
            "var_stats": var_age_ms,
            "lighter_funding": lighter_funding_age_ms,
            "lighter_orderbooks": lighter_meta_age_ms,
        },
    }
    ranked_show = ranked[: max(int(params["top"]), 0)] if int(params["top"]) else ranked
    for net_1d, r, strat, funding_1d, cost, breakeven_days in ranked_show:
        payload["items"].append(
            {
                "symbol": r.symbol,
                "strategy": strat,
                "funding_pnl_1d_usd": funding_1d,
                "round_trip_cost_usd": cost,
                "net_1d_usd": net_1d,
                "breakeven_days": None if not math.isfinite(breakeven_days) else breakeven_days,
                "basis_bps_indicative": None,
                "prices": {
                    "var_mid": r.var_mid,
                    "lighter_last_trade": None,
                    "lighter_best_bid": None,
                    "lighter_best_ask": None,
                    "lighter_price_source": "pending_detail",
                },
                "var": {
                    "rate_frac_per_interval": r.var_rate.rate_frac_per_interval,
                    "interval_s": r.var_rate.interval_s,
                    "round_trip_bps": r.var_round_trip_bps,
                },
                "lighter": {
                    "rate_frac_per_interval": r.lighter_rate.rate_frac_per_interval,
                    "interval_s": r.lighter_rate.interval_s,
                    "round_trip_bps": r.lighter_round_trip_bps,
                },
            }
        )
    return payload


def build_funding_payload(params: dict[str, object]) -> dict:
    if str(params.get("detail_level") or "fast") == "fast":
        return build_funding_payload_fast(params)
    argv = [
        "--json",
        "--notional",
        str(params["notional"]),
        "--top",
        str(params["top"]),
        "--lighter-spread-bps",
        str(params["lighter_spread_bps"]),
        "--var-fee-bps",
        str(params["var_fee_bps"]),
        "--funding-sign",
        str(params["funding_sign"]),
    ]
    if params["fetch_last"]:
        argv += ["--fetch-lighter-last", "--fetch-lighter-last-limit", str(params["fetch_limit"])]

    data = _run_module_json(arb_rank.main, argv, timeout_s=float(params["timeout_s"]))
    data["detail_level"] = str(params.get("detail_level") or "fast")
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
    return data


def build_price_payload_fast(params: dict[str, object]) -> dict:
    t0 = time.time()
    aliases = price_arb_cn.parse_symbol_aliases(str(params.get("symbol_aliases_json") or ""))
    timeout_s = float(params["timeout_s"])
    var_j, var_age_ms = _feed_cache_get_or_fetch(
        "var_stats",
        max_age_s=3.0,
        fetcher=lambda: price_arb_cn.http_get_json(price_arb_cn.VARIATIONAL_STATS_URL, timeout_s=timeout_s),
    )
    lighter_meta_j, lighter_meta_age_ms = _feed_cache_get_or_fetch(
        "lighter_orderbooks",
        max_age_s=60.0,
        fetcher=lambda: price_arb_cn.http_get_json(price_arb_cn.LIGHTER_ORDERBOOKS_URL, timeout_s=timeout_s),
    )

    var_map: dict[str, tuple[float | None, float | None, float | None, float | None]] = {}
    for it in (var_j.get("listings") or []):
        sym = price_arb_cn.canonical_symbol(it.get("ticker"), aliases)
        if not sym:
            continue
        quotes = it.get("quotes") or {}
        q = quotes.get("size_1k") or quotes.get("base") or {}
        bid = price_arb_cn.to_float(q.get("bid")) if isinstance(q, dict) else None
        ask = price_arb_cn.to_float(q.get("ask")) if isinstance(q, dict) else None
        if bid and ask and ask >= bid:
            mid = 0.5 * (bid + ask)
        else:
            mp = price_arb_cn.to_float(it.get("mark_price"))
            mid = mp if mp and mp > 0 else None
        spread = price_arb_cn.bps_from_bid_ask(bid, ask) if bid and ask else None
        var_map[sym] = (bid, ask, mid, spread)

    if int(lighter_meta_j.get("code") or 0) != 200:
        raise RuntimeError(f"Lighter orderBooks 返回 code={lighter_meta_j.get('code')}")

    lighter_markets: list[tuple[str, int, float]] = []
    for it in (lighter_meta_j.get("order_books") or []):
        if not price_arb_cn.is_valid_lighter_market_spec(it):
            continue
        sym = price_arb_cn.canonical_symbol(it.get("symbol"), aliases)
        mid = int(it.get("market_id") or 0)
        if not sym or mid <= 0:
            continue
        taker_fee_frac = price_arb_cn.to_float(it.get("taker_fee")) or 0.0
        lighter_markets.append((sym, mid, float(taker_fee_frac * 1e4)))

    symbol_whitelist = price_arb_cn.build_symbol_whitelist(list(var_map.keys()), [m[0] for m in lighter_markets], aliases)
    lighter_markets = [m for m in lighter_markets if m[0] in symbol_whitelist]
    lighter_markets = lighter_markets[: max(0, int(params["max_markets"]))]

    ws_quotes, ws_meta = _get_lighter_ws_quotes_for_symbols([sym for sym, _mid, _fee in lighter_markets])

    def fetch_one(sym: str, market_id: int) -> tuple[str, float | None, float | None, float | None, str]:
        durl = price_arb_cn.LIGHTER_ORDERBOOK_DETAILS_URL.format(market_id=market_id)
        details_j = price_arb_cn.http_get_json(durl, timeout_s=timeout_s)
        px = price_arb_cn.extract_last_trade_price(details_j)
        src = "last_trade_fast" if px is not None else "none"
        return (sym, px, None, None, src)

    ref_map: dict[str, tuple[float | None, float | None, float | None, str]] = {}
    missing_markets: list[tuple[str, int, float]] = []
    for sym, market_id, taker_fee_bps in lighter_markets:
        ws_quote = ws_quotes.get(sym)
        ws_mid = price_arb_cn.to_float((ws_quote or {}).get("mid"))
        if ws_mid is None:
            missing_markets.append((sym, market_id, taker_fee_bps))
            continue
        ref_map[sym] = (
            ws_mid,
            price_arb_cn.to_float(ws_quote.get("best_bid")),
            price_arb_cn.to_float(ws_quote.get("best_ask")),
            str(ws_quote.get("source") or "ws_orderbook"),
        )

    if missing_markets:
        with ThreadPoolExecutor(max_workers=max(1, int(params["concurrency"]))) as ex:
            futs = [ex.submit(fetch_one, sym, mid) for (sym, mid, _fee) in missing_markets]
            for fu in futs:
                sym, ref_px, ref_bid, ref_ask, ref_src = fu.result()
                ref_map[sym] = (ref_px, ref_bid, ref_ask, ref_src)

    rows: list[price_arb_cn.Row] = []
    for sym, _mid, taker_fee_bps in lighter_markets:
        bid, ask, var_mid, var_spread_bps = var_map.get(sym, (None, None, None, None))
        lighter_ref, lighter_bid, lighter_ask, lighter_src = ref_map.get(sym, (None, None, None, "none"))
        if var_mid is None or lighter_ref is None:
            continue
        if not price_arb_cn.is_reasonable_price_ratio(var_mid, lighter_ref, 0.2, 5.0):
            continue
        diff = price_arb_cn.bps_diff(var_mid, lighter_ref)
        if diff is None:
            continue
        direction = "卖VAR/买Lighter(参考)" if diff > 0 else "买VAR/卖Lighter(参考)"
        var_open_bps = float(var_spread_bps or 0.0) + float(params["var_fee_bps"])
        lighter_open_bps = float(params["lighter_spread_bps"]) + float(taker_fee_bps)
        open_cost_bps = var_open_bps + lighter_open_bps
        round_trip_bps = 2.0 * open_cost_bps
        rows.append(
            price_arb_cn.Row(
                标的=sym,
                方向=direction,
                VAR_bid=bid,
                VAR_ask=ask,
                VAR_mid=var_mid,
                Lighter_last=lighter_ref,
                Lighter_bid=lighter_bid,
                Lighter_ask=lighter_ask,
                Lighter_price_source=lighter_src,
                参考差价_bps=diff,
                VAR点差_bps=var_spread_bps,
                Lighter真实点差_bps=price_arb_cn.bps_from_bid_ask(lighter_bid, lighter_ask)
                if lighter_bid and lighter_ask
                else None,
                Lighter假设点差_bps=float(params["lighter_spread_bps"]),
                Lighter_taker_fee_bps=float(taker_fee_bps),
                开仓成本_bps=open_cost_bps,
                往返成本_bps=round_trip_bps,
                名义本金=float(params["notional"]),
            )
        )

    rows.sort(key=lambda r: r.参考净利润_u_往返() if r.参考净利润_u_往返() is not None else -1e18, reverse=True)
    payload = {
        "asof_unix": int(time.time()),
        "fetch_ms": int((time.time() - t0) * 1000),
        "notional_usd": float(params["notional"]),
        "detail_level": "fast",
        "lighter_ws_connected": bool(ws_meta.get("connected")),
        "lighter_ws_subscribed": int(ws_meta.get("subscribed") or 0),
        "lighter_ws_covered": int(len(ws_quotes)),
        "lighter_ws_age_ms": ws_meta.get("age_ms"),
        "lighter_ws_fallback_count": len(missing_markets),
        "market_feed_age_ms": {
            "var_stats": var_age_ms,
            "lighter_orderbooks": lighter_meta_age_ms,
        },
        "items": [],
    }
    show = rows[: max(0, int(params["top"]))]
    for r in show:
        payload["items"].append(
            {
                "symbol": r.标的,
                "direction_hint": r.方向,
                "diff_bps": r.参考差价_bps,
                "gross_u": r.理论毛利润_u(),
                "round_trip_bps": r.往返成本_bps,
                "net_u_round_trip": r.参考净利润_u_往返(),
                "var": {
                    "bid": r.VAR_bid,
                    "ask": r.VAR_ask,
                    "mid": r.VAR_mid,
                    "spread_bps": r.VAR点差_bps,
                    "fee_bps_per_trade": float(params["var_fee_bps"]),
                },
                "lighter": {
                    "last_trade": r.Lighter_last,
                    "best_bid": r.Lighter_bid,
                    "best_ask": r.Lighter_ask,
                    "price_source": r.Lighter_price_source,
                    "spread_bps_used": r.Lighter真实点差_bps,
                    "taker_fee_bps": r.Lighter_taker_fee_bps,
                    "assumed_spread_bps_per_trade": r.Lighter假设点差_bps,
                },
            }
        )
    return payload


def build_price_payload(params: dict[str, object]) -> dict:
    if str(params.get("detail_level") or "fast") == "fast":
        return build_price_payload_fast(params)
    argv = [
        "--json",
        "--名义本金",
        str(params["notional"]),
        "--显示前N",
        str(params["top"]),
        "--Lighter点差bps",
        str(params["lighter_spread_bps"]),
        "--VAR手续费bps",
        str(params["var_fee_bps"]),
        "--最多市场数",
        str(params["max_markets"]),
        "--并发",
        str(params["concurrency"]),
        "--缓存秒",
        str(params["cache_seconds"]),
    ]
    if params.get("fast_mode"):
        argv.append("--fast-mode")
    data = _run_module_json(price_arb_cn.main, argv, timeout_s=float(params["timeout_s"]))
    data["detail_level"] = str(params.get("detail_level") or "fast")
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
    return data


def _snapshot_work_items(kind: str, detail_level: str) -> list[tuple[str, dict[str, object]]]:
    now = time.time()
    with SNAPSHOT_LOCK:
        bucket = SNAPSHOTS.get(kind, {})
        return [
            (key, dict(entry.get("params") or {}))
            for key, entry in bucket.items()
            if str((entry.get("params") or {}).get("detail_level") or "fast") == detail_level
            if (now - float(entry.get("last_access") or 0.0)) <= SNAPSHOT_ACTIVE_TTL_S
            and (
                float(entry.get("updated_at") or 0.0) <= 0
                or (now - float(entry.get("updated_at") or 0.0))
                >= float(entry.get("refresh_interval_s") or FAST_SNAPSHOT_REFRESH_INTERVAL_S)
            )
        ]


def _refresh_snapshot(kind: str, key: str, params: dict[str, object]) -> None:
    fetcher = build_funding_payload if kind == "funding" else build_price_payload
    try:
        data = fetcher(params)
    except Exception as exc:
        with SNAPSHOT_LOCK:
            entry = SNAPSHOTS.setdefault(kind, {}).setdefault(
                key,
                {
                    "params": dict(params),
                    "data": None,
                    "updated_at": 0.0,
                    "last_access": time.time(),
                    "last_error": None,
                },
            )
            entry["last_error"] = str(exc)
            entry["refresh_interval_s"] = float(params.get("refresh_interval_s") or entry.get("refresh_interval_s") or 0)
        return
    updated_at = time.time()
    with SNAPSHOT_LOCK:
        entry = SNAPSHOTS.setdefault(kind, {}).setdefault(
            key,
            {
                "params": dict(params),
                "data": None,
                "updated_at": 0.0,
                "last_access": updated_at,
                "last_error": None,
            },
        )
        entry["params"] = dict(params)
        entry["data"] = data
        entry["updated_at"] = updated_at
        entry["last_error"] = None
        entry["refresh_interval_s"] = float(params.get("refresh_interval_s") or entry.get("refresh_interval_s") or 0)


def _snapshot_loop(kind: str, detail_level: str) -> None:
    while True:
        seed_snapshot_targets()
        for key, params in _snapshot_work_items(kind, detail_level):
            try:
                _refresh_snapshot(kind, key, params)
            except Exception:
                continue
        time.sleep(0.25)


def _warm_default_fast_snapshots() -> None:
    seed_snapshot_targets()
    items = [
        ("funding", funding_snapshot_key(default_funding_snapshot_params("fast")), default_funding_snapshot_params("fast")),
        ("price", price_snapshot_key(default_price_snapshot_params("fast")), default_price_snapshot_params("fast")),
    ]
    threads = [
        threading.Thread(target=_refresh_snapshot, args=(kind, key, params), daemon=True)
        for kind, key, params in items
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()


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
        force = _qget_bool(q, "force", False)
        timeout_s = _qget_float(q, "timeout_s", 45.0)

        try:
            if u.path == "/api/funding":
                top = _qget_int(q, "top", 30)
                detail_level = _qget(q, "detail_level", "fast").strip().lower()
                if detail_level not in ("fast", "full"):
                    self._send_json(400, {"error": f"invalid detail_level: {detail_level}"})
                    return
                params = {
                    "detail_level": detail_level,
                    "notional": _qget_float(q, "notional", 1000.0),
                    "top": top,
                    "lighter_spread_bps": _qget_float(q, "lighter_spread_bps", 5.0),
                    "var_fee_bps": _qget_float(q, "var_fee_bps", 0.0),
                    "fetch_last": _qget_bool(q, "fetch_lighter_last", detail_level == "full"),
                    "fetch_limit": _qget_int(q, "fetch_lighter_last_limit", 8 if detail_level == "full" else 0),
                    "funding_sign": _qget(q, "funding_sign", "longs_pay"),
                    "timeout_s": timeout_s,
                    "refresh_interval_s": FAST_SNAPSHOT_REFRESH_INTERVAL_S
                    if detail_level == "fast"
                    else FULL_SNAPSHOT_REFRESH_INTERVAL_S,
                }
                snapshot_max_age_s = _qget_float(q, "snapshot_max_age_s", default_snapshot_max_age(detail_level))
                key = funding_snapshot_key(params)
                data = resolve_snapshot_payload(
                    "funding",
                    key,
                    params,
                    force=force,
                    snapshot_max_age_s=snapshot_max_age_s,
                    live_fetcher=lambda: build_funding_payload(params),
                )
                self._send_json(200, data)
                return

            if u.path == "/api/price":
                detail_level = _qget(q, "detail_level", "fast").strip().lower()
                if detail_level not in ("fast", "full"):
                    self._send_json(400, {"error": f"invalid detail_level: {detail_level}"})
                    return
                params = {
                    "detail_level": detail_level,
                    "notional": _qget_float(q, "notional", 1000.0),
                    "top": _qget_int(q, "top", 30),
                    "lighter_spread_bps": _qget_float(q, "lighter_spread_bps", 5.0),
                    "var_fee_bps": _qget_float(q, "var_fee_bps", 0.0),
                    "max_markets": _qget_int(q, "max_markets", 120),
                    "concurrency": _qget_int(q, "concurrency", 16),
                    "cache_seconds": _qget_float(q, "orderbook_cache_s", 1.0 if detail_level == "fast" else 30.0),
                    "timeout_s": timeout_s,
                    "fast_mode": _qget_bool(q, "fast_mode", detail_level == "fast"),
                    "refresh_interval_s": FAST_SNAPSHOT_REFRESH_INTERVAL_S
                    if detail_level == "fast"
                    else FULL_SNAPSHOT_REFRESH_INTERVAL_S,
                }
                snapshot_max_age_s = _qget_float(q, "snapshot_max_age_s", default_snapshot_max_age(detail_level))
                key = price_snapshot_key(params)
                data = resolve_snapshot_payload(
                    "price",
                    key,
                    params,
                    force=force,
                    snapshot_max_age_s=snapshot_max_age_s,
                    live_fetcher=lambda: build_price_payload(params),
                )
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
    seed_snapshot_targets()
    LIGHTER_ORDERBOOK_STREAM.ensure_started()
    for name, url, refresh_s in (
        ("var_stats", VARIATIONAL_STATS_URL, VAR_STATS_FEED_REFRESH_S),
        ("lighter_funding", arb_rank.LIGHTER_FUNDING_URL, LIGHTER_FUNDING_FEED_REFRESH_S),
        ("lighter_orderbooks", LIGHTER_ORDERBOOKS_URL, LIGHTER_ORDERBOOKS_FEED_REFRESH_S),
    ):
        threading.Thread(
            target=_feed_refresh_loop,
            args=(name, url, refresh_s, 10.0),
            name=f"{name}-feed-refresh",
            daemon=True,
        ).start()
    _warm_default_fast_snapshots()
    for kind, detail_level in (
        ("funding", "fast"),
        ("funding", "full"),
        ("price", "fast"),
        ("price", "full"),
    ):
        threading.Thread(
            target=_snapshot_loop,
            args=(kind, detail_level),
            name=f"{kind}-{detail_level}-snapshot",
            daemon=True,
        ).start()
    th = threading.Thread(target=_sampler_loop, name="basis-sampler", daemon=True)
    th.start()
    collector = threading.Thread(target=_collector_loop, name="history-collector", daemon=True)
    collector.start()
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"Dashboard: http://{host}:{port}")
    print("API: /api/funding , /api/price")
    print(
        f"Snapshots: fast={FAST_SNAPSHOT_REFRESH_INTERVAL_S:.1f}s/{FAST_SNAPSHOT_DEFAULT_MAX_AGE_S:.1f}s "
        f"full={FULL_SNAPSHOT_REFRESH_INTERVAL_S:.1f}s/{FULL_SNAPSHOT_DEFAULT_MAX_AGE_S:.1f}s"
    )
    print(
        f"Collector: enabled interval={COLLECTOR_INTERVAL_S:.1f}s batch={COLLECTOR_BATCH_SIZE} workers={COLLECTOR_WORKERS}"
    )
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
