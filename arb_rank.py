#!/usr/bin/env python3
"""
Rank funding-rate (and indicative basis) arbitrage between:
  - Variational (omni-client-api ... /metadata/stats)
  - Lighter (elliot.ai ... /api/v1/*)

This script is intentionally dependency-free (stdlib only).

Important limitations / assumptions (configurable via CLI flags):
  - Variational funding_rate values appear to be in "percent" units (e.g. 0.1095 == 0.1095%).
  - Lighter funding `rate` values appear to be fractional units (e.g. 0.0001 == 0.01%).
  - For indicative basis, this script now prefers Lighter orderBookOrders best bid/ask mid.
    If that data is unavailable, it falls back to orderBookDetails.last_trade_price.
  - Cost model still uses a configurable spread/slippage assumption for ranking.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import math
import sys
import time
import urllib.request
from dataclasses import dataclass
from dataclasses import replace
from typing import Any, Dict, Iterable, List, Optional, Tuple


VARIATIONAL_STATS_URL = (
    "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats"
)
LIGHTER_FUNDING_URL = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"
LIGHTER_ORDERBOOKS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
LIGHTER_ORDERBOOK_DETAILS_URL = (
    "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails?market_id={market_id}"
)
LIGHTER_ORDERBOOK_ORDERS_URL = (
    "https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id={market_id}&limit={limit}"
)


def http_get_json(url: str, timeout_s: float = 20.0) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "arb-ranker/1.0 (+https://local)",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except ValueError:
            return None
    return None


def bps_from_bid_ask(bid: float, ask: float) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = 0.5 * (bid + ask)
    if mid <= 0:
        return None
    return (ask - bid) / mid * 1e4


def norm_symbol(sym: str) -> str:
    # Variational uses tickers like "1000FLOKI", Lighter perp uses "1000FLOKI".
    # Lighter spot symbols look like "LIT/USDC" which we ignore for funding arbs.
    return (sym or "").strip().upper()


def canonical_symbol(sym: str, aliases: Optional[Dict[str, str]] = None) -> str:
    s = norm_symbol(sym)
    if not s:
        return s
    if aliases and s in aliases:
        return norm_symbol(aliases[s])
    return s


def parse_symbol_aliases(raw: str) -> Dict[str, str]:
    if not raw.strip():
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in obj.items():
        ck = norm_symbol(str(k))
        cv = norm_symbol(str(v))
        if ck and cv:
            out[ck] = cv
    return out


def build_symbol_whitelist(
    var_symbols: Iterable[str], lighter_symbols: Iterable[str], aliases: Optional[Dict[str, str]] = None
) -> set[str]:
    var_set = {canonical_symbol(s, aliases) for s in var_symbols if canonical_symbol(s, aliases)}
    lighter_set = {canonical_symbol(s, aliases) for s in lighter_symbols if canonical_symbol(s, aliases)}
    return var_set & lighter_set


def is_valid_lighter_market_spec(meta: Dict[str, Any]) -> bool:
    if (meta.get("market_type") or "").lower() != "perp":
        return False
    if (meta.get("status") or "").lower() != "active":
        return False
    try:
        market_id = int(meta.get("market_id") or 0)
    except Exception:
        market_id = 0
    if market_id <= 0:
        return False
    min_base = to_float(meta.get("min_base_amount"))
    min_quote = to_float(meta.get("min_quote_amount"))
    if min_base is not None and min_base <= 0:
        return False
    if min_quote is not None and min_quote <= 0:
        return False
    return True


def is_reasonable_price_ratio(
    var_mid: Optional[float],
    lighter_last: Optional[float],
    min_ratio: float,
    max_ratio: float,
) -> bool:
    if var_mid is None or lighter_last is None:
        return False
    if var_mid <= 0 or lighter_last <= 0:
        return False
    if min_ratio <= 0 or max_ratio <= 0 or min_ratio > max_ratio:
        return True
    ratio = var_mid / lighter_last
    return min_ratio <= ratio <= max_ratio


def fmt(x: Optional[float], nd: int = 6) -> str:
    if x is None:
        return "-"
    if math.isfinite(x):
        # Compact small values
        if abs(x) >= 1000:
            return f"{x:,.0f}"
        if abs(x) >= 10:
            return f"{x:.2f}"
        if abs(x) >= 1:
            return f"{x:.4f}"
        return f"{x:.{nd}g}"
    return "-"


@dataclass(frozen=True)
class VenueRate:
    rate_frac_per_interval: float  # funding rate as fraction (e.g. 0.0001 == 0.01%) per interval
    interval_s: int
    pos_means_longs_pay: bool

    def pnl_for_side(self, notional_usd: float, side: str) -> float:
        """
        Funding PnL for a position over one funding interval.
        side: "long" or "short"
        Convention used here:
          - If pos_means_longs_pay: long pays rate, short receives rate.
          - Otherwise: long receives rate, short pays rate.
        """
        r = self.rate_frac_per_interval
        if side == "long":
            return (-r if self.pos_means_longs_pay else +r) * notional_usd
        if side == "short":
            return (+r if self.pos_means_longs_pay else -r) * notional_usd
        raise ValueError(f"unknown side: {side}")


@dataclass(frozen=True)
class MarketRow:
    symbol: str
    var_rate: VenueRate
    lighter_rate: VenueRate
    lighter_market_id: int
    # Execution-cost model (bps) for a round-trip on each venue.
    var_round_trip_bps: float
    lighter_round_trip_bps: float
    # Optional pricing signals (indicative only)
    var_mid: Optional[float]
    lighter_last: Optional[float]
    lighter_bid: Optional[float] = None
    lighter_ask: Optional[float] = None
    lighter_price_source: str = "none"

    def best_funding_trade(self, notional_usd: float) -> Tuple[str, float, float]:
        """
        Returns: (strategy_label, pnl_per_day_usd, pnl_per_interval_usd)
        strategy_label describes which venue is long/short.
        """
        # Strategy A: long Variational, short Lighter
        pnl_a = self.var_rate.pnl_for_side(notional_usd, "long") + self.lighter_rate.pnl_for_side(
            notional_usd, "short"
        )
        # Strategy B: short Variational, long Lighter
        pnl_b = self.var_rate.pnl_for_side(notional_usd, "short") + self.lighter_rate.pnl_for_side(
            notional_usd, "long"
        )
        # Normalize to per-day using each leg interval; approximate by converting to per-second.
        # This matters if venues have different funding intervals.
        pnl_a_per_s = (
            self.var_rate.pnl_for_side(notional_usd, "long") / self.var_rate.interval_s
            + self.lighter_rate.pnl_for_side(notional_usd, "short") / self.lighter_rate.interval_s
        )
        pnl_b_per_s = (
            self.var_rate.pnl_for_side(notional_usd, "short") / self.var_rate.interval_s
            + self.lighter_rate.pnl_for_side(notional_usd, "long") / self.lighter_rate.interval_s
        )
        day = 86400.0
        pnl_a_day = pnl_a_per_s * day
        pnl_b_day = pnl_b_per_s * day

        if pnl_b_day > pnl_a_day:
            return ("Short VAR / Long Lighter", pnl_b_day, pnl_b)
        return ("Long VAR / Short Lighter", pnl_a_day, pnl_a)

    def round_trip_cost_usd(self, notional_usd: float) -> float:
        total_bps = self.var_round_trip_bps + self.lighter_round_trip_bps
        return notional_usd * total_bps / 1e4

    def indicative_basis_bps(self) -> Optional[float]:
        if self.var_mid is None or self.lighter_last is None:
            return None
        if self.var_mid <= 0 or self.lighter_last <= 0:
            return None
        mid = 0.5 * (self.var_mid + self.lighter_last)
        return (self.var_mid - self.lighter_last) / mid * 1e4


def parse_variational_markets(
    j: Dict[str, Any],
    notional_bucket: str,
    rate_is_percent: bool,
    pos_means_longs_pay: bool,
    default_fee_bps: float,
) -> Dict[str, Tuple[VenueRate, float, Optional[float]]]:
    """
    Returns map: symbol -> (VenueRate, round_trip_bps, mid_price)
    round_trip_bps includes estimated spread + fees for a full open+close.
    """
    out: Dict[str, Tuple[VenueRate, float, Optional[float]]] = {}
    listings = j.get("listings") or []
    for it in listings:
        sym = norm_symbol(it.get("ticker"))
        if not sym:
            continue
        fr_raw = to_float(it.get("funding_rate"))
        interval_s = int(it.get("funding_interval_s") or 28800)
        if fr_raw is None or interval_s <= 0:
            continue
        # Convert to fraction per interval.
        rate_frac = fr_raw / 100.0 if rate_is_percent else fr_raw

        quotes = it.get("quotes") or {}
        q = quotes.get(notional_bucket) or quotes.get("base") or {}
        bid = to_float((q.get("bid") if isinstance(q, dict) else None))
        ask = to_float((q.get("ask") if isinstance(q, dict) else None))
        spread_bps = bps_from_bid_ask(bid, ask) if bid and ask else None
        mid = None
        if bid and ask and ask >= bid:
            mid = 0.5 * (bid + ask)
        elif to_float(it.get("mark_price")):
            mid = float(it["mark_price"])

        # Round-trip cost bps = spread + 2*fee (fee is per trade, two trades for open+close)
        rt_bps = (spread_bps if spread_bps is not None else 0.0) + 2.0 * default_fee_bps

        out[sym] = (VenueRate(rate_frac, interval_s, pos_means_longs_pay), rt_bps, mid)
    return out


def parse_lighter_funding(
    j: Dict[str, Any],
    rate_is_percent: bool,
    interval_s: int,
    pos_means_longs_pay: bool,
) -> Dict[str, VenueRate]:
    out: Dict[str, VenueRate] = {}
    if int(j.get("code") or 0) != 200:
        raise RuntimeError(f"lighter funding-rates returned code={j.get('code')}")
    items = j.get("funding_rates") or []
    for it in items:
        sym = norm_symbol(it.get("symbol"))
        r_raw = to_float(it.get("rate"))
        if not sym or r_raw is None:
            continue
        rate_frac = r_raw / 100.0 if rate_is_percent else r_raw
        out[sym] = VenueRate(rate_frac, interval_s, pos_means_longs_pay)
    return out


def parse_lighter_orderbooks_meta(j: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if int(j.get("code") or 0) != 200:
        raise RuntimeError(f"lighter orderBooks returned code={j.get('code')}")
    out: Dict[str, Dict[str, Any]] = {}
    items = j.get("order_books") or []
    for it in items:
        sym = norm_symbol(it.get("symbol"))
        if not sym:
            continue
        out[sym] = it
    return out


def parse_lighter_last_trade_price(details_j: Dict[str, Any]) -> Optional[float]:
    """
    Extract a best-effort "last_trade_price" from orderBookDetails.

    Note: this is NOT an executable price (no bid/ask), but can be used as an
    indicative basis signal vs Variational mid/mark.
    """
    if int(details_j.get("code") or 0) != 200:
        return None
    for k in ("order_book_details", "spot_order_book_details"):
        items = details_j.get(k)
        if isinstance(items, list) and items:
            p = to_float(items[0].get("last_trade_price"))
            if p is not None and p > 0:
                return p
    return None


def extract_best_bid_ask(orders_j: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    if int(orders_j.get("code") or 0) != 200:
        return (None, None)
    bids = orders_j.get("bids") or []
    asks = orders_j.get("asks") or []
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    for it in bids:
        p = to_float((it or {}).get("price"))
        if p is None or p <= 0:
            continue
        if best_bid is None or p > best_bid:
            best_bid = p
    for it in asks:
        p = to_float((it or {}).get("price"))
        if p is None or p <= 0:
            continue
        if best_ask is None or p < best_ask:
            best_ask = p
    if best_bid is not None and best_ask is not None and best_ask < best_bid:
        return (None, None)
    return (best_bid, best_ask)


def choose_lighter_reference_price(
    details_j: Dict[str, Any], orders_j: Dict[str, Any]
) -> Tuple[Optional[float], str, Optional[float], Optional[float]]:
    bid, ask = extract_best_bid_ask(orders_j)
    if bid is not None and ask is not None and ask >= bid:
        return (0.5 * (bid + ask), "orderbook_mid", bid, ask)
    last = parse_lighter_last_trade_price(details_j)
    if last is not None and last > 0:
        return (last, "last_trade_fallback", None, None)
    return (None, "none", None, None)


def build_rows(
    var: Dict[str, Tuple[VenueRate, float, Optional[float]]],
    lighter_rates: Dict[str, VenueRate],
    lighter_meta: Dict[str, Dict[str, Any]],
    lighter_spread_bps_assumed: float,
    symbol_whitelist: Optional[set[str]] = None,
) -> List[MarketRow]:
    rows: List[MarketRow] = []
    for sym, (vr, var_rt_bps, var_mid) in var.items():
        if symbol_whitelist is not None and sym not in symbol_whitelist:
            continue
        lr = lighter_rates.get(sym)
        meta = lighter_meta.get(sym)
        if lr is None or meta is None:
            continue
        if not is_valid_lighter_market_spec(meta):
            continue
        mid = int(meta.get("market_id") or 0)
        # Lighter fees are returned as strings like "0.0000" (fractional). Convert to bps.
        taker_fee_frac = to_float(meta.get("taker_fee")) or 0.0
        taker_fee_bps = taker_fee_frac * 1e4
        # Round-trip = assumed spread + 2*taker_fee
        lighter_rt_bps = float(lighter_spread_bps_assumed) + 2.0 * float(taker_fee_bps)

        rows.append(
            MarketRow(
                symbol=sym,
                var_rate=vr,
                lighter_rate=lr,
                lighter_market_id=mid,
                var_round_trip_bps=float(var_rt_bps),
                lighter_round_trip_bps=float(lighter_rt_bps),
                var_mid=var_mid,
                lighter_last=None,  # Lighter public endpoints here don't expose executable prices.
            )
        )
    return rows


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Rank Variational vs Lighter funding-rate arbitrage opportunities."
    )
    ap.add_argument("--notional", type=float, default=1000.0, help="USD notional per leg (default: 1000).")
    ap.add_argument("--top", type=int, default=30, help="Show top N rows (default: 30).")
    ap.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds.")

    ap.add_argument(
        "--var-quote-bucket",
        choices=["size_1k", "size_100k", "base"],
        default="size_1k",
        help="Which Variational quote bucket to use for spread estimate (default: size_1k).",
    )
    ap.add_argument(
        "--var-rate-units",
        choices=["percent", "fraction"],
        default="percent",
        help=(
            "How to interpret Variational funding_rate numbers. "
            "'percent' means 0.1095 == 0.1095%%. 'fraction' means 0.0001 == 0.01%%."
        ),
    )
    ap.add_argument(
        "--lighter-rate-units",
        choices=["percent", "fraction"],
        default="fraction",
        help=(
            "How to interpret Lighter funding `rate` numbers. "
            "'fraction' means 0.0001 == 0.01%% (default)."
        ),
    )
    ap.add_argument(
        "--lighter-funding-interval-s",
        type=int,
        default=28800,
        help="Assumed Lighter funding interval seconds (default: 28800 / 8h).",
    )
    ap.add_argument(
        "--funding-sign",
        choices=["longs_pay", "longs_receive"],
        default="longs_pay",
        help=(
            "Funding sign convention. 'longs_pay' means +rate => longs pay shorts (default). "
            "'longs_receive' means +rate => longs receive from shorts."
        ),
    )
    ap.add_argument(
        "--var-fee-bps",
        type=float,
        default=0.0,
        help="Assumed taker fee (bps) on Variational per trade (default: 0).",
    )
    ap.add_argument(
        "--lighter-spread-bps",
        type=float,
        default=5.0,
        help="Assumed Lighter spread/slippage (bps) per round-trip excluding fees (default: 5).",
    )
    ap.add_argument(
        "--fetch-lighter-last",
        action="store_true",
        help="Fetch Lighter orderBookDetails for some markets to attach last_trade_price (slower; optional).",
    )
    ap.add_argument(
        "--fetch-lighter-last-limit",
        type=int,
        default=30,
        help="How many top-ranked rows to fetch last_trade_price for (default: 30).",
    )
    ap.add_argument(
        "--fetch-lighter-last-workers",
        type=int,
        default=8,
        help="Concurrent workers for fetching Lighter price references (default: 8).",
    )
    ap.add_argument(
        "--lighter-orderbookorders-limit",
        type=int,
        default=50,
        help="Depth size for orderBookOrders (default: 50, max: 250).",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Output full ranking as JSON to stdout (instead of a table).",
    )
    ap.add_argument(
        "--symbol-aliases-json",
        default="",
        help='Optional symbol alias mapping JSON, e.g. {"XBT":"BTC","BTC-PERP":"BTC"}',
    )
    ap.add_argument(
        "--min-price-ratio",
        type=float,
        default=0.2,
        help="Filter out indicative price pairs with VAR_mid/Lighter_last below this ratio (default: 0.2).",
    )
    ap.add_argument(
        "--max-price-ratio",
        type=float,
        default=5.0,
        help="Filter out indicative price pairs with VAR_mid/Lighter_last above this ratio (default: 5.0).",
    )

    args = ap.parse_args(argv)
    pos_means_longs_pay = args.funding_sign == "longs_pay"

    t0 = time.time()
    var_j = http_get_json(VARIATIONAL_STATS_URL, timeout_s=args.timeout)
    lighter_funding_j = http_get_json(LIGHTER_FUNDING_URL, timeout_s=args.timeout)
    lighter_meta_j = http_get_json(LIGHTER_ORDERBOOKS_URL, timeout_s=args.timeout)

    var = parse_variational_markets(
        var_j,
        notional_bucket=args.var_quote_bucket,
        rate_is_percent=(args.var_rate_units == "percent"),
        pos_means_longs_pay=pos_means_longs_pay,
        default_fee_bps=args.var_fee_bps,
    )
    lighter_rates = parse_lighter_funding(
        lighter_funding_j,
        rate_is_percent=(args.lighter_rate_units == "percent"),
        interval_s=args.lighter_funding_interval_s,
        pos_means_longs_pay=pos_means_longs_pay,
    )
    lighter_meta = parse_lighter_orderbooks_meta(lighter_meta_j)
    aliases = parse_symbol_aliases(args.symbol_aliases_json)
    if aliases:
        var = {canonical_symbol(k, aliases): v for k, v in var.items()}
        lighter_rates = {canonical_symbol(k, aliases): v for k, v in lighter_rates.items()}
        lighter_meta = {canonical_symbol(k, aliases): v for k, v in lighter_meta.items()}
    symbol_whitelist = build_symbol_whitelist(var.keys(), lighter_rates.keys(), aliases=aliases)

    rows = build_rows(
        var=var,
        lighter_rates=lighter_rates,
        lighter_meta=lighter_meta,
        lighter_spread_bps_assumed=args.lighter_spread_bps,
        symbol_whitelist=symbol_whitelist,
    )

    ranked: List[Tuple[float, MarketRow, str, float, float, float]] = []
    # Tuple: (net_1d_usd, row, strategy, funding_1d_usd, cost_usd, breakeven_days)
    for r in rows:
        strat, funding_1d, _funding_interval = r.best_funding_trade(args.notional)
        cost = r.round_trip_cost_usd(args.notional)
        net_1d = funding_1d - cost  # assume you hold ~1 day then close
        breakeven_days = (cost / funding_1d) if funding_1d > 0 else math.inf
        ranked.append((net_1d, r, strat, funding_1d, cost, breakeven_days))

    ranked.sort(key=lambda x: x[0], reverse=True)

    # Optional: fetch indicative prices only for the top-N ranked items.
    if args.fetch_lighter_last and args.fetch_lighter_last_limit > 0:
        need = ranked[: min(args.fetch_lighter_last_limit, len(ranked))]
        by_mid: Dict[int, Tuple[Optional[float], str, Optional[float], Optional[float]]] = {}
        orders_limit = max(1, min(250, int(args.lighter_orderbookorders_limit)))

        ids = sorted({r.lighter_market_id for _net_1d, r, _s, _f, _c, _b in need if r.lighter_market_id > 0})

        def _fetch_ref(mid: int) -> Tuple[int, Tuple[Optional[float], str, Optional[float], Optional[float]]]:
            durl = LIGHTER_ORDERBOOK_DETAILS_URL.format(market_id=mid)
            ourl = LIGHTER_ORDERBOOK_ORDERS_URL.format(market_id=mid, limit=orders_limit)
            dj = http_get_json(durl, timeout_s=args.timeout)
            oj = http_get_json(ourl, timeout_s=args.timeout)
            return (mid, choose_lighter_reference_price(dj, oj))

        with cf.ThreadPoolExecutor(max_workers=max(1, int(args.fetch_lighter_last_workers))) as ex:
            futs = [ex.submit(_fetch_ref, mid) for mid in ids]
            for fu in cf.as_completed(futs):
                mid, ref = fu.result()
                by_mid[mid] = ref

        # Update the ranked list rows in-place (via dataclasses.replace).
        updated_ranked: List[Tuple[float, MarketRow, str, float, float, float]] = []
        for net_1d, r, strat, funding_1d, cost, be in ranked:
            last, src, bid, ask = by_mid.get(r.lighter_market_id, (None, "none", None, None))
            if is_reasonable_price_ratio(r.var_mid, last, args.min_price_ratio, args.max_price_ratio):
                r = replace(r, lighter_last=last, lighter_price_source=src, lighter_bid=bid, lighter_ask=ask)
            updated_ranked.append((net_1d, r, strat, funding_1d, cost, be))
        ranked = updated_ranked

    if args.json:
        payload = {
            "asof_unix": int(time.time()),
            "notional_usd": args.notional,
            "assumptions": {
                "var_rate_units": args.var_rate_units,
                "lighter_rate_units": args.lighter_rate_units,
                "funding_sign": args.funding_sign,
                "var_quote_bucket": args.var_quote_bucket,
                "var_fee_bps_per_trade": args.var_fee_bps,
                "lighter_spread_bps_assumed_round_trip_excl_fees": args.lighter_spread_bps,
                "lighter_funding_interval_s": args.lighter_funding_interval_s,
                "lighter_orderbookorders_limit": int(max(1, min(250, int(args.lighter_orderbookorders_limit)))),
                "net_1d_definition": "funding_pnl_over_1_day - estimated_round_trip_cost(open+close)",
                "symbol_aliases": aliases,
                "symbol_whitelist_size": len(symbol_whitelist),
                "price_ratio_guardrail": {"min": args.min_price_ratio, "max": args.max_price_ratio},
            },
            "items": [],
            "fetch_ms": int((time.time() - t0) * 1000),
        }
        ranked_show = ranked[: max(args.top, 0)] if args.top else ranked
        for net_1d, r, strat, funding_1d, cost, breakeven_days in ranked_show:
            basis_bps = r.indicative_basis_bps()
            payload["items"].append(
                {
                    "symbol": r.symbol,
                    "strategy": strat,
                    "funding_pnl_1d_usd": funding_1d,
                    "round_trip_cost_usd": cost,
                    "net_1d_usd": net_1d,
                    "breakeven_days": None if not math.isfinite(breakeven_days) else breakeven_days,
                    "basis_bps_indicative": basis_bps,
                    "prices": {
                        "var_mid": r.var_mid,
                        "lighter_last_trade": r.lighter_last,
                        "lighter_best_bid": r.lighter_bid,
                        "lighter_best_ask": r.lighter_ask,
                        "lighter_price_source": r.lighter_price_source,
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
        json.dump(payload, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0

    # Table output (Chinese column labels as requested).
    print(f"名义本金: {args.notional:.2f} USD/腿 (两边各开一腿)")
    print("排名指标: 预计净收益/天 = 资金费率收益/天 - 预估开平成本(两边合计)")
    print(f"可匹配标的数: {len(ranked)} (VAR 标的 ∩ Lighter 永续标的 ∩ Lighter 资金费率)")
    print("")

    header = (
        "排名  标的         策略                    预计净收益/天($)  资金费率收益/天($)  开平成本($)  回本(天)  "
        "参考基差(bps)  VAR开平(bps)  Lighter开平(bps)  VAR间隔(h)  Lighter间隔(h)  VAR费率/周期    Lighter费率/周期"
    )
    print(header)
    print("-" * len(header))

    for i, (net_1d, r, strat, funding_1d, cost, breakeven_days) in enumerate(
        ranked[: max(args.top, 0)], start=1
    ):
        basis_bps = r.indicative_basis_bps()
        print(
            f"{i:>4}  {r.symbol:<12} {strat:<24} {net_1d:>13.2f}  {funding_1d:>16.2f}  {cost:>10.2f}  "
            f"{(breakeven_days if math.isfinite(breakeven_days) else float('nan')):>7.2f}  "
            f"{(basis_bps if basis_bps is not None else float('nan')):>12.2f}  "
            f"{r.var_round_trip_bps:>11.2f}  {r.lighter_round_trip_bps:>14.2f}  "
            f"{(r.var_rate.interval_s/3600.0):>10.2f}  {(r.lighter_rate.interval_s/3600.0):>13.2f}  "
            f"{r.var_rate.rate_frac_per_interval:>12.6g}  {r.lighter_rate.rate_frac_per_interval:>14.6g}"
        )

    print("")
    print("说明:")
    print("- Lighter 的真实点差/滑点无法从这些公开接口直接得到；请用 --lighter-spread-bps 调整假设。")
    print("- 参考基差(bps) 优先使用 orderBookOrders 的 best bid/ask 中间价，缺失时回退到 last_trade_price。")
    print("- 如果资金费率符号约定相反，请用 --funding-sign longs_receive。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
