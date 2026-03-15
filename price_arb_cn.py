#!/usr/bin/env python3
"""
差价套利(仅差价)信息看板：Variational vs Lighter

重要说明(请务必读)：
1) Variational 侧我们能拿到 size_1k 的 bid/ask，因此可以计算中间价(mid)、点差(bps)并估算滑点/成本。
2) Lighter 侧优先使用 orderBookOrders(best bid/ask) 计算可成交中间价；
   当盘口缺失时回退到 orderBookDetails.last_trade_price。
3) 结果仍为估算，不代表可保证执行利润；请结合真实成交滑点与手续费校验。

依赖：仅 Python 标准库
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from http_json import get_json


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


def http_get_json(url: str, timeout_s: float = 20.0) -> Any:
    return get_json(url, timeout_s=timeout_s, user_agent="price-arb-cn/1.0")


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


def norm_symbol(sym: str) -> str:
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
    var_symbols: List[str], lighter_symbols: List[str], aliases: Optional[Dict[str, str]] = None
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


def bps_from_bid_ask(bid: float, ask: float) -> Optional[float]:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = 0.5 * (bid + ask)
    if mid <= 0:
        return None
    return (ask - bid) / mid * 1e4


def bps_diff(a: float, b: float) -> Optional[float]:
    # (a-b)/mid * 1e4
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    mid = 0.5 * (a + b)
    return (a - b) / mid * 1e4


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def cache_path(cache_dir: str, market_id: int) -> str:
    return os.path.join(cache_dir, f"lighter_orderbookdetails_{market_id}.json")


def orders_cache_path(cache_dir: str, market_id: int, limit: int) -> str:
    return os.path.join(cache_dir, f"lighter_orderbookorders_{market_id}_{limit}.json")


def read_cache(path: str, max_age_s: float) -> Optional[Dict[str, Any]]:
    try:
        st = os.stat(path)
        if max_age_s > 0 and (time.time() - st.st_mtime) > max_age_s:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def write_cache(path: str, obj: Any) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def extract_last_trade_price(details_j: Dict[str, Any]) -> Optional[float]:
    if int(details_j.get("code") or 0) != 200:
        return None
    for k in ("order_book_details", "spot_order_book_details"):
        arr = details_j.get(k)
        if isinstance(arr, list) and arr:
            p = to_float(arr[0].get("last_trade_price"))
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
    last = extract_last_trade_price(details_j)
    if last is not None and last > 0:
        return (last, "last_trade_fallback", None, None)
    return (None, "none", None, None)


@dataclass(frozen=True)
class Row:
    标的: str
    方向: str  # 哪边贵/便宜的参考方向
    VAR_bid: Optional[float]
    VAR_ask: Optional[float]
    VAR_mid: Optional[float]
    Lighter_last: Optional[float]
    Lighter_bid: Optional[float]
    Lighter_ask: Optional[float]
    Lighter_price_source: str
    参考差价_bps: Optional[float]
    VAR点差_bps: Optional[float]
    Lighter真实点差_bps: Optional[float]
    Lighter假设点差_bps: float
    Lighter_taker_fee_bps: float
    开仓成本_bps: float
    往返成本_bps: float
    名义本金: float

    def 理论毛利润_u(self) -> Optional[float]:
        if self.参考差价_bps is None:
            return None
        return abs(self.参考差价_bps) / 1e4 * self.名义本金

    def 参考净利润_u_往返(self) -> Optional[float]:
        if self.参考差价_bps is None:
            return None
        net_bps = abs(self.参考差价_bps) - self.往返成本_bps
        return net_bps / 1e4 * self.名义本金


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Variational vs Lighter 差价套利(参考)排行榜(中文输出)")
    ap.add_argument("--名义本金", type=float, default=1000.0, help="每条腿名义本金(USD/USDT)，默认 1000")
    ap.add_argument(
        "--VAR档位",
        choices=["size_1k", "size_100k", "base"],
        default="size_1k",
        help="使用 Variational quotes 的哪个档位来估点差，默认 size_1k",
    )
    ap.add_argument("--超时", type=float, default=20.0, help="HTTP 超时秒数，默认 20")
    ap.add_argument("--并发", type=int, default=16, help="抓取 Lighter 盘口接口并发数，默认 16")
    ap.add_argument("--最多市场数", type=int, default=120, help="最多处理多少个 Lighter perp 市场(防止跑太久)，默认 120")
    ap.add_argument("--缓存秒", type=float, default=30.0, help="Lighter 盘口缓存有效期(秒)，默认 30，0=不缓存")
    ap.add_argument("--Lighter盘口limit", type=int, default=50, help="orderBookOrders 拉取深度条数，默认 50，最大 250")
    ap.add_argument(
        "--缓存目录",
        default=os.path.join(os.getcwd(), ".cache_price_arb"),
        help="缓存目录，默认当前目录下 .cache_price_arb",
    )
    ap.add_argument(
        "--Lighter点差bps",
        type=float,
        default=5.0,
        help="Lighter 侧无法直接获取盘口，这里用假设点差/滑点(bps)；默认 5bps",
    )
    ap.add_argument(
        "--VAR手续费bps",
        type=float,
        default=0.0,
        help="Variational 每笔 taker 手续费(bps)假设，默认 0",
    )
    ap.add_argument(
        "--symbol_aliases_json",
        default="",
        help='可选：符号别名映射JSON，例如 {"XBT":"BTC","BTC-PERP":"BTC"}',
    )
    ap.add_argument("--min_price_ratio", type=float, default=0.2, help="价格比下限(VAR_mid/L_last)，默认0.2")
    ap.add_argument("--max_price_ratio", type=float, default=5.0, help="价格比上限(VAR_mid/L_last)，默认5.0")
    ap.add_argument("--显示前N", type=int, default=30, help="显示前 N 条，默认 30")
    ap.add_argument("--fast-mode", action="store_true", help="极速模式：只用 last_trade_price，不拉 orderBookOrders")
    ap.add_argument("--json", action="store_true", help="以 JSON 输出(便于网页展示)")
    args = ap.parse_args(argv)

    t0 = time.time()
    var_j = http_get_json(VARIATIONAL_STATS_URL, timeout_s=args.超时)
    lighter_meta_j = http_get_json(LIGHTER_ORDERBOOKS_URL, timeout_s=args.超时)

    aliases = parse_symbol_aliases(args.symbol_aliases_json)

    # 1) 解析 VAR：ticker -> bid/ask/mid/spread_bps
    var_map: Dict[str, Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]] = {}
    for it in (var_j.get("listings") or []):
        sym = canonical_symbol(it.get("ticker"), aliases)
        if not sym:
            continue
        quotes = it.get("quotes") or {}
        q = quotes.get(args.VAR档位) or quotes.get("base") or {}
        bid = to_float(q.get("bid")) if isinstance(q, dict) else None
        ask = to_float(q.get("ask")) if isinstance(q, dict) else None
        mid = None
        if bid and ask and ask >= bid:
            mid = 0.5 * (bid + ask)
        else:
            mp = to_float(it.get("mark_price"))
            mid = mp if mp and mp > 0 else None
        spread = bps_from_bid_ask(bid, ask) if bid and ask else None
        var_map[sym] = (bid, ask, mid, spread)

    # 2) 解析 Lighter perp 市场：symbol -> (market_id, taker_fee_bps)
    if int(lighter_meta_j.get("code") or 0) != 200:
        raise SystemExit(f"Lighter orderBooks 返回 code={lighter_meta_j.get('code')}")

    lighter_markets: List[Tuple[str, int, float]] = []
    for it in (lighter_meta_j.get("order_books") or []):
        if not is_valid_lighter_market_spec(it):
            continue
        sym = canonical_symbol(it.get("symbol"), aliases)
        mid = int(it.get("market_id") or 0)
        if not sym or mid <= 0:
            continue
        taker_fee_frac = to_float(it.get("taker_fee")) or 0.0  # 0.0000
        taker_fee_bps = taker_fee_frac * 1e4
        lighter_markets.append((sym, mid, float(taker_fee_bps)))

    symbol_whitelist = build_symbol_whitelist(list(var_map.keys()), [m[0] for m in lighter_markets], aliases)
    # 只保留 VAR 也有的标的
    lighter_markets = [m for m in lighter_markets if m[0] in symbol_whitelist]
    lighter_markets = lighter_markets[: max(0, args.最多市场数)]

    ensure_dir(args.缓存目录)

    limit = max(1, min(250, int(args.Lighter盘口limit)))

    def fetch_one(sym: str, market_id: int) -> Tuple[str, int, Optional[float], Optional[float], Optional[float], str]:
        cp = cache_path(args.缓存目录, market_id)
        op = orders_cache_path(args.缓存目录, market_id, limit)
        details_j: Optional[Dict[str, Any]] = None
        orders_j: Optional[Dict[str, Any]] = None
        if args.缓存秒 != 0:
            cd = read_cache(cp, max_age_s=float(args.缓存秒))
            if isinstance(cd, dict):
                details_j = cd
            if not args.fast_mode:
                co = read_cache(op, max_age_s=float(args.缓存秒))
                if isinstance(co, dict):
                    orders_j = co
        if details_j is None:
            durl = LIGHTER_ORDERBOOK_DETAILS_URL.format(market_id=market_id)
            details_j = http_get_json(durl, timeout_s=args.超时)
            if args.缓存秒 != 0:
                try:
                    write_cache(cp, details_j)
                except Exception:
                    pass
        if args.fast_mode:
            px = extract_last_trade_price(details_j)
            src = "last_trade_fast" if px is not None else "none"
            return (sym, market_id, px, None, None, src)
        if orders_j is None:
            ourl = LIGHTER_ORDERBOOK_ORDERS_URL.format(market_id=market_id, limit=limit)
            orders_j = http_get_json(ourl, timeout_s=args.超时)
            if args.缓存秒 != 0:
                try:
                    write_cache(op, orders_j)
                except Exception:
                    pass
        px, src, bid, ask = choose_lighter_reference_price(details_j, orders_j)
        return (sym, market_id, px, bid, ask, src)

    # 3) 并发抓 Lighter 可成交盘口(best bid/ask)，缺失时 fallback last_trade_price
    ref_map: Dict[str, Tuple[Optional[float], Optional[float], Optional[float], str]] = {}
    with cf.ThreadPoolExecutor(max_workers=max(1, int(args.并发))) as ex:
        futs = [ex.submit(fetch_one, sym, mid) for (sym, mid, _fee) in lighter_markets]
        for fu in cf.as_completed(futs):
            sym, _mid, ref_px, bid, ask, src = fu.result()
            ref_map[sym] = (ref_px, bid, ask, src)

    # 4) 计算差价套利“参考指标”
    rows: List[Row] = []
    for sym, _mid, taker_fee_bps in lighter_markets:
        bid, ask, var_mid, var_spread_bps = var_map.get(sym, (None, None, None, None))
        lighter_ref, lighter_bid, lighter_ask, lighter_src = ref_map.get(sym, (None, None, None, "none"))
        if var_mid is None or lighter_ref is None:
            continue
        if not is_reasonable_price_ratio(var_mid, lighter_ref, args.min_price_ratio, args.max_price_ratio):
            continue
        diff = bps_diff(var_mid, lighter_ref)  # VAR - Lighter
        if diff is None:
            continue

        # 方向：谁贵谁便宜(参考)
        # diff>0: VAR 价格更高 => (参考) 卖 VAR / 买 Lighter
        if diff > 0:
            direction = "卖VAR/买Lighter(参考)"
        else:
            direction = "买VAR/卖Lighter(参考)"

        # 成本模型：
        # 开仓：两边各一次 taker（点差 + fee）
        var_fee_bps = float(args.VAR手续费bps)
        var_open_bps = float(var_spread_bps or 0.0) + var_fee_bps
        lighter_real_spread_bps = bps_from_bid_ask(lighter_bid, lighter_ask)
        lighter_spread_used_bps = (
            float(lighter_real_spread_bps) if lighter_real_spread_bps is not None else float(args.Lighter点差bps)
        )
        lighter_open_bps = lighter_spread_used_bps + float(taker_fee_bps)
        open_cost_bps = var_open_bps + lighter_open_bps
        # 往返：假设平仓成本与开仓对称(再乘 2)
        round_trip_bps = 2.0 * open_cost_bps

        rows.append(
            Row(
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
                Lighter真实点差_bps=lighter_real_spread_bps,
                Lighter假设点差_bps=float(args.Lighter点差bps),
                Lighter_taker_fee_bps=float(taker_fee_bps),
                开仓成本_bps=open_cost_bps,
                往返成本_bps=round_trip_bps,
                名义本金=float(args.名义本金),
            )
        )

    # 排序：按“参考净利润(往返)”降序
    def key_net(r: Row) -> float:
        v = r.参考净利润_u_往返()
        return v if v is not None else -1e18

    rows.sort(key=key_net, reverse=True)

    ms = int((time.time() - t0) * 1000)

    if args.json:
        payload = {
            "asof_unix": int(time.time()),
            "fetch_ms": ms,
            "notional_usd": float(args.名义本金),
            "assumptions": {
                "var_quote_bucket": args.VAR档位,
                "var_fee_bps_per_trade": float(args.VAR手续费bps),
                "lighter_spread_bps_assumed_per_trade": float(args.Lighter点差bps),
                "cache_seconds": float(args.缓存秒),
                "max_markets": int(args.最多市场数),
                "concurrency": int(args.并发),
                "lighter_orderbookorders_limit": int(limit),
                "symbol_aliases": aliases,
                "symbol_whitelist_size": len(symbol_whitelist),
                "price_ratio_guardrail": {"min": float(args.min_price_ratio), "max": float(args.max_price_ratio)},
            },
            "notes": [
                "diff_bps prefers Lighter orderBookOrders best bid/ask mid; falls back to orderBookDetails.last_trade_price.",
                "net_u_round_trip assumes symmetric open/close costs.",
            ],
            "items": [],
        }
        show = rows[: max(0, int(args.显示前N))]
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
                        "fee_bps_per_trade": float(args.VAR手续费bps),
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
        json.dump(payload, fp=os.sys.stdout, ensure_ascii=False, indent=2)
        os.sys.stdout.write("\n")
        return 0

    # 5) 输出中文表格
    print(f"名义本金: {args.名义本金:.2f} USD/腿")
    print("用途: 仅差价套利(参考)。优先使用 Lighter best bid/ask 中间价；缺失时回退到 last_trade_price。")
    print(
        f"成本假设: VAR手续费={args.VAR手续费bps:.2f}bps/笔, Lighter点差(假设)={args.Lighter点差bps:.2f}bps/笔 + Lighter taker_fee"
    )
    print(f"参与计算标的数: {len(rows)} (过滤掉缺价格/缺盘口后)")
    print("")

    header = (
        "排名  标的         方向(参考)               参考差价(bps)  理论毛利润(U)  往返成本(bps)  参考净利润(U)  "
        "VAR点差(bps)  L_taker(bps)  L点差假设(bps)  VAR_mid  L_last"
    )
    print(header)
    print("-" * len(header))

    show = rows[: max(0, int(args.显示前N))]
    for i, r in enumerate(show, start=1):
        gross = r.理论毛利润_u()
        net = r.参考净利润_u_往返()
        print(
            f"{i:>4}  {r.标的:<12} {r.方向:<22} "
            f"{(r.参考差价_bps if r.参考差价_bps is not None else float('nan')):>12.2f}  "
            f"{(gross if gross is not None else float('nan')):>12.4f}  "
            f"{r.往返成本_bps:>12.2f}  "
            f"{(net if net is not None else float('nan')):>12.4f}  "
            f"{(r.VAR点差_bps if r.VAR点差_bps is not None else 0.0):>11.2f}  "
            f"{r.Lighter_taker_fee_bps:>10.2f}  "
            f"{r.Lighter假设点差_bps:>14.2f}  "
            f"{r.VAR_mid:>7.4f}  {r.Lighter_last:>7.4f}"
        )

    print("")
    print(f"耗时: {ms} ms")
    print("说明:")
    print("- 参考净利润(U) = |参考差价(bps)| - 往返成本(bps)，再乘以名义本金。")
    print("- 往返成本(bps) = 2 * (VAR(点差+手续费) + Lighter(点差假设+taker_fee))。")
    print("- Lighter 成本优先使用 orderBookOrders 实际点差，缺失时才用 --Lighter点差bps 假设。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
