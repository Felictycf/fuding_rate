import time
import unittest
from unittest import mock

import server


class HistoryHelperTests(unittest.TestCase):
    def test_downsample_keeps_order_and_edges(self):
        points = [{"ts": i, "bps": float(i)} for i in range(100)]
        out = server.downsample_points(points, max_points=10)
        self.assertEqual(len(out), 10)
        self.assertEqual(out[0]["ts"], 0)
        self.assertEqual(out[-1]["ts"], 99)
        self.assertTrue(all(out[i]["ts"] < out[i + 1]["ts"] for i in range(len(out) - 1)))

    def test_normalize_history_query_bounds(self):
        # too small / too large should be clamped
        r, l = server.normalize_history_query(range_s=-1, limit=999999)
        self.assertEqual(r, 60)
        self.assertEqual(l, 5000)

    def test_normalize_history_query_nominal(self):
        r, l = server.normalize_history_query(range_s=3600, limit=1200)
        self.assertEqual((r, l), (3600, 1200))

    def test_normalize_history_symbols_query_bounds(self):
        r, l, m = server.normalize_history_symbols_query(range_s=-1, limit=9999, min_points=0)
        self.assertEqual(r, 3600)
        self.assertEqual(l, 500)
        self.assertEqual(m, 1)

    def test_normalize_history_symbols_query_nominal(self):
        r, l, m = server.normalize_history_symbols_query(range_s=86400, limit=120, min_points=5)
        self.assertEqual((r, l, m), (86400, 120, 5))

    def test_take_rotating_batch_wraps(self):
        symbols = ["BTC", "DOGE", "SOL", "ETH"]
        batch, cursor = server.take_rotating_batch(symbols, cursor=3, batch_size=3)
        self.assertEqual(batch, ["ETH", "BTC", "DOGE"])
        self.assertEqual(cursor, 2)

    def test_intersect_collector_symbols(self):
        var_symbols = ["BTC", "DOGE", "SOL", "XRP"]
        lighter_map = {"DOGE": 10, "BTC": 1, "ETH": 2}
        out = server.intersect_collector_symbols(var_symbols, lighter_map)
        self.assertEqual(out, ["BTC", "DOGE"])


class SnapshotHelperTests(unittest.TestCase):
    def setUp(self):
        server.SNAPSHOTS = {"funding": {}, "price": {}}

    def test_seed_snapshot_targets_registers_fast_and_full_dashboard_queries(self):
        server.seed_snapshot_targets()
        self.assertEqual(len(server.SNAPSHOTS["funding"]), 2)
        self.assertEqual(len(server.SNAPSHOTS["price"]), 2)
        funding_levels = {entry["params"]["detail_level"] for entry in server.SNAPSHOTS["funding"].values()}
        price_levels = {entry["params"]["detail_level"] for entry in server.SNAPSHOTS["price"].values()}
        self.assertEqual(funding_levels, {"fast", "full"})
        self.assertEqual(price_levels, {"fast", "full"})
        funding_fast = [e for e in server.SNAPSHOTS["funding"].values() if e["params"]["detail_level"] == "fast"][0]
        self.assertFalse(funding_fast["params"]["fetch_last"])
        price_fast = [e for e in server.SNAPSHOTS["price"].values() if e["params"]["detail_level"] == "fast"][0]
        self.assertTrue(price_fast["params"]["fast_mode"])

    def test_resolve_snapshot_payload_prefers_recent_snapshot(self):
        key = "funding|default"
        server.SNAPSHOTS["funding"][key] = {
            "params": {"top": 30},
            "data": {"items": [{"symbol": "BTC"}], "fetch_ms": 3200},
            "updated_at": time.time() - 0.2,
            "last_access": 0.0,
            "last_error": None,
        }
        live_fetcher = mock.Mock(side_effect=AssertionError("should not fetch live"))

        out = server.resolve_snapshot_payload(
            "funding",
            key,
            {"top": 30, "detail_level": "fast"},
            force=False,
            snapshot_max_age_s=5.0,
            live_fetcher=live_fetcher,
        )

        self.assertEqual(out["source"], "snapshot")
        self.assertEqual(out["detail_level"], "fast")
        self.assertEqual(out["items"][0]["symbol"], "BTC")
        self.assertIn("snapshot_age_ms", out)
        live_fetcher.assert_not_called()

    def test_resolve_snapshot_payload_force_live_bypasses_snapshot(self):
        key = "funding|default"
        server.SNAPSHOTS["funding"][key] = {
            "params": {"top": 30},
            "data": {"items": [{"symbol": "BTC"}], "fetch_ms": 3200},
            "updated_at": time.time(),
            "last_access": 0.0,
            "last_error": None,
        }
        live_fetcher = mock.Mock(return_value={"items": [{"symbol": "ETH"}], "fetch_ms": 111})

        out = server.resolve_snapshot_payload(
            "funding",
            key,
            {"top": 30, "detail_level": "full"},
            force=True,
            snapshot_max_age_s=5.0,
            live_fetcher=live_fetcher,
        )

        self.assertEqual(out["source"], "live")
        self.assertEqual(out["detail_level"], "full")
        self.assertEqual(out["items"][0]["symbol"], "ETH")
        live_fetcher.assert_called_once()

    def test_build_price_payload_fast_prefers_ws_quotes(self):
        params = {
            "notional": 1000.0,
            "top": 5,
            "lighter_spread_bps": 5.0,
            "var_fee_bps": 0.0,
            "max_markets": 5,
            "concurrency": 4,
            "timeout_s": 10.0,
        }
        var_j = {
            "listings": [
                {
                    "ticker": "BTC",
                    "quotes": {"size_1k": {"bid": "100.0", "ask": "100.2"}},
                    "mark_price": "100.1",
                }
            ]
        }
        lighter_meta_j = {
            "code": 200,
            "order_books": [
                {
                    "symbol": "BTC",
                    "market_id": 1,
                    "status": "active",
                    "market_type": "perp",
                    "min_base_amount": "0.001",
                    "min_quote_amount": "1",
                    "taker_fee": "0.0005",
                }
            ],
        }
        detail_urls = []

        def fake_http(url, timeout_s):
            if url == server.price_arb_cn.VARIATIONAL_STATS_URL:
                return var_j
            if url == server.price_arb_cn.LIGHTER_ORDERBOOKS_URL:
                return lighter_meta_j
            detail_urls.append(url)
            return {"code": 200, "order_book_details": [{"last_trade_price": "99.9"}]}

        with (
            mock.patch.object(server.price_arb_cn, "http_get_json", side_effect=fake_http),
            mock.patch.object(
                server,
                "_get_lighter_ws_quotes_for_symbols",
                return_value=(
                    {
                        "BTC": {
                            "best_bid": 99.9,
                            "best_ask": 100.1,
                            "mid": 100.0,
                            "source": "ws_orderbook",
                            "age_ms": 120,
                        }
                    },
                    {"connected": True, "subscribed": 1, "covered": 1, "age_ms": 120},
                ),
                create=True,
            ),
        ):
            payload = server.build_price_payload_fast(params)

        self.assertEqual(detail_urls, [])
        self.assertEqual(payload["lighter_ws_covered"], 1)
        self.assertEqual(payload["lighter_ws_fallback_count"], 0)
        self.assertEqual(payload["items"][0]["lighter"]["price_source"], "ws_orderbook")
        self.assertAlmostEqual(payload["items"][0]["lighter"]["last_trade"], 100.0)

    def test_build_price_payload_fast_falls_back_only_for_missing_ws_quotes(self):
        params = {
            "notional": 1000.0,
            "top": 5,
            "lighter_spread_bps": 5.0,
            "var_fee_bps": 0.0,
            "max_markets": 5,
            "concurrency": 4,
            "timeout_s": 10.0,
        }
        var_j = {
            "listings": [
                {
                    "ticker": "BTC",
                    "quotes": {"size_1k": {"bid": "100.0", "ask": "100.2"}},
                    "mark_price": "100.1",
                },
                {
                    "ticker": "ETH",
                    "quotes": {"size_1k": {"bid": "200.0", "ask": "200.4"}},
                    "mark_price": "200.2",
                },
            ]
        }
        lighter_meta_j = {
            "code": 200,
            "order_books": [
                {
                    "symbol": "BTC",
                    "market_id": 1,
                    "status": "active",
                    "market_type": "perp",
                    "min_base_amount": "0.001",
                    "min_quote_amount": "1",
                    "taker_fee": "0.0005",
                },
                {
                    "symbol": "ETH",
                    "market_id": 2,
                    "status": "active",
                    "market_type": "perp",
                    "min_base_amount": "0.001",
                    "min_quote_amount": "1",
                    "taker_fee": "0.0005",
                },
            ],
        }
        detail_urls = []

        def fake_http(url, timeout_s):
            if url == server.price_arb_cn.VARIATIONAL_STATS_URL:
                return var_j
            if url == server.price_arb_cn.LIGHTER_ORDERBOOKS_URL:
                return lighter_meta_j
            detail_urls.append(url)
            market_id = 1 if "market_id=1" in url else 2
            return {"code": 200, "order_book_details": [{"last_trade_price": str(100 * market_id)}]}

        with (
            mock.patch.object(server.price_arb_cn, "http_get_json", side_effect=fake_http),
            mock.patch.object(
                server,
                "_get_lighter_ws_quotes_for_symbols",
                return_value=(
                    {
                        "BTC": {
                            "best_bid": 99.9,
                            "best_ask": 100.1,
                            "mid": 100.0,
                            "source": "ws_orderbook",
                            "age_ms": 120,
                        }
                    },
                    {"connected": True, "subscribed": 2, "covered": 1, "age_ms": 120},
                ),
                create=True,
            ),
        ):
            payload = server.build_price_payload_fast(params)

        self.assertEqual(len(detail_urls), 1)
        self.assertIn("market_id=2", detail_urls[0])
        self.assertEqual(payload["lighter_ws_covered"], 1)
        self.assertEqual(payload["lighter_ws_fallback_count"], 1)
        self.assertEqual({it["symbol"] for it in payload["items"]}, {"BTC", "ETH"})

    def test_lighter_order_book_state_updates_best_levels(self):
        state = server.LighterOrderBookState()
        state.apply_levels(
            asks=[{"price": "101.0", "size": "2"}, {"price": "102.0", "size": "3"}],
            bids=[{"price": "99.0", "size": "2"}, {"price": "98.0", "size": "3"}],
            updated_at=100.0,
        )
        state.apply_levels(
            asks=[{"price": "101.0", "size": "0"}, {"price": "100.5", "size": "1"}],
            bids=[{"price": "99.0", "size": "0"}, {"price": "99.5", "size": "1.5"}],
            updated_at=101.0,
        )

        snap = state.snapshot(now=101.2)
        self.assertEqual(snap["best_ask"], 100.5)
        self.assertEqual(snap["best_bid"], 99.5)
        self.assertAlmostEqual(snap["mid"], 100.0)
        self.assertEqual(snap["age_ms"], 200)


if __name__ == "__main__":
    unittest.main()
