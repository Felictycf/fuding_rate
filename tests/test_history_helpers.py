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

    def test_seed_snapshot_targets_registers_default_dashboard_queries(self):
        server.seed_snapshot_targets()
        self.assertEqual(len(server.SNAPSHOTS["funding"]), 1)
        self.assertEqual(len(server.SNAPSHOTS["price"]), 1)
        funding_entry = next(iter(server.SNAPSHOTS["funding"].values()))
        price_entry = next(iter(server.SNAPSHOTS["price"].values()))
        self.assertEqual(funding_entry["params"]["top"], 30)
        self.assertEqual(price_entry["params"]["top"], 30)

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
            {"top": 30},
            force=False,
            snapshot_max_age_s=5.0,
            live_fetcher=live_fetcher,
        )

        self.assertEqual(out["source"], "snapshot")
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
            {"top": 30},
            force=True,
            snapshot_max_age_s=5.0,
            live_fetcher=live_fetcher,
        )

        self.assertEqual(out["source"], "live")
        self.assertEqual(out["items"][0]["symbol"], "ETH")
        live_fetcher.assert_called_once()


if __name__ == "__main__":
    unittest.main()
