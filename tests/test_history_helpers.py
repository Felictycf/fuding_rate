import unittest

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


if __name__ == "__main__":
    unittest.main()
