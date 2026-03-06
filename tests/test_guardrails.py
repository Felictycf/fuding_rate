import unittest

import arb_rank
import price_arb_cn


class GuardrailTests(unittest.TestCase):
    def test_symbol_alias_normalization(self):
        aliases = {"XBT": "BTC", "BTC-PERP": "BTC"}
        self.assertEqual(arb_rank.canonical_symbol("xbt", aliases), "BTC")
        self.assertEqual(price_arb_cn.canonical_symbol("BTC-PERP", aliases), "BTC")

    def test_lighter_market_spec_validation(self):
        ok = {
            "market_type": "perp",
            "market_id": 12,
            "status": "active",
            "min_base_amount": "0.001",
            "min_quote_amount": "10",
        }
        bad = {
            "market_type": "spot",
            "market_id": 0,
            "status": "inactive",
            "min_base_amount": "0",
            "min_quote_amount": "0",
        }
        self.assertTrue(arb_rank.is_valid_lighter_market_spec(ok))
        self.assertFalse(arb_rank.is_valid_lighter_market_spec(bad))

    def test_price_ratio_guardrail(self):
        self.assertTrue(price_arb_cn.is_reasonable_price_ratio(100, 105, 0.2, 5.0))
        self.assertFalse(price_arb_cn.is_reasonable_price_ratio(1, 486.65, 0.2, 5.0))


if __name__ == "__main__":
    unittest.main()
