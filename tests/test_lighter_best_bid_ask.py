import unittest

import arb_rank
import price_arb_cn


class LighterBestBidAskTests(unittest.TestCase):
    def test_extract_best_bid_ask(self):
        orders = {
            "code": 200,
            "asks": [{"price": "101.2"}, {"price": "101.1"}],
            "bids": [{"price": "100.8"}, {"price": "100.9"}],
        }
        b1, a1 = arb_rank.extract_best_bid_ask(orders)
        b2, a2 = price_arb_cn.extract_best_bid_ask(orders)
        self.assertEqual((b1, a1), (100.9, 101.1))
        self.assertEqual((b2, a2), (100.9, 101.1))

    def test_choose_lighter_reference_price_prefers_orderbook_mid(self):
        details = {
            "code": 200,
            "order_book_details": [{"last_trade_price": "100.0"}],
        }
        orders = {
            "code": 200,
            "asks": [{"price": "101.0"}],
            "bids": [{"price": "99.0"}],
        }
        p1, src1, bid1, ask1 = arb_rank.choose_lighter_reference_price(details, orders)
        p2, src2, bid2, ask2 = price_arb_cn.choose_lighter_reference_price(details, orders)
        self.assertEqual((p1, src1, bid1, ask1), (100.0, "orderbook_mid", 99.0, 101.0))
        self.assertEqual((p2, src2, bid2, ask2), (100.0, "orderbook_mid", 99.0, 101.0))


if __name__ == "__main__":
    unittest.main()
