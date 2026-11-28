import os
import tempfile
import unittest
from decimal import Decimal

import kalshi_unified_bot as bot


class KalshiUnifiedCoreTests(unittest.TestCase):
    def test_decimal_parser_handles_fixed_point_api_values(self):
        self.assertEqual(bot.dec("0.4200"), Decimal("0.4200"))
        self.assertIsNone(bot.dec("not-a-price"))

    def test_direction_mapping_requires_explicit_contract_language(self):
        self.assertEqual(bot.yes_direction({"_display": "Bitcoin above $100,000?"}), "UP")
        self.assertEqual(bot.yes_direction({"_display": "Ethereum below $3,000?"}), "DOWN")
        self.assertIsNone(bot.yes_direction({"_display": "Bitcoin price tomorrow?"}))

    def test_quote_derives_missing_yes_ask_from_no_bid(self):
        quote = bot.quote_market({
            "yes_bid_dollars": "0.40",
            "yes_ask_dollars": None,
            "no_bid_dollars": "0.55",
            "no_ask_dollars": "0.60",
        })
        self.assertEqual(quote["yes_ask"], Decimal("0.45"))
        self.assertEqual(quote["spread"], Decimal("0.05"))

    def test_db_initializes_in_isolated_path(self):
        old = bot.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as tmp:
                bot.DB_PATH = os.path.join(tmp, "test.sqlite3")
                bot.init_db()
                import sqlite3
                with sqlite3.connect(bot.DB_PATH) as db:
                    tables = {row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertTrue({"market_samples", "active_cards", "paper_events",
                                "market_outcomes"}.issubset(tables))
        finally:
            bot.DB_PATH = old


if __name__ == "__main__":
    unittest.main()
