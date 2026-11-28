import os
import tempfile
import unittest
from decimal import Decimal

from kalshi_unified import core


class KalshiCoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "ledger.sqlite3")
        core.init_ledger(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def market(self, **kwargs):
        data = {
            "ticker": "KXBTC-TEST",
            "status": "open",
            "title": "Bitcoin above $100,000 at settlement?",
            "subtitle": "Test contract",
            "yes_sub_title": "Yes",
            "rules_primary": "Settlement based on official BTC reference price published at the specified settlement time.",
            "close_time": "2027-01-01T00:00:00Z",
        }
        data.update(kwargs)
        return data

    def test_decimal_fixed_point_and_invalid(self):
        self.assertEqual(core.decimal("0.420000"), Decimal("0.420000"))
        self.assertIsNone(core.decimal("not-price"))
        self.assertIsNone(core.decimal("NaN"))

    def test_contract_classifier_fails_closed(self):
        ok, _ = core.contract_is_eligible(self.market())
        self.assertTrue(ok)
        ok, _ = core.contract_is_eligible(self.market(title="Bitcoin price tomorrow?"))
        self.assertFalse(ok)
        ok, _ = core.contract_is_eligible(self.market(rules_primary=""))
        self.assertFalse(ok)
        ok, _ = core.contract_is_eligible(self.market(title="Bitcoin and Ethereum above $100?"))
        self.assertFalse(ok)

    def test_quote_derives_complement_and_rejects_crossed(self):
        q = core.make_quote({
            "yes_bid_dollars": "0.40", "yes_ask_dollars": None,
            "no_bid_dollars": "0.55", "no_ask_dollars": "0.60"})
        self.assertEqual(q["yes_ask"], Decimal("0.45"))
        self.assertEqual(q["yes_spread"], Decimal("0.05"))
        self.assertIsNone(core.make_quote({
            "yes_bid_dollars": "0.70", "yes_ask_dollars": "0.60",
            "no_bid_dollars": "0.30", "no_ask_dollars": "0.40"}))

    def test_signal_requires_second_confirmation_and_quote_quality(self):
        q = core.make_quote({
            "yes_bid_dollars": "0.40", "yes_ask_dollars": "0.42",
            "no_bid_dollars": "0.58", "no_ask_dollars": "0.60"})
        good = core.signal_gate(quote=q, momentum_pct=0.9, sample_age=30)
        self.assertEqual(good["side"], "YES")
        weak = core.signal_gate(quote=q, momentum_pct=0.48, sample_age=30)
        self.assertEqual(weak["action"], "WAIT")
        stale = core.signal_gate(quote=q, momentum_pct=1.5, sample_age=120)
        self.assertEqual(stale["action"], "WAIT")

    def test_sample_momentum_and_age(self):
        m1, _ = core.record_sample(self.db, "T", Decimal("0.50"), now=100)
        self.assertIsNone(m1)
        m2, age = core.record_sample(self.db, "T", Decimal("0.55"), now=140, lookback=30)
        self.assertAlmostEqual(m2, 10.0)
        self.assertEqual(age, 40)

    def test_managed_trade_and_initial_settlement_are_separate(self):
        signal = {"side": "YES", "entry": Decimal("0.40"), "action": "SCALP YES",
                  "target_pct": 0.03, "stop_pct": 0.025, "max_hold_seconds": 90}
        self.assertTrue(core.open_paper_trade(self.db, "T", "BTC", signal, now=10))
        result = core.close_paper_trade(self.db, "T", Decimal("0.50"), "target", now=20)
        self.assertAlmostEqual(result["pnl_pct"], 25.0)
        self.assertTrue(core.record_settlement(self.db, "T", "NO"))
        self.assertFalse(core.record_settlement(self.db, "T", "YES"))
        s = core.stats(self.db)
        self.assertEqual(s["wins"], 0)
        self.assertEqual(s["losses"], 1)
        self.assertEqual(s["managed_closed_trades"], 1)
        self.assertAlmostEqual(s["managed_avg_pnl_pct"], 25.0)

    def test_streaks_count_settled_initial_calls_only(self):
        for ticker, side, result in (("A","YES","YES"),("B","NO","NO"),("C","YES","YES")):
            core.open_paper_trade(self.db, ticker, "ETH",
                {"side":side,"entry":Decimal("0.4"),"action":"SCALP YES"}, now=1)
            core.record_settlement(self.db, ticker, result)
        s = core.stats(self.db)
        self.assertEqual(s["current_initial_streak"], 3)
        self.assertEqual(s["best_initial_win_streak"], 3)
        self.assertEqual(s["win_rate_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()
