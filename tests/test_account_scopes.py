"""Account-scoped broker-free P0 reservations; no live trading."""
import tempfile
import threading
import unittest
from pathlib import Path
from dataclasses import replace

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from proof_helpers import proof


class AccountScopeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dbpath = str(Path(self.tmp.name) / "scoped.sqlite")
        self.l = Ledger(self.dbpath)
        self.av_a = self.l.configure_account("acctA", Mode.LIVE, 10)
        self.av_b = self.l.configure_account("acctB", Mode.LIVE, 6)
        self.av_demo = self.l.configure_account("acctA", Mode.DEMO, 9)
        self.sv_a = self.l.configure("alpha", Mode.LIVE, 7, account_id="acctA")
        self.sv_beta = self.l.configure("beta", Mode.LIVE, 8, account_id="acctA")
        self.sv_b = self.l.configure("alpha", Mode.LIVE, 6, account_id="acctB")
        self.sv_demo = self.l.configure("alpha", Mode.DEMO, 9, account_id="acctA")

    def order(self, client, qty, *, account="acctA", strategy="alpha",
              mode=Mode.LIVE, run="r1", av=None, sv=None):
        versions = {
            ("acctA", Mode.LIVE): self.av_a,
            ("acctB", Mode.LIVE): self.av_b,
            ("acctA", Mode.DEMO): self.av_demo,
        }
        strategies = {
            ("acctA", "alpha", Mode.LIVE): self.sv_a,
            ("acctA", "beta", Mode.LIVE): self.sv_beta,
            ("acctB", "alpha", Mode.LIVE): self.sv_b,
            ("acctA", "alpha", Mode.DEMO): self.sv_demo,
        }
        account_version = versions[(account, mode)] if av is None else av
        strategy_version = strategies[(account, strategy, mode)] if sv is None else sv
        return self.l.reserve(client, strategy, run, mode, qty, strategy_version,
                              account_id=account,
                              expected_account_version=account_version)

    def test_two_accounts_independent_strategy_caps(self):
        self.order("A1", 7)
        self.order("B1", 6, account="acctB")
        self.assertEqual(self.l.account_exposure("acctA", Mode.LIVE), 7)
        self.assertEqual(self.l.account_exposure("acctB", Mode.LIVE), 6)

    def test_strategy_exposure_reports_by_account(self):
        self.order("A1", 4)
        self.order("B1", 5, account="acctB")
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE,
                                               account_id="acctA"), 4)
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE,
                                               account_id="acctB"), 5)
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE), 9)

    def test_account_risk_zero_cap_fail_closed(self):
        version = self.l.configure_account("acctA", Mode.LIVE, 0)
        with self.assertRaises(SafetyError):
            self.order("A1", 1, av=version)
        self.order("B1", 1, account="acctB")

    def test_total_account_budget_across_strategies(self):
        self.order("A1", 6)
        with self.assertRaises(SafetyError):
            self.order("A2", 5, strategy="beta")
        self.order("A3", 4, strategy="beta")
        self.assertEqual(self.l.account_exposure("acctA", Mode.LIVE), 10)

    def test_strategy_cap_across_multiple_runs(self):
        self.order("A1", 5, run="first")
        with self.assertRaises(SafetyError):
            self.order("A2", 3, run="second")
        self.order("A3", 2, run="second")

    def test_exhausted_account_does_not_affect_other_account(self):
        self.order("A1", 7)
        self.order("A2", 3, strategy="beta")
        self.order("B1", 6, account="acctB")
        self.assertEqual(self.l.account_exposure("acctB", Mode.LIVE), 6)

    def test_unknown_halts_its_account_only(self):
        self.order("A1", 2)
        self.l.mark_unknown("A1")
        with self.assertRaises(SafetyError):
            self.order("A2", 2, strategy="beta")
        self.order("B1", 5, account="acctB")

    def test_unknown_does_not_halt_demo(self):
        self.order("A1", 2)
        self.l.mark_unknown("A1")
        self.order("D1", 9, mode=Mode.DEMO)
        self.assertEqual(self.l.account_exposure("acctA", Mode.DEMO), 9)

    def test_demo_unknown_does_not_halt_live(self):
        self.order("D1", 5, mode=Mode.DEMO)
        self.l.mark_unknown("D1")
        self.order("A1", 6)

    def test_stale_account_version_rejected(self):
        self.l.configure_account("acctA", Mode.LIVE, 8)
        with self.assertRaises(SafetyError):
            self.order("A1", 3)
        self.order("B1", 3, account="acctB")

    def test_stale_strategy_version_rejected(self):
        self.l.configure("alpha", Mode.LIVE, 5, account_id="acctA")
        with self.assertRaises(SafetyError):
            self.order("A1", 2)
        self.order("B1", 4, account="acctB")

    def test_account_disable_halts_only_one(self):
        self.l.configure_account("acctA", Mode.LIVE, 10, enabled=False)
        with self.assertRaises(SafetyError):
            self.order("A1", 2, av=self.av_a + 1)
        self.order("B1", 4, account="acctB")

    def test_strategy_disable_halts_only_one(self):
        self.l.configure("alpha", Mode.LIVE, 7, enabled=False, account_id="acctA")
        with self.assertRaises(SafetyError):
            self.order("A1", 2, sv=self.sv_a + 1)
        self.order("A2", 4, strategy="beta")

    def test_missing_account_config_blocks(self):
        with self.assertRaises(SafetyError):
            self.l.configure("alpha", Mode.LIVE, 5, account_id="unregistered")
        with self.assertRaises(SafetyError):
            self.l.reserve("X", "alpha", "r", Mode.LIVE, 1, 1,
                           account_id="unregistered", expected_account_version=1)

    def test_missing_expected_account_version_blocks(self):
        with self.assertRaises(SafetyError):
            self.l.reserve("A1", "alpha", "r", Mode.LIVE, 1, self.sv_a,
                           account_id="acctA")

    def test_invalid_account_config(self):
        for val in ("", "legacy", " ", None):
            with self.assertRaises(SafetyError):
                self.l.configure_account(val, Mode.LIVE, 10)
        for amount in (True, 1.5, -1):
            with self.assertRaises(SafetyError):
                self.l.configure_account("acctC", Mode.LIVE, amount)

    def test_account_over_limit_after_settings_reduction(self):
        self.order("A1", 7)
        changed = self.l.configure_account("acctA", Mode.LIVE, 3)
        with self.assertRaises(SafetyError):
            self.order("A2", 1, strategy="beta", av=changed)
        self.assertEqual(self.l.account_exposure("acctA", Mode.LIVE), 7)

    def test_account_exposure_survives_restart(self):
        self.order("A1", 7)
        self.order("B1", 5, account="acctB")
        after = Ledger(self.dbpath)
        self.assertEqual(after.account_exposure("acctA", Mode.LIVE), 7)
        self.assertEqual(after.account_exposure("acctB", Mode.LIVE), 5)

    def test_same_client_id_across_accounts_rejected(self):
        self.order("shared", 3)
        with self.assertRaises(SafetyError):
            self.order("shared", 2, account="acctB")

    def test_concurrent_strategies_cannot_overdraw_account(self):
        barrier = threading.Barrier(2)
        outcomes = []
        def worker(client, strategy):
            barrier.wait()
            try:
                self.order(client, 7, strategy=strategy)
                outcomes.append(True)
            except SafetyError:
                outcomes.append(False)
        threads = [threading.Thread(target=worker, args=("A1","alpha")),
                   threading.Thread(target=worker, args=("A2","beta"))]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(sorted(outcomes), [False, True])
        self.assertEqual(self.l.account_exposure("acctA", Mode.LIVE), 7)

    def test_unattributed_legacy_reserved_risk_quarantined(self):
        v = self.l.configure("legacy-s", Mode.LIVE, 5)
        self.l.reserve("old1", "legacy-s", "r", Mode.LIVE, 2, v)
        with self.assertRaises(SafetyError):
            self.order("A1", 1)
        with self.assertRaises(SafetyError):
            self.order("B1", 1, account="acctB")

    def test_unattributed_legacy_filled_risk_quarantined(self):
        v = self.l.configure("legacy-s", Mode.LIVE, 5)
        self.l.reserve("old1", "legacy-s", "r", Mode.LIVE, 2, v)
        p = proof("old1", qty=2, filled=2, status=OrderStatus.FILLED,
                  strategy="legacy-s", run="r")
        self.l.reconcile("old1", proof=p)
        with self.assertRaises(SafetyError):
            self.order("B1", 1, account="acctB")

    def test_legacy_canceled_fully_releases_quarantine(self):
        v = self.l.configure("legacy-s", Mode.LIVE, 5)
        self.l.reserve("old1", "legacy-s", "r", Mode.LIVE, 2, v)
        self.l.mark_unknown("old1")
        with self.assertRaises(SafetyError):
            self.order("B1", 1, account="acctB")
        self.l.reconcile("old1", proof=proof("old1", qty=2,
                          strategy="legacy-s", run="r"))
        self.order("B2", 1, account="acctB")

    def test_legacy_unknown_does_not_halt_other_mode(self):
        v = self.l.configure("legacy-s", Mode.LIVE, 5)
        self.l.reserve("old1", "legacy-s", "r", Mode.LIVE, 2, v)
        self.l.mark_unknown("old1")
        self.order("D1", 3, mode=Mode.DEMO)


if __name__ == "__main__":
    unittest.main()
