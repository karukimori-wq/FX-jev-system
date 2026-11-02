"""SQLite-only mock lifecycle tests: no broker credentials or orders."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.positions import CloseExecution, PositionCloseProof
from proof_helpers import proof as make_entry_proof


class PositionsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "position.sqlite")
        self.l = Ledger(self.path)
        self.version = self.l.configure("alpha", Mode.LIVE, 12)
        self.demo_version = self.l.configure("alpha", Mode.DEMO, 12)
        self.l.reserve("entry1", "alpha", "run1", Mode.LIVE, 8,
                       self.version, account_id="acctA")
        self.l.mark_unknown("entry1")
        self.l.reconcile("entry1", proof=self.entry_proof())

    def entry_proof(self, **kwargs):
        p = make_entry_proof("entry1", qty=8, filled=8,
                             status=OrderStatus.FILLED,
                             strategy="alpha", run="run1", mode=Mode.LIVE,
                             broker_id="broker-entry-1")
        p = replace(p, order=replace(p.order, account_id="acctA"),
                    position=replace(p.position, account_id="acctA"))
        return replace(p, **kwargs)

    def close_proof(self, qty=3, *, remaining=None, close_id="close1", **kwargs):
        now = time.time_ns()
        result = PositionCloseProof(
            account_id="acctA", client_id="entry1",
            broker_entry_order_id="broker-entry-1", strategy_id="alpha",
            run_id="run1", mode=Mode.LIVE, close_order_id=close_id,
            close_status=OrderStatus.FILLED, requested_quantity=qty,
            executions=(CloseExecution(close_id, "fill-" + close_id,
                                       qty, now - 3_000_000),),
            remaining_quantity=8 - qty if remaining is None else remaining,
            order_final_at_ns=now - 2_000_000,
            snapshot_at_ns=now,
            order_history_complete=True, fill_history_complete=True,
            position_snapshot_complete=True)
        return replace(result, **kwargs)

    def assert_rejected_unchanged(self, evidence):
        before = self.l.position_remaining("entry1")
        risk = self.l.active_exposure("alpha", Mode.LIVE)
        with self.assertRaises(SafetyError):
            self.l.close_position(proof=evidence)
        self.assertEqual(self.l.position_remaining("entry1"), before)
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE), risk)

    def test_entry_persisted_by_reconcile(self):
        self.assertEqual(self.l.position_remaining("entry1"), 8)
        self.assertEqual(self.l.get("entry1").account_id, "acctA")

    def test_partial_close_releases_only_closed_amount(self):
        digest = self.l.close_position(proof=self.close_proof())
        self.assertEqual(len(digest), 64)
        self.assertEqual(self.l.position_remaining("entry1"), 5)
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE), 5)
        self.l.reserve("entry2", "alpha", "run2", Mode.LIVE, 7,
                       self.version, account_id="acctA")

    def test_full_close_releases_all(self):
        self.l.close_position(proof=self.close_proof(8))
        self.assertEqual(self.l.position_remaining("entry1"), 0)
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE), 0)

    def test_sequential_partial_closes(self):
        self.l.close_position(proof=self.close_proof(3))
        self.l.close_position(proof=self.close_proof(
            2, remaining=3, close_id="close2"))
        self.assertEqual(self.l.position_remaining("entry1"), 3)

    def test_close_after_strategy_stopped(self):
        self.l.configure("alpha", Mode.LIVE, 12, enabled=False)
        self.l.close_position(proof=self.close_proof(3))
        self.assertEqual(self.l.position_remaining("entry1"), 5)
        with self.assertRaises(SafetyError):
            self.l.reserve("entry2", "alpha", "run2", Mode.LIVE,
                           1, self.version, account_id="acctA")

    def test_persists_across_restart(self):
        self.l.close_position(proof=self.close_proof())
        next_process = Ledger(self.path)
        self.assertEqual(next_process.position_remaining("entry1"), 5)
        self.assertEqual(next_process.active_exposure("alpha", Mode.LIVE), 5)

    def test_duplicate_close_order_id(self):
        p = self.close_proof(3)
        self.l.close_position(proof=p)
        self.assert_rejected_unchanged(replace(p, remaining_quantity=2))

    def test_duplicate_execution_across_different_close_orders(self):
        p = self.close_proof(3)
        self.l.close_position(proof=p)
        second = self.close_proof(2, remaining=3, close_id="close2")
        self.assert_rejected_unchanged(replace(second, executions=(
            replace(second.executions[0], execution_id=p.executions[0].execution_id),)))

    def test_partial_close_does_not_over_release(self):
        self.assert_rejected_unchanged(self.close_proof(3, remaining=0))

    def test_overclose(self):
        self.assert_rejected_unchanged(self.close_proof(9, remaining=0))

    def test_close_order_must_be_final(self):
        self.assert_rejected_unchanged(self.close_proof(close_status=OrderStatus.UNKNOWN))

    def test_full_close_needs_full_execution_history(self):
        self.assert_rejected_unchanged(self.close_proof(requested_quantity=6))

    def test_partial_cancel_can_be_reconciled(self):
        p = self.close_proof(3, close_status=OrderStatus.CANCELED,
                             requested_quantity=8)
        self.l.close_position(proof=p)
        self.assertEqual(self.l.position_remaining("entry1"), 5)

    def test_closing_order_cannot_be_entry_order(self):
        self.assert_rejected_unchanged(self.close_proof(close_order_id="broker-entry-1"))

    def test_missing_order_history(self):
        self.assert_rejected_unchanged(self.close_proof(order_history_complete=False))

    def test_missing_fill_history(self):
        self.assert_rejected_unchanged(self.close_proof(fill_history_complete=False))

    def test_missing_position_snapshot(self):
        self.assert_rejected_unchanged(self.close_proof(position_snapshot_complete=False))

    def test_wrong_account(self):
        self.assert_rejected_unchanged(self.close_proof(account_id="acctB"))

    def test_wrong_strategy(self):
        self.assert_rejected_unchanged(self.close_proof(strategy_id="other"))

    def test_wrong_run(self):
        self.assert_rejected_unchanged(self.close_proof(run_id="other"))

    def test_wrong_mode(self):
        self.assert_rejected_unchanged(self.close_proof(mode=Mode.DEMO))

    def test_wrong_entry_broker_id(self):
        self.assert_rejected_unchanged(self.close_proof(broker_entry_order_id="other"))

    def test_stale_snapshot(self):
        p = self.close_proof()
        old = time.time_ns() - 70_000_000_000
        self.assert_rejected_unchanged(replace(
            p, order_final_at_ns=old - 1_000_000, snapshot_at_ns=old))

    def test_snapshot_before_final_order(self):
        p = self.close_proof()
        self.assert_rejected_unchanged(replace(
            p, snapshot_at_ns=p.order_final_at_ns - 1))

    def test_close_fill_from_wrong_order(self):
        p = self.close_proof()
        self.assert_rejected_unchanged(replace(
            p, executions=(replace(p.executions[0], close_order_id="other"),)))

    def test_duplicate_fill_in_same_batch(self):
        p = self.close_proof(3)
        self.assert_rejected_unchanged(replace(p, executions=(p.executions[0], p.executions[0])))

    def test_zero_or_non_integer_fill(self):
        for n in (0, -1, 1.0, True):
            p = self.close_proof()
            self.assert_rejected_unchanged(replace(
                p, executions=(replace(p.executions[0], quantity=n),)))

    def test_no_position_without_entry_reconciliation(self):
        self.l.reserve("orphan", "alpha", "run9", Mode.LIVE, 1,
                       self.version, account_id="acctA")
        self.assertIsNone(self.l.position_remaining("orphan"))
        self.assert_rejected_unchanged(replace(
            self.close_proof(1), client_id="orphan", remaining_quantity=0))

    def test_demo_isolation(self):
        self.l.reserve("demo-entry", "alpha", "run1", Mode.DEMO, 5,
                       self.demo_version, account_id="demo-account")
        live_before = self.l.active_exposure("alpha", Mode.LIVE)
        demo_before = self.l.active_exposure("alpha", Mode.DEMO)
        self.l.close_position(proof=self.close_proof(3))
        self.assertEqual(self.l.active_exposure("alpha", Mode.LIVE), live_before - 3)
        self.assertEqual(self.l.active_exposure("alpha", Mode.DEMO), demo_before)

    def test_immutable_batch_audit(self):
        p = self.close_proof()
        digest = self.l.close_position(proof=p)
        with closing(sqlite3.connect(self.path)) as db:
            record = db.execute("""SELECT quantity,remaining_quantity,evidence_sha256
                FROM position_close_batches WHERE account_id='acctA' AND close_order_id='close1'""").fetchone()
            n = db.execute("SELECT COUNT(*) FROM position_close_executions").fetchone()[0]
        self.assertEqual(record, (3, 5, digest))
        self.assertEqual(n, 1)

    def test_two_concurrent_close_proofs_one_wins(self):
        p1 = self.close_proof(3, close_id="race1")
        p2 = self.close_proof(3, close_id="race2")
        barrier = threading.Barrier(2)
        results = []
        def worker(p):
            barrier.wait()
            try:
                self.l.close_position(proof=p)
                results.append(True)
            except SafetyError:
                results.append(False)
        workers = [threading.Thread(target=worker, args=(p,)) for p in (p1, p2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.l.position_remaining("entry1"), 5)

    def test_reject_close_without_structured_evidence(self):
        self.assert_rejected_unchanged(True)


if __name__ == "__main__":
    unittest.main()
