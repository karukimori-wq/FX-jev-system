"""Real-process SIGKILL and delayed-event fault injection for a broker-free PoC.

Only SQLite files under TemporaryDirectory are touched. No network, broker
credentials, order sender, or live trading functions exist in these tests.
The crash tests require POSIX; unittest skips them on other platforms.
"""
from contextlib import closing
from dataclasses import replace
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.positions import CloseExecution, PositionCloseProof
from proof_helpers import proof as make_entry_proof


class FaultInjectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "fault.sqlite")
        self.l = Ledger(self.path)
        self.av = self.l.configure_account("A", Mode.LIVE, 12)
        self.sv = self.l.configure("s", Mode.LIVE, 12, account_id="A")
        self.bv = self.l.configure_account("B", Mode.LIVE, 12)
        self.bs = self.l.configure("s", Mode.LIVE, 12, account_id="B")
        self.dv = self.l.configure_account("A", Mode.DEMO, 12)
        self.ds = self.l.configure("s", Mode.DEMO, 12, account_id="A")

    def reserve(self, cid="entry", quantity=8, *, account="A", mode=Mode.LIVE):
        version, account_version = (
            (self.sv, self.av) if (account, mode) == ("A", Mode.LIVE)
            else (self.bs, self.bv) if (account, mode) == ("B", Mode.LIVE)
            else (self.ds, self.dv)
        )
        return self.l.reserve(cid, "s", "r", mode, quantity, version,
                              account_id=account,
                              expected_account_version=account_version)

    def entry_proof(self, *, cid="entry", quantity=8, filled=8,
                    status=OrderStatus.FILLED):
        evidence = make_entry_proof(
            cid, qty=quantity, filled=filled, status=status,
            strategy="s", run="r", mode=Mode.LIVE,
            broker_id="broker-" + cid)
        return replace(
            evidence,
            order=replace(evidence.order, account_id="A"),
            position=replace(evidence.position, account_id="A"))

    def prepared_position(self):
        self.reserve()
        self.l.mark_unknown("entry")
        self.l.reconcile("entry", proof=self.entry_proof())
        self.assertEqual(self.l.position_remaining("entry"), 8)

    def close_proof(self, *, close_id="close-1", quantity=3,
                    remaining=5, now_ns=None):
        now_ns = time.time_ns() if now_ns is None else now_ns
        return PositionCloseProof(
            account_id="A", client_id="entry",
            broker_entry_order_id="broker-entry", strategy_id="s",
            run_id="r", mode=Mode.LIVE,
            close_order_id=close_id, close_status=OrderStatus.FILLED,
            requested_quantity=quantity,
            executions=(CloseExecution(
                close_id, "exec-" + close_id, quantity,
                now_ns - 3_000_000),),
            remaining_quantity=remaining,
            order_final_at_ns=now_ns - 2_000_000,
            snapshot_at_ns=now_ns,
            order_history_complete=True, fill_history_complete=True,
            position_snapshot_complete=True)

    def assert_integrity(self):
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def _kill_child_at_marker(self, script):
        if os.name != "posix":
            self.skipTest("SIGKILL fault injection requires POSIX")
        marker = Path(self.tmp.name) / ("worker-" + str(time.time_ns()))
        prefix = ("import sys, time, sqlite3\n"
                  "from pathlib import Path\n"
                  "from fxjev.ledger import Ledger, Mode, OrderStatus\n")
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", prefix + textwrap.dedent(script),
             self.path, str(marker)],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 8
            while not marker.exists():
                if child.poll() is not None:
                    details = child.stderr.read()
                    self.fail("fault worker exited before marker: " + details[-3000:])
                if time.monotonic() >= deadline:
                    self.fail("fault worker timed out before marker")
                time.sleep(.02)
            os.kill(child.pid, signal.SIGKILL)
            self.assertEqual(child.wait(timeout=6), -signal.SIGKILL)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=6)
            if child.stderr is not None:
                child.stderr.close()

    def test_sigkill_after_committed_reservation_survives(self):
        self._kill_child_at_marker("""
            l=Ledger(sys.argv[1])
            l.reserve("durable","s","r",Mode.LIVE,7,1,
                      account_id="A",expected_account_version=1)
            Path(sys.argv[2]).write_text("committed")
            time.sleep(60)
        """)
        after = Ledger(self.path)
        self.assertEqual(after.account_exposure("A", Mode.LIVE), 7)
        self.assertEqual(after.get("durable").status, OrderStatus.RESERVED)
        with self.assertRaises(SafetyError):
            after.reserve("durable","s","r",Mode.LIVE,7,1,
                          account_id="A",expected_account_version=1)
        self.assertEqual(after.recover_unresolved_intents(), ("durable",))
        self.assertEqual(after.get("durable").status, OrderStatus.UNKNOWN)
        self.assertEqual(after.account_exposure("A", Mode.LIVE), 7)
        self.assert_integrity()

    def test_sigkill_during_uncommitted_reservation_rolls_back(self):
        self._kill_child_at_marker("""
            l=Ledger(sys.argv[1])
            with l._tx() as db:
                db.execute("INSERT INTO orders(client_id,strategy_id,run_id,mode,account_id,quantity,filled_quantity,status) VALUES('staged','s','r','live','A',8,0,'reserved')")
                Path(sys.argv[2]).write_text("uncommitted")
                time.sleep(60)
        """)
        after=Ledger(self.path)
        self.assertIsNone(after.get("staged"))
        self.assertEqual(after.account_exposure("A",Mode.LIVE),0)
        after.reserve("safe","s","r",Mode.LIVE,12,1,
                      account_id="A",expected_account_version=1)
        self.assert_integrity()

    def test_sigkill_during_uncommitted_unknown_transition_rolls_back(self):
        self.reserve("pending", 8)
        self._kill_child_at_marker("""
            l=Ledger(sys.argv[1])
            with l._tx() as db:
                db.execute("UPDATE orders SET status='unknown' WHERE client_id='pending'")
                Path(sys.argv[2]).write_text("uncommitted")
                time.sleep(60)
        """)
        after=Ledger(self.path)
        self.assertEqual(after.get("pending").status,OrderStatus.RESERVED)
        self.assertEqual(after.account_exposure("A",Mode.LIVE),8)
        self.assertEqual(after.recover_unresolved_intents(),("pending",))
        self.assertEqual(after.get("pending").status,OrderStatus.UNKNOWN)
        self.assert_integrity()

    def test_sigkill_after_committed_unknown_keeps_halt(self):
        self.reserve("pending", 4)
        self._kill_child_at_marker("""
            l=Ledger(sys.argv[1])
            l.mark_unknown("pending")
            Path(sys.argv[2]).write_text("committed")
            time.sleep(60)
        """)
        after=Ledger(self.path)
        self.assertEqual(after.get("pending").status,OrderStatus.UNKNOWN)
        with self.assertRaises(SafetyError):
            after.reserve("new","s","r",Mode.LIVE,1,1,
                          account_id="A",expected_account_version=1)
        after.reserve("other","s","r",Mode.LIVE,6,1,
                      account_id="B",expected_account_version=1)
        self.assert_integrity()

    def test_sigkill_during_uncommitted_position_reduction_rolls_back(self):
        self.prepared_position()
        self._kill_child_at_marker("""
            l=Ledger(sys.argv[1])
            with l._tx() as db:
                db.execute("UPDATE position_balances SET remaining_quantity=0 WHERE client_id='entry'")
                Path(sys.argv[2]).write_text("uncommitted")
                time.sleep(60)
        """)
        after=Ledger(self.path)
        self.assertEqual(after.position_remaining("entry"),8)
        self.assertEqual(after.account_exposure("A",Mode.LIVE),8)
        self.assert_integrity()

    def test_sigkill_after_committed_partial_close_persists_and_deduplicates(self):
        self.prepared_position()
        self._kill_child_at_marker("""
            from fxjev.positions import CloseExecution, PositionCloseProof
            l=Ledger(sys.argv[1])
            now=time.time_ns()
            p=PositionCloseProof(
                account_id="A",client_id="entry",broker_entry_order_id="broker-entry",
                strategy_id="s",run_id="r",mode=Mode.LIVE,close_order_id="close-kill",
                close_status=OrderStatus.FILLED,requested_quantity=3,
                executions=(CloseExecution("close-kill","exec-close-kill",3,
                                           now-3_000_000),),
                remaining_quantity=5,order_final_at_ns=now-2_000_000,
                snapshot_at_ns=now,order_history_complete=True,
                fill_history_complete=True,position_snapshot_complete=True)
            l.close_position(proof=p)
            Path(sys.argv[2]).write_text("committed")
            time.sleep(60)
        """)
        after=Ledger(self.path)
        self.assertEqual(after.position_remaining("entry"),5)
        self.assertEqual(after.account_exposure("A",Mode.LIVE),5)
        with self.assertRaises(SafetyError):
            after.close_position(proof=self.close_proof(
                close_id="close-kill",quantity=3,remaining=2))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM position_close_batches").fetchone()[0],1)
        self.assert_integrity()

    def test_startup_recovery_all_pending_is_idempotent_and_audited(self):
        self.reserve("first", 2)
        self.reserve("second", 3)
        restarted=Ledger(self.path)
        self.assertEqual(restarted.recover_unresolved_intents(),("first","second"))
        self.assertEqual(restarted.recover_unresolved_intents(),())
        self.assertEqual(restarted.account_exposure("A",Mode.LIVE),5)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM startup_recovery_audit").fetchone()[0],2)
        for cid in ("first","second"):
            self.assertEqual(restarted.get(cid).status,OrderStatus.UNKNOWN)
        self.assert_integrity()

    def test_recovery_halts_affected_account_but_not_unrelated_account(self):
        self.reserve("pending", 5)
        restarted=Ledger(self.path)
        self.assertEqual(restarted.recover_unresolved_intents(),("pending",))
        with self.assertRaises(SafetyError):
            restarted.reserve("blocked","s","r",Mode.LIVE,1,1,
                              account_id="A",expected_account_version=1)
        restarted.reserve("independent","s","r",Mode.LIVE,9,1,
                          account_id="B",expected_account_version=1)
        self.assertEqual(restarted.account_exposure("B",Mode.LIVE),9)

    def test_recovery_live_does_not_halt_demo(self):
        self.reserve("pending", 5)
        restarted=Ledger(self.path)
        restarted.recover_unresolved_intents()
        restarted.reserve("demo","s","r",Mode.DEMO,8,1,
                          account_id="A",expected_account_version=1)
        self.assertEqual(restarted.account_exposure("A",Mode.DEMO),8)

    def test_stale_safety_version_persists_after_restart(self):
        stale=self.sv
        self.l.configure("s",Mode.LIVE,3,account_id="A")
        restarted=Ledger(self.path)
        with self.assertRaises(SafetyError):
            restarted.reserve("old-version","s","r",Mode.LIVE,1,stale,
                              account_id="A",expected_account_version=1)
        self.assertIsNone(restarted.get("old-version"))

    def test_delayed_incomplete_order_evidence_never_releases_risk(self):
        self.reserve("pending", 6)
        self.l.mark_unknown("pending")
        p=self.entry_proof(cid="pending",quantity=6,filled=2,
                           status=OrderStatus.CANCELED)
        with self.assertRaises(SafetyError):
            self.l.reconcile("pending",proof=replace(p,fill_history_complete=False))
        after=Ledger(self.path)
        self.assertEqual(after.get("pending").status,OrderStatus.UNKNOWN)
        self.assertEqual(after.account_exposure("A",Mode.LIVE),6)
        # Later a complete, freshly captured mock snapshot arrives.
        after.reconcile("pending",proof=self.entry_proof(
            cid="pending",quantity=6,filled=2,status=OrderStatus.CANCELED))
        self.assertEqual(after.account_exposure("A",Mode.LIVE),2)

    def test_late_duplicate_order_state_cannot_overwrite_finalized_entry(self):
        self.prepared_position()
        after=Ledger(self.path)
        with self.assertRaises(SafetyError):
            after.reconcile("entry",proof=self.entry_proof(
                quantity=8,filled=0,status=OrderStatus.CANCELED))
        self.assertEqual(after.position_remaining("entry"),8)
        self.assertEqual(after.get("entry").status,OrderStatus.FILLED)

    def test_out_of_order_close_before_entry_is_rejected_then_succeeds(self):
        self.reserve()
        p=self.close_proof()
        with self.assertRaises(SafetyError):
            self.l.close_position(proof=p)
        self.assertEqual(self.l.account_exposure("A",Mode.LIVE),8)
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.entry_proof())
        self.l.close_position(proof=self.close_proof())
        self.assertEqual(self.l.account_exposure("A",Mode.LIVE),5)

    def test_delayed_duplicate_close_after_restart_does_not_double_release(self):
        self.prepared_position()
        first=self.close_proof()
        self.l.close_position(proof=first)
        restarted=Ledger(self.path)
        self.assertEqual(restarted.position_remaining("entry"),5)
        with self.assertRaises(SafetyError):
            restarted.close_position(proof=first)
        self.assertEqual(restarted.account_exposure("A",Mode.LIVE),5)

    def test_stale_close_snapshot_then_fresh_recovery(self):
        self.prepared_position()
        stale_time=time.time_ns()-60_000_000_000
        stale=self.close_proof(now_ns=stale_time)
        with self.assertRaises(SafetyError):
            self.l.close_position(proof=stale)
        self.assertEqual(self.l.account_exposure("A",Mode.LIVE),8)
        self.l.close_position(proof=self.close_proof())
        self.assertEqual(self.l.account_exposure("A",Mode.LIVE),5)

    def test_sigkill_writer_lock_released_for_new_reservation(self):
        self._kill_child_at_marker("""
            l=Ledger(sys.argv[1])
            with l._tx() as db:
                db.execute("UPDATE account_limits SET max_quantity=2 WHERE account_id='A' AND mode='live'")
                Path(sys.argv[2]).write_text("locked")
                time.sleep(60)
        """)
        # Uncommitted reduced limit does not survive, and locks are released.
        restarted=Ledger(self.path)
        restarted.reserve("safe","s","r",Mode.LIVE,12,1,
                          account_id="A",expected_account_version=1)
        self.assertEqual(restarted.account_exposure("A",Mode.LIVE),12)
        self.assert_integrity()


if __name__ == "__main__":
    unittest.main()
