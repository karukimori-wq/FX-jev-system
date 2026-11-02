"""Mock evidence tests; no external broker or credentials."""
from dataclasses import replace
from contextlib import closing
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.reconciliation import BrokerFill, BrokerOrder, ReconciliationProof
from proof_helpers import proof


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"test.sqlite")
        self.ledger=Ledger(self.path)
        self.lv=self.ledger.configure("s",Mode.LIVE,20)
        self.dv=self.ledger.configure("s",Mode.DEMO,20)
        self.ledger.reserve("o1","s","r1",Mode.LIVE,6,self.lv)
        self.ledger.mark_unknown("o1")

    def assert_unchanged(self, evidence):
        before=self.ledger.get("o1")
        before_risk=self.ledger.active_exposure("s",Mode.LIVE)
        with self.assertRaises(SafetyError):
            self.ledger.reconcile("o1",proof=evidence)
        self.assertEqual(self.ledger.get("o1"),before)
        self.assertEqual(self.ledger.active_exposure("s",Mode.LIVE),before_risk)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertFalse(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='reconciliation_audit'").fetchone())

    def test_missing_order_history_blocks_release(self):
        self.assert_unchanged(proof(qty=6,order_history_complete=False))

    def test_missing_fill_history_blocks_release(self):
        self.assert_unchanged(proof(qty=6,fill_history_complete=False))

    def test_missing_position_snapshot_blocks_release(self):
        self.assert_unchanged(proof(qty=6,position_snapshot_complete=False))

    def test_wrong_run_blocks_release(self):
        self.assert_unchanged(proof(qty=6,run="r-other"))

    def test_wrong_mode_blocks_release(self):
        self.assert_unchanged(proof(qty=6,mode=Mode.DEMO))

    def test_wrong_client_order_id_blocks_release(self):
        self.assert_unchanged(proof(client_id="not-o1",qty=6))

    def test_mismatched_owned_position(self):
        p=proof(qty=6,filled=2)
        self.assert_unchanged(replace(p,position=replace(p.position,open_quantity=1)))

    def test_mismatched_execution_broker_id(self):
        p=proof(qty=6,filled=2)
        self.assert_unchanged(replace(p,fills=(replace(p.fills[0],broker_order_id="other"),)))

    def test_duplicate_execution_ids(self):
        p=proof(qty=6,filled=2)
        self.assert_unchanged(replace(p,fills=(p.fills[0],p.fills[0])))

    def test_stale_position_snapshot(self):
        p=proof(qty=6)
        stamp=time.time_ns()-60_000_000_000
        self.assert_unchanged(replace(p,order=replace(p.order,final_at_ns=stamp-1_000),
                                      position=replace(p.position,observed_at_ns=stamp)))

    def test_position_snapshot_before_final_order(self):
        p=proof(qty=6)
        self.assert_unchanged(replace(p,position=replace(p.position,
                                    observed_at_ns=p.order.final_at_ns-1)))

    def test_broker_order_final_filled_needs_all_fills(self):
        self.assert_unchanged(proof(qty=6,filled=5,status=OrderStatus.FILLED))

    def test_oversized_fills(self):
        self.assert_unchanged(proof(qty=6,filled=7))

    def test_unknown_cannot_be_final_status(self):
        self.assert_unchanged(proof(qty=6,status=OrderStatus.UNKNOWN))

    def test_old_boolean_shortcut_removed(self):
        with self.assertRaises(TypeError):
            self.ledger.reconcile("o1",status=OrderStatus.CANCELED,
                                  filled_quantity=0,evidence_complete=True)
        self.assertEqual(self.ledger.get("o1").status,OrderStatus.UNKNOWN)

    def test_unknown_blocks_new_orders_even_if_capacity_exists(self):
        with self.assertRaises(SafetyError):
            self.ledger.reserve("o2","s","r2",Mode.LIVE,1,self.lv)
        self.ledger.configure("other",Mode.LIVE,20)
        with self.assertRaises(SafetyError):
            self.ledger.reserve("o3","other","r3",Mode.LIVE,1,1)

    def test_unknown_does_not_block_demo(self):
        self.ledger.reserve("d1","s","r1",Mode.DEMO,5,self.dv)
        self.assertEqual(self.ledger.active_exposure("s",Mode.DEMO),5)

    def test_canceled_unfilled_releases_reservation_and_audits(self):
        digest=self.ledger.reconcile("o1",proof=proof(qty=6))
        self.assertEqual(len(digest),64)
        self.assertEqual(self.ledger.active_exposure("s",Mode.LIVE),0)
        self.ledger.reserve("o2","s","r2",Mode.LIVE,20,self.lv)
        with closing(sqlite3.connect(self.path)) as db:
            rec=db.execute("SELECT evidence_sha256,final_status,filled_quantity FROM reconciliation_audit WHERE client_id='o1'").fetchone()
        self.assertEqual(rec,(digest,"canceled",0))

    def test_partial_fill_keeps_ownership_reservation_after_restart(self):
        self.ledger.reconcile("o1",proof=proof(qty=6,filled=2))
        after=Ledger(self.path)
        self.assertEqual(after.get("o1").filled_quantity,2)
        self.assertEqual(after.active_exposure("s",Mode.LIVE),2)
        after.reserve("o2","s","r2",Mode.LIVE,18,self.lv)

    def test_fully_filled_preserves_entire_risk(self):
        self.ledger.reconcile("o1",proof=proof(qty=6,filled=6,status=OrderStatus.FILLED))
        self.assertEqual(self.ledger.get("o1").status,OrderStatus.FILLED)
        self.assertEqual(self.ledger.active_exposure("s",Mode.LIVE),6)

    def test_cannot_reconcile_twice(self):
        self.ledger.reconcile("o1",proof=proof(qty=6))
        with self.assertRaises(SafetyError):
            self.ledger.reconcile("o1",proof=proof(qty=6))

    def test_broker_order_id_cannot_attach_to_two_intents(self):
        self.ledger.reserve("o2","s","r2",Mode.DEMO,5,self.dv)
        self.ledger.reconcile("o1",proof=proof(qty=6,broker_id="brokersame"))
        # even a cross-mode duplicate broker reference cannot alias two intents
        with self.assertRaises(SafetyError):
            self.ledger.reconcile("o2",proof=proof(client_id="o2",qty=5,run="r2",
                                            mode=Mode.DEMO,broker_id="brokersame"))
        self.assertEqual(self.ledger.get("o2").status,OrderStatus.RESERVED)

    def test_invalid_proof_type(self):
        self.assert_unchanged("all okay")


if __name__ == "__main__":
    unittest.main()
