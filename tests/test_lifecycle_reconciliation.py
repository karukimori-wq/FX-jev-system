"""No broker access: historical close + late entry fill mock review tests."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.lifecycle_reconciliation import (
    HistoricalClose, LifecycleReviewProof, reconcile_late_demo_lifecycle,
)
from fxjev.positions import CloseExecution, PositionCloseProof
from fxjev.reconciliation import BrokerFill
from fxjev.startup import TradingRuntime
from proof_helpers import proof as make_entry


class LifecycleReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"lifecycle.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,20)
        self.sv=self.l.configure("alpha",Mode.DEMO,20,account_id="A")
        self.bv=self.l.configure_account("B",Mode.DEMO,20)
        self.bs=self.l.configure("alpha",Mode.DEMO,20,account_id="B")
        self.close_proofs=[]

    def entry(self, initial=6, requested=10, client="entry"):
        self.l.reserve(client,"alpha","r1",Mode.DEMO,requested,self.sv,
                       account_id="A",expected_account_version=self.av)
        self.l.mark_unknown(client)
        p=self._entry_proof(initial,requested,client)
        self.l.reconcile(client,proof=p)

    def _entry_proof(self,total,requested=10,client="entry",extra=None,
                     status=OrderStatus.CANCELED):
        p=make_entry(client,qty=requested,filled=total,status=status,
                     strategy="alpha",run="r1",mode=Mode.DEMO,
                     broker_id="broker-"+client)
        p=replace(p,order=replace(p.order,account_id="A"),
                  position=replace(p.position,account_id="A"))
        if extra is not None:
            earlier=total-extra[1]
            if earlier<=0:
                raise ValueError("test requires earlier filled quantity")
            now=time.time_ns()
            p=replace(p,fills=(
                BrokerFill("broker-"+client,"exec-"+client,earlier,
                           now-3_000_000),
                BrokerFill("broker-"+client,extra[0],extra[1],
                           now-2_000_000)))
        return p

    def close(self, quantity, remaining, close_id="close1",
              status=OrderStatus.FILLED,requested=None):
        requested=quantity if requested is None else requested
        now=time.time_ns()
        execution=CloseExecution(close_id,"exec-"+close_id,quantity,now-3_000_000)
        p=PositionCloseProof(
            account_id="A",client_id="entry",broker_entry_order_id="broker-entry",
            strategy_id="alpha",run_id="r1",mode=Mode.DEMO,
            close_order_id=close_id,close_status=status,
            requested_quantity=requested,executions=(execution,),
            remaining_quantity=remaining,
            order_final_at_ns=now-2_000_000,snapshot_at_ns=now,
            order_history_complete=True,fill_history_complete=True,
            position_snapshot_complete=True)
        self.l.close_position(proof=p)
        self.close_proofs.append(HistoricalClose(
            close_order_id=close_id,status=status,
            requested_quantity=requested,executions=(execution,),
            filled_quantity=quantity,recorded_remaining_quantity=remaining))

    def late(self, quantity=2, execution="late", client="entry", account="A"):
        notice=FillNotice(account,Mode.DEMO,client,"broker-"+client,
                          execution,quantity,time.time_ns())
        return ingest_mock_fill(self.l,notice)

    def scenario(self):
        self.entry()
        self.close(2,4,"close1")
        self.close(1,3,"close2")
        self.late(quantity=2)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),10)

    def review_proof(self, total=8, requested=10, new=2, current=5):
        p=self._entry_proof(total,requested,extra=("late",new))
        net=replace(p.position,open_quantity=current,
                    observed_at_ns=time.time_ns())
        return LifecycleReviewProof(
            entry=p,closes=tuple(self.close_proofs),
            net_position=net,close_history_complete=True)

    def review(self,p=None):
        return reconcile_late_demo_lifecycle(
            self.l,p or self.review_proof(),reviewer_id="mock-operator",
            review_ref="mock-lifecycle-check")

    def rows(self,query,args=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(query,args).fetchall()

    def assert_rejected_and_reserved(self,proof):
        before=self.l.get("entry")
        remaining=self.l.position_remaining("entry")
        exposed=self.l.account_exposure("A",Mode.DEMO)
        with self.assertRaises(SafetyError): self.review(proof)
        self.assertEqual(self.l.get("entry"),before)
        self.assertEqual(self.l.position_remaining("entry"),remaining)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),exposed)
        self.assertTrue(self.rows("SELECT 1 FROM mock_event_holds WHERE client_id='entry'"))
        self.assertFalse(self.rows(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mock_lifecycle_revisions'"))

    def test_two_partials_then_late_fill_updates_only_open_position(self):
        self.scenario()
        old_batch=self.rows("""SELECT close_order_id,quantity,remaining_quantity,
             evidence_sha256 FROM position_close_batches ORDER BY rowid""")
        original=self.rows("SELECT evidence_sha256 FROM reconciliation_audit")[0]
        digest=self.review()
        self.assertEqual(len(digest),64)
        self.assertEqual(self.l.position_remaining("entry"),5)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),5)
        self.assertEqual(self.l.get("entry").filled_quantity,8)
        self.assertEqual(self.rows("""SELECT close_order_id,quantity,remaining_quantity,
             evidence_sha256 FROM position_close_batches ORDER BY rowid"""),old_batch)
        self.assertEqual(self.rows("SELECT evidence_sha256 FROM reconciliation_audit")[0],original)
        self.assertEqual(self.rows("""SELECT old_entry_total,new_entry_total,
            historical_closed_quantity,old_open_quantity,new_open_quantity
            FROM mock_lifecycle_revisions"""),[(6,8,3,3,5)])
        self.assertFalse(self.rows("SELECT 1 FROM mock_event_holds WHERE client_id='entry'"))

    def test_full_close_then_late_fill_reopens_attributed_mock_position(self):
        self.entry()
        self.close(6,0,"close-full")
        self.late(quantity=2)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),10)
        p=self.review_proof(current=2)
        self.review(p)
        self.assertEqual(self.l.position_remaining("entry"),2)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),2)

    def test_partial_canceled_close_history_is_supported(self):
        self.entry()
        self.close(2,4,"close-cancel",OrderStatus.CANCELED,requested=4)
        self.late(quantity=2)
        self.review(self.review_proof(current=6))
        self.assertEqual(self.l.position_remaining("entry"),6)

    def test_multiple_close_history_is_required(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(p,closes=p.closes[:1]))

    def test_extra_fake_close_history_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(p,closes=p.closes+(p.closes[-1],)))

    def test_close_history_complete_false_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(p,close_history_complete=False))

    def test_close_execution_id_mismatch_rejected(self):
        self.scenario()
        p=self.review_proof()
        close=replace(p.closes[0],executions=(
            replace(p.closes[0].executions[0],execution_id="wrong"),))
        self.assert_rejected_and_reserved(replace(p,closes=(close,)+p.closes[1:]))

    def test_close_execution_quantity_mismatch_rejected(self):
        self.scenario()
        p=self.review_proof()
        close=replace(p.closes[0],executions=(
            replace(p.closes[0].executions[0],quantity=1),))
        self.assert_rejected_and_reserved(replace(p,closes=(close,)+p.closes[1:]))

    def test_close_order_status_mismatch_rejected(self):
        self.scenario()
        p=self.review_proof()
        close=replace(p.closes[0],status=OrderStatus.CANCELED)
        self.assert_rejected_and_reserved(replace(p,closes=(close,)+p.closes[1:]))

    def test_historical_close_remaining_mismatch_rejected(self):
        self.scenario()
        p=self.review_proof()
        close=replace(p.closes[0],recorded_remaining_quantity=9)
        self.assert_rejected_and_reserved(replace(p,closes=(close,)+p.closes[1:]))

    def test_wrong_net_balance_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(
            p,net_position=replace(p.net_position,open_quantity=4)))

    def test_missing_incremental_fill_in_entry_proof_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(
            p,entry=replace(p.entry,fills=p.entry.fills[:1])))

    def test_account_mismatch_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(
            p,net_position=replace(p.net_position,account_id="B")))

    def test_snapshot_stale_rejected(self):
        self.scenario()
        p=self.review_proof()
        stale=time.time_ns()-70_000_000_000
        self.assert_rejected_and_reserved(replace(
            p,net_position=replace(p.net_position,observed_at_ns=stale)))

    def test_new_total_not_increased_rejected(self):
        self.scenario()
        p=self.review_proof()
        entry=self._entry_proof(6,10)
        self.assert_rejected_and_reserved(replace(
            p,entry=entry,net_position=replace(p.net_position,open_quantity=3)))

    def test_wrong_terminal_order_status_rejected(self):
        self.scenario()
        p=self.review_proof()
        entry=replace(p.entry,order=replace(
            p.entry.order,status=OrderStatus.FILLED))
        self.assert_rejected_and_reserved(replace(p,entry=entry))

    def test_insufficient_order_or_fill_evidence_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.assert_rejected_and_reserved(replace(
            p,entry=replace(p.entry,fill_history_complete=False)))

    def test_collision_blocks_review(self):
        self.scenario()
        notice=FillNotice("A",Mode.DEMO,"entry","broker-entry",
                          "late",3,time.time_ns())
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,notice)
        self.assert_rejected_and_reserved(self.review_proof())

    def test_overfilled_order_rejected(self):
        self.scenario()
        p=self.review_proof(total=11,new=5,current=8)
        self.assert_rejected_and_reserved(p)

    def test_duplicate_review_after_success_rejected(self):
        self.scenario()
        p=self.review_proof()
        self.review(p)
        with self.assertRaises(SafetyError): self.review(p)
        self.assertEqual(self.rows(
            "SELECT COUNT(*) FROM mock_lifecycle_revisions")[0][0],1)

    def test_other_account_unchanged_after_review(self):
        self.l.reserve("other","alpha","r1",Mode.DEMO,4,self.bs,
                       account_id="B",expected_account_version=self.bv)
        self.scenario()
        before=self.l.account_exposure("B",Mode.DEMO)
        self.review()
        self.assertEqual(self.l.account_exposure("B",Mode.DEMO),before)

    def test_remains_safe_after_restart(self):
        self.scenario()
        self.review()
        next_ledger=Ledger(self.path)
        self.assertEqual(next_ledger.position_remaining("entry"),5)
        self.assertEqual(next_ledger.get("entry").filled_quantity,8)
        self.assertEqual(self.rows(
            "SELECT COUNT(*) FROM mock_lifecycle_revisions")[0][0],1)

    def test_runtime_gate_does_not_auto_reopen(self):
        self.entry()
        self.close(2,4,"close1")
        runtime=TradingRuntime(self.path)
        runtime.approve_demo("A",operator_id="test",review_ref="before",
                             expected_account_version=self.av)
        self.late(quantity=2)
        p=self.review_proof(current=6)
        self.review(p)
        with self.assertRaises(SafetyError):
            runtime.reserve_demo("new","alpha","r1",1,self.sv,
                                 account_id="A",expected_account_version=self.av)
        runtime.approve_demo("A",operator_id="test",review_ref="after",
                             expected_account_version=self.av)
        runtime.reserve_demo("new","alpha","r1",1,self.sv,
                             account_id="A",expected_account_version=self.av)

    def test_live_mode_is_never_accepted(self):
        self.scenario()
        p=self.review_proof()
        entry=replace(p.entry,order=replace(p.entry.order,mode=Mode.LIVE))
        with self.assertRaises(SafetyError): self.review(replace(p,entry=entry))

    def test_missing_mock_review_label_rejected(self):
        self.scenario()
        with self.assertRaises(SafetyError):
            reconcile_late_demo_lifecycle(
                self.l,self.review_proof(),reviewer_id="",review_ref="ref")
        self.assertTrue(self.rows(
            "SELECT 1 FROM mock_event_holds WHERE client_id='entry'"))


if __name__=="__main__":
    unittest.main()
