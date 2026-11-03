"""Safety tests for POST-FINAL *DEMO mock* fill revisions only."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.reconciliation import BrokerFill
from fxjev.late_reconciliation import reconcile_late_demo_fill
from fxjev.positions import CloseExecution, PositionCloseProof
from fxjev.startup import TradingRuntime
from proof_helpers import proof as mock_proof


class LateFillReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=str(Path(self.temp.name)/"late.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,12)
        self.sv=self.l.configure("alpha",Mode.DEMO,12,account_id="A")
        self.bv=self.l.configure_account("B",Mode.DEMO,12)
        self.bs=self.l.configure("alpha",Mode.DEMO,12,account_id="B")

    def entry(self,client="entry",qty=6,filled=0,account="A",mode=Mode.DEMO):
        av,sv=(self.av,self.sv) if account=="A" else (self.bv,self.bs)
        self.l.reserve(client,"alpha","r1",mode,qty,sv,
                       account_id=account,expected_account_version=av)
        self.l.mark_unknown(client)
        self.l.reconcile(client,proof=self.p(client,qty=qty,filled=filled,account=account,mode=mode))

    def p(self,client="entry",qty=6,filled=2,account="A",
          mode=Mode.DEMO,status=OrderStatus.CANCELED):
        p=mock_proof(client,qty=qty,filled=filled,status=status,
                     strategy="alpha",run="r1",mode=mode,
                     broker_id="broker-"+client)
        return replace(p,order=replace(p.order,account_id=account),
                       position=replace(p.position,account_id=account))

    def notice(self,client="entry",execution=None,quantity=2,account="A",
               mode=Mode.DEMO,broker=None):
        return FillNotice(account,mode,client,broker or "broker-"+client,
                          execution or "exec-"+client,quantity,time.time_ns())

    def review(self,p,reviewer="mock-reviewer",ref="mock-review-1"):
        return reconcile_late_demo_fill(self.l,p,reviewer_id=reviewer,review_ref=ref)

    def table(self,sql,args=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(sql,args).fetchall()

    def assert_retained(self,review_proof):
        before=self.l.account_exposure("A",Mode.DEMO)
        old=self.l.get("entry")
        with self.assertRaises(SafetyError):
            self.review(review_proof)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),before)
        self.assertEqual(self.l.get("entry"),old)
        self.assertTrue(self.table(
            "SELECT 1 FROM mock_event_holds WHERE client_id='entry'"))
        self.assertFalse(self.table(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mock_terminal_revisions'"))

    def test_late_fill_on_canceled_zero_adds_position_and_clears_own_hold(self):
        self.entry()
        original=self.table("SELECT filled_quantity,evidence_sha256 FROM reconciliation_audit")[0]
        ingest_mock_fill(self.l,self.notice())
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),6)
        digest=self.review(self.p(filled=2))
        self.assertEqual(len(digest),64)
        self.assertEqual(self.l.get("entry").status,OrderStatus.CANCELED)
        self.assertEqual(self.l.get("entry").filled_quantity,2)
        self.assertEqual(self.l.position_remaining("entry"),2)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),2)
        self.assertEqual(self.table("SELECT filled_quantity,evidence_sha256 FROM reconciliation_audit")[0],original)
        row=self.table("""SELECT previous_filled_quantity,revised_filled_quantity,
                          reviewer_id,evidence_sha256 FROM mock_terminal_revisions""")[0]
        self.assertEqual(row,(0,2,"mock-reviewer",digest))
        self.assertEqual(self.table("SELECT COUNT(*) FROM mock_event_holds")[0][0],0)

    def test_late_fill_after_initial_partial_adds_open_balance(self):
        self.entry(filled=2)
        ingest_mock_fill(self.l,self.notice(execution="late-1",quantity=1))
        p=self.p(filled=3)
        now=time.time_ns()
        first=BrokerFill("broker-entry","exec-entry",2,now-3_000_000)
        second=BrokerFill("broker-entry","late-1",1,now-2_000_000)
        p=replace(p,fills=(first,second))
        self.review(p)
        self.assertEqual(self.l.position_remaining("entry"),3)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),3)

    def test_multiple_sequential_reviews_have_append_only_audit(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice(quantity=2))
        self.review(self.p(filled=2),ref="first-review")
        ingest_mock_fill(self.l,self.notice(execution="late-2",quantity=1))
        p=self.p(filled=3)
        now=time.time_ns()
        p=replace(p,fills=(
            BrokerFill("broker-entry","exec-entry",2,now-3_000_000),
            BrokerFill("broker-entry","late-2",1,now-2_000_000)))
        self.review(p,ref="second-review")
        self.assertEqual(self.l.position_remaining("entry"),3)
        self.assertEqual(self.table("""SELECT previous_filled_quantity,
            revised_filled_quantity FROM mock_terminal_revisions
            ORDER BY revision_id"""),[(0,2),(2,3)])

    def test_review_does_not_open_runtime_admission_gate(self):
        runtime=TradingRuntime(self.path)
        runtime.approve_demo("A",operator_id="mock",review_ref="first",
                             expected_account_version=self.av)
        runtime.reserve_demo("entry","alpha","r1",6,self.sv,
                             account_id="A",expected_account_version=self.av)
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.p(filled=0))
        runtime.approve_demo("A",operator_id="mock",review_ref="second",
                             expected_account_version=self.av)
        ingest_mock_fill(self.l,self.notice())
        self.review(self.p(filled=2))
        with self.assertRaises(SafetyError):
            runtime.reserve_demo("next","alpha","r1",1,self.sv,
                                 account_id="A",expected_account_version=self.av)
        runtime.approve_demo("A",operator_id="mock",review_ref="third",
                             expected_account_version=self.av)
        runtime.reserve_demo("next","alpha","r1",1,self.sv,
                             account_id="A",expected_account_version=self.av)

    def test_incomplete_order_proof_is_rejected(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        self.assert_retained(replace(self.p(filled=2),order_history_complete=False))

    def test_incomplete_fill_proof_is_rejected(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        self.assert_retained(replace(self.p(filled=2),fill_history_complete=False))

    def test_incomplete_position_proof_is_rejected(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        self.assert_retained(replace(self.p(filled=2),position_snapshot_complete=False))

    def test_missing_one_inbox_event_blocks_review(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice(execution="late-one",quantity=1))
        ingest_mock_fill(self.l,self.notice(execution="late-two",quantity=2))
        p=self.p(filled=2)
        p=replace(p,fills=(BrokerFill("broker-entry","late-two",2,time.time_ns()),))
        self.assert_retained(p)

    def test_mismatch_inbox_execution_quantity_blocks_review(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice(quantity=3))
        self.assert_retained(self.p(filled=2))

    def test_different_broker_order_id_blocks_review(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice(broker="unexpected"))
        self.assert_retained(self.p(filled=2))

    def test_conflicting_duplicate_execution_id_blocks_review(self):
        self.entry()
        event=self.notice()
        ingest_mock_fill(self.l,event)
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,replace(event,quantity=3))
        self.assert_retained(self.p(filled=2))

    def test_wrong_account_proof_blocks_review(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        self.assert_retained(self.p(filled=2,account="B"))

    def test_different_account_observed_event_blocks_review(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice(account="B"))
        # Own-account hold is created by event handler to quarantine real owner.
        self.assert_retained(self.p(filled=2))

    def test_snapshot_before_received_event_blocks_review(self):
        self.entry()
        old=self.p(filled=2)
        ingest_mock_fill(self.l,self.notice())
        self.assert_retained(old)

    def test_lower_or_equal_total_does_not_clear_hold(self):
        self.entry(filled=2)
        ingest_mock_fill(self.l,self.notice(execution="late-1",quantity=1))
        self.assert_retained(self.p(filled=2))

    def test_terminal_state_cannot_be_changed(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        self.assert_retained(self.p(filled=6,status=OrderStatus.FILLED))

    def test_more_fills_than_request_are_rejected(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice(quantity=7))
        self.assert_retained(self.p(filled=7))

    def test_untrusted_reviewer_missing_is_rejected(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        with self.assertRaises(SafetyError):
            self.review(self.p(filled=2),reviewer="")
        self.assertEqual(self.l.position_remaining("entry"),None)

    def test_no_received_event_rejects_revision(self):
        self.entry()
        with self.assertRaises(SafetyError):
            self.review(self.p(filled=2))
        self.assertEqual(self.l.position_remaining("entry"),None)

    def test_has_prior_close_history_refuses_to_rewrite_position(self):
        self.entry(filled=3)
        now=time.time_ns()
        close=PositionCloseProof(
            account_id="A",client_id="entry",broker_entry_order_id="broker-entry",
            strategy_id="alpha",run_id="r1",mode=Mode.DEMO,
            close_order_id="close1",close_status=OrderStatus.FILLED,
            requested_quantity=1,executions=(
                CloseExecution("close1","close-exec1",1,now-2_000_000),),
            remaining_quantity=2,order_final_at_ns=now-1_000_000,
            snapshot_at_ns=now,order_history_complete=True,
            fill_history_complete=True,position_snapshot_complete=True)
        self.l.close_position(proof=close)
        ingest_mock_fill(self.l,self.notice(execution="late-1",quantity=1))
        p=self.p(filled=4)
        now=time.time_ns()
        p=replace(p,fills=(
            BrokerFill("broker-entry","exec-entry",3,now-3_000_000),
            BrokerFill("broker-entry","late-1",1,now-2_000_000)))
        self.assert_retained(p)
        self.assertEqual(self.l.position_remaining("entry"),2)

    def test_review_keeps_other_account_holds(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        ingest_mock_fill(self.l,self.notice(client="other",account="B"))
        self.review(self.p(filled=2))
        self.assertFalse(self.table(
            "SELECT 1 FROM mock_event_holds WHERE account_id='A'"))
        self.assertTrue(self.table(
            "SELECT 1 FROM mock_event_holds WHERE account_id='B'"))

    def test_replay_cannot_revise_same_event_twice(self):
        self.entry()
        e=self.notice()
        ingest_mock_fill(self.l,e)
        p=self.p(filled=2)
        self.review(p)
        self.assertEqual(ingest_mock_fill(self.l,e),"duplicate")
        with self.assertRaises(SafetyError): self.review(p)
        self.assertEqual(self.table("SELECT COUNT(*) FROM mock_terminal_revisions")[0][0],1)

    def test_review_persists_after_restart(self):
        self.entry()
        ingest_mock_fill(self.l,self.notice())
        self.review(self.p(filled=2))
        fresh=Ledger(self.path)
        self.assertEqual(fresh.get("entry").filled_quantity,2)
        self.assertEqual(fresh.position_remaining("entry"),2)
        self.assertEqual(self.table("SELECT COUNT(*) FROM mock_terminal_revisions")[0][0],1)

    def test_other_account_never_changed(self):
        self.entry()
        self.entry(client="Bentry",account="B",qty=4)
        ingest_mock_fill(self.l,self.notice())
        self.review(self.p(filled=2))
        self.assertEqual(self.l.account_exposure("B",Mode.DEMO),0)
        self.assertEqual(self.l.get("Bentry").filled_quantity,0)


if __name__=="__main__":
    unittest.main()
