"""Broker-free mock event inbox and risk-quarantine tests."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.startup import TradingRuntime
from proof_helpers import proof as make_proof


class EventInboxTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"events.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,30)
        self.sv=self.l.configure("alpha",Mode.DEMO,30,account_id="A")
        self.bv=self.l.configure_account("B",Mode.DEMO,30)
        self.bs=self.l.configure("alpha",Mode.DEMO,30,account_id="B")
        self.lv=self.l.configure_account("A",Mode.LIVE,30)
        self.ls=self.l.configure("alpha",Mode.LIVE,30,account_id="A")

    def reserve(self,client="entry",qty=6,account="A",mode=Mode.DEMO):
        av,sv=(self.av,self.sv) if (account,mode)==("A",Mode.DEMO) else (
            (self.bv,self.bs) if (account,mode)==("B",Mode.DEMO) else
            (self.lv,self.ls))
        return self.l.reserve(client,"alpha","r1",mode,qty,sv,
            account_id=account,expected_account_version=av)

    def notice(self, client="entry", execution="exec-entry", qty=2,
               account="A", mode=Mode.DEMO, broker=None):
        return FillNotice(account,mode,client,
                          broker or ("broker-"+client),
                          execution,qty,time.time_ns())

    def proof(self,client="entry",qty=6,filled=2,
              status=OrderStatus.CANCELED,account="A",
              mode=Mode.DEMO):
        p=make_proof(client,qty=qty,filled=filled,status=status,
                     strategy="alpha",run="r1",mode=mode,
                     broker_id="broker-"+client)
        return replace(p,order=replace(p.order,account_id=account),
                       position=replace(p.position,account_id=account))

    def count(self,table):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute("SELECT COUNT(*) FROM "+table).fetchone()[0]

    def assert_blocked(self,account="A",client="blocked",mode=Mode.DEMO):
        with self.assertRaises(SafetyError):
            self.reserve(client,1,account=account,mode=mode)

    def test_new_event_persisted_and_reserved_becomes_unknown(self):
        self.reserve()
        self.assertEqual(ingest_mock_fill(self.l,self.notice()),"quarantined")
        self.assertEqual(self.l.get("entry").status,OrderStatus.UNKNOWN)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),6)
        self.assertEqual(self.count("mock_fill_inbox"),1)
        self.assertEqual(self.count("mock_event_holds"),1)
        self.assert_blocked()

    def test_exact_duplicate_replay_is_idempotent(self):
        self.reserve()
        event=self.notice()
        self.assertEqual(ingest_mock_fill(self.l,event),"quarantined")
        self.assertEqual(ingest_mock_fill(self.l,event),"duplicate")
        self.assertEqual(self.count("mock_fill_inbox"),1)
        self.assertEqual(self.count("mock_event_conflicts"),0)

    def test_duplicate_event_id_with_changed_quantity_is_audited_and_blocked(self):
        self.reserve()
        event=self.notice()
        ingest_mock_fill(self.l,event)
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,replace(event,quantity=3))
        self.assertEqual(self.count("mock_event_conflicts"),1)
        self.assertEqual(self.count("mock_fill_inbox"),1)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),6)
        self.assert_blocked()

    def test_collision_between_client_ids_holds_both(self):
        self.reserve("entry",6)
        self.reserve("other",4)
        first=self.notice()
        ingest_mock_fill(self.l,first)
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,replace(first,client_id="other"))
        with closing(sqlite3.connect(self.path)) as db:
            held={r[0] for r in db.execute(
                "SELECT client_id FROM mock_event_holds WHERE account_id='A'")}
        self.assertEqual(held,{"entry","other"})
        self.assert_blocked()

    def test_unmatched_orphan_event_halts_claimed_account(self):
        self.assertEqual(ingest_mock_fill(self.l,self.notice("orphan")),"quarantined")
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),0)
        self.assert_blocked()
        self.reserve("other",1,account="B")

    def test_mismatched_account_halts_claimed_and_real_account(self):
        self.reserve("entry",6,account="B")
        e=self.notice(account="A")
        ingest_mock_fill(self.l,e)
        self.assert_blocked("A")
        self.assert_blocked("B",client="another")
        self.assertEqual(self.l.get("entry").status,OrderStatus.RESERVED)

    def test_mismatched_mode_halts_both_modes(self):
        self.reserve(mode=Mode.LIVE)
        ingest_mock_fill(self.l,self.notice(mode=Mode.DEMO))
        self.assert_blocked(mode=Mode.DEMO)
        self.assert_blocked(mode=Mode.LIVE,client="live-other")

    def test_account_B_isolated_from_account_A_hold(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice())
        self.reserve("B1",2,account="B")
        self.assertEqual(self.l.account_exposure("B",Mode.DEMO),2)

    def test_demo_hold_does_not_block_live_without_cross_mode_owner(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice())
        self.reserve("live1",2,mode=Mode.LIVE)

    def test_legacy_hold_quarantines_new_accounts(self):
        ingest_mock_fill(self.l,self.notice(client="orphan",account="legacy"))
        self.assert_blocked("A")
        self.assert_blocked("B",client="Bnew")

    def test_final_cancel_zero_then_late_fill_restores_conservative_risk(self):
        self.reserve()
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.proof(filled=0))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),0)
        ingest_mock_fill(self.l,self.notice())
        self.assertEqual(self.l.get("entry").status,OrderStatus.CANCELED)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),6)
        self.assert_blocked()

    def test_partial_cancel_then_delayed_fill_never_lowers_risk(self):
        self.reserve()
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.proof(filled=2))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),2)
        ingest_mock_fill(self.l,self.notice(execution="late-2",qty=3))
        self.assertGreaterEqual(self.l.account_exposure("A",Mode.DEMO),6)
        self.assert_blocked()

    def test_full_entry_then_late_fill_keeps_more_than_original_if_needed(self):
        self.reserve()
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.proof(
            filled=6,status=OrderStatus.FILLED))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),6)
        ingest_mock_fill(self.l,self.notice(execution="late",qty=4))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),10)
        self.assert_blocked()

    def test_multiple_late_fills_increase_conservative_floor(self):
        self.reserve()
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.proof(filled=0))
        ingest_mock_fill(self.l,self.notice(execution="e1",qty=7))
        ingest_mock_fill(self.l,self.notice(execution="e2",qty=8))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),15)

    def test_no_release_if_initial_proof_omits_prior_event(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice(qty=2))
        with self.assertRaises(SafetyError):
            self.l.reconcile("entry",proof=self.proof(filled=0))
        self.assertEqual(self.l.get("entry").status,OrderStatus.UNKNOWN)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),6)
        self.assert_blocked()

    def test_initial_proof_with_matching_event_can_clear_hold(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice(qty=2))
        self.l.reconcile("entry",proof=self.proof(filled=2))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),2)
        self.assertEqual(self.count("mock_event_holds"),0)
        self.reserve("new",2)

    def test_initial_proof_with_mismatched_fill_quantity_rejected(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice(qty=3))
        with self.assertRaises(SafetyError):
            self.l.reconcile("entry",proof=self.proof(filled=2))
        self.assert_blocked()

    def test_conflicting_event_never_auto_clears_by_proof(self):
        self.reserve()
        e=self.notice(qty=2)
        ingest_mock_fill(self.l,e)
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,replace(e,quantity=3))
        with self.assertRaises(SafetyError):
            self.l.reconcile("entry",proof=self.proof(filled=2))
        self.assert_blocked()

    def test_prior_event_wrong_broker_id_blocks_finalization(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice(broker="unexpected"))
        with self.assertRaises(SafetyError):
            self.l.reconcile("entry",proof=self.proof(filled=2))
        self.assert_blocked()

    def test_event_for_other_account_prevents_releasing_real_order(self):
        self.reserve()
        ingest_mock_fill(self.l,self.notice(account="B"))
        self.l.mark_unknown("entry")
        with self.assertRaises(SafetyError):
            self.l.reconcile("entry",proof=self.proof(filled=2))
        self.assert_blocked()

    def test_duplicate_events_concurrent_only_one_new_row(self):
        self.reserve()
        event=self.notice()
        barrier=threading.Barrier(2)
        statuses=[]
        def worker():
            barrier.wait()
            statuses.append(ingest_mock_fill(Ledger(self.path),event))
        threads=[threading.Thread(target=worker) for _ in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(sorted(statuses),["duplicate","quarantined"])
        self.assertEqual(self.count("mock_fill_inbox"),1)

    def test_invalid_notice_fails_without_database_mutation(self):
        self.reserve()
        good=self.notice()
        for bad in (None,replace(good,quantity=0),replace(good,quantity=True),
                    replace(good,execution_id=""),replace(good,mode="demo"),
                    replace(good,occurred_at_ns=-1)):
            with self.assertRaises(SafetyError):
                ingest_mock_fill(self.l,bad)
        self.assertEqual(self.count("mock_fill_inbox"),0)
        self.assertEqual(self.l.get("entry").status,OrderStatus.RESERVED)

    def test_duplicate_after_restart_and_audit_persist(self):
        self.reserve()
        e=self.notice()
        ingest_mock_fill(self.l,e)
        new=Ledger(self.path)
        self.assertEqual(ingest_mock_fill(new,e),"duplicate")
        self.assertEqual(new.account_exposure("A",Mode.DEMO),6)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0],"ok")

    def test_runtime_gate_is_revoked_after_event_even_if_initial_proof_later_valid(self):
        runtime=TradingRuntime(self.path)
        runtime.approve_demo("A",operator_id="demo",review_ref="mock",
            expected_account_version=self.av)
        runtime.reserve_demo("entry","alpha","r1",6,self.sv,
            account_id="A",expected_account_version=self.av)
        ingest_mock_fill(self.l,self.notice())
        with self.assertRaises(SafetyError):
            runtime.reserve_demo("new","alpha","r1",1,self.sv,
                account_id="A",expected_account_version=self.av)
        self.l.reconcile("entry",proof=self.proof(filled=2))
        with self.assertRaises(SafetyError):
            runtime.reserve_demo("new","alpha","r1",1,self.sv,
                account_id="A",expected_account_version=self.av)
        runtime.approve_demo("A",operator_id="demo2",review_ref="review",
            expected_account_version=self.av)
        runtime.reserve_demo("new","alpha","r1",1,self.sv,
            account_id="A",expected_account_version=self.av)

    def test_finalized_late_fill_prevents_mock_reapproval(self):
        self.reserve()
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.proof(filled=0))
        runtime=TradingRuntime(self.path)
        runtime.approve_demo("A",operator_id="demo",review_ref="mock",
            expected_account_version=self.av)
        ingest_mock_fill(self.l,self.notice())
        with self.assertRaises(SafetyError):
            runtime.approve_demo("A",operator_id="demo",review_ref="mock",
                expected_account_version=self.av)
        self.assert_blocked()


if __name__ == "__main__":
    unittest.main()
