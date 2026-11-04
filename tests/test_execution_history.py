"""Immutable mock entry execution IDs and chained revision verification tests."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.execution_history import verify_entry_lineage
from fxjev.late_reconciliation import reconcile_late_demo_fill
from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.positions import CloseExecution, PositionCloseProof
from fxjev.reconciliation import BrokerFill
from proof_helpers import proof as evidence


class ExecutionChainTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"chain.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,20)
        self.sv=self.l.configure("alpha",Mode.DEMO,20,account_id="A")

    def dbrows(self,query,params=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(query,params).fetchall()

    def entry(self,filled=4,client="entry",quantity=10):
        self.l.reserve(client,"alpha","r1",Mode.DEMO,quantity,self.sv,
                       account_id="A",expected_account_version=self.av)
        self.l.mark_unknown(client)
        p=self.original(client=client,filled=filled,quantity=quantity)
        self.l.reconcile(client,proof=p)

    def original(self,client="entry",filled=4,quantity=10):
        p=evidence(client,qty=quantity,filled=filled,
                   status=OrderStatus.CANCELED,strategy="alpha",
                   run="r1",mode=Mode.DEMO,broker_id="broker-"+client)
        return replace(p,order=replace(p.order,account_id="A"),
                       position=replace(p.position,account_id="A"))

    def late_event(self,eid="late1",qty=2):
        e=FillNotice("A",Mode.DEMO,"entry","broker-entry",
                     eid,qty,time.time_ns())
        ingest_mock_fill(self.l,e)

    def updated(self,parts=(("exec-entry",4),("late1",2))):
        total=sum(q for _,q in parts)
        p=self.original(filled=total)
        now=time.time_ns()
        return replace(p,fills=tuple(
            BrokerFill("broker-entry",eid,qty,now-2_000_000)
            for eid,qty in parts))

    def review(self,proof=None):
        return reconcile_late_demo_fill(
            self.l,proof or self.updated(),
            reviewer_id="mock",review_ref="mock-reviewed")

    def test_genesis_stores_entire_execution_id_and_quantity(self):
        self.entry()
        self.assertEqual(self.dbrows("""SELECT execution_id,broker_order_id,quantity,
            first_version FROM entry_executions WHERE client_id='entry'"""),
            [("exec-entry","broker-entry",4,1)])
        self.assertEqual(self.dbrows("""SELECT version,kind,filled_quantity
            FROM entry_revision_chain WHERE client_id='entry'"""),
            [(1,"initial",4)])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(verify_entry_lineage(db,"entry")[2],4)

    def test_genesis_zero_fills_is_recorded_not_inferred(self):
        self.entry(filled=0)
        self.assertEqual(self.dbrows(
            "SELECT COUNT(*) FROM entry_executions")[0][0],0)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(verify_entry_lineage(db,"entry"),(
                1,self.dbrows("SELECT chain_sha256 FROM entry_revision_chain")[0][0],0))

    def test_first_late_fill_appends_without_overwriting_initial_id(self):
        self.entry()
        self.late_event()
        self.review()
        self.assertEqual(self.dbrows("""SELECT execution_id,quantity,first_version
             FROM entry_executions ORDER BY execution_id"""),
             [("exec-entry",4,1),("late1",2,2)])
        rows=self.dbrows("""SELECT version,kind,filled_quantity,
            previous_sha256,chain_sha256 FROM entry_revision_chain ORDER BY version""")
        self.assertEqual([x[:3] for x in rows],[
            (1,"initial",4),(2,"late-no-close",6)])
        self.assertEqual(rows[1][3],rows[0][4])
        self.assertEqual(self.l.position_remaining("entry"),6)

    def test_multiple_late_revisions_create_linked_chain(self):
        self.entry()
        self.late_event()
        self.review()
        self.late_event("late2",1)
        self.review(self.updated((("exec-entry",4),("late1",2),("late2",1))))
        rows=self.dbrows("""SELECT version,previous_sha256,chain_sha256
           FROM entry_revision_chain ORDER BY version""")
        self.assertEqual(len(rows),3)
        self.assertEqual(rows[1][1],rows[0][2])
        self.assertEqual(rows[2][1],rows[1][2])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(verify_entry_lineage(db,"entry")[2],7)

    def test_original_timestamp_persisted_even_with_new_mock_proof(self):
        self.entry()
        t=self.dbrows("""SELECT executed_at_ns FROM entry_executions
             WHERE execution_id='exec-entry'""")[0][0]
        self.late_event()
        self.review()
        self.assertEqual(self.dbrows("""SELECT executed_at_ns FROM entry_executions
             WHERE execution_id='exec-entry'""")[0][0],t)

    def test_older_fill_id_removed_from_new_proof_is_rejected(self):
        self.entry()
        self.late_event("late1",6)
        with self.assertRaises(SafetyError):
            self.review(self.updated((("late1",6),)))
        self.assertEqual(self.l.get("entry").filled_quantity,4)
        self.assertEqual(len(self.dbrows("SELECT * FROM entry_revision_chain")),1)

    def test_older_execution_quantity_changed_in_new_proof_is_rejected(self):
        self.entry()
        self.late_event("late1",1)
        with self.assertRaises(SafetyError):
            self.review(self.updated((("exec-entry",5),("late1",1))))
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),10)

    def test_old_entry_execution_id_cannot_be_reused_as_close_id(self):
        self.entry()
        now=time.time_ns()
        close=PositionCloseProof(
            account_id="A",client_id="entry",broker_entry_order_id="broker-entry",
            strategy_id="alpha",run_id="r1",mode=Mode.DEMO,
            close_order_id="close1",close_status=OrderStatus.FILLED,
            requested_quantity=1,
            executions=(CloseExecution("close1","exec-entry",1,now-3_000_000),),
            remaining_quantity=3,order_final_at_ns=now-2_000_000,
            snapshot_at_ns=now,order_history_complete=True,
            fill_history_complete=True,position_snapshot_complete=True)
        with self.assertRaises(SafetyError):
            self.l.close_position(proof=close)
        self.assertEqual(self.l.position_remaining("entry"),4)

    def test_two_orders_cannot_share_execution_id_under_one_account(self):
        self.entry()
        self.l.reserve("other","alpha","r2",Mode.DEMO,5,self.sv,
                       account_id="A",expected_account_version=self.av)
        self.l.mark_unknown("other")
        p=self.original(client="other",filled=2,quantity=5)
        p=replace(p,order=replace(p.order,run_id="r2"),
                  position=replace(p.position,run_id="r2"),
                  fills=(BrokerFill("broker-other","exec-entry",2,
                                   time.time_ns()-1_000_000),))
        with self.assertRaises(SafetyError):
            self.l.reconcile("other",proof=p)
        self.assertEqual(self.l.get("other").status,OrderStatus.UNKNOWN)

    def test_tampered_entry_quantity_breaks_chain_and_preserves_hold(self):
        self.entry()
        self.late_event()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE entry_executions SET quantity=5
                WHERE execution_id='exec-entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.review()
        self.assertEqual(self.l.position_remaining("entry"),4)
        self.assertTrue(self.dbrows("SELECT 1 FROM mock_event_holds"))

    def test_tampered_execution_timestamp_breaks_snapshot(self):
        self.entry()
        self.late_event()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE entry_executions SET executed_at_ns=0
                WHERE execution_id='exec-entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.review()

    def test_tampered_chain_link_is_detected(self):
        self.entry()
        self.late_event()
        self.review()
        self.late_event("late2",1)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE entry_revision_chain
                SET previous_sha256='not-original' WHERE version=2""")
            db.commit()
        with self.assertRaises(SafetyError):
            self.review(self.updated((("exec-entry",4),("late1",2),("late2",1))))
        self.assertTrue(self.dbrows("SELECT 1 FROM mock_event_holds"))

    def test_changed_genesis_audit_hash_blocks_new_revision(self):
        self.entry()
        self.late_event()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE reconciliation_audit
                SET evidence_sha256='tampered' WHERE client_id='entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.review()

    def test_execution_account_is_immutable_under_chain_check(self):
        self.entry()
        self.late_event()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE entry_executions
                SET account_id='B' WHERE execution_id='exec-entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.review()

    def test_missing_legacy_ids_are_not_guessed_during_revision(self):
        self.entry()
        self.late_event()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM entry_revision_chain WHERE client_id='entry'")
            db.execute("DELETE FROM entry_executions WHERE client_id='entry'")
            db.commit()
        with self.assertRaises(SafetyError): self.review()
        self.assertTrue(self.dbrows("SELECT 1 FROM mock_event_holds"))
        self.assertEqual(self.l.get("entry").filled_quantity,4)

    def test_legacy_missing_history_cannot_release_risk_on_new_close(self):
        self.entry()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM entry_revision_chain WHERE client_id='entry'")
            db.execute("DELETE FROM entry_executions WHERE client_id='entry'")
            db.commit()
        now=time.time_ns()
        close=PositionCloseProof(
            account_id="A",client_id="entry",broker_entry_order_id="broker-entry",
            strategy_id="alpha",run_id="r1",mode=Mode.DEMO,
            close_order_id="close1",close_status=OrderStatus.FILLED,
            requested_quantity=1,
            executions=(CloseExecution("close1","close-exec",1,now-3_000_000),),
            remaining_quantity=3,order_final_at_ns=now-2_000_000,
            snapshot_at_ns=now,order_history_complete=True,
            fill_history_complete=True,position_snapshot_complete=True)
        with self.assertRaises(SafetyError): self.l.close_position(proof=close)
        self.assertEqual(self.l.position_remaining("entry"),4)

    def test_reopen_database_retains_complete_chain(self):
        self.entry()
        self.late_event()
        self.review()
        next_ledger=Ledger(self.path)
        with next_ledger._tx() as db:
            self.assertEqual(verify_entry_lineage(db,"entry")[2],6)

    def test_replay_same_execution_id_does_not_create_second_revision(self):
        self.entry()
        event=FillNotice("A",Mode.DEMO,"entry","broker-entry","late1",2,time.time_ns())
        self.assertEqual(ingest_mock_fill(self.l,event),"quarantined")
        self.review()
        self.assertEqual(ingest_mock_fill(self.l,event),"duplicate")
        with self.assertRaises(SafetyError): self.review()
        self.assertEqual(self.dbrows(
            "SELECT COUNT(*) FROM entry_revision_chain")[0][0],2)


if __name__=="__main__":
    unittest.main()
