"""Accounting-order replay regression for broker-free, DEMO-only SQLite."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from fxjev.accounting_journal import verify_position_journal
from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.positions import CloseExecution, PositionCloseProof
from fxjev.reconciliation import BrokerFill
from fxjev.late_reconciliation import reconcile_late_demo_fill
from proof_helpers import proof as mock_proof


class AccountingJournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"journal.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,30)
        self.sv=self.l.configure("alpha",Mode.DEMO,30,account_id="A")

    def rows(self,sql,args=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(sql,args).fetchall()

    def entry_proof(self,qty=6,requested=12,extra=()):
        p=mock_proof("entry",qty=requested,filled=qty,
                     status=OrderStatus.CANCELED,strategy="alpha",
                     run="r1",mode=Mode.DEMO,broker_id="broker-entry")
        p=replace(p,order=replace(p.order,account_id="A"),
                  position=replace(p.position,account_id="A"))
        if extra:
            now=time.time_ns()
            original=qty-sum(q for _,q in extra)
            p=replace(p,fills=(
                (BrokerFill("broker-entry","exec-entry",original,now-3_000_000),)
                if original else ())+
                tuple(BrokerFill("broker-entry",eid,q,now-2_000_000)
                      for eid,q in extra))
        return p

    def entry(self,qty=6):
        self.l.reserve("entry","alpha","r1",Mode.DEMO,12,self.sv,
                       account_id="A",expected_account_version=self.av)
        self.l.mark_unknown("entry")
        self.l.reconcile("entry",proof=self.entry_proof(qty=qty))

    def close(self,qty=2,close_id="close1"):
        remaining=self.l.position_remaining("entry")-qty
        now=time.time_ns()
        execution=CloseExecution(close_id,"exec-"+close_id,qty,now-3_000_000)
        evidence=PositionCloseProof(
            account_id="A",client_id="entry",broker_entry_order_id="broker-entry",
            strategy_id="alpha",run_id="r1",mode=Mode.DEMO,
            close_order_id=close_id,close_status=OrderStatus.FILLED,
            requested_quantity=qty,executions=(execution,),
            remaining_quantity=remaining,order_final_at_ns=now-2_000_000,
            snapshot_at_ns=now,order_history_complete=True,
            fill_history_complete=True,position_snapshot_complete=True)
        self.l.close_position(proof=evidence)
        return evidence

    def event(self,eid="late1",qty=2):
        return ingest_mock_fill(self.l,FillNotice(
            "A",Mode.DEMO,"entry","broker-entry",eid,qty,time.time_ns()))

    def review_no_close(self,new=2):
        p=self.entry_proof(qty=6+new,extra=(("late1",new),))
        return reconcile_late_demo_fill(self.l,p,
            reviewer_id="mock-reviewer",review_ref="review-not-authenticated")

    def verified(self):
        with self.l._tx() as db:
            return verify_position_journal(db,"entry")

    def test_entry_genesis_accounting_and_timestamps(self):
        self.entry()
        self.assertEqual(self.verified()[2:],(6,0,6))
        row=self.rows("""SELECT kind,ref_id,entry_total,closed_total,remaining
            FROM mock_position_journal""")[0]
        self.assertEqual(row,("entry_initial","v1",6,0,6))
        self.assertEqual(self.rows("""SELECT COUNT(*) FROM mock_position_journal
            WHERE row_sha256=previous_sha256""")[0][0],0)

    def test_close_appends_execution_time_and_decreases_remaining(self):
        self.entry()
        evidence=self.close()
        self.assertEqual(self.verified()[2:],(6,2,4))
        self.assertEqual(self.rows("""SELECT executed_at_ns FROM
            position_close_executions""")[0][0],
            evidence.executions[0].executed_at_ns)
        self.assertEqual(self.rows("""SELECT kind,entry_total,closed_total,
            remaining FROM mock_position_journal ORDER BY seq"""),
            [("entry_initial",6,0,6),("close",6,2,4)])

    def test_late_no_close_appends_own_incremental_history(self):
        self.entry()
        self.event()
        self.review_no_close()
        self.assertEqual(self.verified()[2:],(8,0,8))
        self.assertEqual(self.rows("""SELECT kind,ref_id FROM
            mock_position_journal ORDER BY seq"""),
            [("entry_initial","v1"),("entry_revision","v2")])

    def test_full_close_can_reopen_after_late_event_review_with_close_history(self):
        # Lifecycle review itself is covered by test_lifecycle_reconciliation.
        self.entry()
        self.close(6)
        self.assertEqual(self.verified()[2:],(6,6,0))
        self.assertEqual(self.l.position_remaining("entry"),0)

    def test_restart_replays_committed_history(self):
        self.entry()
        self.close(2)
        self.close(1,close_id="close2")
        fresh=Ledger(self.path)
        with fresh._tx() as db:
            self.assertEqual(verify_position_journal(db,"entry")[2:],(6,3,3))

    def test_tamper_accounting_digest_fails_close_without_release(self):
        self.entry()
        self.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE mock_position_journal SET row_sha256='bad'
                WHERE client_id='entry' AND seq=1""")
            db.commit()
        with self.assertRaises(SafetyError): self.close(1,close_id="close2")
        self.assertEqual(self.l.position_remaining("entry"),4)

    def test_tamper_journal_quantity_fails_close_without_release(self):
        self.entry()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE mock_position_journal SET entry_total=7
                WHERE client_id='entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.close()
        self.assertEqual(self.l.position_remaining("entry"),6)

    def test_tamper_execution_json_breaks_replay(self):
        self.entry()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE mock_position_journal
                SET executions_json='[]' WHERE seq=1""")
            db.commit()
        with self.assertRaises(SafetyError): self.verified()

    def test_tamper_recorded_close_execution_time_breaks_replay(self):
        self.entry()
        self.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE position_close_executions
                SET executed_at_ns=0 WHERE client_id='entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.verified()

    def test_remove_close_journal_row_breaks_replay(self):
        self.entry()
        self.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""DELETE FROM mock_position_journal
                WHERE kind='close'""")
            db.commit()
        with self.assertRaises(SafetyError): self.verified()

    def test_remove_original_journal_cannot_be_silently_backfilled(self):
        self.entry()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM mock_position_journal WHERE client_id='entry'")
            db.commit()
        with self.assertRaises(SafetyError): self.close()
        self.assertEqual(self.l.position_remaining("entry"),6)

    def test_tamper_position_without_events_breaks_replay(self):
        self.entry()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE position_balances SET remaining_quantity=5
                WHERE client_id='entry'""")
            db.commit()
        with self.assertRaises(SafetyError): self.verified()

    def test_missing_close_timestamp_rejected_not_reconstructed(self):
        self.entry()
        self.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE position_close_executions
                SET executed_at_ns=NULL""")
            db.commit()
        with self.assertRaises(SafetyError): self.verified()

    def test_late_event_hold_cannot_be_bypassed_by_close(self):
        self.entry()
        self.event()
        with self.assertRaises(SafetyError): self.close()
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),12)
        self.assertEqual(self.verified()[2:],(6,0,6))

    def test_chain_links_each_accounting_update(self):
        self.entry()
        self.close()
        self.close(1,"close2")
        rows=self.rows("""SELECT seq,previous_sha256,row_sha256
            FROM mock_position_journal ORDER BY seq""")
        self.assertEqual(len(rows),3)
        self.assertEqual(rows[0][1],"0"*64)
        self.assertEqual(rows[1][1],rows[0][2])
        self.assertEqual(rows[2][1],rows[1][2])

    def test_extra_forged_event_not_corresponding_to_any_close_fails(self):
        self.entry()
        with closing(sqlite3.connect(self.path)) as db:
            row=db.execute("""SELECT client_id,account_id,mode,executions_json,
                evidence_sha256,recorded_at_ns FROM mock_position_journal
                WHERE seq=1""").fetchone()
            db.execute("""INSERT INTO mock_position_journal
                (client_id,account_id,mode,seq,kind,ref_id,entry_total,
                 closed_total,remaining,executions_json,evidence_sha256,
                 previous_sha256,recorded_at_ns,row_sha256)
                VALUES(?,?,?,2,'close','fake',6,0,6,?,?,?,?,?)""",
                (row[0],row[1],row[2],"[]",row[4],
                 self.rows("SELECT row_sha256 FROM mock_position_journal")[0][0],
                 row[5],"malicious"))
            db.commit()
        with self.assertRaises(SafetyError): self.verified()


if __name__=="__main__":
    unittest.main()
