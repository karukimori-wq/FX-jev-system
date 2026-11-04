"""Broker-free sequenced feed gap/replay/quarantine regression tests."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.ledger import Ledger, Mode, SafetyError
from fxjev.sequenced_feed import (
    MockSequencedFill, ingest_mock_sequenced_fill, observe_mock_watermark,
)
from fxjev.startup import TradingRuntime


class SequencedFeedTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=str(Path(self.temp.name)/"feed.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,20)
        self.sv=self.l.configure("alpha",Mode.DEMO,20,account_id="A")
        self.bv=self.l.configure_account("B",Mode.DEMO,20)
        self.bs=self.l.configure("alpha",Mode.DEMO,20,account_id="B")
        self.live_av=self.l.configure_account("A",Mode.LIVE,20)
        self.live_sv=self.l.configure("alpha",Mode.LIVE,20,account_id="A")

    def rows(self,sql,args=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(sql,args).fetchall()

    def notice(self,sequence,*,account="A",mode=Mode.DEMO,
               execution=None,qty=1,client="entry"):
        return FillNotice(account,mode,client,"broker-"+client,
            execution or f"e{sequence}",qty,time.time_ns())

    def feed(self,sequence,*,stream="feed-A",notice=None,account="A",
             mode=Mode.DEMO,execution=None,qty=1):
        n=notice or self.notice(sequence,account=account,mode=mode,
                                execution=execution,qty=qty)
        return ingest_mock_sequenced_fill(
            self.l,MockSequencedFill(stream,sequence,n))

    def state(self,account="A",mode=Mode.DEMO):
        data=self.rows("""SELECT stream_id,next_expected,high_watermark,
            has_alarm FROM mock_feed_streams WHERE account_id=? AND mode=?""",
            (account,mode.value))
        return data[0] if data else None

    def reserve(self,client="new",account="A",mode=Mode.DEMO):
        av,sv=(self.av,self.sv) if (account,mode)==("A",Mode.DEMO) else (
            (self.bv,self.bs) if (account,mode)==("B",Mode.DEMO)
            else (self.live_av,self.live_sv))
        return self.l.reserve(client,"alpha","r1",mode,1,sv,
            account_id=account,expected_account_version=av)

    def test_contiguous_sequence_advances_checkpoint(self):
        self.assertEqual(self.feed(1),"quarantined")
        self.assertEqual(self.feed(2),"quarantined")
        self.assertEqual(self.state(),("feed-A",3,0,0))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_messages"),[(2,)])
        self.assertFalse(self.rows("SELECT 1 FROM mock_feed_alerts"))

    def test_missing_prefix_detected_and_remains_alarmed_after_late_arrival(self):
        self.feed(3)
        self.assertEqual(self.state(),("feed-A",1,0,1))
        self.feed(1)
        self.assertEqual(self.state(),("feed-A",2,0,1))
        self.feed(2)
        self.assertEqual(self.state(),("feed-A",4,0,1))
        self.assertTrue(self.rows(
            "SELECT 1 FROM mock_feed_alerts WHERE kind='sequence-gap'"))
        with self.assertRaises(SafetyError): self.reserve()

    def test_middle_gap_repaired_but_never_auto_reapproved(self):
        self.feed(1)
        self.feed(3)
        self.feed(2)
        self.assertEqual(self.state()[1:],(4,0,1))
        runtime=TradingRuntime(self.path)
        with self.assertRaises(SafetyError):
            runtime.approve_demo("A",operator_id="mock",review_ref="gap",
                expected_account_version=self.av)

    def test_exact_same_sequence_replay_is_idempotent(self):
        n=self.notice(1)
        self.feed(1,notice=n)
        self.assertEqual(self.feed(1,notice=n),"duplicate")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_messages"),[(1,)])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_fill_inbox"),[(1,)])
        self.assertFalse(self.rows("SELECT 1 FROM mock_feed_alerts"))

    def test_same_sequence_different_quantity_commits_conflict_alarm(self):
        n=self.notice(1)
        self.feed(1,notice=n)
        with self.assertRaises(SafetyError):
            self.feed(1,notice=replace(n,quantity=3))
        self.assertEqual(self.state(),("feed-A",2,0,1))
        self.assertEqual(self.rows("""SELECT kind FROM mock_feed_alerts"""),
                         [("conflicting-sequence",)])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_fill_inbox"),[(1,)])

    def test_same_sequence_changed_execution_id_is_conflict(self):
        n=self.notice(1)
        self.feed(1,notice=n)
        with self.assertRaises(SafetyError):
            self.feed(1,notice=replace(n,execution_id="different"))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_messages"),[(1,)])

    def test_same_execution_id_on_another_sequence_alarmed(self):
        self.feed(1)
        with self.assertRaises(SafetyError):
            self.feed(2,execution="e1")
        self.assertEqual(self.state(),("feed-A",3,0,1))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_fill_inbox"),[(1,)])
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='execution-reused-at-new-sequence'"""))

    def test_same_execution_id_changed_payload_at_new_sequence_keeps_both_audits(self):
        self.feed(1)
        with self.assertRaises(SafetyError):
            self.feed(2,execution="e1",qty=4)
        self.assertTrue(self.rows("SELECT 1 FROM mock_event_conflicts"))
        self.assertTrue(self.rows("SELECT 1 FROM mock_feed_alerts"))
        self.assertEqual(self.state()[3],1)

    def test_stream_switch_is_rejected_and_does_not_replace_checkpoint(self):
        self.feed(1)
        with self.assertRaises(SafetyError):
            self.feed(2,stream="unexpected-stream")
        self.assertEqual(self.state(),("feed-A",2,0,1))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_messages"),[(1,)])
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='unexpected-stream'"""))

    def test_watermark_detects_missing_tail_not_yet_observed(self):
        self.feed(1)
        self.assertTrue(observe_mock_watermark(self.l,account_id="A",
            mode=Mode.DEMO,stream_id="feed-A",last_sequence=3))
        self.assertEqual(self.state(),("feed-A",2,3,1))
        self.feed(2)
        self.feed(3)
        self.assertEqual(self.state(),("feed-A",4,3,1))
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='watermark-gap'"""))

    def test_watermark_without_prior_messages_declares_missing_prefix(self):
        gap=observe_mock_watermark(self.l,account_id="A",mode=Mode.DEMO,
                                  stream_id="feed-A",last_sequence=2)
        self.assertTrue(gap)
        self.assertEqual(self.state(),("feed-A",1,2,1))

    def test_regressed_watermark_is_audited_and_quarantined(self):
        self.feed(1)
        self.assertFalse(observe_mock_watermark(self.l,account_id="A",
            mode=Mode.DEMO,stream_id="feed-A",last_sequence=1))
        self.assertTrue(observe_mock_watermark(self.l,account_id="A",
            mode=Mode.DEMO,stream_id="feed-A",last_sequence=0))
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='regressed-watermark'"""))

    def test_unexpected_watermark_stream_is_alarmed(self):
        self.feed(1)
        self.assertTrue(observe_mock_watermark(self.l,account_id="A",
            mode=Mode.DEMO,stream_id="other",last_sequence=2))
        self.assertEqual(self.state()[0],"feed-A")

    def test_high_watermark_can_be_monotonic_and_complete_without_alarm(self):
        self.feed(1)
        self.feed(2)
        self.assertFalse(observe_mock_watermark(self.l,account_id="A",
            mode=Mode.DEMO,stream_id="feed-A",last_sequence=2))
        self.assertEqual(self.state(),("feed-A",3,2,0))

    def test_bare_ingestion_cannot_bypass_sequenced_stream(self):
        self.feed(1)
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,self.notice(2))
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_fill_inbox"),[(1,)])
        self.assertTrue(self.rows("""SELECT 1 FROM mock_event_holds
            WHERE client_id='__feed__:unsequenced'"""))

    def test_unsequenced_bypass_alarm_is_durable_after_hold_deleted(self):
        self.feed(1)
        with self.assertRaises(SafetyError):
            ingest_mock_fill(self.l,self.notice(2))
        self.assertEqual(self.state()[3],1)
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='unsequenced-bypass'"""))
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM mock_event_holds WHERE account_id='A'")
            db.commit()
        runtime=TradingRuntime(self.path)
        with self.assertRaises(SafetyError):
            runtime.approve_demo("A",operator_id="mock",review_ref="test",
                                 expected_account_version=self.av)

    def test_no_alarm_account_B_still_allows_reservation(self):
        self.feed(3)
        self.reserve("other",account="B")
        self.assertEqual(self.l.account_exposure("B",Mode.DEMO),1)
        self.assertIsNone(self.state("B"))

    def test_mode_isolation_of_alarm(self):
        self.feed(3,mode=Mode.LIVE)
        self.assertEqual(self.state("A",Mode.LIVE)[3],1)
        self.reserve("demowork",mode=Mode.DEMO)

    def test_durable_alarm_blocks_mock_approval_even_if_hold_deleted(self):
        self.feed(3)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM mock_event_holds WHERE account_id='A'")
            db.commit()
        runtime=TradingRuntime(self.path)
        with self.assertRaises(SafetyError):
            runtime.approve_demo("A",operator_id="mock",review_ref="unsafe",
                                 expected_account_version=self.av)
        with self.assertRaises(SafetyError): self.reserve()

    def test_checkpoint_alarm_survives_new_ledger_instance(self):
        self.feed(3)
        other=Ledger(self.path)
        second=MockSequencedFill("feed-A",1,self.notice(1))
        self.assertEqual(ingest_mock_sequenced_fill(other,second),"quarantined")
        self.assertEqual(self.state(),("feed-A",2,0,1))

    def test_invalid_types_do_not_create_stream_state(self):
        good=MockSequencedFill("feed-A",1,self.notice(1))
        for bad in (MockSequencedFill("",1,good.notice),
                    MockSequencedFill("feed-A",0,good.notice),
                    MockSequencedFill("feed-A",True,good.notice),
                    MockSequencedFill("feed-A",1,replace(good.notice,quantity=0))):
            with self.assertRaises(SafetyError):
                ingest_mock_sequenced_fill(self.l,bad)
        self.assertIsNone(self.state())

    def test_invalid_watermark_has_no_side_effects(self):
        for bad in (-1,True,"3"):
            with self.assertRaises(SafetyError):
                observe_mock_watermark(self.l,account_id="A",
                    mode=Mode.DEMO,stream_id="feed-A",last_sequence=bad)
        self.assertIsNone(self.state())

    def test_parallel_same_sequence_is_atomic(self):
        n=self.notice(1)
        barrier=threading.Barrier(2)
        result=[]
        def do():
            barrier.wait()
            result.append(ingest_mock_sequenced_fill(
                Ledger(self.path),MockSequencedFill("feed-A",1,n)))
        threads=[threading.Thread(target=do) for _ in range(2)]
        for t in threads:t.start()
        for t in threads:t.join()
        self.assertEqual(sorted(result),["duplicate","quarantined"])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_messages"),[(1,)])

    def test_sequence_gap_alert_does_not_auto_disappear_after_full_replay(self):
        self.feed(2)
        self.feed(1)
        self.assertEqual(self.state()[1],3)
        self.assertEqual(self.state()[3],1)
        self.assertEqual(self.rows("""SELECT COUNT(*) FROM mock_feed_alerts
            WHERE kind='sequence-gap'"""),[(1,)])


if __name__=="__main__":
    unittest.main()
