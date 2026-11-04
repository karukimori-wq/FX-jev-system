"""Broker-free mock sequenced-feed migration and read-only audit tests."""
from contextlib import closing
from dataclasses import replace
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from fxjev.events import FillNotice, ingest_mock_fill
from fxjev.feed_inspection import inspect_mock_feed
from fxjev.ledger import Ledger, Mode, SafetyError
from fxjev.sequenced_feed import (
    MockSequencedFill, ingest_mock_sequenced_fill, observe_mock_watermark,
)
from fxjev.startup import TradingRuntime


class MockFeedInspectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"audit.sqlite")
        self.l=Ledger(self.path)
        self.av=self.l.configure_account("A",Mode.DEMO,15)
        self.sv=self.l.configure("alpha",Mode.DEMO,15,account_id="A")
        self.bv=self.l.configure_account("B",Mode.DEMO,15)
        self.bs=self.l.configure("alpha",Mode.DEMO,15,account_id="B")

    def notice(self,seq,account="A",mode=Mode.DEMO,client="entry"):
        return FillNotice(account,mode,client,"broker-"+client,
                          "event-"+str(seq),1,time.time_ns())

    def send(self,seq,account="A",mode=Mode.DEMO,stream="mock-feed"):
        return ingest_mock_sequenced_fill(
            self.l,MockSequencedFill(stream,seq,
                self.notice(seq,account=account,mode=mode)))

    def report(self,account="A",mode=Mode.DEMO,max_ranges=128):
        return inspect_mock_feed(self.l,account_id=account,
                                 mode=mode,max_ranges=max_ranges)

    def rows(self,sql,args=()):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(sql,args).fetchall()

    def test_uninitialized_stream_is_not_reported_as_safe(self):
        with self.assertRaises(SafetyError): self.report()

    def test_contiguous_sequence_reports_no_known_gaps_but_no_broker_proof(self):
        self.send(1)
        self.send(2)
        report=self.report()
        self.assertEqual((report.next_expected,report.observed_count,
                          report.observed_max_sequence),(3,2,2))
        self.assertEqual(report.missing_total,0)
        self.assertEqual(report.missing_ranges,())
        self.assertTrue(report.checkpoint_consistent)
        self.assertTrue(report.message_stream_consistent)
        self.assertFalse(report.has_alarm)

    def test_initial_hole_then_late_gap_fill_remains_alarmed(self):
        self.send(3)
        first=self.report()
        self.assertEqual(first.missing_ranges,((1,2),))
        self.assertEqual(first.missing_total,2)
        self.send(1)
        self.send(2)
        after=self.report()
        self.assertEqual(after.missing_total,0)
        self.assertEqual(after.next_expected,4)
        self.assertTrue(after.has_alarm)
        self.assertGreater(after.alert_count,0)

    def test_multiple_holes_and_caller_watermark_tail(self):
        self.send(1)
        self.send(3)
        self.send(5)
        observe_mock_watermark(self.l,account_id="A",mode=Mode.DEMO,
                               stream_id="mock-feed",last_sequence=8)
        report=self.report()
        self.assertEqual(report.missing_ranges,((2,2),(4,4),(6,8)))
        self.assertEqual(report.missing_total,5)
        self.assertEqual(report.high_watermark,8)

    def test_range_limit_does_not_undercount_missing_events(self):
        self.send(2)
        self.send(4)
        self.send(6)
        report=self.report(max_ranges=1)
        self.assertEqual(report.missing_ranges,((1,1),))
        self.assertEqual(report.missing_total,3)
        self.assertTrue(report.ranges_truncated)
        self.assertFalse(self.report(max_ranges=5).ranges_truncated)

    def test_unclaimed_tail_without_watermark_not_invented(self):
        self.send(1)
        report=self.report()
        self.assertEqual(report.observed_max_sequence,1)
        self.assertEqual(report.high_watermark,0)
        self.assertEqual(report.missing_total,0)

    def test_watermark_only_initializes_stream_with_known_missing_tail(self):
        self.assertTrue(observe_mock_watermark(
            self.l,account_id="A",mode=Mode.DEMO,
            stream_id="mock-feed",last_sequence=3))
        report=self.report()
        self.assertEqual(report.observed_count,0)
        self.assertEqual(report.missing_ranges,((1,3),))
        self.assertTrue(report.has_alarm)

    def test_explicit_zero_watermark_does_not_prove_completeness(self):
        self.assertFalse(observe_mock_watermark(
            self.l,account_id="A",mode=Mode.DEMO,
            stream_id="mock-feed",last_sequence=0))
        report=self.report()
        self.assertEqual(report.observed_count,0)
        self.assertEqual(report.high_watermark,0)
        self.assertFalse(report.has_alarm)

    def test_preexisting_unsequenced_event_is_quarantined_on_first_stream_event(self):
        ingest_mock_fill(self.l,self.notice(99))
        self.send(1)
        report=self.report()
        self.assertTrue(report.has_alarm)
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='preexisting-unsequenced-history'"""))
        self.assertTrue(self.rows("""SELECT 1 FROM mock_event_holds
            WHERE client_id='__feed__:mock-feed'"""))
        self.assertEqual(report.observed_count,1)
        with self.assertRaises(SafetyError):
            self.l.reserve("new","alpha","r1",Mode.DEMO,1,self.sv,
                           account_id="A",expected_account_version=self.av)

    def test_preexisting_history_detected_even_when_stream_started_by_watermark(self):
        ingest_mock_fill(self.l,self.notice(91))
        self.assertTrue(observe_mock_watermark(
            self.l,account_id="A",mode=Mode.DEMO,
            stream_id="mock-feed",last_sequence=0))
        report=self.report()
        self.assertEqual(report.observed_count,0)
        self.assertTrue(report.has_alarm)
        self.assertTrue(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE kind='preexisting-unsequenced-history'"""))

    def test_legacy_alert_cannot_disappear_when_order_holds_deleted(self):
        ingest_mock_fill(self.l,self.notice(91))
        self.send(1)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM mock_event_holds WHERE account_id='A'")
            db.commit()
        self.assertTrue(self.report().has_alarm)
        runtime=TradingRuntime(self.path)
        with self.assertRaises(SafetyError):
            runtime.approve_demo("A",operator_id="mock",review_ref="evidence",
                                 expected_account_version=self.av)

    def test_preexisting_other_account_does_not_quarantine_new_stream(self):
        ingest_mock_fill(self.l,self.notice(91,account="B"))
        self.send(1)
        self.assertFalse(self.report().has_alarm)
        self.assertFalse(self.rows("""SELECT 1 FROM mock_feed_alerts
            WHERE account_id='A' AND kind='preexisting-unsequenced-history'"""))
        self.assertIsNone(self.rows("""SELECT stream_id FROM mock_feed_streams
            WHERE account_id='B'""")[0:1] or None)

    def test_preexisting_other_mode_does_not_contaminate_demo(self):
        ingest_mock_fill(self.l,self.notice(91,mode=Mode.LIVE))
        self.send(1)
        self.assertFalse(self.report().has_alarm)

    def test_read_only_inspection_never_clears_alarm(self):
        self.send(3)
        before=self.rows("SELECT has_alarm,next_expected FROM mock_feed_streams")
        count=self.rows("SELECT COUNT(*) FROM mock_feed_alerts")
        for _ in range(3):
            self.assertTrue(self.report().has_alarm)
        self.assertEqual(self.rows(
            "SELECT has_alarm,next_expected FROM mock_feed_streams"),before)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_alerts"),count)

    def test_deleted_message_can_be_detected_as_checkpoint_inconsistency(self):
        self.send(1)
        self.send(2)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM mock_feed_messages WHERE sequence=1")
            db.commit()
        rep=self.report()
        self.assertFalse(rep.checkpoint_consistent)
        self.assertEqual(rep.missing_ranges,((1,1),))

    def test_changed_message_stream_id_detected_read_only(self):
        self.send(1)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""UPDATE mock_feed_messages SET stream_id='forged'
                WHERE sequence=1""")
            db.commit()
        self.assertFalse(self.report().message_stream_consistent)

    def test_report_persists_across_restart_and_mode_isolation(self):
        self.send(4)
        next_ledger=Ledger(self.path)
        r=inspect_mock_feed(next_ledger,account_id="A",mode=Mode.DEMO)
        self.assertEqual(r.missing_ranges,((1,3),))
        self.assertTrue(r.has_alarm)
        with self.assertRaises(SafetyError):
            inspect_mock_feed(next_ledger,account_id="A",mode=Mode.LIVE)

    def test_invalid_inputs_do_not_change_stream_data(self):
        self.send(1)
        before=self.rows("SELECT COUNT(*) FROM mock_feed_messages")
        cases=((None,Mode.DEMO,128),("",Mode.DEMO,128),
               ("A","demo",128),("A",Mode.DEMO,0),
               ("A",Mode.DEMO,True),("A",Mode.DEMO,1025))
        for account,mode,limit in cases:
            with self.assertRaises(SafetyError):
                inspect_mock_feed(self.l,account_id=account,
                                  mode=mode,max_ranges=limit)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM mock_feed_messages"),before)


if __name__=="__main__":
    unittest.main()
