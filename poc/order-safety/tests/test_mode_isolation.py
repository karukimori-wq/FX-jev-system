from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from fx_poc import Intent, Mode, Run, Side
from fx_poc.durable import SQLiteLedger
from fx_poc.engine import SafetyError
from fx_poc.worker import MockWorker


class ForbiddenBroker:
    def submit_open(self, *args):
        raise AssertionError('external send')


class ModeIsolationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.ledger = SQLiteLedger(Path(self.tmp.name)/'ledger.db')
        self.ledger.set_logic_limit('L',100)
        self.ledger.add_run(Run('live','L',Mode.LIVE,100))
        self.ledger.add_run(Run('demo','L',Mode.DEMO,0),demo_quantity_limit=100)

    def tearDown(self):
        self.tmp.cleanup()

    def terminal_entry(self, run):
        with self.ledger._connect() as db:
            db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES (?,?, 'L','USD_JPY','BUY',10,'CANCELED')", ('order-'+run,run))

    def intent(self, run):
        return Intent('new-'+run,run,'USD_JPY',Side.BUY,1)

    def test_late_entry_isolation_in_both_directions(self):
        for affected, other in (('demo','live'),('live','demo')):
            with self.subTest(affected=affected):
                # Each direction has an independent database.
                if affected == 'live':
                    self.tearDown(); self.setUp()
                self.terminal_entry(affected)
                self.ledger.ingest_entry_fill('order-'+affected,'late-'+affected,1)
                with self.ledger._connect() as db:
                    self.assertEqual(1,db.execute("SELECT enabled FROM runs WHERE run_id=?",(other,)).fetchone()[0])
                    self.assertIsNone(db.execute("SELECT 1 FROM stop_reasons WHERE run_id=?",(other,)).fetchone())
                self.assertTrue(self.ledger.reserve(self.intent(other))[1])
                with self.assertRaises(SafetyError):
                    self.ledger.reserve(self.intent(affected))

    def test_late_close_does_not_stop_other_mode(self):
        with self.ledger._connect() as db:
            db.execute("INSERT INTO positions VALUES ('demo-position','demo',10)")
            db.execute("INSERT INTO closes(close_id,run_id,position_id,quantity,state) VALUES ('demo-close','demo','demo-position',10,'CANCELED')")
        self.ledger.ingest_close_fill('demo-close','late-close',1)
        self.assertTrue(self.ledger.reserve(self.intent('live'))[1])
        with self.ledger._connect() as db:
            self.assertEqual(1,db.execute("SELECT enabled FROM runs WHERE run_id='live'").fetchone()[0])
            self.assertEqual(10,db.execute("SELECT quantity FROM positions WHERE position_id='demo-position'").fetchone()[0])

    def test_other_mode_quarantine_does_not_invalidate_or_block_review(self):
        worker = MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.LIVE)
        token = worker.review_token('live')
        self.terminal_entry('demo')
        self.ledger.ingest_entry_fill('order-demo','demo-late',1)
        self.assertEqual(token,worker.review_token('live'))
        worker.approve_resume('live','tester','isolated review',token)

    def test_other_mode_cancel_unknown_does_not_block_resume(self):
        worker = MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.LIVE)
        token = worker.review_token('live')
        self.terminal_entry('demo')
        with self.ledger._connect() as db:
            db.execute("INSERT INTO cancel_requests VALUES ('order-demo','test','UNKNOWN')")
        self.assertEqual(token,worker.review_token('live'))
        worker.approve_resume('live','tester','review',token)

    def test_mode_startup_does_not_stop_or_invalidate_other_mode(self):
        live = MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.LIVE)
        token = live.review_token('live')
        demo = MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.DEMO)
        self.assertEqual(token,live.review_token('live'))
        live.approve_resume('live','tester','review',token)
        MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.DEMO)
        with self.ledger._connect() as db:
            self.assertEqual(1,db.execute("SELECT enabled FROM runs WHERE run_id='live'").fetchone()[0])
        with self.assertRaisesRegex(SafetyError,'worker mode mismatch'):
            demo.review_token('live')
        with self.assertRaisesRegex(SafetyError,'worker mode mismatch'):
            demo.submit(self.intent('live'))

    def test_live_limit_changes_do_not_invalidate_demo_review(self):
        demo = MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.DEMO)
        token = demo.review_token('demo')
        self.ledger.set_logic_limit('L',90)
        self.assertEqual(token,demo.review_token('demo'))
        demo.approve_resume('demo','tester','review',token)

    def test_same_mode_new_startup_still_invalidates_review(self):
        live = MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.LIVE)
        token = live.review_token('live')
        MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.LIVE)
        with self.assertRaisesRegex(SafetyError,'stale'):
            live.approve_resume('live','tester','old',token)

    def test_legacy_global_startup_migrates_both_mode_requirements(self):
        with self.ledger._connect() as db:
            db.execute("CREATE TABLE worker_review_generation(id INTEGER PRIMARY KEY,generation INTEGER)")
            db.execute("INSERT INTO worker_review_generation VALUES (1,5)")
        MockWorker(self.ledger,ForbiddenBroker(),mode=Mode.DEMO)
        with self.assertRaisesRegex(SafetyError,'worker review token required'):
            self.ledger.approve_mock_resume('live','tester','legacy bypass')
        with self.ledger._connect() as db:
            self.assertEqual([('demo',6),('live',5)],db.execute("SELECT * FROM worker_review_generations ORDER BY mode").fetchall())
