from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
import sqlite3
import tempfile
import unittest

from fx_poc import Intent, Mode, Run, Side
from fx_poc.durable import SQLiteLedger, DurableEngine, DurableCloseEngine
from fx_poc.engine import SafetyError
from fx_poc.models import OrderState


class ForbiddenBroker:
    def submit_open(self, order):
        raise AssertionError('external open called')

    def submit_close(self, *args):
        raise AssertionError('external close called')


class LogicLimitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'ledger.db'
        self.ledger = SQLiteLedger(self.path)
        self.ledger.set_logic_limit('L', 100)
        for run in ('old', 'new'):
            self.ledger.add_run(Run(run, 'L', Mode.LIVE, 100))

    def tearDown(self):
        self.tmp.cleanup()

    def intent(self, key, run='new', qty=1, side=Side.BUY, pair='USD_JPY'):
        return Intent(key, run, pair, side, qty)

    def test_cross_run_parallel_reservations_are_atomic(self):
        barrier = Barrier(2)
        def reserve(run):
            ledger = SQLiteLedger(self.path)
            barrier.wait()
            try:
                ledger.reserve(self.intent(run, run, 60))
                return True
            except SafetyError:
                return False
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sorted(pool.map(reserve, ['old', 'new'])), [False, True])
        self.assertEqual(self.ledger.live_exposure('L'), 60)

    def test_stopped_old_run_positions_and_opposite_pairs_count_gross(self):
        self.ledger.reserve(self.intent('old-order', 'old', 80))
        self.ledger.apply_fill('old-order', 'fill', 50)
        self.ledger.stop_run('old')
        self.assertEqual(self.ledger.live_exposure('L'), 80)
        self.ledger.reserve(self.intent('sell', qty=20, side=Side.SELL, pair='EUR_JPY'))
        with self.assertRaises(SafetyError):
            self.ledger.reserve(self.intent('overflow'))

    def test_partial_fill_cancel_releases_only_unfilled_amount(self):
        self.ledger.reserve(self.intent('order', 'old', 100))
        self.ledger.apply_fill('order', 'fill', 40)
        self.assertEqual(self.ledger.live_exposure('L'), 100)
        self.ledger.set_state('order', OrderState.UNKNOWN)
        with self.assertRaises(SafetyError):
            self.ledger.reserve(self.intent('blocked'))
        # Use another order for confirmed cancellation (UNKNOWN needs reconciliation).
        self.ledger.set_logic_limit('L', 200)
        self.ledger.reserve(self.intent('cancel', qty=100))
        self.ledger.set_state('cancel', OrderState.OPEN)
        self.ledger.apply_fill('cancel', 'fill2', 30)
        self.ledger.set_state('cancel', OrderState.CANCEL_PENDING)
        self.assertEqual(self.ledger.live_exposure('L'), 200)
        self.ledger.confirm_cancel('cancel')
        self.assertEqual(self.ledger.live_exposure('L'), 130)

    def test_run_cap_is_independent_from_logic_cap(self):
        self.ledger.set_logic_limit('L', 200)
        with self.assertRaisesRegex(SafetyError, 'run quantity'):
            self.ledger.reserve(self.intent('large', qty=101))

    def test_missing_limit_fails_closed_and_zero_blocks_live(self):
        self.ledger.add_run(Run('other', 'other', Mode.LIVE, 100))
        with self.assertRaisesRegex(SafetyError, 'not configured'):
            self.ledger.reserve(self.intent('missing', 'other'))
        self.ledger.set_logic_limit('L', 0)
        with self.assertRaises(SafetyError):
            self.ledger.reserve(self.intent('zero'))

    def test_lowering_preserves_positions_and_allows_owned_close(self):
        self.ledger.reserve(self.intent('order', 'old', 80))
        self.ledger.apply_fill('order', 'filled', 80)
        self.ledger.set_logic_limit('L', 50)
        ledger = SQLiteLedger(self.path)
        self.assertEqual(ledger.live_exposure('L'), 80)
        with self.assertRaises(SafetyError):
            ledger.reserve(self.intent('blocked'))
        ledger.reserve_close('close', 'old', 'fill-filled', 40)
        ledger.apply_close_fill('close', 'closed', 40)
        ledger.reserve(self.intent('fits', qty=10))
        self.assertEqual(ledger.live_exposure('L'), 50)

    def test_demo_open_close_and_reopen_never_reach_external_broker(self):
        self.ledger.add_run(Run('demo', 'L', Mode.DEMO, 0), demo_quantity_limit=200)
        self.ledger.reserve(self.intent('live', qty=100))
        engine = DurableEngine(self.ledger, ForbiddenBroker())
        order = engine.submit_intent(self.intent('demo-order', 'demo', 200))
        self.assertEqual(order.state, OrderState.OPEN)
        self.assertTrue(order.external_order_id.startswith('demo-'))
        self.ledger.apply_fill('demo-order', 'demo-fill', 200)
        self.assertEqual(self.ledger.live_exposure('L'), 100)
        ledger = SQLiteLedger(self.path)
        self.assertEqual(DurableEngine(ledger, ForbiddenBroker()).submit_intent(self.intent('demo-order', 'demo', 200)), ledger.get_order('demo-order'))
        self.assertEqual(DurableCloseEngine(ledger, ForbiddenBroker()).submit('demo-close', 'demo', 'fill-demo-fill', 200), 'OPEN')
        ledger.apply_close_fill('demo-close', 'demo-close-fill', 200)
        self.assertEqual(ledger.live_exposure('L'), 100)
        with self.assertRaises(SafetyError):
            ledger.reserve_close('steal', 'new', 'fill-demo-fill', 1)

    def test_demo_limit_and_settings_are_separate(self):
        self.ledger.add_run(Run('demo', 'L', Mode.DEMO, 0), demo_quantity_limit=50)
        self.ledger.set_logic_limit('L', 0)
        self.ledger.reserve(self.intent('demo', 'demo', 50))
        with self.assertRaises(SafetyError):
            self.ledger.reserve(self.intent('over', 'demo'))
        with self.assertRaises(SafetyError):
            self.ledger.add_run(Run('bad-demo', 'L', Mode.DEMO, 0))
        with self.assertRaises(SafetyError):
            self.ledger.add_run(Run('bad-live', 'L', Mode.LIVE, 0))

    def test_invalid_limits_and_quantities_are_rejected(self):
        for value in (-1, 1000001, True, 1.5):
            with self.assertRaises(SafetyError):
                self.ledger.set_logic_limit('L', value)
        for value in (0, -1, True, 1.5):
            with self.assertRaises(SafetyError):
                self.ledger.reserve(self.intent('invalid', qty=value))

    def test_existing_database_migration_does_not_infer_logic_limit(self):
        path = Path(self.tmp.name) / 'legacy.db'
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE runs (run_id TEXT PRIMARY KEY, logic_id TEXT NOT NULL, mode TEXT NOT NULL, max_quantity INTEGER NOT NULL, enabled INTEGER NOT NULL DEFAULT 1)')
            db.execute("INSERT INTO runs VALUES ('legacy','L','live',100,1)")
        ledger = SQLiteLedger(path)
        with self.assertRaisesRegex(SafetyError, 'not configured'):
            ledger.reserve(self.intent('legacy-order', 'legacy'))
        ledger.set_logic_limit('L', 100)
        ledger.reserve(self.intent('legacy-order', 'legacy'))

    def test_limit_change_invalidates_worker_review(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        token = worker.review_token('new')
        self.ledger.set_logic_limit('L', 90)
        with self.assertRaisesRegex(SafetyError, 'stale'):
            worker.approve_resume('new', 'tester', 'mock review', token)

    def test_other_run_reservation_invalidates_review(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        token = worker.review_token('new')
        with self.ledger._connect() as db:
            db.execute("UPDATE runs SET enabled=1 WHERE run_id='old'")
        self.ledger.reserve(self.intent('other', 'old', 10))
        with self.assertRaisesRegex(SafetyError, 'stale'):
            worker.approve_resume('new', 'tester', 'review', token)

    def test_lazy_close_cancel_evidence_invalidates_review(self):
        from fx_poc.worker import MockWorker
        with self.ledger._connect() as db:
            db.execute("INSERT INTO positions VALUES ('p','old',50)")
            db.execute("INSERT INTO closes(close_id,run_id,position_id,quantity,state) VALUES ('c','old','p',10,'OPEN')")
        worker = MockWorker(self.ledger, ForbiddenBroker())
        token = worker.review_token('new')
        with self.ledger._connect() as db:
            db.execute("CREATE TABLE close_cancel_requests(close_id TEXT PRIMARY KEY,status TEXT)")
            db.execute("INSERT INTO close_cancel_requests VALUES ('c','UNKNOWN')")
        with self.assertRaisesRegex(SafetyError, 'stale'):
            worker.approve_resume('new', 'tester', 'review', token)

    def test_unrelated_logic_does_not_invalidate_review(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        token = worker.review_token('new')
        self.ledger.set_logic_limit('other-logic', 10)
        self.ledger.add_run(Run('unrelated', 'other-logic', Mode.LIVE, 10))
        self.ledger.reserve(self.intent('unrelated-order', 'unrelated', 1))
        self.assertEqual(token, worker.review_token('new'))
        worker.approve_resume('new', 'tester', 'review', token)

    def test_late_fill_and_repair_evidence_invalidate_review(self):
        from fx_poc.worker import MockWorker
        with self.ledger._connect() as db:
            db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('d','old','L','USD_JPY','BUY',10,'CANCELED')")
        worker = MockWorker(self.ledger, ForbiddenBroker())
        token = worker.review_token('new')
        with self.ledger._connect() as db:
            db.execute("CREATE TABLE late_entry_fills(execution_id TEXT PRIMARY KEY,decision_id TEXT,quantity INTEGER)")
            db.execute("INSERT INTO late_entry_fills VALUES ('late','d',1)")
        self.assertNotEqual(token, worker.review_token('new'))
        token = worker.review_token('new')
        with self.ledger._connect() as db:
            db.execute("CREATE TABLE fill_repairs(execution_id TEXT PRIMARY KEY,evidence TEXT)")
            db.execute("INSERT INTO fill_repairs VALUES ('late','mock evidence')")
        with self.assertRaisesRegex(SafetyError, 'stale'):
            worker.approve_resume('new', 'tester', 'review', token)

    def test_latest_review_rejects_other_run_unresolved_states_atomically(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        for state in ('SUBMITTING', 'UNKNOWN', 'CANCEL_PENDING'):
            with self.subTest(state=state):
                with self.ledger._connect() as db:
                    db.execute("INSERT OR REPLACE INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('risk','old','L','USD_JPY','BUY',10,?)", (state,))
                with self.assertRaisesRegex(SafetyError, 'unresolved orders'):
                    worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))
                with self.ledger._connect() as db:
                    self.assertEqual(0, db.execute("SELECT enabled FROM runs WHERE run_id='new'").fetchone()[0])
                    self.assertEqual(1, db.execute("SELECT active FROM stop_reasons WHERE run_id='new' AND reason='STARTUP_REVIEW'").fetchone()[0])
                    self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='worker_approvals'").fetchone())

    def test_latest_review_rejects_other_run_stop(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        with self.ledger._connect() as db:
            db.execute("INSERT INTO stop_reasons VALUES ('old','MANUAL_REVIEW',1)")
        with self.assertRaisesRegex(SafetyError, 'other stop reasons'):
            worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))

    def test_latest_review_rejects_cancel_unknown_and_quarantine(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        with self.ledger._connect() as db:
            db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('risk','old','L','USD_JPY','BUY',10,'CANCELED')")
            db.execute("INSERT INTO cancel_requests VALUES ('risk','review','UNKNOWN')")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))
        with self.ledger._connect() as db:
            db.execute("UPDATE cancel_requests SET status='CONFIRMED'")
            db.execute("CREATE TABLE late_entry_fills(execution_id TEXT PRIMARY KEY,decision_id TEXT,quantity INTEGER)")
            db.execute("INSERT INTO late_entry_fills VALUES ('late','risk',1)")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))

    def test_resolved_other_run_allows_resume(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        with self.ledger._connect() as db:
            db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('risk','old','L','USD_JPY','BUY',10,'CANCELED')")
            db.execute("INSERT INTO cancel_requests VALUES ('risk','review','CONFIRMED')")
        worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))
        with self.ledger._connect() as db:
            self.assertEqual(1, db.execute("SELECT enabled FROM runs WHERE run_id='new'").fetchone()[0])
            self.assertEqual(0, db.execute("SELECT enabled FROM runs WHERE run_id='old'").fetchone()[0])

    def test_latest_review_rejects_other_run_close_risks(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        with self.ledger._connect() as db:
            db.execute("INSERT INTO positions VALUES ('p','old',50)")
            db.execute("INSERT INTO closes(close_id,run_id,position_id,quantity,state) VALUES ('c','old','p',10,'CANCELED')")
            db.execute("CREATE TABLE close_cancel_requests(close_id TEXT PRIMARY KEY,status TEXT)")
            db.execute("INSERT INTO close_cancel_requests VALUES ('c','UNKNOWN')")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))
        with self.ledger._connect() as db:
            db.execute("UPDATE close_cancel_requests SET status='CONFIRMED_CANCELED'")
            db.execute("CREATE TABLE close_conflicts(position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)")
            db.execute("INSERT INTO close_conflicts VALUES ('p',60,50)")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))
        with self.ledger._connect() as db:
            db.execute("DELETE FROM close_conflicts")
            db.execute("CREATE TABLE late_close_fills(execution_id TEXT PRIMARY KEY,close_id TEXT,quantity INTEGER)")
            db.execute("INSERT INTO late_close_fills VALUES ('late-close','c',1)")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            worker.approve_resume('new', 'tester', 'review', worker.review_token('new'))

    def test_legacy_resume_rejects_other_run_unresolved_order(self):
        with self.ledger._connect() as db:
            db.execute("UPDATE runs SET enabled=0 WHERE run_id='new'")
            db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('risk','old','L','USD_JPY','BUY',10,'UNKNOWN')")
        with self.assertRaisesRegex(SafetyError, 'unresolved orders'):
            self.ledger.approve_mock_resume('new', 'tester', 'legacy review')
        with self.ledger._connect() as db:
            self.assertEqual(0, db.execute("SELECT enabled FROM runs WHERE run_id='new'").fetchone()[0])
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='resume_approvals'").fetchone())

    def test_legacy_resume_rejects_other_run_quarantine_and_cancel(self):
        with self.ledger._connect() as db:
            db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('risk','old','L','USD_JPY','BUY',10,'CANCELED')")
            db.execute("INSERT INTO cancel_requests VALUES ('risk','review','UNKNOWN')")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            self.ledger.approve_mock_resume('new', 'tester', 'review')
        with self.ledger._connect() as db:
            db.execute("UPDATE cancel_requests SET status='CONFIRMED'")
            db.execute("CREATE TABLE late_entry_fills(execution_id TEXT PRIMARY KEY,decision_id TEXT,quantity INTEGER)")
            db.execute("INSERT INTO late_entry_fills VALUES ('late','risk',1)")
        with self.assertRaisesRegex(SafetyError, 'unresolved logic evidence'):
            self.ledger.approve_mock_resume('new', 'tester', 'review')

    def test_legacy_resume_cannot_bypass_worker_review_after_stop_clearance(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, ForbiddenBroker())
        self.ledger.clear_mock_stop('new','STARTUP_REVIEW','tester')
        self.ledger.clear_mock_stop('old','STARTUP_REVIEW','tester')
        with self.assertRaisesRegex(SafetyError, 'worker review token required'):
            self.ledger.approve_mock_resume('new','tester','legacy bypass')
        worker.approve_resume('new','tester','fresh review',worker.review_token('new'))

    def test_legacy_resume_rejects_other_run_stop_and_accepts_clear_state(self):
        with self.ledger._connect() as db:
            db.execute("UPDATE runs SET enabled=0 WHERE run_id='new'")
            db.execute("INSERT INTO stop_reasons VALUES ('old','MANUAL',1)")
        with self.assertRaisesRegex(SafetyError, 'other stop reasons'):
            self.ledger.approve_mock_resume('new','tester','review')
        self.ledger.clear_mock_stop('old','MANUAL','tester')
        self.ledger.approve_mock_resume('new','tester','resolved review')
        with self.ledger._connect() as db:
            self.assertEqual(1, db.execute("SELECT enabled FROM runs WHERE run_id='new'").fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM resume_approvals").fetchone()[0])
