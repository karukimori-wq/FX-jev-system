from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from threading import Event
import tempfile
import unittest

from fx_poc import Intent, Mode, Run, Side
from fx_poc.durable import SQLiteLedger, DurableEngine
from fx_poc.models import OrderState


class Broker:
    def __init__(self):
        self.opens = 0
        self.cancels = 0
        self.fail_cancel = False

    def submit_open(self, order):
        self.opens += 1
        return 'external-' + order.decision_id

    def cancel_open(self, order):
        self.cancels += 1
        if self.fail_cancel:
            raise TimeoutError('unknown cancellation')


class DispatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'ledger.db'
        self.ledger = SQLiteLedger(self.path)
        self.ledger.set_logic_limit('L',100)
        self.ledger.add_run(Run('r','L',Mode.LIVE,100))
        self.broker = Broker()
        self.engine = DurableEngine(self.ledger,self.broker)
        self.intent = Intent('d','r','USD_JPY',Side.BUY,100)

    def tearDown(self):
        self.tmp.cleanup()

    def test_stop_before_dispatch_discards_unsent(self):
        self.ledger.reserve(self.intent)
        SQLiteLedger(self.path).stop_run('r')
        self.assertEqual(self.ledger.dispatch_open('d',self.broker).state,OrderState.CANCELED)
        self.assertEqual(self.broker.opens,0)
        self.assertEqual(self.ledger.live_exposure('L'),0)

    def test_lower_cap_discards_unsent_and_excludes_demo(self):
        self.ledger.add_run(Run('demo','L',Mode.DEMO,0),demo_quantity_limit=100)
        self.ledger.reserve(Intent('demo','demo','USD_JPY',Side.BUY,100))
        self.ledger.reserve(self.intent)
        self.ledger.set_logic_limit('L',50)
        self.assertEqual(self.ledger.dispatch_open('d',self.broker).state,OrderState.CANCELED)
        self.assertEqual(self.ledger.dispatch_open('demo',self.broker).state,OrderState.OPEN)
        self.assertEqual(self.broker.opens,0)

    def test_cancel_ack_keeps_reservation_and_partial_fill(self):
        self.engine.submit_intent(self.intent)
        self.ledger.apply_fill('d','e1',30)
        self.ledger.set_logic_limit('L',50)
        self.engine.process_cancellations()
        self.assertEqual(self.ledger.get_order('d').state,OrderState.CANCEL_PENDING)
        self.assertEqual(self.ledger.live_exposure('L'),100)
        self.ledger.apply_fill('d','e2',20)
        self.assertEqual(self.ledger.live_exposure('L'),100)
        self.ledger.confirm_cancel('d')
        self.assertEqual(self.ledger.live_exposure('L'),50)
        self.assertEqual(self.ledger.pending_cancels()[0][2],'CONFIRMED')
        self.assertEqual(self.broker.cancels,1)

    def test_full_fill_wins_cancel_race(self):
        self.engine.submit_intent(self.intent)
        self.ledger.stop_run('r')
        self.ledger.apply_fill('d','full',100)
        self.assertEqual(self.ledger.get_order('d').state,OrderState.FILLED)
        self.engine.process_cancellations()
        self.assertEqual(self.broker.cancels,0)
        self.assertEqual(self.ledger.live_exposure('L'),100)

    def test_unknown_cancel_does_not_resend_after_reopen(self):
        self.engine.submit_intent(self.intent)
        self.ledger.stop_run('r')
        self.broker.fail_cancel = True
        self.engine.process_cancellations()
        DurableEngine(SQLiteLedger(self.path),self.broker).process_cancellations()
        self.assertEqual(self.broker.cancels,1)
        self.assertEqual(self.ledger.pending_cancels()[0][2],'UNKNOWN')
        self.assertEqual(self.ledger.live_exposure('L'),100)

    def test_durable_dispatch_claim_survives_reopen_without_send(self):
        self.ledger.reserve(self.intent)
        with self.ledger._connect() as db:
            db.execute("INSERT INTO dispatch_attempts(decision_id) VALUES ('d')")
        ledger = SQLiteLedger(self.path)
        self.assertEqual(ledger.dispatch_open('d',self.broker).state,OrderState.SUBMITTING)
        ledger.stop_run('r')
        self.assertEqual(ledger.get_order('d').state,OrderState.SUBMITTING)
        self.assertEqual(ledger.live_exposure('L'),100)
        self.assertEqual(self.broker.opens,0)
        self.assertEqual(len(ledger.pending_cancels()),1)

    def test_send_in_progress_serializes_stop_then_queues_cancel(self):
        entered, release, stopping = Event(), Event(), Event()
        broker = self.broker
        original = broker.submit_open
        def blocking(order):
            entered.set()
            if not release.wait(3):
                raise TimeoutError('test timeout')
            return original(order)
        broker.submit_open = blocking
        def stop():
            stopping.set()
            SQLiteLedger(self.path).stop_run('r')
        with ThreadPoolExecutor(2) as pool:
            sending = pool.submit(self.engine.submit_intent,self.intent)
            self.assertTrue(entered.wait(3))
            stopped = pool.submit(stop)
            self.assertTrue(stopping.wait(3))
            release.set()
            sending.result(3)
            stopped.result(3)
        self.assertEqual(broker.opens,1)
        self.assertEqual(self.ledger.get_order('d').state,OrderState.CANCEL_PENDING)
        self.engine.process_cancellations()
        self.assertEqual(broker.cancels,1)

    def test_pending_cancel_survives_restart(self):
        self.engine.submit_intent(self.intent)
        self.ledger.set_logic_limit('L',90)
        DurableEngine(SQLiteLedger(self.path),self.broker).process_cancellations()
        self.assertEqual(self.broker.cancels,1)
        self.assertEqual(self.ledger.live_exposure('L'),100)

    def test_terminal_evidence_is_unique_exact_and_idempotent(self):
        self.engine.submit_intent(self.intent)
        self.ledger.stop_run('r')
        c=dict(decision_id='d',run_id='r',quantity=100,filled=0,external_id='external-d',state='CANCELED')
        for evidence in ([],[c,c],[dict(c,filled=20)],[dict(c,run_id='other')],[dict(c,external_id='wrong')]):
            self.assertFalse(self.ledger.reconcile_terminal_entry('d',evidence))
            self.assertEqual(self.ledger.live_exposure('L'),100)
        self.assertTrue(self.ledger.reconcile_terminal_entry('d',[c]))
        self.assertTrue(SQLiteLedger(self.path).reconcile_terminal_entry('d',[c]))
        self.assertEqual(self.ledger.live_exposure('L'),0)

    def test_terminal_cancel_preserves_partial_holdings(self):
        self.engine.submit_intent(self.intent)
        self.ledger.apply_fill('d','part',40)
        self.ledger.stop_run('r')
        c=dict(decision_id='d',run_id='r',quantity=100,filled=40,external_id='external-d',state='CANCELED')
        self.assertFalse(self.ledger.reconcile_terminal_entry('d',[dict(c,state='FILLED')]))
        self.assertTrue(self.ledger.reconcile_terminal_entry('d',[c]))
        self.assertEqual(self.ledger.live_exposure('L'),40)

    def test_terminal_close_exact_evidence_preserves_remaining_position(self):
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        self.ledger.apply_close_fill('c','partial-close',40)
        self.ledger.transition_close('c','UNKNOWN')
        c=dict(close_id='c',run_id='r',position_id='p',quantity=100,filled=40,state='CANCELED',external_id='close-ext')
        for evidence in ([],[c,c],[dict(c,position_id='wrong')],[dict(c,filled=50)],[dict(c,state='FILLED')]):
            self.assertFalse(self.ledger.reconcile_terminal_close('c',evidence))
        self.assertTrue(self.ledger.reconcile_terminal_close('c',[c]))
        self.assertTrue(SQLiteLedger(self.path).reconcile_terminal_close('c',[c]))
        self.assertEqual(self.ledger.live_exposure('L'),60)
        self.assertTrue(self.ledger.reserve_close('next','r','p',60))

    def test_terminal_close_rejected_requires_zero_fills(self):
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        c=dict(close_id='c',run_id='r',position_id='p',quantity=100,filled=0,state='REJECTED',external_id='close-ext')
        self.assertTrue(self.ledger.reconcile_terminal_close('c',[c]))
        self.assertEqual(self.ledger.live_exposure('L'),100)
        self.assertFalse(self.ledger.reconcile_terminal_close('c',[dict(c,state='FILLED',filled=100)]))

    def test_late_terminal_fill_is_persisted_and_stops_logic(self):
        self.engine.submit_intent(self.intent)
        self.ledger.stop_run('r')
        self.ledger.confirm_cancel('d')
        self.assertFalse(self.ledger.ingest_entry_fill('d','late',20))
        self.assertFalse(SQLiteLedger(self.path).ingest_entry_fill('d','late',20))
        with self.ledger._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM late_entry_fills").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT active FROM stop_reasons WHERE reason='LATE_FILL_REVIEW'").fetchone()[0],1)
        self.assertEqual(self.ledger.get_order('d').state,OrderState.CANCELED)

    def test_recorded_fill_replay_does_not_raise_late_alert(self):
        self.engine.submit_intent(self.intent)
        self.assertTrue(self.ledger.ingest_entry_fill('d','full',100))
        self.assertFalse(self.ledger.ingest_entry_fill('d','full',100))
        self.assertEqual(self.ledger.live_exposure('L'),100)

    def test_late_fill_repair_is_exact_idempotent_and_keeps_stop(self):
        from fx_poc.engine import SafetyError
        self.engine.submit_intent(self.intent)
        self.ledger.stop_run('r')
        self.ledger.confirm_cancel('d')
        self.ledger.ingest_entry_fill('d','late',20)
        e=dict(execution_id='late',decision_id='d',run_id='r',external_id='external-d',quantity=20,cumulative_filled=20,terminal_state='CANCELED')
        with self.assertRaises(SafetyError):
            self.ledger.repair_late_entry_fill('late',dict(e,run_id='wrong'))
        self.assertEqual(self.ledger.live_exposure('L'),0)
        self.assertTrue(self.ledger.repair_late_entry_fill('late',e))
        self.assertFalse(SQLiteLedger(self.path).repair_late_entry_fill('late',e))
        self.assertEqual(self.ledger.live_exposure('L'),20)
        with self.assertRaises(SafetyError):
            self.ledger.reserve(Intent('blocked','r','USD_JPY',Side.BUY,1))

    def test_late_fill_repair_rolls_back_on_position_write_failure(self):
        import sqlite3
        self.engine.submit_intent(self.intent)
        self.ledger.stop_run('r')
        self.ledger.confirm_cancel('d')
        self.ledger.ingest_entry_fill('d','late',20)
        with self.ledger._connect() as db:
            db.execute("CREATE TRIGGER fail_repair BEFORE INSERT ON positions BEGIN SELECT RAISE(ABORT,'injected'); END")
        e=dict(execution_id='late',decision_id='d',run_id='r',external_id='external-d',quantity=20,cumulative_filled=20,terminal_state='CANCELED')
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.repair_late_entry_fill('late',e)
        with self.ledger._connect() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM executions WHERE execution_id='late'").fetchone())
        self.assertEqual(self.ledger.live_exposure('L'),0)

    def test_late_close_is_quarantined_without_reducing_position(self):
        from fx_poc.engine import SafetyError
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        self.ledger.transition_close('c','OPEN')
        self.ledger.transition_close('c','CANCEL_PENDING')
        self.ledger.transition_close('c','CANCELED')
        self.assertFalse(self.ledger.ingest_close_fill('c','late-close',20))
        self.assertFalse(SQLiteLedger(self.path).ingest_close_fill('c','late-close',20))
        self.assertEqual(self.ledger.live_exposure('L'),100)
        self.ledger.add_run(Run('new','L',Mode.LIVE,100))
        with self.assertRaises(SafetyError):
            self.ledger.reserve(Intent('blocked','new','USD_JPY',Side.BUY,1))
        with self.assertRaises(SafetyError):
            self.ledger.ingest_close_fill('c','late-close',21)

    def test_known_close_fill_replay_is_idempotent(self):
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        self.assertTrue(self.ledger.ingest_close_fill('c','full-close',100))
        self.assertFalse(self.ledger.ingest_close_fill('c','full-close',100))
        self.assertEqual(self.ledger.live_exposure('L'),0)

    def test_late_close_repair_exact_and_idempotent(self):
        from fx_poc.engine import SafetyError
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        self.ledger.attach_close_external_id('c','ext')
        self.ledger.transition_close('c','OPEN')
        self.ledger.transition_close('c','CANCEL_PENDING')
        self.ledger.transition_close('c','CANCELED')
        self.ledger.ingest_close_fill('c','late',20)
        e=dict(execution_id='late',close_id='c',run_id='r',position_id='p',external_id='ext',quantity=20,cumulative_filled=20,terminal_state='CANCELED')
        with self.assertRaises(SafetyError):
            self.ledger.repair_late_close_fill('late',dict(e,position_id='wrong'))
        self.assertTrue(self.ledger.repair_late_close_fill('late',e))
        self.assertFalse(SQLiteLedger(self.path).repair_late_close_fill('late',e))
        self.assertEqual(self.ledger.live_exposure('L'),80)
        with self.assertRaises(SafetyError):
            self.ledger.reserve(Intent('blocked','r','USD_JPY',Side.BUY,1))

    def test_late_close_repair_rejects_insufficient_holdings(self):
        from fx_poc.engine import SafetyError
        self.ledger.seed_position('p','r',10)
        self.ledger.reserve_close('c','r','p',10)
        self.ledger.attach_close_external_id('c','ext')
        self.ledger.transition_close('c','OPEN')
        self.ledger.transition_close('c','CANCEL_PENDING')
        self.ledger.transition_close('c','CANCELED')
        self.ledger.ingest_close_fill('c','late',20)
        e=dict(execution_id='late',close_id='c',run_id='r',position_id='p',external_id='ext',quantity=20,cumulative_filled=20,terminal_state='FILLED')
        with self.assertRaises(SafetyError):
            self.ledger.repair_late_close_fill('late',e)
        self.assertEqual(self.ledger.live_exposure('L'),10)

    def test_late_repair_records_competing_close_conflict(self):
        from fx_poc.engine import SafetyError
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('old','r','p',100)
        self.ledger.attach_close_external_id('old','ext')
        self.ledger.transition_close('old','OPEN')
        self.ledger.transition_close('old','CANCEL_PENDING')
        self.ledger.transition_close('old','CANCELED')
        self.ledger.reserve_close('replacement','r','p',100)
        self.ledger.ingest_close_fill('old','late',20)
        e=dict(execution_id='late',close_id='old',run_id='r',position_id='p',external_id='ext',quantity=20,cumulative_filled=20,terminal_state='CANCELED')
        self.ledger.repair_late_close_fill('late',e)
        with self.ledger._connect() as db:
            self.assertEqual(db.execute("SELECT pending_quantity,remaining_quantity FROM close_conflicts WHERE position_id='p'").fetchone(),(100,80))
        with self.assertRaisesRegex(SafetyError,'conflict'):
            SQLiteLedger(self.path).reserve_close('new','r','p',1)

    def test_close_send_rechecks_conflict_after_reservation(self):
        from fx_poc.durable import DurableCloseEngine
        self.ledger.seed_position('p','r',100)
        original = self.ledger.reserve_close
        def reserve(*args):
            result = original(*args)
            with self.ledger._connect() as db:
                db.execute("UPDATE positions SET quantity=80 WHERE position_id='p'")
            return result
        self.ledger.reserve_close = reserve
        class NeverBroker:
            def submit_close(self,*args):
                raise AssertionError('must not send')
        self.assertEqual(DurableCloseEngine(self.ledger,NeverBroker()).submit('c','r','p',100),'REJECTED')
        self.assertEqual(self.ledger.live_exposure('L'),80)

    def test_conflicting_close_cancel_ack_keeps_reservation_and_never_resends(self):
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        self.ledger.attach_close_external_id('c','ext')
        self.ledger.transition_close('c','OPEN')
        with self.ledger._connect() as db:
            db.execute('CREATE TABLE close_conflicts (position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)')
            db.execute("INSERT INTO close_conflicts VALUES ('p',100,80)")
        self.assertEqual(self.ledger.queue_conflicting_close_cancels('p'),1)
        class CancelBroker:
            calls=0
            def cancel_close(self,external):
                self.calls+=1
        broker=CancelBroker()
        self.assertTrue(self.ledger.send_close_cancel('c',broker))
        self.assertFalse(SQLiteLedger(self.path).send_close_cancel('c',broker))
        self.assertEqual(broker.calls,1)
        self.assertEqual(self.ledger.close_state('c'),'CANCEL_PENDING')
        self.assertEqual(self.ledger.live_exposure('L'),100)

    def test_close_terminal_evidence_synchronizes_cancel_request(self):
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',100)
        self.ledger.attach_close_external_id('c','ext')
        self.ledger.transition_close('c','OPEN')
        with self.ledger._connect() as db:
            db.execute('CREATE TABLE close_conflicts (position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)')
            db.execute("INSERT INTO close_conflicts VALUES ('p',100,80)")
        self.ledger.queue_conflicting_close_cancels('p')
        c=dict(close_id='c',run_id='r',position_id='p',quantity=100,filled=0,state='CANCELED',external_id='ext')
        self.assertFalse(self.ledger.reconcile_terminal_close('c',[dict(c,filled=20)]))
        with self.ledger._connect() as db:
            self.assertEqual(db.execute("SELECT status FROM close_cancel_requests").fetchone()[0],'PENDING')
        self.assertTrue(self.ledger.reconcile_terminal_close('c',[c]))
        with self.ledger._connect() as db:
            self.assertEqual(db.execute("SELECT status FROM close_cancel_requests").fetchone()[0],'CONFIRMED_CANCELED')
        class NeverBroker:
            def cancel_close(self,*args):
                raise AssertionError('must not resend')
        self.assertFalse(SQLiteLedger(self.path).send_close_cancel('c',NeverBroker()))
        self.assertEqual(self.ledger.live_exposure('L'),100)

    def test_conflict_clearance_checks_pending_cancels_and_retains_stop(self):
        from fx_poc.engine import SafetyError
        self.ledger.seed_position('p','r',80)
        with self.ledger._connect() as db:
            db.execute('CREATE TABLE close_conflicts (position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)')
            db.execute("INSERT INTO close_conflicts VALUES ('p',100,80)")
            db.execute("INSERT INTO closes(close_id,run_id,position_id,quantity,state,external_id) VALUES ('c','r','p',100,'OPEN','ext')")
        self.ledger.queue_conflicting_close_cancels('p')
        with self.assertRaises(SafetyError):
            self.ledger.clear_close_conflict('p','operator','review')
        c=dict(close_id='c',run_id='r',position_id='p',quantity=100,filled=0,state='CANCELED',external_id='ext')
        self.assertTrue(self.ledger.reconcile_terminal_close('c',[c]))
        self.ledger.stop_run('r')
        SQLiteLedger(self.path).clear_close_conflict('p','operator','mock reviewed')
        with self.ledger._connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM close_conflict_clearances').fetchone()[0],1)
            self.assertEqual(db.execute("SELECT enabled FROM runs WHERE run_id='r'").fetchone()[0],0)
        self.assertTrue(self.ledger.reserve_close('safe','r','p',80))

    def test_conflict_clearance_rolls_back_audit_on_delete_failure(self):
        import sqlite3
        self.ledger.seed_position('p','r',80)
        with self.ledger._connect() as db:
            db.execute('CREATE TABLE close_conflicts (position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)')
            db.execute("INSERT INTO close_conflicts VALUES ('p',100,80)")
            db.execute("CREATE TRIGGER fail_clear BEFORE DELETE ON close_conflicts BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.clear_close_conflict('p','operator','review')
        with self.ledger._connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM close_conflicts').fetchone()[0],1)
            db.execute('DROP TRIGGER fail_clear')
        self.ledger.clear_close_conflict('p','operator','review')
        with self.ledger._connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM close_conflict_clearances').fetchone()[0],1)
            self.assertIsNotNone(db.execute('SELECT cleared_at FROM close_conflict_clearances').fetchone()[0])

    def test_unknown_cancel_blocks_clearance_even_when_quantity_fits(self):
        from fx_poc.engine import SafetyError
        self.ledger.seed_position('p','r',100)
        self.ledger.reserve_close('c','r','p',20)
        self.ledger.attach_close_external_id('c','ext')
        self.ledger.transition_close('c','OPEN')
        with self.ledger._connect() as db:
            db.execute('CREATE TABLE close_conflicts (position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)')
            db.execute("INSERT INTO close_conflicts VALUES ('p',120,100)")
        self.ledger.queue_conflicting_close_cancels('p')
        class LostResponse:
            def cancel_close(self,*args):
                raise TimeoutError()
        self.ledger.send_close_cancel('c',LostResponse())
        with self.assertRaises(SafetyError):
            self.ledger.clear_close_conflict('p','operator','review')

    def test_resume_waits_for_dispatch_gate_and_rechecks_stop(self):
        from contextlib import contextmanager
        from fx_poc.worker import MockWorker
        from fx_poc.engine import SafetyError
        worker = MockWorker(self.ledger, self.broker)
        token = worker.review_token('r')
        waiting = Event()
        original_gate = self.ledger.dispatch_gate
        @contextmanager
        def observed_gate():
            waiting.set()
            with original_gate():
                yield
        self.ledger.dispatch_gate = observed_gate
        with ThreadPoolExecutor(max_workers=1) as pool:
            with original_gate():
                task = pool.submit(worker.approve_resume,'r','operator','review',token)
                self.assertTrue(waiting.wait(2), 'approval did not enter dispatch gate')
                self.assertFalse(task.done())
                # Simulate a control-state change before the waiting approval
                # obtains the gate; its transaction must review the newer state.
                with self.ledger._connect() as db:
                    db.execute("INSERT INTO stop_reasons VALUES ('r','MANUAL',1)")
            with self.assertRaisesRegex(SafetyError, 'stale'):
                task.result(timeout=2)
        with self.ledger._connect() as db:
            self.assertEqual(0, db.execute("SELECT enabled FROM runs WHERE run_id='r'").fetchone()[0])

    def test_new_worker_start_invalidates_identical_state_review(self):
        from fx_poc.worker import MockWorker
        from fx_poc.engine import SafetyError
        worker = MockWorker(self.ledger,self.broker)
        old_token = worker.review_token('r')
        reopened = MockWorker(SQLiteLedger(self.path),self.broker)
        with self.assertRaisesRegex(SafetyError, 'stale'):
            reopened.approve_resume('r','operator','old review',old_token)
        reopened.approve_resume('r','operator','fresh review',reopened.review_token('r'))
        with self.ledger._connect() as db:
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM worker_approvals").fetchone()[0])

    def test_stop_after_resume_blocks_new_dispatch(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger,self.broker)
        worker.approve_resume('r','operator','review',worker.review_token('r'))
        self.ledger.reserve(self.intent)
        SQLiteLedger(self.path).stop_run('r')
        self.assertEqual(self.ledger.dispatch_open('d',self.broker).state,OrderState.CANCELED)
        self.assertEqual(0,self.broker.opens)
