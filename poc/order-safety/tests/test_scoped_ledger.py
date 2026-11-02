import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
from fx_poc import Run, Mode, Side
from fx_poc.durable import SQLiteLedger
from fx_poc.scoped_ledger import RunLedger
from fx_poc.engine import SafetyError

class ScopedLedgerTests(unittest.TestCase):
    def test_same_source_ids_complete_independently_and_replay(self):
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'db'; ledger=SQLiteLedger(path); ledger.set_logic_limit('L',100)
            for mode in Mode:
                ledger.add_run(Run(mode.value,'L',mode,0 if mode==Mode.DEMO else 100),**({'demo_quantity_limit':100} if mode==Mode.DEMO else {}))
                bound=RunLedger(ledger,mode.value)
                self.assertTrue(bound.reserve('d','USD_JPY',Side.BUY,10)[1])
                self.assertTrue(bound.ingest_entry_fill('d','e',10))
                self.assertTrue(bound.reserve_close('c',bound.filled_position('e'),10))
                self.assertTrue(bound.ingest_close_fill('c','ce',10))
                reopened=RunLedger(SQLiteLedger(path),mode.value)
                self.assertFalse(reopened.ingest_entry_fill('d','e',10))
                self.assertFalse(reopened.ingest_close_fill('c','ce',10))
            self.assertEqual(0,ledger.live_exposure('L'))

    def test_foreign_position_and_missing_order_rejected(self):
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100)
            for run in ('a','b'): ledger.add_run(Run(run,'L',Mode.LIVE,100))
            a,b=RunLedger(ledger,'a'),RunLedger(ledger,'b')
            a.reserve('d','USD_JPY',Side.BUY,10);a.ingest_entry_fill('d','e',10)
            with self.assertRaises(SafetyError): b.reserve_close('c',a.filled_position('e'),1)
            with self.assertRaises(SafetyError): b.ingest_entry_fill('d','e',1)
            self.assertEqual(10,ledger.live_exposure('L'))

    def test_scoped_submission_sends_only_live_once_across_restart(self):
        from fx_poc.scoped_ledger import RunEngine
        class Broker:
            def __init__(self): self.opens=[]; self.closes=[]
            def submit_open(self, order):
                self.opens.append(order.decision_id);return 'external-open'
            def submit_close(self, *args):
                self.closes.append(args);return 'external-close'
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'db';ledger=SQLiteLedger(path);ledger.set_logic_limit('L',100);broker=Broker()
            for mode in Mode:
                run=mode.value
                ledger.add_run(Run(run,'L',mode,0 if mode==Mode.DEMO else 100),**({'demo_quantity_limit':100} if mode==Mode.DEMO else {}))
                engine=RunEngine(ledger,broker,run)
                engine.submit_entry('same','USD_JPY',Side.BUY,10)
                scope=engine.scope;scope.ingest_entry_fill('same','fill',10)
                position=scope.filled_position('fill')
                self.assertEqual('OPEN',engine.submit_close('same',position,10))
                reopened=RunEngine(SQLiteLedger(path),broker,run)
                reopened.submit_entry('same','USD_JPY',Side.BUY,10)
                self.assertEqual('OPEN',reopened.submit_close('same',position,10))
            self.assertEqual(1,len(broker.opens));self.assertEqual(1,len(broker.closes))

    def test_scoped_submission_rejects_foreign_close_before_send(self):
        from fx_poc.scoped_ledger import RunEngine
        class Forbidden:
            def submit_close(self,*args): raise AssertionError('foreign close sent')
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100)
            for run in ('a','b'):ledger.add_run(Run(run,'L',Mode.LIVE,100))
            ledger.seed_position('foreign','a',10)
            with self.assertRaises(SafetyError):RunEngine(ledger,Forbidden(),'b').submit_close('c','foreign',1)
            with ledger._connect() as db:self.assertEqual(0,db.execute('SELECT COUNT(*) FROM closes').fetchone()[0])

    def test_run_cancellation_does_not_drain_other_run_or_demo(self):
        from fx_poc.scoped_ledger import RunEngine
        class Broker:
            def __init__(self): self.canceled=[]
            def submit_open(self,order): return 'ext-'+order.decision_id
            def cancel_open(self,order): self.canceled.append(order.run_id)
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100);broker=Broker()
            engines={}
            for run,mode in [('a',Mode.LIVE),('b',Mode.LIVE),('demo',Mode.DEMO)]:
                ledger.add_run(Run(run,'L',mode,0 if mode==Mode.DEMO else 100),**({'demo_quantity_limit':100} if mode==Mode.DEMO else {}))
                engine=RunEngine(ledger,broker,run);engine.submit_entry('same','USD_JPY',Side.BUY,10);engine.stop();engines[run]=engine
            self.assertEqual(1,engines['a'].process_cancellations())
            self.assertEqual(['a'],broker.canceled)
            self.assertEqual(0,engines['a'].process_cancellations())
            self.assertEqual(1,engines['demo'].process_cancellations())
            self.assertEqual(['a'],broker.canceled)
            self.assertEqual(1,engines['b'].process_cancellations())
            self.assertEqual(['a','b'],broker.canceled)
            self.assertEqual(20,ledger.live_exposure('L'))

    def test_run_cancel_unknown_survives_restart_without_retry(self):
        from fx_poc.scoped_ledger import RunEngine
        class Broker:
            def __init__(self): self.calls=0
            def submit_open(self,order): return 'external'
            def cancel_open(self,order): self.calls+=1;raise TimeoutError()
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'db';ledger=SQLiteLedger(path);ledger.set_logic_limit('L',100)
            ledger.add_run(Run('a','L',Mode.LIVE,100));broker=Broker();engine=RunEngine(ledger,broker,'a')
            engine.submit_entry('d','USD_JPY',Side.BUY,10);engine.stop()
            self.assertEqual(1,engine.process_cancellations())
            self.assertEqual(0,RunEngine(SQLiteLedger(path),broker,'a').process_cancellations())
            self.assertEqual(1,broker.calls);self.assertEqual(10,ledger.live_exposure('L'))
            self.assertEqual('UNKNOWN',ledger.pending_cancels()[0][2])

    def test_scoped_terminal_evidence_keeps_foreign_ids_and_releases_only_owner(self):
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100)
            for run in ('a','b'):ledger.add_run(Run(run,'L',Mode.LIVE,100))
            a,b=RunLedger(ledger,'a'),RunLedger(ledger,'b')
            for scope in (a,b):scope.reserve('d','USD_JPY',Side.BUY,10)
            evidence=dict(decision_id=a.identifier('decision','d'),run_id='a',quantity=10,filled=0,state='CANCELED',external_id='ea')
            original=dict(evidence)
            self.assertFalse(b.reconcile_terminal_entry('d',[evidence]))
            self.assertEqual(original,evidence);self.assertEqual(20,ledger.live_exposure('L'))
            self.assertTrue(a.reconcile_terminal_entry('d',[evidence]))
            self.assertEqual(10,ledger.live_exposure('L'))

    def test_scoped_close_terminal_evidence_rejects_foreign_run(self):
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100)
            for run in ('a','b'):
                ledger.add_run(Run(run,'L',Mode.LIVE,100));ledger.seed_position('p-'+run,run,10)
                RunLedger(ledger,run).reserve_close('c','p-'+run,10)
            a,b=RunLedger(ledger,'a'),RunLedger(ledger,'b')
            evidence=dict(close_id=a.identifier('close','c'),run_id='a',position_id='p-a',quantity=10,filled=0,state='CANCELED',external_id='ca')
            self.assertFalse(b.reconcile_terminal_close('c',[evidence]))
            self.assertTrue(a.reconcile_terminal_close('c',[evidence]))
            self.assertEqual('SUBMITTING',ledger.close_state(b.identifier('close','c')))

    def test_full_close_fill_finalizes_pending_cancel_atomically(self):
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.add_run(Run('a','L',Mode.LIVE,100));ledger.seed_position('p','a',10)
            ledger.reserve_close('c','a','p',10);ledger.transition_close('c','OPEN');ledger.transition_close('c','CANCEL_PENDING')
            with ledger._connect() as db:
                db.execute("CREATE TABLE close_cancel_requests(close_id TEXT PRIMARY KEY,status TEXT)")
                db.execute("INSERT INTO close_cancel_requests VALUES ('c','PENDING')")
            ledger.apply_close_fill('c','e',10)
            self.assertEqual('FILLED',ledger.close_state('c'))
            with ledger._connect() as db:
                self.assertEqual('CONFIRMED_FILLED',db.execute("SELECT status FROM close_cancel_requests").fetchone()[0])
                self.assertEqual(0,db.execute("SELECT quantity FROM positions WHERE position_id='p'").fetchone()[0])
            self.assertFalse(ledger.send_close_cancel('c',object()))
            self.assertFalse(ledger.apply_close_fill('c','e',10))

    def test_cancel_ack_cannot_overwrite_full_fill_during_send(self):
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.add_run(Run('a','L',Mode.LIVE,100));ledger.seed_position('p','a',10)
            ledger.reserve_close('c','a','p',10);ledger.transition_close('c','OPEN');ledger.attach_close_external_id('c','ext');ledger.transition_close('c','CANCEL_PENDING')
            with ledger._connect() as db:
                db.execute("CREATE TABLE close_cancel_requests(close_id TEXT PRIMARY KEY,status TEXT)")
                db.execute("INSERT INTO close_cancel_requests VALUES ('c','PENDING')")
            class Broker:
                def cancel_close(self,external):ledger.apply_close_fill('c','during-send',10)
            self.assertTrue(ledger.send_close_cancel('c',Broker()))
            with ledger._connect() as db:self.assertEqual('CONFIRMED_FILLED',db.execute('SELECT status FROM close_cancel_requests').fetchone()[0])

    def test_close_fill_cancel_write_failure_rolls_back_every_record(self):
        import sqlite3
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'db';ledger=SQLiteLedger(path)
            ledger.add_run(Run('a','L',Mode.LIVE,100));ledger.seed_position('p','a',10)
            ledger.reserve_close('c','a','p',10);ledger.transition_close('c','OPEN');ledger.transition_close('c','CANCEL_PENDING')
            with ledger._connect() as db:
                db.execute("CREATE TABLE close_cancel_requests(close_id TEXT PRIMARY KEY,status TEXT)")
                db.execute("INSERT INTO close_cancel_requests VALUES ('c','UNKNOWN')")
                db.execute("CREATE TRIGGER fail_cancel_update BEFORE UPDATE ON close_cancel_requests BEGIN SELECT RAISE(ABORT,'injected failure'); END")
            with self.assertRaises(sqlite3.IntegrityError):ledger.apply_close_fill('c','e',10)
            reopened=SQLiteLedger(path)
            with reopened._connect() as db:
                self.assertEqual((10,),db.execute("SELECT quantity FROM positions WHERE position_id='p'").fetchone())
                self.assertEqual((0,'CANCEL_PENDING'),db.execute("SELECT filled,state FROM closes WHERE close_id='c'").fetchone())
                self.assertEqual(('UNKNOWN',),db.execute('SELECT status FROM close_cancel_requests').fetchone())
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='close_executions'").fetchone())
                db.execute('DROP TRIGGER fail_cancel_update')
            self.assertTrue(reopened.apply_close_fill('c','e',10))
            self.assertFalse(reopened.apply_close_fill('c','e',10))
            with reopened._connect() as db:
                self.assertEqual((0,),db.execute("SELECT quantity FROM positions WHERE position_id='p'").fetchone())
                self.assertEqual(('CONFIRMED_FILLED',),db.execute('SELECT status FROM close_cancel_requests').fetchone())

    def test_partial_close_fill_keeps_cancel_uncertainty_until_full(self):
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.add_run(Run('a','L',Mode.LIVE,100));ledger.seed_position('p','a',10)
            ledger.reserve_close('c','a','p',10);ledger.transition_close('c','OPEN');ledger.transition_close('c','CANCEL_PENDING')
            with ledger._connect() as db:
                db.execute("CREATE TABLE close_cancel_requests(close_id TEXT PRIMARY KEY,status TEXT)")
                db.execute("INSERT INTO close_cancel_requests VALUES ('c','UNKNOWN')")
            ledger.apply_close_fill('c','partial',4)
            with ledger._connect() as db:
                self.assertEqual(('UNKNOWN',),db.execute('SELECT status FROM close_cancel_requests').fetchone())
                self.assertEqual((6,),db.execute("SELECT quantity FROM positions WHERE position_id='p'").fetchone())
            self.assertEqual('CANCEL_PENDING',ledger.close_state('c'))
            ledger.apply_close_fill('c','rest',6)
            with ledger._connect() as db:self.assertEqual(('CONFIRMED_FILLED',),db.execute('SELECT status FROM close_cancel_requests').fetchone())

    def test_close_cancellation_queue_is_run_owned_and_unknown_not_retried(self):
        from fx_poc.scoped_ledger import RunEngine
        class Broker:
            def __init__(self):self.calls=[]
            def cancel_close(self,external):self.calls.append(external);raise TimeoutError()
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'db';ledger=SQLiteLedger(path);broker=Broker()
            with ledger._connect() as db:db.execute('CREATE TABLE close_conflicts(position_id TEXT PRIMARY KEY,pending_quantity INTEGER,remaining_quantity INTEGER)')
            engines={}
            for run in ('a','b'):
                ledger.add_run(Run(run,'L',Mode.LIVE,100));ledger.seed_position('p-'+run,run,10)
                engine=RunEngine(ledger,broker,run);engines[run]=engine
                engine.scope.reserve_close('c','p-'+run,10)
                close=engine.scope.identifier('close','c');ledger.transition_close(close,'OPEN');ledger.attach_close_external_id(close,'ext-'+run)
                with ledger._connect() as db:db.execute('INSERT INTO close_conflicts VALUES (?,20,10)',('p-'+run,))
            with self.assertRaises(SafetyError):engines['a'].queue_conflicting_close_cancels('p-b')
            for run in ('a','b'):engines[run].queue_conflicting_close_cancels('p-'+run)
            self.assertEqual(1,engines['a'].process_close_cancellations());self.assertEqual(['ext-a'],broker.calls)
            self.assertEqual(0,RunEngine(SQLiteLedger(path),broker,'a').process_close_cancellations())
            with ledger._connect() as db:
                self.assertEqual('PENDING',db.execute('SELECT status FROM close_cancel_requests WHERE close_id=?',(engines['b'].scope.identifier('close','c'),)).fetchone()[0])
                self.assertEqual(10,db.execute("SELECT quantity FROM positions WHERE position_id='p-a'").fetchone()[0])

    def test_close_cancel_processor_handles_absent_queue(self):
        from fx_poc.scoped_ledger import RunEngine
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.add_run(Run('a','L',Mode.LIVE,100))
            self.assertEqual(0,RunEngine(ledger,object(),'a').process_close_cancellations())

    def test_worker_scoped_entry_requires_startup_review(self):
        from fx_poc.worker import MockWorker
        class Broker:
            def __init__(self):self.calls=0
            def submit_open(self,order):self.calls+=1;return 'ext'
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100);ledger.add_run(Run('a','L',Mode.LIVE,100));broker=Broker()
            worker=MockWorker(ledger,broker,mode=Mode.LIVE);engine=worker.run_engine('a')
            with self.assertRaises(SafetyError):engine.submit_entry('d','USD_JPY',Side.BUY,1)
            self.assertEqual(0,broker.calls)
            worker.approve_resume('a','operator','review',worker.review_token('a'))
            order=engine.submit_entry('d','USD_JPY',Side.BUY,1)
            self.assertEqual(engine.scope.identifier('decision','d'),order.decision_id)
            self.assertEqual(1,broker.calls)

    def test_mode_worker_refuses_other_mode_run_engine(self):
        from fx_poc.worker import MockWorker
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.add_run(Run('demo','L',Mode.DEMO,0),demo_quantity_limit=100)
            worker=MockWorker(ledger,object(),mode=Mode.LIVE)
            with self.assertRaisesRegex(SafetyError,'worker mode mismatch'):worker.run_engine('demo')

    def test_explicit_deadline_persists_and_rejects_expired_entry_but_allows_close(self):
        from datetime import datetime,timezone
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'db';ledger=SQLiteLedger(path);ledger.set_logic_limit('L',100)
            ledger.add_run(Run('a','L',Mode.LIVE,100,ends_at=datetime(2000,1,1,tzinfo=timezone.utc)))
            ledger=SQLiteLedger(path)
            with self.assertRaisesRegex(SafetyError,'deadline expired'):RunLedger(ledger,'a').reserve('d','USD_JPY',Side.BUY,1)
            ledger.seed_position('p','a',10)
            self.assertTrue(RunLedger(ledger,'a').reserve_close('c','p',10))

    def test_deadline_rechecked_before_external_send(self):
        from datetime import datetime,timezone
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db');ledger.set_logic_limit('L',100)
            ledger.add_run(Run('a','L',Mode.LIVE,100,ends_at=datetime(2100,1,1,tzinfo=timezone.utc)))
            scope=RunLedger(ledger,'a');scope.reserve('d','USD_JPY',Side.BUY,10)
            with ledger._connect() as db:db.execute("UPDATE run_deadlines SET ends_at='2000-01-01T00:00:00+00:00'")
            self.assertEqual('CANCELED',ledger.dispatch_open(scope.identifier('decision','d'),object()).state)
            self.assertEqual(0,ledger.live_exposure('L'))

    def test_naive_deadline_rejected(self):
        from datetime import datetime
        with TemporaryDirectory() as tmp:
            ledger=SQLiteLedger(Path(tmp)/'db')
            with self.assertRaisesRegex(SafetyError,'timezone-aware'):ledger.add_run(Run('a','L',Mode.LIVE,100,ends_at=datetime(2100,1,1)))
