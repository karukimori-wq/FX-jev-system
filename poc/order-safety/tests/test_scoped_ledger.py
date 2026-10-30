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
