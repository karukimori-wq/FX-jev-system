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
