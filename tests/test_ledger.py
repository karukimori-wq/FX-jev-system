import tempfile
import threading
import unittest
from pathlib import Path
from fxjev.ledger import Ledger,Mode,OrderStatus,SafetyError

class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"test.sqlite")
        self.l=Ledger(self.path)
        self.v=self.l.configure("s",Mode.LIVE,10)
        self.dv=self.l.configure("s",Mode.DEMO,50)

    def order(self,id="o1",run="r1",q=5,mode=Mode.LIVE,v=None):
        return self.l.reserve(id,"s",run,mode,q,
            self.v if v is None and mode==Mode.LIVE else
            self.dv if v is None else v)

    def test_restart(self):
        self.order()
        self.assertEqual(Ledger(self.path).get("o1").quantity,5)

    def test_limit_across_runs(self):
        self.order(q=7)
        with self.assertRaises(SafetyError): self.order("o2","r2",4)

    def test_demo_live_isolation(self):
        self.order(q=10)
        self.order("d1",q=50,mode=Mode.DEMO)
        self.assertEqual(self.l.active_exposure("s",Mode.LIVE),10)
        self.assertEqual(self.l.active_exposure("s",Mode.DEMO),50)

    def test_duplicate_id_after_cancel(self):
        self.order()
        self.l.reconcile("o1",status=OrderStatus.CANCELED,filled_quantity=0,evidence_complete=True)
        with self.assertRaises(SafetyError): self.order()

    def test_unknown_holds_risk(self):
        self.order(q=10)
        self.l.mark_unknown("o1")
        self.assertEqual(Ledger(self.path).active_exposure("s",Mode.LIVE),10)
        with self.assertRaises(SafetyError): self.order("o2","r2",1)

    def test_unknown_needs_evidence(self):
        self.order()
        self.l.mark_unknown("o1")
        with self.assertRaises(SafetyError):
            self.l.reconcile("o1",status=OrderStatus.CANCELED,filled_quantity=0,evidence_complete=False)
        self.assertEqual(self.l.get("o1").status,OrderStatus.UNKNOWN)

    def test_partial_fill_cancel(self):
        self.order(q=8)
        self.l.mark_unknown("o1")
        self.l.reconcile("o1",status=OrderStatus.CANCELED,filled_quantity=3,evidence_complete=True)
        self.assertEqual(self.l.active_exposure("s",Mode.LIVE),3)
        self.order("o2","r2",7)

    def test_bad_full_fill(self):
        self.order()
        with self.assertRaises(SafetyError):
            self.l.reconcile("o1",status=OrderStatus.FILLED,filled_quantity=4,evidence_complete=True)

    def test_stale_version(self):
        self.l.configure("s",Mode.LIVE,9)
        with self.assertRaises(SafetyError): self.order()

    def test_stop_retains_exposure(self):
        self.order()
        self.l.configure("s",Mode.LIVE,10,enabled=False)
        with self.assertRaises(SafetyError): self.order("o2",v=2)
        self.assertEqual(self.l.active_exposure("s",Mode.LIVE),5)

    def test_concurrent_limit(self):
        barrier=threading.Barrier(2)
        results=[]
        def worker(i):
            barrier.wait()
            try:
                self.order("o"+str(i),"r"+str(i),7)
                results.append(True)
            except SafetyError:
                results.append(False)
        ts=[threading.Thread(target=worker,args=(i,)) for i in range(2)]
        for t in ts: t.start()
        for t in ts: t.join()
        self.assertEqual(sorted(results),[False,True])

    def test_invalid_quantity(self):
        for q in (0,-1,1.5,float("nan")):
            with self.assertRaises(SafetyError): self.order(q=q)

if __name__=="__main__":
    unittest.main()
