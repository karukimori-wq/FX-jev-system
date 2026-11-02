"""Startup gate acceptance tests. All orders are SQLite mock reservations."""
from contextlib import closing
from dataclasses import replace
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from fxjev.startup import TradingRuntime
from proof_helpers import proof as simulated_order_proof


class StartupGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"runtime.sqlite")
        self.l=Ledger(self.path)
        self.account_version=self.l.configure_account("A",Mode.DEMO,10)
        self.strategy_version=self.l.configure("alpha",Mode.DEMO,10,account_id="A")
        self.other_version=self.l.configure_account("B",Mode.DEMO,8)
        self.other_strategy=self.l.configure("alpha",Mode.DEMO,8,account_id="B")
        self.live_version=self.l.configure_account("A",Mode.LIVE,10)
        self.live_strategy=self.l.configure("alpha",Mode.LIVE,10,account_id="A")
        self.runtime=TradingRuntime(self.path)

    def approve(self, runtime=None, account="A", version=None):
        runtime=runtime or self.runtime
        runtime.approve_demo(account,operator_id="mock-operator",
            review_ref="mock-checklist-1",
            expected_account_version=(self.account_version if account=="A"
                                     else self.other_version) if version is None else version)

    def reserve(self,cid="o1",qty=3,runtime=None,account="A"):
        runtime=runtime or self.runtime
        return runtime.reserve_demo(cid,"alpha","run1",qty,
            self.strategy_version if account=="A" else self.other_strategy,
            account_id=account,
            expected_account_version=self.account_version if account=="A" else self.other_version)

    def proof(self,cid="o1",qty=3):
        p=simulated_order_proof(cid,qty=qty,filled=0,
             status=OrderStatus.CANCELED,strategy="alpha",run="run1",
             mode=Mode.DEMO)
        return replace(p,order=replace(p.order,account_id="A"),
                       position=replace(p.position,account_id="A"))

    def audit(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute("""SELECT action,account_id,mode
                FROM runtime_gate_audit ORDER BY event_id""").fetchall()

    def test_initially_blocked(self):
        with self.assertRaises(SafetyError): self.reserve()
        self.assertEqual(self.runtime.recovered_ids,())

    def test_direct_ledger_bypass_is_rejected(self):
        self.approve()
        with self.assertRaises(SafetyError):
            self.l.reserve("bypass","alpha","run1",Mode.DEMO,2,
                           self.strategy_version,account_id="A",
                           expected_account_version=self.account_version)
        self.assertIsNone(self.l.get("bypass"))

    def test_spoofed_admission_token_is_rejected(self):
        self.approve()
        with self.assertRaises(SafetyError):
            self.l.reserve("bad","alpha","run1",Mode.DEMO,2,
                           self.strategy_version,account_id="A",
                           expected_account_version=self.account_version,
                           admission_token="wrong")
        self.assertIsNone(self.l.get("bad"))

    def test_demo_approval_opens_admission(self):
        self.approve()
        ticket=self.reserve()
        self.assertEqual(ticket.mode,Mode.DEMO)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),3)

    def test_live_is_never_enabled_even_with_mock_approval(self):
        with self.assertRaises(SafetyError):
            self.runtime.approve_live("A",operator_id="operator",review_ref="test")
        with self.assertRaises(SafetyError):
            self.l.reserve("live","alpha","run1",Mode.LIVE,1,
               self.live_strategy,account_id="A",
               expected_account_version=self.live_version,admission_token="anything")
        self.assertIsNone(self.l.get("live"))

    def test_legacy_orders_can_never_enter_managed_runtime(self):
        self.l.configure("legacy_strategy",Mode.DEMO,10)
        with self.assertRaises(SafetyError):
            self.l.reserve("legacy","legacy_strategy","r",Mode.DEMO,1,1)
        self.assertIsNone(self.l.get("legacy"))

    def test_operator_identity_and_review_reference_required(self):
        for operator,reference in (("", "ok"),("someone",""),(None,"ok")):
            with self.assertRaises(SafetyError):
                self.runtime.approve_demo("A",operator_id=operator,
                    review_ref=reference,expected_account_version=self.account_version)
        with self.assertRaises(SafetyError):
            self.runtime.approve_demo("A",operator_id="admin",
                    review_ref="check",expected_account_version=-1)

    def test_disabled_account_cannot_be_approved(self):
        new_version=self.l.configure_account("A",Mode.DEMO,10,enabled=False)
        with self.assertRaises(SafetyError): self.approve(version=new_version)

    def test_stale_account_review_is_rejected(self):
        self.l.configure_account("A",Mode.DEMO,9)
        with self.assertRaises(SafetyError): self.approve()

    def test_config_change_revokes_open_gate_immediately(self):
        self.approve()
        self.l.configure_account("A",Mode.DEMO,9)
        with self.assertRaises(SafetyError): self.reserve()
        self.assertIn(("SAFETY_CHANGE","A","demo"),self.audit())

    def test_strategy_change_revokes_open_gate(self):
        self.approve()
        self.l.configure("alpha",Mode.DEMO,9,account_id="A")
        with self.assertRaises(SafetyError): self.reserve()
        self.assertIn(("SAFETY_CHANGE","A","demo"),self.audit())

    def test_unknown_revokes_gate_and_prevents_new_approval(self):
        self.approve()
        self.reserve("o1")
        self.l.mark_unknown("o1")
        with self.assertRaises(SafetyError): self.reserve("o2")
        with self.assertRaises(SafetyError): self.approve()
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),3)

    def test_after_complete_cancel_proof_requires_fresh_approval(self):
        self.approve()
        self.reserve("o1")
        self.l.mark_unknown("o1")
        self.l.reconcile("o1",proof=self.proof())
        with self.assertRaises(SafetyError): self.reserve("o2")
        self.approve()
        self.reserve("o2")
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),3)

    def test_manual_recovery_revokes_approval_even_after_reconciliation(self):
        self.approve()
        self.reserve("o1")
        self.assertEqual(self.l.recover_unresolved_intents(),("o1",))
        self.assertIn(("SAFETY_CHANGE","A","demo"),self.audit())
        self.l.reconcile("o1",proof=self.proof())
        with self.assertRaises(SafetyError):
            self.reserve("o2")
        self.approve()
        self.reserve("o2")
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),3)

    def test_manual_recovery_does_not_revoke_unrelated_account(self):
        self.approve()
        self.approve(account="B")
        self.reserve("a1")
        self.assertEqual(self.l.recover_unresolved_intents(),("a1",))
        self.reserve("b1",account="B",qty=2)
        self.assertEqual(self.l.account_exposure("B",Mode.DEMO),2)

    def test_explicit_block_removes_token(self):
        self.approve()
        self.runtime.block("A",Mode.DEMO,reason="operator stop")
        with self.assertRaises(SafetyError): self.reserve()
        self.assertIn(("BLOCK","A","demo"),self.audit())

    def test_restart_blocks_old_token_and_recovers_reserved(self):
        self.approve()
        self.reserve("o1",7)
        prior_generation=self.runtime.generation
        restart=TradingRuntime(self.path)
        self.assertEqual(restart.generation,prior_generation+1)
        self.assertEqual(restart.recovered_ids,("o1",))
        self.assertEqual(restart.ledger.get("o1").status,OrderStatus.UNKNOWN)
        self.assertEqual(restart.ledger.account_exposure("A",Mode.DEMO),7)
        with self.assertRaises(SafetyError): self.reserve("o2")
        with self.assertRaises(SafetyError): restart.reserve_demo(
            "o3","alpha","run1",1,self.strategy_version,
            account_id="A",expected_account_version=self.account_version)
        with self.assertRaises(SafetyError): self.approve(runtime=restart)

    def test_new_boot_fences_prior_runtime_even_without_pending_order(self):
        self.approve()
        after=TradingRuntime(self.path)
        with self.assertRaises(SafetyError): self.reserve("o1")
        self.approve(runtime=after)
        after.reserve_demo("o2","alpha","run1",2,self.strategy_version,
                           account_id="A",expected_account_version=self.account_version)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),2)

    def test_global_boot_closes_other_account_until_approved(self):
        self.approve()
        self.approve(account="B")
        restart=TradingRuntime(self.path)
        with self.assertRaises(SafetyError):
            restart.reserve_demo("bad","alpha","r",2,self.other_strategy,
                   account_id="B",expected_account_version=self.other_version)
        self.approve(runtime=restart,account="B")
        self.reserve("yes",runtime=restart,account="B",qty=2)
        self.assertEqual(self.l.account_exposure("B",Mode.DEMO),2)

    def test_unresolved_other_account_does_not_block_demo_account_A(self):
        self.approve(account="B")
        self.reserve("b1",runtime=self.runtime,account="B",qty=2)
        restart=TradingRuntime(self.path)
        self.assertEqual(restart.ledger.get("b1").status,OrderStatus.UNKNOWN)
        self.approve(runtime=restart,account="A")
        self.reserve("a1",runtime=restart)
        self.assertEqual(restart.ledger.account_exposure("A",Mode.DEMO),3)

    def test_legacy_risk_quarantines_all_demo_accounts(self):
        # The legacy order exists before runtime initialization.
        db2=str(Path(self.tmp.name)/"legacy.sqlite")
        legacy=Ledger(db2)
        legacy.configure("old",Mode.DEMO,5)
        legacy.reserve("legacy","old","r",Mode.DEMO,2,1)
        legacy.configure_account("A",Mode.DEMO,10)
        legacy.configure("alpha",Mode.DEMO,10,account_id="A")
        after=TradingRuntime(db2)
        with self.assertRaises(SafetyError):
            after.approve_demo("A",operator_id="mock-operator",
                  review_ref="mock",expected_account_version=1)
        self.assertEqual(after.ledger.get("legacy").status,OrderStatus.UNKNOWN)

    def test_audit_survives_restart(self):
        self.approve()
        original=self.audit()
        restart=TradingRuntime(self.path)
        events=self.audit()
        self.assertEqual(events[:len(original)],original)
        self.assertEqual(events[-1][0],"BOOT_BLOCK")
        self.assertGreater(restart.generation,self.runtime.generation)

    def test_no_side_effects_on_failed_approval(self):
        self.approve()
        self.reserve("o1")
        self.l.mark_unknown("o1")
        count=len(self.audit())
        with self.assertRaises(SafetyError): self.approve()
        self.assertEqual(len(self.audit()),count)
        self.assertEqual(self.l.get("o1").status,OrderStatus.UNKNOWN)

    def test_atomic_fence_when_two_processes_boot(self):
        # Both runtimes may exist, but only the most recent generation's
        # token may admit; a stale runtime can never reopen its old generation.
        self.approve()
        old=self.runtime
        mid=TradingRuntime(self.path)
        last=TradingRuntime(self.path)
        with self.assertRaises(SafetyError): old.reserve_demo(
            "from-old","alpha","run1",1,self.strategy_version,
            account_id="A",expected_account_version=self.account_version)
        with self.assertRaises(SafetyError): self.approve(runtime=mid)
        self.approve(runtime=last)
        self.reserve("latest",runtime=last)
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),3)

    def test_preboot_direct_reservation_is_recovered_not_resubmitted(self):
        path=str(Path(self.tmp.name)/"preboot.sqlite")
        l=Ledger(path)
        l.configure_account("X",Mode.DEMO,5)
        l.configure("alpha",Mode.DEMO,5,account_id="X")
        l.reserve("first","alpha","run",Mode.DEMO,5,1,
                  account_id="X",expected_account_version=1)
        boot=TradingRuntime(path)
        self.assertEqual(boot.recovered_ids,("first",))
        with self.assertRaises(SafetyError):
            l.reserve("first","alpha","run",Mode.DEMO,5,1,
                      account_id="X",expected_account_version=1)
        self.assertEqual(l.account_exposure("X",Mode.DEMO),5)

    def test_demo_bootstrap_cli_forces_boot_and_recovers_intent(self):
        self.approve()
        self.reserve("pending",qty=4)
        previous_generation=self.runtime.generation
        proc=subprocess.run([
            sys.executable,"-m","fxjev.demo_service","--db",self.path],
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,text=True,check=True)
        report=json.loads(proc.stdout)
        self.assertEqual(report["generation"],previous_generation+1)
        self.assertEqual(report["recovered_intent_count"],1)
        self.assertEqual(report["live_trading"],"disabled")
        self.assertIn("blocked",report["admissions"])
        self.assertEqual(self.l.get("pending").status,OrderStatus.UNKNOWN)
        with self.assertRaises(SafetyError): self.reserve("other")
        self.assertEqual(self.l.account_exposure("A",Mode.DEMO),4)

    def test_demo_bootstrap_cli_new_database_defaults_blocked(self):
        path=str(Path(self.tmp.name)/"fresh-demo.sqlite")
        proc=subprocess.run([
            sys.executable,"-m","fxjev.demo_service","--db",path],
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,text=True,check=True)
        report=json.loads(proc.stdout)
        self.assertEqual(report["recovered_intent_count"],0)
        self.assertEqual(report["live_trading"],"disabled")
        self.assertEqual(report["generation"],1)
        boot=Ledger(path)
        with self.assertRaises(SafetyError):
            boot.reserve("any","s","r",Mode.DEMO,1,1,
                         account_id="A",expected_account_version=1)

    def test_gate_cannot_authorize_closed_account_with_manual_token(self):
        self.approve()
        self.runtime.block("A",Mode.DEMO,reason="safety")
        with closing(sqlite3.connect(self.path)) as db:
            state=db.execute("""SELECT state,token FROM runtime_gates
                WHERE account_id='A' AND mode='demo'""").fetchone()
        self.assertEqual(state,("blocked",""))
        with self.assertRaises(SafetyError):
            self.l.reserve("bad","alpha","run1",Mode.DEMO,1,
                self.strategy_version,account_id="A",
                expected_account_version=self.account_version,
                admission_token="fake")


if __name__=="__main__":
    unittest.main()
