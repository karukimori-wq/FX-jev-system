"""Migration and strict account identity tests for the broker-free P0 PoC."""
from dataclasses import replace
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from fxjev.ledger import Ledger, Mode, OrderStatus, SafetyError
from proof_helpers import proof as entry_proof


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "old.sqlite")

    def test_existing_orders_migrate_account_identity_conservatively(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TABLE strategies (
                strategy_id TEXT NOT NULL, mode TEXT NOT NULL,
                max_quantity INTEGER NOT NULL, enabled INTEGER NOT NULL,
                settings_version INTEGER NOT NULL,
                PRIMARY KEY(strategy_id,mode))""")
            db.execute("""CREATE TABLE orders (
                client_id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL,
                run_id TEXT NOT NULL, mode TEXT NOT NULL, quantity INTEGER NOT NULL,
                filled_quantity INTEGER NOT NULL, status TEXT NOT NULL)""")
            db.execute("INSERT INTO strategies VALUES ('s','live',12,1,1)")
            db.execute("INSERT INTO orders VALUES ('legacy1','s','r1','live',5,5,'filled')")
            db.commit()
        ledger = Ledger(self.path)
        self.assertEqual(ledger.get("legacy1").account_id, "legacy")
        self.assertIsNone(ledger.position_remaining("legacy1"))
        self.assertEqual(ledger.active_exposure("s", Mode.LIVE), 5)
        # Never infer a newly verified position from historical order records.
        ledger.reserve("new1", "s", "r2", Mode.LIVE, 7, 1, account_id="acctB")
        with self.assertRaises(SafetyError):
            ledger.reserve("new2", "s", "r2", Mode.LIVE, 1, 1, account_id="acctB")

    def test_older_position_close_audit_schema_gets_new_columns(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("""CREATE TABLE position_close_batches (
                account_id TEXT NOT NULL,
                close_order_id TEXT NOT NULL,
                client_id TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                remaining_quantity INTEGER NOT NULL,
                evidence_sha256 TEXT NOT NULL,
                recorded_at_ns INTEGER NOT NULL,
                PRIMARY KEY(account_id,close_order_id))""")
            db.commit()
        Ledger(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            cols={r[1] for r in db.execute("PRAGMA table_info(position_close_batches)")}
        self.assertIn("requested_quantity",cols)
        self.assertIn("close_status",cols)

    def test_entry_reconciliation_rejects_account_substitution(self):
        ledger=Ledger(self.path)
        v=ledger.configure("s", Mode.LIVE, 10)
        ledger.reserve("entry", "s", "run", Mode.LIVE, 6, v, account_id="acctB")
        ledger.mark_unknown("entry")
        p=entry_proof("entry", qty=6, filled=6, status=OrderStatus.FILLED,
                      strategy="s",run="run",mode=Mode.LIVE)
        with self.assertRaises(SafetyError):
            ledger.reconcile("entry",proof=p)
        self.assertEqual(ledger.get("entry").status,OrderStatus.UNKNOWN)
        corrected=replace(
            p,order=replace(p.order,account_id="acctB"),
            position=replace(p.position,account_id="acctB"))
        ledger.reconcile("entry",proof=corrected)
        self.assertEqual(ledger.position_remaining("entry"),6)

if __name__ == "__main__":
    unittest.main()
