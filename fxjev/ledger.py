"""Broker-independent SQLite reservation proof of concept. No broker sends."""
import sqlite3
from contextlib import contextmanager, closing
from dataclasses import dataclass
from enum import Enum

class SafetyError(Exception):
    pass

class Mode(str, Enum):
    DEMO = "demo"
    LIVE = "live"

class OrderStatus(str, Enum):
    RESERVED = "reserved"
    UNKNOWN = "unknown"
    FILLED = "filled"
    CANCELED = "canceled"

@dataclass(frozen=True)
class Reservation:
    client_id: str
    strategy_id: str
    run_id: str
    mode: Mode
    quantity: int
    filled_quantity: int
    status: OrderStatus
    account_id: str = "legacy"

class Ledger:
    """Uses BEGIN IMMEDIATE to serialize risk reservations across processes.
    No broker adapter or position-closing implementation is included.
    """
    def __init__(self, filename: str):
        self.filename = filename
        with self._tx() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS strategies (
                    strategy_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    max_quantity INTEGER NOT NULL CHECK(max_quantity>=0),
                    enabled INTEGER NOT NULL DEFAULT 1,
                    settings_version INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(strategy_id,mode)
                );
                CREATE TABLE IF NOT EXISTS orders (
                    client_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    account_id TEXT NOT NULL DEFAULT 'legacy',
                    quantity INTEGER NOT NULL CHECK(quantity>0),
                    filled_quantity INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL CHECK(status IN ('reserved','unknown','filled','canceled')),
                    CHECK(filled_quantity BETWEEN 0 AND quantity),
                    FOREIGN KEY(strategy_id,mode) REFERENCES strategies(strategy_id,mode)
                );
            """)
            # Backward-compatible migration of existing P0 SQLite databases:
            # old orders retain "legacy" ownership until independently verified.
            fields = {row[1] for row in db.execute("PRAGMA table_info(orders)")}
            if "account_id" not in fields:
                db.execute("ALTER TABLE orders ADD COLUMN account_id TEXT NOT NULL DEFAULT 'legacy'")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS position_balances (
                    client_id TEXT PRIMARY KEY REFERENCES orders(client_id),
                    account_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    broker_entry_order_id TEXT NOT NULL,
                    opened_quantity INTEGER NOT NULL CHECK(opened_quantity>0),
                    remaining_quantity INTEGER NOT NULL CHECK(remaining_quantity>=0),
                    CHECK(remaining_quantity<=opened_quantity),
                    UNIQUE(account_id,broker_entry_order_id)
                );
                CREATE TABLE IF NOT EXISTS position_close_batches (
                    account_id TEXT NOT NULL,
                    close_order_id TEXT NOT NULL,
                    client_id TEXT NOT NULL REFERENCES position_balances(client_id),
                    close_status TEXT NOT NULL DEFAULT 'filled',
                    requested_quantity INTEGER NOT NULL DEFAULT 0,
                    quantity INTEGER NOT NULL CHECK(quantity>0),
                    remaining_quantity INTEGER NOT NULL CHECK(remaining_quantity>=0),
                    evidence_sha256 TEXT NOT NULL,
                    recorded_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_id,close_order_id)
                );
                CREATE TABLE IF NOT EXISTS position_close_executions (
                    account_id TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    close_order_id TEXT NOT NULL,
                    client_id TEXT NOT NULL REFERENCES position_balances(client_id),
                    quantity INTEGER NOT NULL CHECK(quantity>0),
                    PRIMARY KEY(account_id,execution_id),
                    FOREIGN KEY(account_id,close_order_id)
                      REFERENCES position_close_batches(account_id,close_order_id)
                );
            """)
            # Compatible with databases created by the earliest P0 position PoC.
            close_fields = {row[1] for row in
                            db.execute("PRAGMA table_info(position_close_batches)")}
            if "close_status" not in close_fields:
                db.execute("""ALTER TABLE position_close_batches
                    ADD COLUMN close_status TEXT NOT NULL DEFAULT 'filled'""")
            if "requested_quantity" not in close_fields:
                db.execute("""ALTER TABLE position_close_batches
                    ADD COLUMN requested_quantity INTEGER NOT NULL DEFAULT 0""")

    @contextmanager
    def _tx(self):
        db=sqlite3.connect(self.filename,timeout=10,isolation_level=None)
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def configure(self,strategy_id: str,mode: Mode,max_quantity: int,*,enabled=True)->int:
        if not strategy_id or type(max_quantity) is not int or max_quantity<0:
            raise SafetyError("invalid configuration")
        with self._tx() as db:
            row=db.execute("SELECT settings_version FROM strategies WHERE strategy_id=? AND mode=?",
                (strategy_id,mode.value)).fetchone()
            version=row[0]+1 if row else 1
            db.execute("""INSERT INTO strategies(strategy_id,mode,max_quantity,enabled,settings_version)
                VALUES(?,?,?,?,?) ON CONFLICT(strategy_id,mode) DO UPDATE SET
                max_quantity=excluded.max_quantity,enabled=excluded.enabled,
                settings_version=excluded.settings_version""",
                (strategy_id,mode.value,max_quantity,int(enabled),version))
            return version

    def reserve(self,client_id: str,strategy_id: str,run_id: str,mode: Mode,
                quantity: int,expected_settings_version: int,*,account_id: str="legacy")->Reservation:
        if (not all(type(v) is str and v.strip() for v in (client_id,strategy_id,run_id,account_id))
                or type(mode) is not Mode or type(quantity) is not int or quantity<=0):
            raise SafetyError("invalid order request")
        with self._tx() as db:
            if db.execute("SELECT 1 FROM orders WHERE client_id=?",(client_id,)).fetchone():
                raise SafetyError("duplicate client ID: reconcile instead of resending")
            row=db.execute("""SELECT max_quantity,enabled,settings_version FROM strategies
                WHERE strategy_id=? AND mode=?""",(strategy_id,mode.value)).fetchone()
            if not row or not row[1] or row[2]!=expected_settings_version:
                raise SafetyError("missing, disabled or stale safety settings")
            # An unresolved order blocks new reservations in this trading mode.
            # This is conservative: account-scoped halts require an account ledger.
            if db.execute("SELECT 1 FROM orders WHERE mode=? AND status='unknown' LIMIT 1",
                          (mode.value,)).fetchone():
                raise SafetyError("UNKNOWN order blocks new reservations in this mode")
            used=db.execute("""SELECT COALESCE(SUM(CASE o.status
                WHEN 'canceled' THEN COALESCE(p.remaining_quantity,o.filled_quantity)
                WHEN 'filled' THEN COALESCE(p.remaining_quantity,o.filled_quantity)
                ELSE o.quantity END),0)
                FROM orders o LEFT JOIN position_balances p ON p.client_id=o.client_id
                WHERE o.strategy_id=? AND o.mode=?""",
                (strategy_id,mode.value)).fetchone()[0]
            if used+quantity>row[0]:
                raise SafetyError("strategy quantity limit exceeded")
            db.execute("""INSERT INTO orders(client_id,strategy_id,run_id,mode,
                account_id,quantity,status) VALUES(?,?,?,?,?,?,?)""",
                (client_id,strategy_id,run_id,mode.value,account_id,
                 quantity,OrderStatus.RESERVED.value))
        return Reservation(client_id,strategy_id,run_id,mode,quantity,0,
                           OrderStatus.RESERVED,account_id)

    def mark_unknown(self,client_id: str)->None:
        with self._tx() as db:
            changed=db.execute("""UPDATE orders SET status='unknown'
                WHERE client_id=? AND status='reserved'""",(client_id,)).rowcount
            if not changed:
                raise SafetyError("unresolved reserved order required")

    def reconcile(self, client_id: str, *, proof):
        """Reconcile with three checked sources; no boolean evidence shortcut."""
        from .reconciliation import reconcile_order
        return reconcile_order(self, client_id, proof)

    def close_position(self, *, proof):
        """Apply simulated independently attributed close evidence."""
        from .positions import close_position
        return close_position(self, proof)

    def position_remaining(self, client_id: str):
        with closing(sqlite3.connect(self.filename)) as db:
            row=db.execute("SELECT remaining_quantity FROM position_balances WHERE client_id=?",
                           (client_id,)).fetchone()
        return row[0] if row else None

    def get(self,client_id: str):
        with closing(sqlite3.connect(self.filename)) as db:
            row=db.execute("""SELECT client_id,strategy_id,run_id,mode,quantity,
                filled_quantity,status,account_id FROM orders WHERE client_id=?""",
                (client_id,)).fetchone()
        return Reservation(row[0],row[1],row[2],Mode(row[3]),row[4],
                           row[5],OrderStatus(row[6]),row[7]) if row else None

    def active_exposure(self,strategy_id: str,mode: Mode)->int:
        with closing(sqlite3.connect(self.filename)) as db:
            return db.execute("""SELECT COALESCE(SUM(CASE o.status
                WHEN 'canceled' THEN COALESCE(p.remaining_quantity,o.filled_quantity)
                WHEN 'filled' THEN COALESCE(p.remaining_quantity,o.filled_quantity)
                ELSE o.quantity END),0)
                FROM orders o LEFT JOIN position_balances p ON p.client_id=o.client_id
                WHERE o.strategy_id=? AND o.mode=?""",
                (strategy_id,mode.value)).fetchone()[0]
