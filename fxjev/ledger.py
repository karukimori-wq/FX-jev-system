"""Broker-independent SQLite reservation proof of concept. No broker sends."""
import sqlite3
from contextlib import contextmanager
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
                    quantity INTEGER NOT NULL CHECK(quantity>0),
                    filled_quantity INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL CHECK(status IN ('reserved','unknown','filled','canceled')),
                    CHECK(filled_quantity BETWEEN 0 AND quantity),
                    FOREIGN KEY(strategy_id,mode) REFERENCES strategies(strategy_id,mode)
                );
            """)

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
                quantity: int,expected_settings_version: int)->Reservation:
        if not all((client_id,strategy_id,run_id)) or type(quantity) is not int or quantity<=0:
            raise SafetyError("invalid order request")
        with self._tx() as db:
            if db.execute("SELECT 1 FROM orders WHERE client_id=?",(client_id,)).fetchone():
                raise SafetyError("duplicate client ID: reconcile instead of resending")
            row=db.execute("""SELECT max_quantity,enabled,settings_version FROM strategies
                WHERE strategy_id=? AND mode=?""",(strategy_id,mode.value)).fetchone()
            if not row or not row[1] or row[2]!=expected_settings_version:
                raise SafetyError("missing, disabled or stale safety settings")
            used=db.execute("""SELECT COALESCE(SUM(CASE status
                WHEN 'canceled' THEN filled_quantity ELSE quantity END),0)
                FROM orders WHERE strategy_id=? AND mode=?""",
                (strategy_id,mode.value)).fetchone()[0]
            if used+quantity>row[0]:
                raise SafetyError("strategy quantity limit exceeded")
            db.execute("""INSERT INTO orders(client_id,strategy_id,run_id,mode,quantity,status)
                VALUES(?,?,?,?,?,?)""",(client_id,strategy_id,run_id,mode.value,
                quantity,OrderStatus.RESERVED.value))
        return Reservation(client_id,strategy_id,run_id,mode,quantity,0,OrderStatus.RESERVED)

    def mark_unknown(self,client_id: str)->None:
        with self._tx() as db:
            changed=db.execute("""UPDATE orders SET status='unknown'
                WHERE client_id=? AND status='reserved'""",(client_id,)).rowcount
            if not changed:
                raise SafetyError("unresolved reserved order required")

    def reconcile(self,client_id: str,*,status: OrderStatus,
                  filled_quantity: int,evidence_complete: bool)->None:
        if not evidence_complete or status not in (OrderStatus.CANCELED,OrderStatus.FILLED):
            raise SafetyError("complete final broker evidence required")
        if type(filled_quantity) is not int or filled_quantity<0:
            raise SafetyError("invalid fill quantity")
        with self._tx() as db:
            row=db.execute("SELECT quantity,status FROM orders WHERE client_id=?",
                (client_id,)).fetchone()
            if not row or row[1] not in ("unknown","reserved"):
                raise SafetyError("no unresolved intent")
            if filled_quantity>row[0] or (status==OrderStatus.FILLED and filled_quantity!=row[0]):
                raise SafetyError("inconsistent fill evidence")
            db.execute("UPDATE orders SET status=?,filled_quantity=? WHERE client_id=?",
                (status.value,filled_quantity,client_id))

    def get(self,client_id: str):
        with sqlite3.connect(self.filename) as db:
            row=db.execute("""SELECT client_id,strategy_id,run_id,mode,quantity,
                filled_quantity,status FROM orders WHERE client_id=?""",(client_id,)).fetchone()
        return Reservation(row[0],row[1],row[2],Mode(row[3]),row[4],
                           row[5],OrderStatus(row[6])) if row else None

    def active_exposure(self,strategy_id: str,mode: Mode)->int:
        with sqlite3.connect(self.filename) as db:
            return db.execute("""SELECT COALESCE(SUM(CASE status
                WHEN 'canceled' THEN filled_quantity ELSE quantity END),0)
                FROM orders WHERE strategy_id=? AND mode=?""",
                (strategy_id,mode.value)).fetchone()[0]
