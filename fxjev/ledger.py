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
    """Uses BEGIN IMMEDIATE for process-safe SQLite mock risk accounting.
    No broker adapter or real order sender is included; close evidence is simulated.
    The startup recovery gate must be invoked explicitly by a supervisor.
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
            db.executescript("""
                CREATE TABLE IF NOT EXISTS account_limits (
                    account_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    max_quantity INTEGER NOT NULL CHECK(max_quantity>=0),
                    enabled INTEGER NOT NULL DEFAULT 1,
                    settings_version INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(account_id,mode)
                );
                CREATE TABLE IF NOT EXISTS account_strategies (
                    account_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    max_quantity INTEGER NOT NULL CHECK(max_quantity>=0),
                    enabled INTEGER NOT NULL DEFAULT 1,
                    settings_version INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY(account_id,strategy_id,mode),
                    FOREIGN KEY(account_id,mode)
                      REFERENCES account_limits(account_id,mode)
                );
            """)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS mock_fill_inbox (
                    account_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    execution_id TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    broker_order_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity>0),
                    occurred_at_ns INTEGER NOT NULL CHECK(occurred_at_ns>=0),
                    payload_sha256 TEXT NOT NULL,
                    received_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_id,mode,execution_id)
                );
                CREATE TABLE IF NOT EXISTS mock_event_holds (
                    account_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                    client_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    held_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(account_id,mode,client_id)
                );
                CREATE TABLE IF NOT EXISTS mock_event_conflicts (
                    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    execution_id TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    old_sha256 TEXT NOT NULL,
                    new_sha256 TEXT NOT NULL,
                    detected_at_ns INTEGER NOT NULL
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

    @staticmethod
    def _invalidate_gate(db, account_id: str, mode: Mode,
                         reason: str) -> None:
        """Invalidate any active runtime token in the configuration transaction."""
        if not db.execute("""SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='runtime_gates'""").fetchone():
            return
        result=db.execute("""UPDATE runtime_gates SET state='blocked',
            token='',approved_account_version=NULL,
            approved_by=NULL,approved_ref=NULL
            WHERE account_id=? AND mode=? AND state='open'""",
            (account_id,mode.value))
        if result.rowcount:
            import time
            generation=db.execute("""SELECT generation FROM runtime_control
                WHERE id=1""").fetchone()[0]
            db.execute("""INSERT INTO runtime_gate_audit
                (generation,account_id,mode,action,actor,reference,at_ns)
                VALUES(?,?,?,'SAFETY_CHANGE','system',?,?)""",
                (generation,account_id,mode.value,reason,time.time_ns()))

    def configure_account(self, account_id: str, mode: Mode,
                          max_quantity: int, *, enabled: bool = True) -> int:
        """Register an explicit account-wide risk budget (mock only).

        Legacy orders are never attributed to this account automatically.
        Each update increments a version checked in the reservation transaction.
        """
        if (type(account_id) is not str or not account_id.strip()
                or account_id == "legacy" or type(mode) is not Mode
                or type(max_quantity) is not int or max_quantity < 0
                or type(enabled) is not bool):
            raise SafetyError("invalid account configuration")
        with self._tx() as db:
            prior = db.execute("""SELECT settings_version FROM account_limits
                WHERE account_id=? AND mode=?""",
                (account_id, mode.value)).fetchone()
            version = prior[0] + 1 if prior else 1
            db.execute("""INSERT INTO account_limits
                (account_id,mode,max_quantity,enabled,settings_version)
                VALUES(?,?,?,?,?)
                ON CONFLICT(account_id,mode) DO UPDATE SET
                max_quantity=excluded.max_quantity,
                enabled=excluded.enabled,
                settings_version=excluded.settings_version""",
                (account_id,mode.value,max_quantity,int(enabled),version))
            self._invalidate_gate(db,account_id,mode,"account settings version changed")
        return version

    def configure(self, strategy_id: str, mode: Mode, max_quantity: int,
                  *, enabled: bool = True, account_id: str = "legacy") -> int:
        """Configure a strategy limit for one account and mode.

        The unscoped legacy interface remains for old PoC databases only.
        Explicit account settings and account-wide budgets are separate.
        """
        if (type(strategy_id) is not str or not strategy_id.strip()
                or type(account_id) is not str or not account_id.strip()
                or type(mode) is not Mode or type(max_quantity) is not int
                or max_quantity < 0 or type(enabled) is not bool):
            raise SafetyError("invalid strategy configuration")
        with self._tx() as db:
            if account_id == "legacy":
                row=db.execute("SELECT settings_version FROM strategies WHERE strategy_id=? AND mode=?",
                    (strategy_id,mode.value)).fetchone()
                version=row[0]+1 if row else 1
                db.execute("""INSERT INTO strategies(strategy_id,mode,max_quantity,enabled,settings_version)
                    VALUES(?,?,?,?,?) ON CONFLICT(strategy_id,mode) DO UPDATE SET
                    max_quantity=excluded.max_quantity,enabled=excluded.enabled,
                    settings_version=excluded.settings_version""",
                    (strategy_id,mode.value,max_quantity,int(enabled),version))
            else:
                # Old orders FK still references strategies(strategy_id,mode).
                # A disabled/zero-limit legacy anchor preserves that FK while
                # actual scoped permission comes only from account_strategies.
                db.execute("""INSERT OR IGNORE INTO strategies
                    (strategy_id,mode,max_quantity,enabled,settings_version)
                    VALUES(?,?,0,0,1)""", (strategy_id,mode.value))
                if not db.execute("""SELECT 1 FROM account_limits
                        WHERE account_id=? AND mode=?""",
                        (account_id,mode.value)).fetchone():
                    raise SafetyError("account budget must be configured first")
                row=db.execute("""SELECT settings_version FROM account_strategies
                    WHERE account_id=? AND strategy_id=? AND mode=?""",
                    (account_id,strategy_id,mode.value)).fetchone()
                version=row[0]+1 if row else 1
                db.execute("""INSERT INTO account_strategies
                    (account_id,strategy_id,mode,max_quantity,enabled,settings_version)
                    VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,strategy_id,mode)
                    DO UPDATE SET max_quantity=excluded.max_quantity,
                    enabled=excluded.enabled,
                    settings_version=excluded.settings_version""",
                    (account_id,strategy_id,mode.value,max_quantity,
                     int(enabled),version))
            self._invalidate_gate(db,account_id,mode,"strategy settings version changed")
        return version

    @staticmethod
    def _exposure_query(where_clause: str) -> str:
        return ("""SELECT COALESCE(SUM(CASE
            WHEN EXISTS (SELECT 1 FROM mock_event_holds h
                WHERE h.client_id=o.client_id AND h.mode=o.mode
                  AND h.account_id=o.account_id) THEN o.quantity
            WHEN o.status='canceled' THEN
                COALESCE(p.remaining_quantity,o.filled_quantity)
            WHEN o.status='filled' THEN
                COALESCE(p.remaining_quantity,o.filled_quantity)
            ELSE o.quantity END),0)
            FROM orders o LEFT JOIN position_balances p ON p.client_id=o.client_id
            WHERE """ + where_clause)

    def reserve(self, client_id: str, strategy_id: str, run_id: str, mode: Mode,
                quantity: int, expected_settings_version: int,
                *, account_id: str = "legacy",
                expected_account_version: int | None = None,
                admission_token: str | None = None) -> Reservation:
        if (not all(type(v) is str and v.strip() for v in (
                client_id,strategy_id,run_id,account_id))
                or type(mode) is not Mode or type(quantity) is not int
                or quantity <= 0 or type(expected_settings_version) is not int):
            raise SafetyError("invalid order request")
        with self._tx() as db:
            # A runtime-managed database requires the current boot's
            # admission token; the check and reservation are ONE transaction.
            if db.execute("""SELECT 1 FROM sqlite_master WHERE type='table'
                AND name='runtime_control'""").fetchone():
                if mode is Mode.LIVE or account_id=="legacy":
                    raise SafetyError("runtime gate prohibits LIVE and legacy admissions")
                current=db.execute("SELECT generation FROM runtime_control WHERE id=1").fetchone()
                gate=db.execute("""SELECT state,token,generation,
                    approved_account_version FROM runtime_gates
                    WHERE account_id=? AND mode=?""",
                    (account_id,mode.value)).fetchone()
                if (not current or not gate or gate[0]!="open"
                        or not admission_token or type(admission_token) is not str
                        or gate[1]!=admission_token or gate[2]!=current[0]
                        or gate[3]!=expected_account_version):
                    raise SafetyError("startup gate is closed, stale or unauthorized")
            # Durable event anomalies override any previous mock approval.
            if db.execute("""SELECT 1 FROM mock_event_holds
                WHERE mode=? AND account_id IN (?, 'legacy') LIMIT 1""",
                (mode.value,account_id)).fetchone():
                raise SafetyError("mock fill anomaly quarantines account admissions")
            if db.execute("SELECT 1 FROM orders WHERE client_id=?",
                          (client_id,)).fetchone():
                raise SafetyError("duplicate client ID: reconcile instead of resending")
            if account_id == "legacy":
                if expected_account_version is not None:
                    raise SafetyError("legacy reservations cannot claim an account version")
                strategy=db.execute("""SELECT max_quantity,enabled,settings_version
                    FROM strategies WHERE strategy_id=? AND mode=?""",
                    (strategy_id,mode.value)).fetchone()
                # Legacy attribution is unknown; keep its mode-wide halt behavior.
                unknown=db.execute("""SELECT 1 FROM orders
                    WHERE mode=? AND status='unknown' LIMIT 1""",
                    (mode.value,)).fetchone()
                account_cap=None
            else:
                if type(expected_account_version) is not int:
                    raise SafetyError("explicit account settings version is mandatory")
                account_cap=db.execute("""SELECT max_quantity,enabled,settings_version
                    FROM account_limits WHERE account_id=? AND mode=?""",
                    (account_id,mode.value)).fetchone()
                if (not account_cap or not account_cap[1]
                        or account_cap[2] != expected_account_version):
                    raise SafetyError("missing, disabled or stale account safety settings")
                strategy=db.execute("""SELECT max_quantity,enabled,settings_version
                    FROM account_strategies
                    WHERE account_id=? AND strategy_id=? AND mode=?""",
                    (account_id,strategy_id,mode.value)).fetchone()
                # Unattributed legacy live exposure could belong to ANY account.
                # Never allocate it to a new account or silently ignore it.
                if db.execute(self._exposure_query(
                        "o.account_id='legacy' AND o.mode=?"),
                        (mode.value,)).fetchone()[0] > 0:
                    raise SafetyError("unattributed legacy risk blocks account reservations")
                unknown=db.execute("""SELECT 1 FROM orders
                    WHERE mode=? AND account_id=? AND status='unknown' LIMIT 1""",
                    (mode.value,account_id)).fetchone()
            if (not strategy or not strategy[1]
                    or strategy[2] != expected_settings_version):
                raise SafetyError("missing, disabled or stale strategy settings")
            if unknown:
                raise SafetyError("UNKNOWN order blocks this account and mode")
            used=db.execute(self._exposure_query(
                "o.account_id=? AND o.strategy_id=? AND o.mode=?"),
                (account_id,strategy_id,mode.value)).fetchone()[0]
            if used+quantity > strategy[0]:
                raise SafetyError("strategy quantity limit exceeded")
            if account_cap is not None:
                account_used=db.execute(self._exposure_query(
                    "o.account_id=? AND o.mode=?"),
                    (account_id,mode.value)).fetchone()[0]
                if account_used+quantity > account_cap[0]:
                    raise SafetyError("account total quantity limit exceeded")
            db.execute("""INSERT INTO orders(client_id,strategy_id,run_id,mode,
                account_id,quantity,filled_quantity,status) VALUES(?,?,?,?,?,?,0,?)""",
                (client_id,strategy_id,run_id,mode.value,account_id,
                 quantity,OrderStatus.RESERVED.value))
        return Reservation(client_id,strategy_id,run_id,mode,quantity,0,
                           OrderStatus.RESERVED,account_id)

    def account_exposure(self, account_id: str, mode: Mode) -> int:
        """Read account-wide exposure; callers must not use this to place orders."""
        if (type(account_id) is not str or not account_id.strip()
                or type(mode) is not Mode):
            raise SafetyError("invalid account exposure query")
        with closing(sqlite3.connect(self.filename)) as db:
            return db.execute(self._exposure_query(
                "o.account_id=? AND o.mode=?"),
                (account_id,mode.value)).fetchone()[0]

    def mark_unknown(self,client_id: str)->None:
        with self._tx() as db:
            row=db.execute("""SELECT account_id,mode FROM orders
                WHERE client_id=? AND status='reserved'""",
                (client_id,)).fetchone()
            if not row:
                raise SafetyError("unresolved reserved order required")
            db.execute("""UPDATE orders SET status='unknown'
                WHERE client_id=? AND status='reserved'""",(client_id,))
            self._invalidate_gate(db,row[0],Mode(row[1]),"order became UNKNOWN")

    def recover_unresolved_intents(self) -> tuple[str, ...]:
        """Explicit startup safety gate after an unclean or uncertain shutdown.

        RESERVED does not prove that an order was never sent. Conservatively
        turn all such intents into UNKNOWN and keep their *full* risk reserved.
        Call before enabling admissions on EVERY restart; this prototype has no
        process supervisor to enforce that prerequisite automatically.
        """
        import time

        with self._tx() as db:
            rows=db.execute("""SELECT client_id,account_id,mode FROM orders
                WHERE status='reserved' ORDER BY client_id""").fetchall()
            if not rows:
                return ()
            db.execute("""CREATE TABLE IF NOT EXISTS startup_recovery_audit (
                client_id TEXT PRIMARY KEY REFERENCES orders(client_id),
                from_status TEXT NOT NULL,
                recovered_at_ns INTEGER NOT NULL
            )""")
            now=time.time_ns()
            for (client_id,account_id,mode_name) in rows:
                db.execute("""INSERT INTO startup_recovery_audit
                    (client_id,from_status,recovered_at_ns)
                    VALUES(?,'reserved',?)""",(client_id,now))
                self._invalidate_gate(db,account_id,Mode(mode_name),
                                      "explicit recovery found unresolved intent")
            db.execute("""UPDATE orders SET status='unknown'
                WHERE status='reserved'""")
        return tuple(row[0] for row in rows)

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

    def active_exposure(self, strategy_id: str, mode: Mode,
                        *, account_id: str | None = None) -> int:
        """Report strategy exposure, optionally restricted to a single account.

        Omitted account_id preserves earlier reporting behavior by aggregating
        every account; order admission always uses explicit scoped SQL instead.
        """
        if (type(strategy_id) is not str or not strategy_id.strip()
                or type(mode) is not Mode
                or (account_id is not None and
                    (type(account_id) is not str or not account_id.strip()))):
            raise SafetyError("invalid strategy exposure query")
        clause="o.strategy_id=? AND o.mode=?"
        args=(strategy_id,mode.value)
        if account_id is not None:
            clause+=" AND o.account_id=?"
            args+=(account_id,)
        with closing(sqlite3.connect(self.filename)) as db:
            return db.execute(self._exposure_query(clause),args).fetchone()[0]
