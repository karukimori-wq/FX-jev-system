"""SQLite-backed reservation/idempotency slice for restart and concurrency tests.

This is still a local prototype: it deliberately has no live broker adapter.
"""
from __future__ import annotations

import sqlite3
import fcntl
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from .engine import AmbiguousSubmit, SafetyError
from .models import Intent, Mode, OrderState, Run, Side


@dataclass(frozen=True)
class DurableOrder:
    decision_id: str
    run_id: str
    logic_id: str
    pair: str
    side: Side
    quantity: int
    state: OrderState
    external_order_id: str | None


class SQLiteLedger:
    """Small transactional ledger. Each operation uses its own SQLite connection."""

    ACTIVE = ("SUBMITTING", "UNKNOWN", "OPEN", "PARTIAL", "CANCEL_PENDING")

    def __init__(self, path: str | Path) -> None:
        self.path = str(Path(path).resolve())
        self.lock = RLock()
        with self._connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, logic_id TEXT NOT NULL,
                    mode TEXT NOT NULL, max_quantity INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS positions (
                    position_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity >= 0)
                );
                CREATE TABLE IF NOT EXISTS orders (
                    decision_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                    logic_id TEXT NOT NULL, pair TEXT NOT NULL, side TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity > 0),
                    state TEXT NOT NULL, external_order_id TEXT
                );
            """)

            db.execute("CREATE TABLE IF NOT EXISTS logic_limits (logic_id TEXT PRIMARY KEY, live_cap INTEGER NOT NULL CHECK(live_cap BETWEEN 0 AND 1000000))")
            db.execute("CREATE TABLE IF NOT EXISTS demo_limits (run_id TEXT PRIMARY KEY, quantity_cap INTEGER NOT NULL CHECK(quantity_cap > 0))")
            db.execute("CREATE TABLE IF NOT EXISTS limit_changes (id INTEGER PRIMARY KEY, logic_id TEXT, live_cap INTEGER, changed_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            db.execute("CREATE TABLE IF NOT EXISTS closes (close_id TEXT PRIMARY KEY, run_id TEXT, position_id TEXT, quantity INTEGER, filled INTEGER DEFAULT 0)")
            close_columns = {r[1] for r in db.execute("PRAGMA table_info(closes)")}
            if "state" not in close_columns:
                db.execute("ALTER TABLE closes ADD COLUMN state TEXT NOT NULL DEFAULT 'SUBMITTING'")
            if "external_id" not in close_columns:
                db.execute("ALTER TABLE closes ADD COLUMN external_id TEXT")
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS unique_close_external_id ON closes(external_id) WHERE external_id IS NOT NULL")
            db.execute("CREATE TABLE IF NOT EXISTS close_reconciliations (id INTEGER PRIMARY KEY, close_id TEXT, evidence TEXT, resolved INTEGER)")
            db.execute("CREATE TABLE IF NOT EXISTS stop_reasons (run_id TEXT NOT NULL, reason TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(run_id, reason))")
            db.execute("CREATE TABLE IF NOT EXISTS stop_clearances (id INTEGER PRIMARY KEY, run_id TEXT, reason TEXT, actor TEXT, cleared_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            columns = {r[1] for r in db.execute("PRAGMA table_info(orders)")}
            if "filled_quantity" not in columns:
                db.execute("ALTER TABLE orders ADD COLUMN filled_quantity INTEGER NOT NULL DEFAULT 0")
            db.execute("CREATE TABLE IF NOT EXISTS executions (execution_id TEXT PRIMARY KEY, decision_id TEXT NOT NULL, quantity INTEGER NOT NULL)")
            # Legacy SUBMITTING rows may have reached a broker; never infer unsent.
            db.execute("BEGIN IMMEDIATE")
            existed = db.execute("SELECT 1 FROM sqlite_master WHERE name='dispatch_attempts'").fetchone()
            db.execute("CREATE TABLE IF NOT EXISTS dispatch_attempts (decision_id TEXT PRIMARY KEY, attempted_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            if not existed:
                db.execute("INSERT INTO dispatch_attempts(decision_id) SELECT decision_id FROM orders")
            db.execute("CREATE TABLE IF NOT EXISTS cancel_requests (decision_id TEXT PRIMARY KEY, reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING')")
            db.commit()


    def attach_close_external_id(self, close_id: str, external_id: str) -> None:
        if not external_id:
            raise SafetyError("missing external ID")
        with self._connect() as db:
            result = db.execute("UPDATE closes SET external_id=? WHERE close_id=? AND (external_id IS NULL OR external_id=?)", (external_id, close_id, external_id))
            if result.rowcount != 1:
                raise SafetyError("external ID conflict")

    def reconcile_close(self, close_id: str, candidates: list[dict]) -> bool:
        """Consume trusted mock-adapter evidence, never infer absence as rejection."""
        import json
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT run_id, position_id, quantity, filled, state, external_id FROM closes WHERE close_id=?", (close_id,)).fetchone()
            if not row or row[4] != "UNKNOWN":
                raise SafetyError("reconciliation requires UNKNOWN")
            candidate = candidates[0] if len(candidates) == 1 else None
            valid = bool(candidate and candidate.get("close_id") == close_id
                         and candidate.get("run_id") == row[0]
                         and candidate.get("position_id") == row[1]
                         and candidate.get("quantity") == row[2]
                         and candidate.get("filled") == row[3]
                         and 0 <= row[3] < row[2]
                         and candidate.get("state") == ("OPEN" if row[3] == 0 else "PARTIAL")
                         and candidate.get("external_id")
                         and (row[5] is None or row[5] == candidate.get("external_id")))
            db.execute("INSERT INTO close_reconciliations(close_id, evidence, resolved) VALUES (?, ?, ?)", (close_id, json.dumps(candidates, sort_keys=True), int(valid)))
            if valid:
                db.execute("UPDATE closes SET state=?, external_id=? WHERE close_id=?", (candidate["state"], candidate["external_id"], close_id))
            db.commit()
            return valid
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def clear_close_conflict(self, position_id, actor, reason):
        """Mock local review; does not clear quarantine or resume trading."""
        if not actor.strip() or not reason.strip():
            raise SafetyError("actor and reason required")
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='close_conflicts'").fetchone() or not db.execute("SELECT 1 FROM close_conflicts WHERE position_id=?", (position_id,)).fetchone():
                raise SafetyError("no recorded close conflict")
            pos = db.execute("SELECT quantity FROM positions WHERE position_id=?", (position_id,)).fetchone()
            unresolved = db.execute("SELECT 1 FROM closes WHERE position_id=? AND state IN ('SUBMITTING','UNKNOWN')", (position_id,)).fetchone()
            pending = db.execute("SELECT COALESCE(SUM(quantity-filled),0) FROM closes WHERE position_id=? AND state IN ('SUBMITTING','UNKNOWN','OPEN','PARTIAL','CANCEL_PENDING')", (position_id,)).fetchone()[0]
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='close_cancel_requests'").fetchone():
                unresolved = unresolved or db.execute("SELECT 1 FROM close_cancel_requests q JOIN closes c ON c.close_id=q.close_id WHERE c.position_id=? AND q.status NOT IN ('CONFIRMED_CANCELED','CONFIRMED_REJECTED','CONFIRMED_FILLED')", (position_id,)).fetchone()
            if not pos or unresolved or pending > pos[0]:
                raise SafetyError("unresolved close conflict")
            db.execute("CREATE TABLE IF NOT EXISTS close_conflict_clearances (id INTEGER PRIMARY KEY,position_id TEXT,actor TEXT,reason TEXT,pending_quantity INTEGER,remaining_quantity INTEGER)")
            columns = {c[1] for c in db.execute("PRAGMA table_info(close_conflict_clearances)")}
            if 'cleared_at' not in columns:
                db.execute("ALTER TABLE close_conflict_clearances ADD COLUMN cleared_at TEXT")
            db.execute("INSERT INTO close_conflict_clearances(position_id,actor,reason,pending_quantity,remaining_quantity) VALUES (?,?,?,?,?)", (position_id,actor,reason,pending,pos[0]))
            db.execute("UPDATE close_conflict_clearances SET cleared_at=CURRENT_TIMESTAMP WHERE id=last_insert_rowid()")
            db.execute("DELETE FROM close_conflicts WHERE position_id=?", (position_id,))
            db.commit()

    def queue_conflicting_close_cancels(self, position_id):
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS close_cancel_requests (close_id TEXT PRIMARY KEY,status TEXT NOT NULL DEFAULT 'PENDING')")
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='close_conflicts'").fetchone() or not db.execute("SELECT 1 FROM close_conflicts WHERE position_id=?", (position_id,)).fetchone():
                raise SafetyError("no recorded close conflict")
            rows = db.execute("SELECT close_id FROM closes WHERE position_id=? AND state IN ('OPEN','PARTIAL','CANCEL_PENDING') AND external_id IS NOT NULL", (position_id,)).fetchall()
            for (close_id,) in rows:
                db.execute("INSERT OR IGNORE INTO close_cancel_requests(close_id) VALUES (?)", (close_id,))
                db.execute("UPDATE closes SET state='CANCEL_PENDING' WHERE close_id=?", (close_id,))
            db.commit()
            return len(rows)

    def send_close_cancel(self, close_id, adapter):
        with self.dispatch_gate():
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                request = db.execute("SELECT status FROM close_cancel_requests WHERE close_id=?", (close_id,)).fetchone()
                row = db.execute("SELECT run_id,external_id,state FROM closes WHERE close_id=?", (close_id,)).fetchone()
                if not request or request[0] != 'PENDING' or not row or row[2] != 'CANCEL_PENDING':
                    db.commit()
                    return False
                db.execute("UPDATE close_cancel_requests SET status='SENDING' WHERE close_id=?", (close_id,))
                db.commit()
            try:
                if self.run_mode(row[0]) != Mode.DEMO:
                    adapter.cancel_close(row[1])
            except Exception:
                status = 'UNKNOWN'
            else:
                status = 'ACKNOWLEDGED'
            with self._connect() as db:
                db.execute("UPDATE close_cancel_requests SET status=? WHERE close_id=? AND status='SENDING'", (status,close_id))
            return True

    def repair_late_close_fill(self, execution_id, evidence):
        """Trusted mock evidence only; retain quarantine and new-entry stop."""
        import json
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS close_fill_repairs (execution_id TEXT PRIMARY KEY,evidence TEXT NOT NULL)")
            encoded = json.dumps(evidence,sort_keys=True)
            prior = db.execute("SELECT evidence FROM close_fill_repairs WHERE execution_id=?", (execution_id,)).fetchone()
            if prior:
                if prior[0] != encoded:
                    raise SafetyError("close repair evidence mismatch")
                db.commit()
                return False
            late = db.execute("SELECT close_id,quantity FROM late_close_fills WHERE execution_id=?", (execution_id,)).fetchone()
            if not late:
                raise SafetyError("quarantined close fill not found")
            row = db.execute("SELECT run_id,position_id,quantity,filled,state,external_id FROM closes WHERE close_id=?", (late[0],)).fetchone()
            pos = db.execute("SELECT run_id,quantity FROM positions WHERE position_id=?", (row[1],)).fetchone() if row else None
            valid = (row and pos and row[4] == 'CANCELED' and row[5]
                and pos[0] == row[0] and pos[1] >= late[1]
                and evidence.get('execution_id') == execution_id
                and evidence.get('close_id') == late[0] and evidence.get('run_id') == row[0]
                and evidence.get('position_id') == row[1] and evidence.get('external_id') == row[5]
                and type(evidence.get('quantity')) is int and evidence['quantity'] == late[1]
                and type(evidence.get('cumulative_filled')) is int
                and evidence['cumulative_filled'] == row[3]+late[1] <= row[2]
                and evidence.get('terminal_state') == ('FILLED' if row[3]+late[1] == row[2] else 'CANCELED'))
            if not valid:
                raise SafetyError("late close evidence mismatch")
            pending = db.execute("SELECT COALESCE(SUM(quantity-filled),0) FROM closes WHERE position_id=? AND close_id!=? AND state IN ('SUBMITTING','UNKNOWN','OPEN','PARTIAL','CANCEL_PENDING')", (row[1],late[0])).fetchone()[0]
            if pending > pos[1]-late[1]:
                db.execute("CREATE TABLE IF NOT EXISTS close_conflicts (position_id TEXT PRIMARY KEY, pending_quantity INTEGER, remaining_quantity INTEGER)")
                db.execute("INSERT INTO close_conflicts VALUES (?,?,?) ON CONFLICT(position_id) DO UPDATE SET pending_quantity=excluded.pending_quantity,remaining_quantity=excluded.remaining_quantity", (row[1],pending,pos[1]-late[1]))
            db.execute("INSERT INTO close_executions VALUES (?,?,?)", (execution_id,late[0],late[1]))
            db.execute("UPDATE positions SET quantity=quantity-? WHERE position_id=?", (late[1],row[1]))
            db.execute("UPDATE closes SET filled=?,state=? WHERE close_id=?", (evidence['cumulative_filled'],evidence['terminal_state'],late[0]))
            db.execute("INSERT INTO close_fill_repairs VALUES (?,?)", (execution_id,encoded))
            db.commit()
            return True

    def ingest_close_fill(self, close_id, execution_id, quantity):
        """Quarantine unknown terminal close fills without changing holdings."""
        self._quantity(quantity,1)
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS close_executions (execution_id TEXT PRIMARY KEY, close_id TEXT, quantity INTEGER)")
            row = db.execute("SELECT run_id,state FROM closes WHERE close_id=?", (close_id,)).fetchone()
            if not row:
                raise SafetyError("unknown close")
            prior = db.execute("SELECT close_id,quantity FROM close_executions WHERE execution_id=?", (execution_id,)).fetchone()
            if prior:
                if prior != (close_id,quantity):
                    raise SafetyError("close execution mismatch")
                db.commit()
                return False
            if row[1] in ('CANCELED','REJECTED','FILLED'):
                db.execute("CREATE TABLE IF NOT EXISTS late_close_fills (execution_id TEXT PRIMARY KEY, close_id TEXT, quantity INTEGER)")
                old = db.execute("SELECT close_id,quantity FROM late_close_fills WHERE execution_id=?", (execution_id,)).fetchone()
                if old and old != (close_id,quantity):
                    raise SafetyError("late close execution mismatch")
                db.execute("INSERT OR IGNORE INTO late_close_fills VALUES (?,?,?)", (execution_id,close_id,quantity))
                logic = db.execute("SELECT logic_id FROM runs WHERE run_id=?", (row[0],)).fetchone()[0]
                db.execute("UPDATE runs SET enabled=0 WHERE logic_id=?", (logic,))
                db.execute("INSERT INTO stop_reasons(run_id,reason,active) SELECT run_id,'LATE_CLOSE_REVIEW',1 FROM runs WHERE logic_id=? ON CONFLICT(run_id,reason) DO UPDATE SET active=1", (logic,))
                db.commit()
                return False
            db.commit()
            return self.apply_close_fill(close_id,execution_id,quantity)

    def repair_late_entry_fill(self, execution_id, evidence):
        """Apply one quarantined fill using trusted mock evidence; keep stop active."""
        import json
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS fill_repairs (execution_id TEXT PRIMARY KEY, evidence TEXT NOT NULL)")
            encoded = json.dumps(evidence,sort_keys=True)
            old = db.execute("SELECT evidence FROM fill_repairs WHERE execution_id=?", (execution_id,)).fetchone()
            if old:
                if old[0] != encoded:
                    raise SafetyError("repair evidence mismatch")
                db.commit()
                return False
            late = db.execute("SELECT decision_id,quantity FROM late_entry_fills WHERE execution_id=?", (execution_id,)).fetchone()
            if not late:
                raise SafetyError("quarantined fill not found")
            row = db.execute("SELECT run_id,quantity,filled_quantity,state,external_order_id FROM orders WHERE decision_id=?", (late[0],)).fetchone()
            valid = (row and row[3] == 'CANCELED' and row[4]
                and evidence.get('execution_id') == execution_id
                and evidence.get('decision_id') == late[0]
                and evidence.get('run_id') == row[0]
                and evidence.get('external_id') == row[4]
                and type(evidence.get('quantity')) is int and evidence['quantity'] == late[1]
                and type(evidence.get('cumulative_filled')) is int
                and evidence['cumulative_filled'] == row[2]+late[1] <= row[1]
                and evidence.get('terminal_state') == ('FILLED' if row[2]+late[1] == row[1] else 'CANCELED'))
            if not valid:
                raise SafetyError("late fill evidence mismatch")
            db.execute("INSERT INTO executions VALUES (?,?,?)", (execution_id,late[0],late[1]))
            db.execute("INSERT INTO positions VALUES (?,?,?)", ('fill-'+execution_id,row[0],late[1]))
            db.execute("UPDATE orders SET filled_quantity=?,state=? WHERE decision_id=?", (evidence['cumulative_filled'],evidence['terminal_state'],late[0]))
            db.execute("INSERT INTO fill_repairs VALUES (?,?)", (execution_id,encoded))
            db.commit()
            return True

    def ingest_entry_fill(self, decision_id, execution_id, quantity):
        """Persist late terminal events for review; never silently drop exposure."""
        self._quantity(quantity, 1)
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT run_id,state FROM orders WHERE decision_id=?", (decision_id,)).fetchone()
            if not row:
                raise SafetyError("unknown order")
            prior = db.execute("SELECT decision_id,quantity FROM executions WHERE execution_id=?", (execution_id,)).fetchone()
            if prior:
                if prior != (decision_id,quantity):
                    raise SafetyError("execution ID payload mismatch")
                db.commit()
                return False
            if row[1] in ('CANCELED','REJECTED','FILLED'):
                db.execute("CREATE TABLE IF NOT EXISTS late_entry_fills (execution_id TEXT PRIMARY KEY, decision_id TEXT, quantity INTEGER)")
                old = db.execute("SELECT decision_id,quantity FROM late_entry_fills WHERE execution_id=?", (execution_id,)).fetchone()
                if old and old != (decision_id,quantity):
                    raise SafetyError("late execution ID mismatch")
                db.execute("INSERT OR IGNORE INTO late_entry_fills VALUES (?,?,?)", (execution_id,decision_id,quantity))
                logic = db.execute("SELECT logic_id FROM runs WHERE run_id=?", (row[0],)).fetchone()[0]
                db.execute("UPDATE runs SET enabled=0 WHERE logic_id=?", (logic,))
                db.execute("INSERT INTO stop_reasons(run_id,reason,active) SELECT run_id,'LATE_FILL_REVIEW',1 FROM runs WHERE logic_id=? ON CONFLICT(run_id,reason) DO UPDATE SET active=1", (logic,))
                db.commit()
                return False
            db.commit()
            return self.apply_fill(decision_id,execution_id,quantity)

    def reconcile_terminal_close(self, close_id, candidates):
        """Trusted mock terminal evidence; never infer missing executions."""
        import json
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT run_id,position_id,quantity,filled,state,external_id FROM closes WHERE close_id=?", (close_id,)).fetchone()
            c = candidates[0] if len(candidates) == 1 else None
            valid = bool(row and c and c.get('close_id') == close_id
                and c.get('run_id') == row[0] and c.get('position_id') == row[1]
                and type(c.get('quantity')) is int and c['quantity'] == row[2]
                and type(c.get('filled')) is int and c['filled'] == row[3]
                and isinstance(c.get('external_id'),str) and c['external_id']
                and (row[5] is None or row[5] == c['external_id'])
                and c.get('state') in ('CANCELED','REJECTED','FILLED')
                and (row[4] in self.ACTIVE or row[4] == c['state'])
                and (c['state'] != 'FILLED' or row[3] == row[2])
                and (c['state'] != 'REJECTED' or row[3] == 0)
                and (c['state'] != 'CANCELED' or row[3] < row[2]))
            db.execute("INSERT INTO close_reconciliations(close_id,evidence,resolved) VALUES (?,?,?)", (close_id,json.dumps(candidates,sort_keys=True),int(valid)))
            if valid:
                db.execute("UPDATE closes SET state=?,external_id=? WHERE close_id=?", (c['state'],c['external_id'],close_id))
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='close_cancel_requests'").fetchone():
                    db.execute("UPDATE close_cancel_requests SET status=? WHERE close_id=?", ('CONFIRMED_'+c['state'],close_id))
            db.commit()
            return valid

    def close_state(self, close_id: str) -> str:
        with self._connect() as db:
            row = db.execute("SELECT state FROM closes WHERE close_id=?", (close_id,)).fetchone()
            if not row:
                raise SafetyError("unknown close")
            return row[0]

    def transition_close(self, close_id: str, state: str) -> None:
        # Only confirmed broker events may enter a terminal state.
        allowed = {
            "OPEN": ("SUBMITTING",),
            "UNKNOWN": ("SUBMITTING", "OPEN", "PARTIAL", "CANCEL_PENDING"),
            "CANCEL_PENDING": ("OPEN", "PARTIAL"),
            "CANCELED": ("CANCEL_PENDING",),
            "REJECTED": ("SUBMITTING",),
        }
        if state not in allowed:
            raise SafetyError("invalid close transition")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            marks = ",".join("?" for _ in allowed[state])
            result = db.execute(f"UPDATE closes SET state=? WHERE close_id=? AND state IN ({marks})", (state, close_id, *allowed[state]))
            if state == "UNKNOWN":
                db.execute("UPDATE runs SET enabled=0 WHERE run_id=(SELECT run_id FROM closes WHERE close_id=?)", (close_id,))
            if result.rowcount != 1:
                raise SafetyError("invalid close transition")

            db.commit()

    @staticmethod
    def _check_resume_risks(db, run_id, *, allow_startup_review=False):
        """Shared predicate; caller must hold dispatch gate and write transaction."""
        if not db.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone():
            raise SafetyError("unknown run")
        scope = "SELECT run_id FROM runs WHERE logic_id=(SELECT logic_id FROM runs WHERE run_id=?)"
        for table in ("orders", "closes"):
            if db.execute(f"SELECT 1 FROM {table} WHERE run_id IN ({scope}) AND state IN ('SUBMITTING','UNKNOWN','CANCEL_PENDING')", (run_id,)).fetchone():
                raise SafetyError("unresolved orders prevent resume")
        if db.execute(f"SELECT 1 FROM stop_reasons WHERE run_id IN ({scope}) AND active=1 AND (?=0 OR reason!='STARTUP_REVIEW')", (run_id, int(allow_startup_review))).fetchone():
            raise SafetyError("other stop reasons prevent resume")
        existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        checks = {
            "cancel_requests": f"decision_id IN (SELECT decision_id FROM orders WHERE run_id IN ({scope})) AND status NOT IN ('CONFIRMED','FILLED')",
            "close_cancel_requests": f"close_id IN (SELECT close_id FROM closes WHERE run_id IN ({scope})) AND status NOT IN ('CONFIRMED_CANCELED','CONFIRMED_REJECTED','CONFIRMED_FILLED')",
            "late_entry_fills": f"decision_id IN (SELECT decision_id FROM orders WHERE run_id IN ({scope}))",
            "late_close_fills": f"close_id IN (SELECT close_id FROM closes WHERE run_id IN ({scope}))",
            "close_conflicts": f"position_id IN (SELECT position_id FROM positions WHERE run_id IN ({scope}))",
        }
        for table, condition in checks.items():
            if table in existing and db.execute(f"SELECT 1 FROM {table} WHERE {condition}", (run_id,)).fetchone():
                raise SafetyError("unresolved logic evidence prevents resume")

    def approve_mock_resume(self, run_id: str, actor: str, reason: str) -> None:
        """Local mock gate only; not a substitute for live account reconciliation."""
        if not actor.strip() or not reason.strip():
            raise SafetyError("actor and reason required")
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_resume_risks(db, run_id)
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='worker_review_generation'").fetchone():
                raise SafetyError("worker review token required after startup")
            db.execute("CREATE TABLE IF NOT EXISTS resume_approvals (id INTEGER PRIMARY KEY, run_id TEXT, actor TEXT, reason TEXT, approved_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            db.execute("INSERT INTO resume_approvals(run_id, actor, reason) VALUES (?, ?, ?)", (run_id, actor, reason))
            db.execute("UPDATE runs SET enabled=1 WHERE run_id=?", (run_id,))
            db.commit()

    def reserve_close(self, close_id: str, run_id: str, position_id: str, quantity: int) -> bool:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS closes (close_id TEXT PRIMARY KEY, run_id TEXT, position_id TEXT, quantity INTEGER, filled INTEGER DEFAULT 0)")
            old = db.execute("SELECT run_id, position_id, quantity FROM closes WHERE close_id=?", (close_id,)).fetchone()
            if old:
                if old != (run_id, position_id, quantity):
                    raise SafetyError("close ID payload mismatch")
                db.commit()
                return False
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='close_conflicts'").fetchone():
                if db.execute("SELECT 1 FROM close_conflicts WHERE position_id=?", (position_id,)).fetchone():
                    raise SafetyError("close reservation conflict requires review")
            pos = db.execute("SELECT run_id, quantity FROM positions WHERE position_id=?", (position_id,)).fetchone()
            if not pos or pos[0] != run_id:
                raise SafetyError("run does not own this position")
            pending = db.execute("SELECT COALESCE(SUM(quantity-filled),0) FROM closes WHERE position_id=? AND state NOT IN ('CANCELED', 'REJECTED', 'FILLED')", (position_id,)).fetchone()[0]
            if quantity <= 0 or quantity > pos[1] - pending:
                raise SafetyError("close quantity exceeds unreserved position")
            db.execute("INSERT INTO closes (close_id, run_id, position_id, quantity) VALUES (?, ?, ?, ?)", (close_id, run_id, position_id, quantity))
            db.commit()
            return True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def apply_close_fill(self, close_id: str, execution_id: str, quantity: int) -> bool:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS close_executions (execution_id TEXT PRIMARY KEY, close_id TEXT, quantity INTEGER)")
            old = db.execute("SELECT close_id, quantity FROM close_executions WHERE execution_id=?", (execution_id,)).fetchone()
            if old:
                if old != (close_id, quantity):
                    raise SafetyError("close execution mismatch")
                db.commit()
                return False
            row = db.execute("SELECT position_id, quantity, filled, state FROM closes WHERE close_id=?", (close_id,)).fetchone()
            if not row or row[3] not in self.ACTIVE or quantity <= 0 or quantity > row[1]-row[2]:
                raise SafetyError("invalid close fill")
            db.execute("UPDATE positions SET quantity=quantity-? WHERE position_id=?", (quantity, row[0]))
            new_state = "FILLED" if row[2] + quantity == row[1] else ("CANCEL_PENDING" if row[3] == "CANCEL_PENDING" else "PARTIAL")
            db.execute("UPDATE closes SET filled=filled+?, state=? WHERE close_id=?", (quantity, new_state, close_id))
            db.execute("INSERT INTO close_executions VALUES (?, ?, ?)", (execution_id, close_id, quantity))
            db.commit()
            return True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def stop_run(self, run_id: str, reason: str = "MANUAL") -> None:
        if not reason.strip():
            raise SafetyError("stop reason required")
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("UPDATE runs SET enabled=0 WHERE run_id=?", (run_id,)).rowcount != 1:
                raise SafetyError("unknown run")
            db.execute("INSERT INTO stop_reasons VALUES (?, ?, 1) ON CONFLICT(run_id, reason) DO UPDATE SET active=1", (run_id, reason))
            self._cancel_entries(db, "run_id=?", (run_id,), reason)
            db.commit()

    def startup_scan(self) -> list[tuple[str, str, str]]:
        """Explicit worker-start gate; never infer whether SUBMITTING was sent."""
        found = []
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS worker_review_generation (id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER NOT NULL)")
            db.execute("INSERT INTO worker_review_generation VALUES (1,1) ON CONFLICT(id) DO UPDATE SET generation=generation+1")
            for table, key in (("orders", "decision_id"), ("closes", "close_id")):
                found.extend((table, row[0], row[1]) for row in db.execute(f"SELECT {key}, run_id FROM {table} WHERE state IN ('SUBMITTING', 'UNKNOWN')"))
            # Every persisted run requires review on worker start, including apparently clean ones.
            for (run_id,) in db.execute("SELECT run_id FROM runs").fetchall():
                db.execute("UPDATE runs SET enabled=0 WHERE run_id=?", (run_id,))
                db.execute("INSERT INTO stop_reasons VALUES (?, 'STARTUP_REVIEW', 1) ON CONFLICT(run_id, reason) DO UPDATE SET active=1", (run_id,))
            db.commit()
        return found

    def clear_mock_stop(self, run_id: str, reason: str, actor: str) -> None:
        if not actor.strip():
            raise SafetyError("actor required")
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for table in ("orders", "closes"):
                if db.execute(f"SELECT 1 FROM {table} WHERE run_id=? AND state IN ('SUBMITTING','UNKNOWN')", (run_id,)).fetchone():
                    raise SafetyError("unresolved orders prevent clearance")
            if db.execute("UPDATE stop_reasons SET active=0 WHERE run_id=? AND reason=? AND active=1", (run_id, reason)).rowcount != 1:
                raise SafetyError("active stop reason not found")
            db.execute("INSERT INTO stop_clearances(run_id, reason, actor) VALUES (?, ?, ?)", (run_id, reason, actor))
            db.commit()

    def apply_fill(self, decision_id: str, execution_id: str, quantity: int) -> bool:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT decision_id, quantity FROM executions WHERE execution_id=?", (execution_id,)).fetchone()
            if prior:
                if prior != (decision_id, quantity):
                    raise SafetyError("execution ID payload mismatch")
                db.commit()
                return False
            row = db.execute("SELECT run_id, quantity, filled_quantity, state FROM orders WHERE decision_id=?", (decision_id,)).fetchone()
            if row is None or quantity <= 0 or quantity > row[1] - row[2] or row[3] not in self.ACTIVE:
                raise SafetyError("invalid fill")
            filled = row[2] + quantity
            state = "FILLED" if filled == row[1] else ("CANCEL_PENDING" if row[3] == "CANCEL_PENDING" else "PARTIAL")
            db.execute("INSERT INTO executions VALUES (?, ?, ?)", (execution_id, decision_id, quantity))
            db.execute("INSERT INTO positions VALUES (?, ?, ?)", ("fill-" + execution_id, row[0], quantity))
            db.execute("UPDATE orders SET filled_quantity=?, state=? WHERE decision_id=?", (filled, state, decision_id))
            if state == 'FILLED':
                db.execute("UPDATE cancel_requests SET status='FILLED' WHERE decision_id=?", (decision_id,))
            db.commit()
            return True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def confirm_cancel(self, decision_id: str) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            result = db.execute("UPDATE orders SET state='CANCELED' WHERE decision_id=? AND state='CANCEL_PENDING'", (decision_id,))
            if result.rowcount != 1:
                raise SafetyError("cancel confirmation requires CANCEL_PENDING")
            db.execute("UPDATE cancel_requests SET status='CONFIRMED' WHERE decision_id=?", (decision_id,))
            db.commit()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.execute("PRAGMA busy_timeout=10000")
        return db

    @staticmethod
    def _quantity(value, minimum=0, maximum=1_000_000):
        if type(value) is not int or not minimum <= value <= maximum:
            raise SafetyError("invalid integer quantity")

    def set_logic_limit(self, logic_id: str, live_cap: int) -> None:
        """Explicit configuration, never inferred from run caps; lowering keeps holdings."""
        self._quantity(live_cap)
        if not logic_id:
            raise SafetyError("logic ID required")
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT live_cap FROM logic_limits WHERE logic_id=?", (logic_id,)).fetchone()
            db.execute("INSERT INTO logic_limits VALUES (?, ?) ON CONFLICT(logic_id) DO UPDATE SET live_cap=excluded.live_cap", (logic_id, live_cap))
            db.execute("INSERT INTO limit_changes(logic_id,live_cap) VALUES (?, ?)", (logic_id, live_cap))
            if previous and live_cap < previous[0]:
                self._cancel_entries(db, "run_id IN (SELECT run_id FROM runs WHERE logic_id=? AND mode='live')", (logic_id,), "LIMIT_REDUCED")
            db.commit()

    @contextmanager
    def dispatch_gate(self):
        """Local Unix same-file gate; adapters must not call control APIs recursively."""
        with open(self.path + ".dispatch.lock", "a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    @staticmethod
    def _cancel_entries(db, predicate, args, reason):
        rows = db.execute(f"SELECT decision_id, state FROM orders WHERE ({predicate}) AND state IN ('SUBMITTING','UNKNOWN','OPEN','PARTIAL','CANCEL_PENDING')", args).fetchall()
        for decision_id, state in rows:
            attempted = db.execute("SELECT 1 FROM dispatch_attempts WHERE decision_id=?", (decision_id,)).fetchone()
            if state == 'SUBMITTING' and not attempted:
                db.execute("UPDATE orders SET state='CANCELED' WHERE decision_id=?", (decision_id,))
            else:
                db.execute("INSERT OR IGNORE INTO cancel_requests(decision_id,reason) VALUES (?,?)", (decision_id, reason))
                if state in ('OPEN', 'PARTIAL'):
                    db.execute("UPDATE orders SET state='CANCEL_PENDING' WHERE decision_id=?", (decision_id,))

    def dispatch_open(self, decision_id, adapter):
        # Durable claim is committed BEFORE I/O; a crash after claim never retries.
        with self.dispatch_gate():
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT run_id,state FROM orders WHERE decision_id=?", (decision_id,)).fetchone()
                if not row:
                    raise SafetyError("unknown order")
                attempted = db.execute("SELECT 1 FROM dispatch_attempts WHERE decision_id=?", (decision_id,)).fetchone()
                if attempted or row[1] != 'SUBMITTING':
                    db.commit()
                    return self.get_order(decision_id)
                run = db.execute("SELECT enabled,logic_id,mode FROM runs WHERE run_id=?", (row[0],)).fetchone()
                cap = db.execute("SELECT live_cap FROM logic_limits WHERE logic_id=?", (run[1],)).fetchone()
                blocked = not run[0] or (run[2] == 'live' and (not cap or self._live_exposure(db,run[1]) > cap[0]))
                if blocked:
                    db.execute("UPDATE orders SET state='CANCELED' WHERE decision_id=?", (decision_id,))
                    db.commit()
                    return self.get_order(decision_id)
                db.execute("INSERT INTO dispatch_attempts(decision_id) VALUES (?)", (decision_id,))
                db.commit()
            if self.run_mode(row[0]) == Mode.DEMO:
                adapter = DemoBroker()
            try:
                external_id = adapter.submit_open(self.get_order(decision_id))
                if not isinstance(external_id, str) or not external_id:
                    raise SafetyError("missing broker external ID")
            except Exception:
                self.set_state(decision_id, OrderState.UNKNOWN)
            else:
                self.set_state(decision_id, OrderState.OPEN, external_id)
            return self.get_order(decision_id)

    def reconcile_terminal_entry(self, decision_id, candidates):
        """Exact trusted mock evidence; missing fills are never invented."""
        import json
        with self.dispatch_gate(), self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("CREATE TABLE IF NOT EXISTS entry_reconciliations (id INTEGER PRIMARY KEY, decision_id TEXT, evidence TEXT, resolved INTEGER)")
            row = db.execute("SELECT run_id,quantity,filled_quantity,state,external_order_id FROM orders WHERE decision_id=?", (decision_id,)).fetchone()
            c = candidates[0] if len(candidates) == 1 else None
            valid = bool(row and c and c.get('decision_id') == decision_id
                and c.get('run_id') == row[0] and type(c.get('quantity')) is int and c['quantity'] == row[1]
                and type(c.get('filled')) is int and c['filled'] == row[2]
                and isinstance(c.get('external_id'),str) and c['external_id']
                and (row[4] is None or row[4] == c['external_id'])
                and c.get('state') in ('CANCELED','REJECTED','FILLED')
                and (row[3] in self.ACTIVE or row[3] == c['state'])
                and (c['state'] != 'FILLED' or row[2] == row[1])
                and (c['state'] != 'REJECTED' or row[2] == 0)
                and (c['state'] != 'CANCELED' or row[2] < row[1]))
            db.execute("INSERT INTO entry_reconciliations(decision_id,evidence,resolved) VALUES (?,?,?)", (decision_id,json.dumps(candidates,sort_keys=True),int(valid)))
            if valid:
                db.execute("UPDATE orders SET state=?,external_order_id=? WHERE decision_id=?", (c['state'],c['external_id'],decision_id))
                db.execute("UPDATE cancel_requests SET status='CONFIRMED' WHERE decision_id=?", (decision_id,))
            db.commit()
            return valid

    def pending_cancels(self):
        with self._connect() as db:
            return db.execute("SELECT decision_id,reason,status FROM cancel_requests ORDER BY decision_id").fetchall()

    def send_cancel(self, decision_id, adapter):
        """An ACK is not final cancellation. Unknown cancellation is never resent."""
        with self.dispatch_gate():
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT status FROM cancel_requests WHERE decision_id=?", (decision_id,)).fetchone()
                order = self.get_order(decision_id)
                if not row or row[0] != 'PENDING' or not order or order.state != OrderState.CANCEL_PENDING or not order.external_order_id:
                    db.commit()
                    return False
                db.execute("UPDATE cancel_requests SET status='SENDING' WHERE decision_id=?", (decision_id,))
                db.commit()
            if self.run_mode(order.run_id) == Mode.DEMO:
                adapter = DemoBroker()
            try:
                adapter.cancel_open(order)
            except Exception:
                status = 'UNKNOWN'
            else:
                status = 'ACKNOWLEDGED'
            with self._connect() as db:
                db.execute("UPDATE cancel_requests SET status=? WHERE decision_id=? AND status='SENDING'", (status, decision_id))
            return True

    def run_mode(self, run_id: str) -> Mode:
        with self._connect() as db:
            row = db.execute("SELECT mode FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                raise SafetyError("unknown run")
            return Mode(row[0])

    def add_run(self, run: Run, *, demo_quantity_limit: int | None = None) -> None:
        self._quantity(run.max_quantity)
        if run.mode == Mode.DEMO:
            if run.max_quantity != 0:
                raise SafetyError("demo requires max_quantity=0")
            self._quantity(demo_quantity_limit, 1)
        elif run.mode != Mode.LIVE or run.max_quantity == 0 or demo_quantity_limit is not None:
            raise SafetyError("live requires positive cap and no demo settings")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?)",
                       (run.run_id, run.logic_id, run.mode.value, run.max_quantity, int(run.enabled)))
            if run.mode == Mode.DEMO:
                db.execute("INSERT INTO demo_limits VALUES (?, ?)", (run.run_id, demo_quantity_limit))
            db.commit()

    @classmethod
    def _live_exposure(cls, db, logic_id):
        held = db.execute("SELECT COALESCE(SUM(p.quantity),0) FROM positions p JOIN runs r ON p.run_id=r.run_id WHERE r.logic_id=? AND r.mode='live'", (logic_id,)).fetchone()[0]
        marks = ",".join("?" for _ in cls.ACTIVE)
        pending = db.execute(f"SELECT COALESCE(SUM(o.quantity-o.filled_quantity),0) FROM orders o JOIN runs r ON o.run_id=r.run_id WHERE r.logic_id=? AND r.mode='live' AND o.state IN ({marks})", (logic_id, *cls.ACTIVE)).fetchone()[0]
        return held + pending

    def live_exposure(self, logic_id):
        with self._connect() as db:
            db.execute("BEGIN")
            value = self._live_exposure(db, logic_id)
            db.commit()
            return value

    def seed_position(self, position_id: str, run_id: str, quantity: int) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO positions VALUES (?, ?, ?)", (position_id, run_id, quantity))

    def reserve(self, intent: Intent) -> tuple[DurableOrder, bool]:
        """Atomically return existing decision or persist a new reservation."""
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT decision_id, run_id, logic_id, pair, side, quantity, state, external_order_id FROM orders WHERE decision_id=?",
                             (intent.decision_id,)).fetchone()
            if row:
                order = self._to_order(row)
                if (order.run_id, order.pair, order.side, order.quantity) != (
                    intent.run_id, intent.pair, intent.side, intent.quantity
                ):
                    raise SafetyError("decision_id was reused with different order intent")
                db.commit()
                return order, False

            run = db.execute("SELECT logic_id, mode, max_quantity, enabled FROM runs WHERE run_id=?",
                             (intent.run_id,)).fetchone()
            if run is None:
                raise SafetyError("unknown run")
            logic_id, mode, cap, enabled = run
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='late_close_fills'").fetchone():
                if db.execute("SELECT 1 FROM late_close_fills f JOIN closes c ON c.close_id=f.close_id JOIN runs r ON r.run_id=c.run_id WHERE r.logic_id=?", (logic_id,)).fetchone():
                    raise SafetyError("late close fill requires reconciliation")
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='late_entry_fills'").fetchone():
                if db.execute("SELECT 1 FROM late_entry_fills f JOIN orders o ON o.decision_id=f.decision_id WHERE o.logic_id=?", (logic_id,)).fetchone():
                    raise SafetyError("late fill requires reconciliation")
            if db.execute("SELECT 1 FROM closes WHERE run_id=? AND state='UNKNOWN'", (intent.run_id,)).fetchone():
                raise SafetyError("run has UNKNOWN close")
            if not enabled:
                raise SafetyError("run is stopped")
            self._quantity(intent.quantity, 1)
            if mode == Mode.DEMO.value:
                setting = db.execute("SELECT quantity_cap FROM demo_limits WHERE run_id=?", (intent.run_id,)).fetchone()
                if not setting:
                    raise SafetyError("demo quantity limit is not configured")
                cap = setting[0]
            elif mode == Mode.LIVE.value:
                setting = db.execute("SELECT live_cap FROM logic_limits WHERE logic_id=?", (logic_id,)).fetchone()
                if not setting:
                    raise SafetyError("logic live limit is not configured")
                if self._live_exposure(db, logic_id) + intent.quantity > setting[0]:
                    raise SafetyError("logic live quantity cap exceeded")
            else:
                raise SafetyError("invalid mode")
            held = db.execute("SELECT COALESCE(SUM(quantity),0) FROM positions WHERE run_id=?", (intent.run_id,)).fetchone()[0]
            reserved = db.execute(
                "SELECT COALESCE(SUM(quantity - filled_quantity),0) FROM orders WHERE run_id=? AND state IN (?, ?, ?, ?, ?)",
                (intent.run_id, *self.ACTIVE),
            ).fetchone()[0]
            if held + reserved + intent.quantity > cap:
                raise SafetyError("run quantity cap exceeded")
            db.execute("INSERT INTO orders (decision_id, run_id, logic_id, pair, side, quantity, state, external_order_id) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
                       (intent.decision_id, intent.run_id, logic_id, intent.pair, intent.side.value,
                        intent.quantity, OrderState.SUBMITTING.value))
            db.commit()
            return DurableOrder(intent.decision_id, intent.run_id, logic_id, intent.pair,
                                intent.side, intent.quantity, OrderState.SUBMITTING, None), True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def set_state(self, decision_id: str, state: OrderState, external_id: str | None = None) -> None:
        allowed = {
            OrderState.OPEN: ("SUBMITTING",),
            OrderState.UNKNOWN: ("SUBMITTING", "OPEN", "PARTIAL", "CANCEL_PENDING"),
            OrderState.CANCEL_PENDING: ("OPEN", "PARTIAL"),
        }
        if state not in allowed:
            raise SafetyError("state requires a dedicated event handler")
        with self._connect() as db:
            placeholders = ",".join("?" for _ in allowed[state])
            result = db.execute(
                f"UPDATE orders SET state=?, external_order_id=COALESCE(?, external_order_id) WHERE decision_id=? AND state IN ({placeholders})",
                (state.value, external_id, decision_id, *allowed[state]))
            if result.rowcount != 1:
                raise SafetyError("invalid order state transition")

    def get_order(self, decision_id: str) -> DurableOrder | None:
        with self._connect() as db:
            row = db.execute("SELECT decision_id, run_id, logic_id, pair, side, quantity, state, external_order_id FROM orders WHERE decision_id=?",
                             (decision_id,)).fetchone()
            return self._to_order(row) if row else None

    @staticmethod
    def _to_order(row: tuple) -> DurableOrder:
        return DurableOrder(row[0], row[1], row[2], row[3], Side(row[4]), row[5], OrderState(row[6]), row[7])


class DurableEngine:
    """Persists reservation before calling a fake broker, and never resends repeats."""

    def __init__(self, ledger: SQLiteLedger, broker) -> None:
        self.ledger = ledger
        self.broker = broker
        self.demo_broker = DemoBroker()

    def submit_intent(self, intent: Intent) -> DurableOrder:
        order, is_new = self.ledger.reserve(intent)
        if not is_new:
            return order
        adapter = self.demo_broker if self.ledger.run_mode(order.run_id) == Mode.DEMO else self.broker
        return self.ledger.dispatch_open(intent.decision_id, adapter)

    def process_cancellations(self):
        for decision_id, _, _ in self.ledger.pending_cancels():
            order = self.ledger.get_order(decision_id)
            adapter = self.demo_broker if self.ledger.run_mode(order.run_id) == Mode.DEMO else self.broker
            self.ledger.send_cancel(decision_id, adapter)



class DurableCloseEngine:
    """Mock-adapter orchestration. Persist before send; repeats never send."""
    def __init__(self, ledger, broker):
        self.ledger, self.broker = ledger, broker
        self.demo_broker = DemoBroker()

    def submit(self, close_id, run_id, position_id, quantity):
        if not self.ledger.reserve_close(close_id, run_id, position_id, quantity):
            return self.ledger.close_state(close_id)
        with self.ledger.dispatch_gate():
            with self.ledger._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT run_id,quantity FROM positions WHERE position_id=?", (position_id,)).fetchone()
                pending = db.execute("SELECT COALESCE(SUM(quantity-filled),0) FROM closes WHERE position_id=? AND state IN ('SUBMITTING','UNKNOWN','OPEN','PARTIAL','CANCEL_PENDING')", (position_id,)).fetchone()[0]
                conflict = False
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='close_conflicts'").fetchone():
                    conflict = bool(db.execute("SELECT 1 FROM close_conflicts WHERE position_id=?", (position_id,)).fetchone())
                if conflict or not row or row[0] != run_id or pending > row[1]:
                    db.execute("UPDATE closes SET state='REJECTED' WHERE close_id=? AND state='SUBMITTING'", (close_id,))
                    db.commit()
                    return self.ledger.close_state(close_id)
                db.commit()
            try:
                adapter = self.demo_broker if self.ledger.run_mode(run_id) == Mode.DEMO else self.broker
                external_id = adapter.submit_close(run_id, position_id, quantity)
                if not isinstance(external_id, str) or not external_id:
                    raise SafetyError("missing broker external ID")
                self.ledger.attach_close_external_id(close_id, external_id)
            except Exception:
                self.ledger.transition_close(close_id, "UNKNOWN")
            else:
                self.ledger.transition_close(close_id, "OPEN")
            return self.ledger.close_state(close_id)



class DemoBroker:
    """No external I/O. Fills are explicit simulated ledger events, never automatic."""
    def submit_open(self, order):
        return "demo-open-" + order.decision_id

    def cancel_open(self, order):
        return None  # acknowledgement only; explicit simulated terminal event required

    def submit_close(self, run_id, position_id, quantity):
        from uuid import uuid4
        return "demo-close-" + uuid4().hex
