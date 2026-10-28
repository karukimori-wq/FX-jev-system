"""Keyless worker entry point; review evidence is mock-only, never live proof."""
import hashlib
import json

from .durable import DurableEngine, SQLiteLedger
from .engine import SafetyError


class MockWorker:
    def __init__(self, ledger: SQLiteLedger, broker):
        self.ledger = ledger
        self.engine = DurableEngine(ledger, broker)
        self.startup_findings = ledger.startup_scan()

    @staticmethod
    def _snapshot(db, run_id):
        # A logic shares capacity across runs. Review all of its evidence in
        # one read transaction, including lazily created quarantine tables.
        payload = {"requested_run": run_id}
        scopes = {
            "run_id": "SELECT run_id FROM runs WHERE logic_id=(SELECT logic_id FROM runs WHERE run_id=?)",
            "decision_id": "SELECT decision_id FROM orders WHERE run_id IN ({runs})",
            "close_id": "SELECT close_id FROM closes WHERE run_id IN ({runs})",
            "position_id": "SELECT position_id FROM positions WHERE run_id IN ({runs})",
        }
        scopes = {key: query.format(runs=scopes["run_id"]) for key, query in scopes.items()}
        tables = {
            "run_id": ("runs", "orders", "positions", "closes", "stop_reasons", "demo_limits", "stop_clearances"),
            "decision_id": ("dispatch_attempts", "cancel_requests", "executions", "entry_reconciliations", "late_entry_fills"),
            "close_id": ("close_cancel_requests", "close_executions", "close_reconciliations", "late_close_fills"),
            "position_id": ("close_conflicts", "close_conflict_clearances"),
        }
        existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for key, names in tables.items():
            for table in names:
                payload[table] = sorted(db.execute(
                    f"SELECT * FROM {table} WHERE {key} IN ({scopes[key]})", (run_id,)
                ).fetchall(), key=repr) if table in existing else []
        for table, source, key in (("fill_repairs", "late_entry_fills", "decision_id"),
                                   ("close_fill_repairs", "late_close_fills", "close_id")):
            payload[table] = sorted(db.execute(
                f"SELECT * FROM {table} WHERE execution_id IN (SELECT execution_id FROM {source} WHERE {key} IN ({scopes[key]}))", (run_id,)
            ).fetchall(), key=repr) if table in existing and source in existing else []
        for table in ("logic_limits", "limit_changes"):
            payload[table] = sorted(db.execute(
                f"SELECT * FROM {table} WHERE logic_id=(SELECT logic_id FROM runs WHERE run_id=?)", (run_id,)
            ).fetchall(), key=repr)
        payload["startup_generation"] = db.execute("SELECT * FROM worker_review_generation ORDER BY id").fetchall() if "worker_review_generation" in existing else []
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def review_token(self, run_id):
        with self.ledger._connect() as db:
            db.execute("BEGIN")
            token = self._snapshot(db, run_id)
            db.commit()
            return token

    def approve_resume(self, run_id, actor, reason, reviewed_token):
        if not actor.strip() or not reason.strip():
            raise SafetyError("actor and reason required")
        with self.ledger.dispatch_gate(), self.ledger._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if self._snapshot(db, run_id) != reviewed_token:
                raise SafetyError("review is stale")
            self.ledger._check_resume_risks(db, run_id, allow_startup_review=True)
            db.execute("UPDATE stop_reasons SET active=0 WHERE run_id=? AND reason='STARTUP_REVIEW'", (run_id,))
            db.execute("CREATE TABLE IF NOT EXISTS worker_approvals (id INTEGER PRIMARY KEY, run_id TEXT, actor TEXT, reason TEXT, reviewed_token TEXT, approved_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            db.execute("INSERT INTO worker_approvals(run_id,actor,reason,reviewed_token) VALUES (?,?,?,?)", (run_id, actor, reason, reviewed_token))
            db.execute("UPDATE runs SET enabled=1 WHERE run_id=?", (run_id,))
            db.commit()

    def submit(self, intent):
        return self.engine.submit_intent(intent)
