"""Fail-closed, broker-free startup gate with per-boot fencing.

Only DEMO reservations may be enabled. Neither operator_id nor review_ref is
authenticated by this prototype; they are audit labels, NOT production approvals.
Real broker calls and live order senders must never depend on this module.
"""
import secrets
import time

from .ledger import Ledger, Mode, SafetyError


class TradingRuntime:
    """A new instance represents a *new boot*, not a copied trading session.

    Boot atomically closes all existing gates and moves old RESERVED intents
    to UNKNOWN before allowing admissions; active previous-process sessions
    are fenced by generation plus a random per-gate token. Each account/mode
    must be explicitly reapproved after boot; LIVE can never be opened here.
    """

    def __init__(self, filename: str):
        self.ledger = Ledger(filename)
        self._tokens: dict[tuple[str, Mode], str] = {}
        with self.ledger._tx() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS runtime_control (
                id INTEGER PRIMARY KEY CHECK(id=1),
                generation INTEGER NOT NULL CHECK(generation>=0)
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS runtime_gates (
                account_id TEXT NOT NULL,
                mode TEXT NOT NULL CHECK(mode IN ('demo','live')),
                state TEXT NOT NULL CHECK(state IN ('blocked','open')),
                generation INTEGER NOT NULL,
                token TEXT NOT NULL DEFAULT '',
                approved_account_version INTEGER,
                approved_by TEXT,
                approved_ref TEXT,
                PRIMARY KEY(account_id,mode)
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS runtime_gate_audit (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                generation INTEGER NOT NULL,
                account_id TEXT,
                mode TEXT,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                reference TEXT,
                at_ns INTEGER NOT NULL
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS startup_recovery_audit (
                client_id TEXT PRIMARY KEY REFERENCES orders(client_id),
                from_status TEXT NOT NULL,
                recovered_at_ns INTEGER NOT NULL
            )""")
            db.execute("INSERT OR IGNORE INTO runtime_control(id,generation) VALUES(1,0)")
            db.execute("UPDATE runtime_control SET generation=generation+1 WHERE id=1")
            generation=db.execute(
                "SELECT generation FROM runtime_control WHERE id=1").fetchone()[0]
            db.execute("""UPDATE runtime_gates SET state='blocked',token='',
                generation=?,approved_account_version=NULL,
                approved_by=NULL,approved_ref=NULL""", (generation,))
            now=time.time_ns()
            pending=db.execute("""SELECT client_id FROM orders
                WHERE status='reserved' ORDER BY client_id""").fetchall()
            for (cid,) in pending:
                db.execute("""INSERT OR IGNORE INTO startup_recovery_audit
                    (client_id,from_status,recovered_at_ns)
                    VALUES(?,'reserved',?)""", (cid, now))
            db.execute("UPDATE orders SET status='unknown' WHERE status='reserved'")
            db.execute("""INSERT INTO runtime_gate_audit
                (generation,account_id,mode,action,actor,reference,at_ns)
                VALUES(?,NULL,NULL,'BOOT_BLOCK','system',NULL,?)""",
                (generation,now))
            self.recovered_ids=tuple(x[0] for x in pending)
            self.generation=generation

    def _check_generation(self, db):
        row=db.execute("SELECT generation FROM runtime_control WHERE id=1").fetchone()
        if not row or row[0]!=self.generation:
            raise SafetyError("stale runtime boot; restart and review required")

    def approve_demo(self, account_id: str, *, operator_id: str,
                     review_ref: str, expected_account_version: int) -> None:
        """Open only an unblocked DEMO account with no uncertain orders.

        operator_id and review_ref are untrusted labels; external identity
        verification and broker reconciliation are not implemented here.
        """
        if (not all(type(x) is str and x.strip() for x in
                    (account_id,operator_id,review_ref))
                or account_id=="legacy"
                or type(expected_account_version) is not int):
            raise SafetyError("invalid mock approval data")
        token=secrets.token_hex(32)
        with self.ledger._tx() as db:
            self._check_generation(db)
            row=db.execute("""SELECT enabled,settings_version FROM account_limits
                WHERE account_id=? AND mode='demo'""",(account_id,)).fetchone()
            if not row or not row[0] or row[1]!=expected_account_version:
                raise SafetyError("account disabled or changed since review")
            # An in-progress or UNKNOWN intent requires explicit reconciliation.
            if db.execute("""SELECT 1 FROM orders WHERE account_id=?
                AND mode='demo' AND status IN ('reserved','unknown') LIMIT 1""",
                (account_id,)).fetchone():
                raise SafetyError("unresolved orders prohibit mock restart approval")
            if db.execute(self.ledger._exposure_query(
                    "o.account_id='legacy' AND o.mode='demo'")).fetchone()[0]>0:
                raise SafetyError("unattributed legacy risk blocks mock approval")
            db.execute("""INSERT INTO runtime_gates
                (account_id,mode,state,generation,token,
                 approved_account_version,approved_by,approved_ref)
                VALUES(?,'demo','open',?,?,?,?,?)
                ON CONFLICT(account_id,mode) DO UPDATE SET
                state='open',generation=excluded.generation,token=excluded.token,
                approved_account_version=excluded.approved_account_version,
                approved_by=excluded.approved_by,approved_ref=excluded.approved_ref""",
                (account_id,self.generation,token,expected_account_version,
                 operator_id,review_ref))
            db.execute("""INSERT INTO runtime_gate_audit
                (generation,account_id,mode,action,actor,reference,at_ns)
                VALUES(?,?,'demo','MOCK_APPROVE',?,?,?)""",
                (self.generation,account_id,operator_id,review_ref,time.time_ns()))
        self._tokens[(account_id,Mode.DEMO)]=token

    def approve_live(self, *_args, **_kwargs):
        raise SafetyError("LIVE is permanently disabled in this broker-free prototype")

    def block(self, account_id: str, mode: Mode, *, reason: str) -> None:
        if (type(account_id) is not str or not account_id.strip()
                or type(mode) is not Mode
                or type(reason) is not str or not reason.strip()):
            raise SafetyError("invalid block parameters")
        with self.ledger._tx() as db:
            self._check_generation(db)
            db.execute("""UPDATE runtime_gates SET state='blocked',token='',
                approved_account_version=NULL,approved_by=NULL,approved_ref=NULL
                WHERE account_id=? AND mode=?""", (account_id,mode.value))
            db.execute("""INSERT INTO runtime_gate_audit
                (generation,account_id,mode,action,actor,reference,at_ns)
                VALUES(?,?,?,'BLOCK','system',?,?)""",
                (self.generation,account_id,mode.value,reason,time.time_ns()))
        self._tokens.pop((account_id,mode),None)

    def reserve_demo(self, client_id: str, strategy_id: str, run_id: str,
                     quantity: int, expected_settings_version: int, *,
                     account_id: str, expected_account_version: int):
        """The only public order-admission surface of this mock runtime."""
        token=self._tokens.get((account_id,Mode.DEMO))
        if not token:
            raise SafetyError("no active DEMO gate token")
        return self.ledger.reserve(
            client_id,strategy_id,run_id,Mode.DEMO,quantity,
            expected_settings_version,account_id=account_id,
            expected_account_version=expected_account_version,
            admission_token=token)
