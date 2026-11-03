"""Durable *mock* broker-fill inbox with conservative anomaly quarantine.

NEVER trust caller-supplied FillNotice as authenticated broker evidence.
This module does not execute orders, change position balances, or auto-release
risk. A full trusted broker reconciliation remains a future prerequisite.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import time

from .ledger import Ledger, Mode, SafetyError


@dataclass(frozen=True)
class FillNotice:
    account_id: str
    mode: Mode
    client_id: str
    broker_order_id: str
    execution_id: str
    quantity: int
    occurred_at_ns: int


def _hold(db, ledger: Ledger, account: str, mode: Mode,
          client: str, reason: str, now: int) -> None:
    db.execute("""INSERT OR IGNORE INTO mock_event_holds
        (account_id,mode,client_id,reason,held_at_ns)
        VALUES(?,?,?,?,?)""",(account,mode.value,client,reason,now))
    ledger._invalidate_gate(db,account,mode,"new mock fill anomaly: " + reason)


def ingest_mock_fill(ledger: Ledger, notice: FillNotice) -> str:
    """Persist once; quarantine the affected scope; preserve full risk.

    Returns 'quarantined' for a new accepted event or 'duplicate' for an exact
    replay. A reused event ID with different contents records a conflict and
    raises SafetyError *after the conflict and holds are committed*.
    """
    if (type(notice) is not FillNotice
            or type(notice.mode) is not Mode
            or not all(type(value) is str and value.strip() for value in
                       (notice.account_id,notice.client_id,
                        notice.broker_order_id,notice.execution_id))
            or type(notice.quantity) is not int or notice.quantity <= 0
            or type(notice.occurred_at_ns) is not int
            or notice.occurred_at_ns <= 0):
        raise SafetyError("invalid mock fill notice")
    digest=hashlib.sha256(json.dumps(
        asdict(notice),sort_keys=True,separators=(",",":"),
        default=lambda v:v.value).encode("utf-8")).hexdigest()
    now=time.time_ns()
    collision=False
    duplicate=False
    with ledger._tx() as db:
        found=db.execute("""SELECT client_id,payload_sha256 FROM mock_fill_inbox
            WHERE account_id=? AND mode=? AND execution_id=?""",
            (notice.account_id,notice.mode.value,notice.execution_id)).fetchone()
        if found:
            if found[1] == digest:
                duplicate=True
            else:
                collision=True
                db.execute("""INSERT INTO mock_event_conflicts
                    (account_id,mode,execution_id,client_id,old_sha256,
                     new_sha256,detected_at_ns) VALUES(?,?,?,?,?,?,?)""",
                    (notice.account_id,notice.mode.value,notice.execution_id,
                     notice.client_id,found[1],digest,now))
                _hold(db,ledger,notice.account_id,notice.mode,notice.client_id,
                      "conflicting duplicate execution ID",now)
                if found[0]!=notice.client_id:
                    _hold(db,ledger,notice.account_id,notice.mode,found[0],
                          "conflicting duplicate execution ID",now)
        else:
            db.execute("""INSERT INTO mock_fill_inbox
                (account_id,mode,execution_id,client_id,broker_order_id,
                 quantity,occurred_at_ns,payload_sha256,received_at_ns)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                (notice.account_id,notice.mode.value,notice.execution_id,
                 notice.client_id,notice.broker_order_id,notice.quantity,
                 notice.occurred_at_ns,digest,now))
            owned=db.execute("""SELECT account_id,mode,status FROM orders
                WHERE client_id=?""",(notice.client_id,)).fetchone()
            reason="new mock fill requires complete order/position reconciliation"
            if owned is None:
                reason="orphan fill: no matching client order"
            elif owned[:2]!=(notice.account_id,notice.mode.value):
                reason="fill account or mode conflicts with stored intent"
                _hold(db,ledger,owned[0],Mode(owned[1]),notice.client_id,
                      reason,now)
            else:
                broker=db.execute("""SELECT broker_order_id FROM reconciliation_audit
                    WHERE client_id=?""",(notice.client_id,)).fetchone() if db.execute(
                    """SELECT 1 FROM sqlite_master
                       WHERE type='table' AND name='reconciliation_audit'""").fetchone() else None
                if broker and broker[0]!=notice.broker_order_id:
                    reason="fill broker order ID conflicts with finalized evidence"
                if owned[2]=="reserved":
                    db.execute("""UPDATE orders SET status='unknown'
                        WHERE client_id=? AND status='reserved'""",
                        (notice.client_id,))
            _hold(db,ledger,notice.account_id,notice.mode,notice.client_id,
                  reason,now)
    if collision:
        raise SafetyError("conflicting mock execution ID quarantined and audited")
    return "duplicate" if duplicate else "quarantined"
