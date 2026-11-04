"""Durable mock fill inbox: caller evidence is UNAUTHENTICATED.

No broker API, real trade, or implied completeness. Sequenced mock envelopes
share the SAME SQLite transaction through _ingest_mock_fill_tx.
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


def _digest_notice(notice: FillNotice) -> str:
    if (type(notice) is not FillNotice
            or type(notice.mode) is not Mode
            or not all(type(value) is str and value.strip() for value in (
                notice.account_id,notice.client_id,notice.broker_order_id,
                notice.execution_id))
            or type(notice.quantity) is not int or notice.quantity<=0
            or type(notice.occurred_at_ns) is not int
            or notice.occurred_at_ns<=0):
        raise SafetyError("invalid mock fill notice")
    return hashlib.sha256(json.dumps(
        asdict(notice),sort_keys=True,separators=(",",":"),
        default=lambda v:v.value).encode("utf-8")).hexdigest()


def _hold(db, ledger: Ledger, account: str, mode: Mode,
          client: str, reason: str, now: int) -> None:
    db.execute("""INSERT OR IGNORE INTO mock_event_holds
        (account_id,mode,client_id,reason,held_at_ns)
        VALUES(?,?,?,?,?)""",(account,mode.value,client,reason,now))
    ledger._invalidate_gate(db,account,mode,"new mock fill anomaly: "+reason)


def _ingest_mock_fill_tx(db, ledger: Ledger, notice: FillNotice,
                         digest: str, now: int) -> tuple[str,bool]:
    """Caller must hold Ledger._tx(). Return (status, conflict_committed)."""
    found=db.execute("""SELECT client_id,payload_sha256 FROM mock_fill_inbox
        WHERE account_id=? AND mode=? AND execution_id=?""",
        (notice.account_id,notice.mode.value,notice.execution_id)).fetchone()
    if found:
        if found[1]==digest:
            return "duplicate",False
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
        return "conflict",True
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
        _hold(db,ledger,owned[0],Mode(owned[1]),notice.client_id,reason,now)
    else:
        # The very first fill may arrive before any order reconciliation;
        # preserve legacy behavior without guessing a broker order mapping.
        has_audit=db.execute("""SELECT 1 FROM sqlite_master WHERE
            type='table' AND name='reconciliation_audit'""").fetchone()
        broker=(db.execute("""SELECT broker_order_id FROM reconciliation_audit
            WHERE client_id=?""",(notice.client_id,)).fetchone()
            if has_audit else None)
        if broker and broker[0]!=notice.broker_order_id:
            reason="fill broker order ID conflicts with finalized evidence"
        if owned[2]=="reserved":
            db.execute("""UPDATE orders SET status='unknown'
                WHERE client_id=? AND status='reserved'""",(notice.client_id,))
    _hold(db,ledger,notice.account_id,notice.mode,notice.client_id,reason,now)
    return "quarantined",False


def ingest_mock_fill(ledger: Ledger, notice: FillNotice) -> str:
    """Persist once; quarantine affected account, keep full risk reserved.

    An execution ID collision is audited and COMMITTED before raising.
    """
    digest=_digest_notice(notice)
    now=time.time_ns()
    bypass=False
    with ledger._tx() as db:
        stream=db.execute("""SELECT stream_id FROM mock_feed_streams
            WHERE account_id=? AND mode=?""",
            (notice.account_id,notice.mode.value)).fetchone()
        if stream:
            # Once a sequenced stream exists, bare fills cannot evade it.
            # Persist an independent alarm in addition to the event hold,
            # so clearing a hold cannot silently authorize restart.
            db.execute("""UPDATE mock_feed_streams SET has_alarm=1
                WHERE account_id=? AND mode=?""",
                (notice.account_id,notice.mode.value))
            db.execute("""INSERT INTO mock_feed_alerts
                (account_id,mode,stream_id,sequence,kind,payload_sha256,
                 detected_at_ns) VALUES(?,?,?,0,'unsequenced-bypass',?,?)""",
                (notice.account_id,notice.mode.value,stream[0],digest,now))
            _hold(db,ledger,notice.account_id,notice.mode,
                  "__feed__:unsequenced",
                  "unsequenced fill attempted on active mock feed",now)
            bypass=True
        else:
            status,conflict=_ingest_mock_fill_tx(db,ledger,notice,digest,now)
    if bypass:
        raise SafetyError("sequenced feed required; unsequenced attempt quarantined")
    if conflict:
        raise SafetyError("conflicting mock execution ID quarantined and audited")
    return status
