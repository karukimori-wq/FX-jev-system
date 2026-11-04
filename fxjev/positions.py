"""Isolated SQLite mock position close evidence and risk accounting.

No broker API, authentication, execution or proof-of-completeness is included.
Close evidence is caller-supplied and must NOT be trusted for live trading.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import sqlite3
import time

from .ledger import Ledger, Mode, OrderStatus, SafetyError
from .reconciliation import MAX_SNAPSHOT_AGE_NS
from .execution_history import verify_entry_lineage


@dataclass(frozen=True)
class CloseExecution:
    close_order_id: str
    execution_id: str
    quantity: int
    executed_at_ns: int


@dataclass(frozen=True)
class PositionCloseProof:
    account_id: str
    client_id: str
    broker_entry_order_id: str
    strategy_id: str
    run_id: str
    mode: Mode
    close_order_id: str
    close_status: OrderStatus
    requested_quantity: int
    executions: tuple[CloseExecution, ...]
    remaining_quantity: int
    order_final_at_ns: int
    snapshot_at_ns: int
    order_history_complete: bool
    fill_history_complete: bool
    position_snapshot_complete: bool


def _identifier(value):
    return type(value) is str and bool(value.strip())


def _quantity(value):
    return type(value) is int and value >= 0


def _validate(proof, now_ns):
    if (type(proof) is not PositionCloseProof
            or type(proof.mode) is not Mode
            or type(proof.close_status) is not OrderStatus
            or proof.close_status not in (OrderStatus.FILLED, OrderStatus.CANCELED)
            or type(proof.executions) is not tuple
            or not proof.executions
            or not all(type(item) is CloseExecution for item in proof.executions)):
        raise SafetyError("structured close evidence is required")
    if (proof.order_history_complete is not True
            or proof.fill_history_complete is not True
            or proof.position_snapshot_complete is not True):
        raise SafetyError("complete close order, fills and position evidence is required")
    if not all(_identifier(v) for v in (
            proof.account_id, proof.client_id, proof.broker_entry_order_id,
            proof.strategy_id, proof.run_id, proof.close_order_id)):
        raise SafetyError("invalid close identity")
    if (not _quantity(proof.requested_quantity)
            or proof.requested_quantity == 0
            or proof.close_order_id == proof.broker_entry_order_id
            or not _quantity(proof.remaining_quantity)
            or not _quantity(proof.order_final_at_ns)
            or not _quantity(proof.snapshot_at_ns)
            or proof.snapshot_at_ns < proof.order_final_at_ns
            or proof.snapshot_at_ns > now_ns
            or now_ns - proof.snapshot_at_ns > MAX_SNAPSHOT_AGE_NS):
        raise SafetyError("stale, invalid or out-of-order close snapshot")
    seen = set()
    quantity = 0
    for fill in proof.executions:
        if (not _identifier(fill.close_order_id)
                or not _identifier(fill.execution_id)
                or fill.close_order_id != proof.close_order_id
                or fill.execution_id in seen
                or not _quantity(fill.quantity) or fill.quantity == 0
                or not _quantity(fill.executed_at_ns)
                or fill.executed_at_ns > proof.order_final_at_ns):
            raise SafetyError("duplicate, invalid or unowned close fill")
        seen.add(fill.execution_id)
        quantity += fill.quantity
    if (quantity > proof.requested_quantity
            or (proof.close_status == OrderStatus.FILLED
                and quantity != proof.requested_quantity)):
        raise SafetyError("close fills do not match final close order state")
    return quantity


def close_position(ledger: Ledger, proof: PositionCloseProof) -> str:
    """Apply a *simulated*, independently attributed close order.

    The remaining quantity is never inferred from an account-wide net position.
    This module does NOT place a close trade and cannot prove the caller's
    broker evidence is authentic or complete.
    """
    now_ns = time.time_ns()
    closed = _validate(proof, now_ns)
    payload = json.dumps(asdict(proof), sort_keys=True, separators=(",", ":"),
                         default=lambda val: val.value)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    with ledger._tx() as db:
        # Any uncertain extra entry fill makes the open balance untrustworthy.
        # Never release the reserved risk on mock close evidence while held.
        if db.execute("""SELECT 1 FROM mock_event_holds WHERE
            client_id=? AND account_id=? AND mode=? LIMIT 1""",
            (proof.client_id,proof.account_id,proof.mode.value)).fetchone():
            raise SafetyError("unresolved fill anomaly prevents close risk release")
        row = db.execute("""SELECT account_id,strategy_id,run_id,mode,
            broker_entry_order_id,opened_quantity,remaining_quantity
            FROM position_balances WHERE client_id=?""",
            (proof.client_id,)).fetchone()
        if not row or row[:5] != (
                proof.account_id, proof.strategy_id, proof.run_id,
                proof.mode.value, proof.broker_entry_order_id):
            raise SafetyError("no position with matching account and owner")
        # Keep a verified original entry execution trail before reducing
        # booked position risk. An old database without execution IDs cannot
        # be silently backfilled or used to authorize a new close revision.
        verify_entry_lineage(db,proof.client_id,expected_total=row[5])
        for fill in proof.executions:
            if db.execute("""SELECT 1 FROM entry_executions
                WHERE account_id=? AND execution_id=? LIMIT 1""",
                (proof.account_id,fill.execution_id)).fetchone():
                raise SafetyError("close execution ID aliases an entry execution")
        old_remaining = row[6]
        if closed > old_remaining or old_remaining - closed != proof.remaining_quantity:
            raise SafetyError("close fills disagree with remaining owned position")

        try:
            db.execute("""INSERT INTO position_close_batches
                (account_id,close_order_id,client_id,close_status,
                 requested_quantity,quantity,remaining_quantity,
                 evidence_sha256,recorded_at_ns)
                VALUES(?,?,?,?,?,?,?,?,?)""", (
                proof.account_id,proof.close_order_id,proof.client_id,
                proof.close_status.value,proof.requested_quantity,closed,
                proof.remaining_quantity,digest,now_ns))
            for fill in proof.executions:
                db.execute("""INSERT INTO position_close_executions
                    (account_id,execution_id,close_order_id,client_id,quantity)
                    VALUES(?,?,?,?,?)""", (
                    proof.account_id,fill.execution_id,proof.close_order_id,
                    proof.client_id,fill.quantity))
        except sqlite3.IntegrityError as exc:
            raise SafetyError("close order or execution was already applied") from exc
        changed = db.execute("""UPDATE position_balances
            SET remaining_quantity=? WHERE client_id=? AND remaining_quantity=?""",
            (proof.remaining_quantity,proof.client_id,old_remaining)).rowcount
        if changed != 1:
            raise SafetyError("position changed during close reconciliation")
    return digest
