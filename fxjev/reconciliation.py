"""Fail-closed, broker-independent *evidence validator* for order recovery.

The inputs are simulated snapshots. This module does not fetch, authenticate,
attest, or establish completeness of real broker data. Production adapters and
broker-account ownership attribution are NOT implemented; never use it for live
trading without independent review of those additional safeguards.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import sqlite3
import time

from .ledger import Ledger, Mode, OrderStatus, SafetyError

MAX_SNAPSHOT_AGE_NS = 30 * 1_000_000_000


@dataclass(frozen=True)
class BrokerOrder:
    client_id: str
    broker_order_id: str
    strategy_id: str
    run_id: str
    mode: Mode
    requested_quantity: int
    status: OrderStatus
    final_at_ns: int


@dataclass(frozen=True)
class BrokerFill:
    broker_order_id: str
    execution_id: str
    quantity: int
    executed_at_ns: int


@dataclass(frozen=True)
class OwnedPosition:
    """Attributed position from a broker snapshot, not an inferred total.

    Netting brokers may not supply sufficient attribution. In that case, no
    credible OwnedPosition can be created and reconciliation MUST remain UNKNOWN.
    """
    client_id: str
    broker_order_id: str
    strategy_id: str
    run_id: str
    mode: Mode
    open_quantity: int
    observed_at_ns: int


@dataclass(frozen=True)
class ReconciliationProof:
    order: BrokerOrder
    fills: tuple[BrokerFill, ...]
    position: OwnedPosition
    order_history_complete: bool
    fill_history_complete: bool
    position_snapshot_complete: bool


def _is_nonneg_int(value):
    return type(value) is int and value >= 0


def _validate(proof: ReconciliationProof, now_ns: int) -> int:
    if (type(proof) is not ReconciliationProof
            or type(proof.order) is not BrokerOrder
            or type(proof.position) is not OwnedPosition
            or type(proof.fills) is not tuple
            or not all(type(x) is BrokerFill for x in proof.fills)):
        raise SafetyError("structured broker evidence is required")
    if (proof.order_history_complete is not True
            or proof.fill_history_complete is not True
            or proof.position_snapshot_complete is not True):
        raise SafetyError("all three broker evidence sets must be complete")
    order, position = proof.order, proof.position
    if (not all(type(x) is str and x.strip() for x in (
            order.client_id, order.broker_order_id, order.strategy_id, order.run_id))
            or type(order.mode) is not Mode
            or type(position.mode) is not Mode
            or type(order.status) is not OrderStatus
            or order.status not in (OrderStatus.CANCELED, OrderStatus.FILLED)
            or not _is_nonneg_int(order.requested_quantity)
            or order.requested_quantity == 0
            or not _is_nonneg_int(position.open_quantity)
            or not _is_nonneg_int(order.final_at_ns)
            or not _is_nonneg_int(position.observed_at_ns)):
        raise SafetyError("invalid order or position evidence")
    if (position.client_id, position.broker_order_id,
            position.strategy_id, position.run_id, position.mode) != (
            order.client_id, order.broker_order_id, order.strategy_id,
            order.run_id, order.mode):
        raise SafetyError("position ownership does not match order")
    if (position.observed_at_ns < order.final_at_ns
            or position.observed_at_ns > now_ns
            or now_ns - position.observed_at_ns > MAX_SNAPSHOT_AGE_NS):
        raise SafetyError("position evidence is stale, future-dated, or out of order")

    execution_ids = set()
    filled_quantity = 0
    for fill in proof.fills:
        if (type(fill.broker_order_id) is not str
                or type(fill.execution_id) is not str
                or not fill.execution_id.strip()
                or fill.broker_order_id != order.broker_order_id
                or fill.execution_id in execution_ids
                or type(fill.quantity) is not int or fill.quantity <= 0
                or not _is_nonneg_int(fill.executed_at_ns)
                or fill.executed_at_ns > position.observed_at_ns):
            raise SafetyError("invalid, duplicate, or mismatched execution evidence")
        execution_ids.add(fill.execution_id)
        filled_quantity += fill.quantity

    if (filled_quantity > order.requested_quantity
            or (order.status == OrderStatus.FILLED
                and filled_quantity != order.requested_quantity)
            or position.open_quantity != filled_quantity):
        raise SafetyError("fills, final order state and owned position disagree")
    return filled_quantity


def reconcile_order(ledger: Ledger, client_id: str,
                    proof: ReconciliationProof) -> str:
    """Atomically mark a final order and retain an immutable evidence fingerprint.

    Returns SHA-256 of evidence after committing. No broker calls are made.
    The old boolean 'evidence_complete' shortcut is intentionally unsupported.
    """
    now_ns = time.time_ns()
    filled_quantity = _validate(proof, now_ns)
    order = proof.order
    if client_id != order.client_id:
        raise SafetyError("client order ID mismatch")
    data = json.dumps(asdict(proof), sort_keys=True, separators=(",", ":"),
                      default=lambda value: value.value)
    digest = hashlib.sha256(data.encode("utf-8")).hexdigest()

    with ledger._tx() as db:
        row = db.execute("""SELECT strategy_id,run_id,mode,quantity,status
            FROM orders WHERE client_id=?""", (client_id,)).fetchone()
        if (not row or row[:4] != (order.strategy_id, order.run_id,
                                    order.mode.value, order.requested_quantity)
                or row[4] not in ("unknown", "reserved")):
            raise SafetyError("order intent missing, finalized, or inconsistent")
        db.execute("""CREATE TABLE IF NOT EXISTS reconciliation_audit (
            client_id TEXT PRIMARY KEY REFERENCES orders(client_id),
            broker_order_id TEXT NOT NULL UNIQUE,
            final_status TEXT NOT NULL,
            filled_quantity INTEGER NOT NULL,
            evidence_sha256 TEXT NOT NULL,
            reconciled_at_ns INTEGER NOT NULL
        )""")
        try:
            db.execute("""INSERT INTO reconciliation_audit
                (client_id,broker_order_id,final_status,filled_quantity,
                 evidence_sha256,reconciled_at_ns) VALUES(?,?,?,?,?,?)""",
                (client_id,order.broker_order_id,order.status.value,filled_quantity,
                 digest,now_ns))
        except sqlite3.IntegrityError as exc:
            raise SafetyError("broker order identity already reconciled") from exc
        changed = db.execute("""UPDATE orders SET status=?,filled_quantity=?
            WHERE client_id=? AND status IN ('unknown','reserved')""",
            (order.status.value,filled_quantity,client_id)).rowcount
        if changed != 1:
            raise SafetyError("order changed during reconciliation")
    return digest
