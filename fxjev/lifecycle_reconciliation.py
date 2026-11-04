"""DEMO-only mock late-entry-fill review *after* recorded position closes.

The original entry and close audit rows remain immutable. This requires
caller-supplied, UNAUTHENTICATED snapshots and history; never connect it
to a real broker or use it as live-trading authorization.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import sqlite3
import time

from .ledger import Ledger, Mode, OrderStatus, SafetyError
from .positions import CloseExecution
from .execution_history import record_late_entry
from .reconciliation import (
    MAX_SNAPSHOT_AGE_NS, OwnedPosition, ReconciliationProof, _validate,
)


@dataclass(frozen=True)
class HistoricalClose:
    close_order_id: str
    status: OrderStatus
    requested_quantity: int
    executions: tuple[CloseExecution, ...]
    filled_quantity: int
    recorded_remaining_quantity: int


@dataclass(frozen=True)
class LifecycleReviewProof:
    entry: ReconciliationProof
    closes: tuple[HistoricalClose, ...]
    net_position: OwnedPosition
    close_history_complete: bool


def reconcile_late_demo_lifecycle(
    ledger: Ledger, proof: LifecycleReviewProof, *,
    reviewer_id: str, review_ref: str,
) -> str:
    """Review complete mock entry fills + historical closes + latest net position.

    Prior close records are never edited. A positive increase in the entry's
    cumulative fills increases the owned open position by the same difference,
    but ONLY when the fresh attributed net-position snapshot agrees.
    """
    if (type(proof) is not LifecycleReviewProof
            or type(proof.entry) is not ReconciliationProof
            or type(proof.closes) is not tuple or not proof.closes
            or type(proof.net_position) is not OwnedPosition
            or proof.close_history_complete is not True
            or not all(type(x) is HistoricalClose for x in proof.closes)
            or not all(type(v) is str and v.strip()
                       for v in (reviewer_id,review_ref))):
        raise SafetyError("complete structured mock lifecycle review required")
    now=time.time_ns()
    new_total=_validate(proof.entry,now)
    order=proof.entry.order
    if order.mode is not Mode.DEMO:
        raise SafetyError("mock lifecycle revision is DEMO-only")
    net=proof.net_position
    if (not isinstance(net.mode, Mode)
            or (net.client_id,net.broker_order_id,net.strategy_id,
                net.run_id,net.mode,net.account_id) != (
                order.client_id,order.broker_order_id,order.strategy_id,
                order.run_id,order.mode,order.account_id)
            or type(net.open_quantity) is not int or net.open_quantity<0
            or type(net.observed_at_ns) is not int
            or net.observed_at_ns < proof.entry.position.observed_at_ns
            or net.observed_at_ns>now
            or now-net.observed_at_ns>MAX_SNAPSHOT_AGE_NS):
        raise SafetyError("untrusted, stale or incorrectly attributed net position")

    entry_fills={item.execution_id:(item.broker_order_id,item.quantity)
                 for item in proof.entry.fills}
    close_ids=set()
    all_close_executions=set()
    for close in proof.closes:
        if (type(close.close_order_id) is not str or not close.close_order_id.strip()
                or close.close_order_id==order.broker_order_id
                or close.close_order_id in close_ids
                or type(close.status) is not OrderStatus
                or close.status not in (OrderStatus.CANCELED,OrderStatus.FILLED)
                or type(close.requested_quantity) is not int
                or close.requested_quantity<=0
                or type(close.filled_quantity) is not int
                or close.filled_quantity<=0
                or type(close.recorded_remaining_quantity) is not int
                or close.recorded_remaining_quantity<0
                or type(close.executions) is not tuple or not close.executions
                or not all(type(x) is CloseExecution for x in close.executions)):
            raise SafetyError("invalid historical close")
        close_ids.add(close.close_order_id)
        total=0
        for fill in close.executions:
            if (type(fill.close_order_id) is not str
                    or fill.close_order_id!=close.close_order_id
                    or type(fill.execution_id) is not str or not fill.execution_id.strip()
                    or fill.execution_id in all_close_executions
                    or fill.execution_id in entry_fills
                    or type(fill.quantity) is not int or fill.quantity<=0
                    or type(fill.executed_at_ns) is not int or fill.executed_at_ns<=0
                    or fill.executed_at_ns>net.observed_at_ns):
                raise SafetyError("duplicate or conflicting close execution")
            all_close_executions.add(fill.execution_id)
            total+=fill.quantity
        if (total!=close.filled_quantity
                or total>close.requested_quantity
                or (close.status is OrderStatus.FILLED
                    and total!=close.requested_quantity)):
            raise SafetyError("historical close quantity mismatch")

    digest=hashlib.sha256(json.dumps(
        asdict(proof),sort_keys=True,separators=(",",":"),
        default=lambda value:value.value).encode("utf-8")).hexdigest()
    with ledger._tx() as db:
        old=db.execute("""SELECT strategy_id,run_id,mode,account_id,
            quantity,status,filled_quantity FROM orders
            WHERE client_id=?""",(order.client_id,)).fetchone()
        if (not old or old[:6] != (
                order.strategy_id,order.run_id,order.mode.value,
                order.account_id,order.requested_quantity,order.status.value)
                or old[5] not in ("canceled","filled")):
            raise SafetyError("final entry identity or state mismatch")
        previous=old[6]
        first=db.execute("""SELECT broker_order_id,final_status,filled_quantity
            FROM reconciliation_audit WHERE client_id=?""",
            (order.client_id,)).fetchone()
        if (not first or first[0]!=order.broker_order_id
                or first[1]!=order.status.value
                or first[2]>previous or new_total<=previous):
            raise SafetyError("original entry or positive incremental fill missing")

        rows=db.execute("""SELECT close_order_id,close_status,
            requested_quantity,quantity,remaining_quantity
            FROM position_close_batches
            WHERE client_id=? AND account_id=? ORDER BY rowid""",
            (order.client_id,order.account_id)).fetchall()
        if len(rows)!=len(proof.closes):
            raise SafetyError("close history is missing or has extra batches")
        # Original close snapshots were taken against the original entry
        # cumulative fill, not the latest revised entry total. We can replay
        # that immutable close sequence after later entry-only revisions.
        # Any closes added after an earlier revision do not fit that original
        # sequence, so they must fail closed until a chronological journal exists.
        historical_remaining=first[2]
        historical_closed=0
        for recorded,provided in zip(rows,proof.closes):
            if recorded!=(provided.close_order_id,provided.status.value,
                         provided.requested_quantity,provided.filled_quantity,
                         provided.recorded_remaining_quantity):
                raise SafetyError("historical close batch does not match audit")
            real_executions=db.execute("""SELECT execution_id,quantity FROM
                position_close_executions WHERE account_id=? AND
                close_order_id=? AND client_id=?""",
                (order.account_id,provided.close_order_id,order.client_id)).fetchall()
            supplied=sorted((x.execution_id,x.quantity)
                            for x in provided.executions)
            if sorted(real_executions)!=supplied:
                raise SafetyError("close execution history does not match audit")
            historical_closed+=provided.filled_quantity
            historical_remaining-=provided.filled_quantity
            if (historical_remaining<0
                    or historical_remaining!=provided.recorded_remaining_quantity):
                raise SafetyError("historical close balance chain is inconsistent")

        previous_remaining=previous-historical_closed
        if previous_remaining<0:
            raise SafetyError("recorded closes exceed previous entry fill total")
        position=db.execute("""SELECT account_id,strategy_id,run_id,mode,
            broker_entry_order_id,opened_quantity,remaining_quantity
            FROM position_balances WHERE client_id=?""",
            (order.client_id,)).fetchone()
        if (not position or position!=(
                order.account_id,order.strategy_id,order.run_id,
                order.mode.value,order.broker_order_id,previous,
                previous_remaining)):
            raise SafetyError("current position conflicts with stored close history")
        expected_remaining=new_total-historical_closed
        if (expected_remaining<previous_remaining
                or net.open_quantity!=expected_remaining):
            raise SafetyError("new entry fills and close history disagree with net position")

        if not db.execute("""SELECT 1 FROM mock_event_holds WHERE
            client_id=? AND account_id=? AND mode=?""",
            (order.client_id,order.account_id,order.mode.value)).fetchone():
            raise SafetyError("no matching quarantined event")
        if db.execute("""SELECT 1 FROM mock_fill_inbox
            WHERE client_id=? AND (account_id!=? OR mode!=?) LIMIT 1""",
            (order.client_id,order.account_id,order.mode.value)).fetchone():
            raise SafetyError("event attributed to the wrong account or mode")
        if db.execute("""SELECT 1 FROM mock_event_conflicts c
            WHERE c.client_id=? OR EXISTS (
              SELECT 1 FROM mock_fill_inbox e WHERE e.client_id=?
              AND e.account_id=c.account_id AND e.mode=c.mode
              AND e.execution_id=c.execution_id
            ) LIMIT 1""",(order.client_id,order.client_id)).fetchone():
            raise SafetyError("execution conflict needs independent review")
        events=db.execute("""SELECT execution_id,broker_order_id,quantity,
            occurred_at_ns,received_at_ns FROM mock_fill_inbox
            WHERE client_id=? AND account_id=? AND mode=?""",
            (order.client_id,order.account_id,order.mode.value)).fetchall()
        if not events:
            raise SafetyError("no received incremental fills")
        for eid,bid,qty,occurred,received in events:
            if (entry_fills.get(eid)!=(bid,qty)
                    or bid!=order.broker_order_id
                    or occurred>net.observed_at_ns
                    or received>net.observed_at_ns):
                raise SafetyError("review proof missing an observed fill")

        # If there were earlier mock late revisions, they are part of the
        # immutable chain and the last revision must agree with current fills.
        if db.execute("""SELECT 1 FROM sqlite_master WHERE type='table'
            AND name='mock_terminal_revisions'""").fetchone():
            prior=db.execute("""SELECT revised_filled_quantity
                FROM mock_terminal_revisions WHERE client_id=?
                ORDER BY revision_id DESC LIMIT 1""",
                (order.client_id,)).fetchone()
            if prior and prior[0]!=previous:
                raise SafetyError("prior late review chain is inconsistent")

        # Prevent revisions from silently dropping/relabeling historical
        # entry execution IDs. Persist new IDs with a linked audit snapshot.
        record_late_entry(
            db,proof.entry,expected_previous=previous,evidence_hash=digest,
            kind="late-with-closes",recorded_at_ns=now)
        db.execute("""CREATE TABLE IF NOT EXISTS mock_lifecycle_revisions (
            revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL REFERENCES orders(client_id),
            account_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            old_entry_total INTEGER NOT NULL,
            new_entry_total INTEGER NOT NULL,
            historical_closed_quantity INTEGER NOT NULL,
            old_open_quantity INTEGER NOT NULL,
            new_open_quantity INTEGER NOT NULL,
            reviewer_id TEXT NOT NULL,
            review_ref TEXT NOT NULL,
            evidence_sha256 TEXT NOT NULL,
            reviewed_at_ns INTEGER NOT NULL
        )""")
        changed=db.execute("""UPDATE position_balances SET opened_quantity=?,
            remaining_quantity=? WHERE client_id=? AND opened_quantity=?
            AND remaining_quantity=?""",
            (new_total,expected_remaining,order.client_id,previous,
             previous_remaining)).rowcount
        if changed!=1:
            raise SafetyError("position changed during mock lifecycle review")
        changed=db.execute("""UPDATE orders SET filled_quantity=?
            WHERE client_id=? AND filled_quantity=? AND status=?""",
            (new_total,order.client_id,previous,order.status.value)).rowcount
        if changed!=1:
            raise SafetyError("order changed during mock lifecycle review")
        db.execute("""INSERT INTO mock_lifecycle_revisions
            (client_id,account_id,mode,old_entry_total,new_entry_total,
             historical_closed_quantity,old_open_quantity,new_open_quantity,
             reviewer_id,review_ref,evidence_sha256,reviewed_at_ns)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (order.client_id,order.account_id,order.mode.value,previous,
             new_total,historical_closed,previous_remaining,
             expected_remaining,reviewer_id,review_ref,digest,now))
        db.execute("""DELETE FROM mock_event_holds
            WHERE client_id=? AND account_id=? AND mode=?""",
            (order.client_id,order.account_id,order.mode.value))
        # Do not re-enable the gate. A separate DEMO mock review is required.
    return digest
