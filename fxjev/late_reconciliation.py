"""Post-terminal incremental fill revision for *DEMO mock only*.

The evidence is caller-supplied and UNAUTHENTICATED. This implementation never
speaks to an FX broker, places a trade, or enables LIVE. It refuses to revise
an entry with recorded closes or conflicting events. Original reconciliation
audit remains immutable; every mock revision receives a new audit row.
"""
from dataclasses import asdict
import hashlib
import json
import sqlite3
import time

from .ledger import Ledger, Mode, OrderStatus, SafetyError
from .reconciliation import ReconciliationProof, _validate
from .execution_history import record_late_entry
from .accounting_journal import verify_position_journal, record_entry_journal


def reconcile_late_demo_fill(
    ledger: Ledger, proof: ReconciliationProof, *,
    reviewer_id: str, review_ref: str,
) -> str:
    """Revise a finalized DEMO entry using complete *simulated* evidence.

    A positive increase in total fills is required; no order state change,
    automatic reapproval, or release of recorded closes is permitted.
    """
    if (not all(type(v) is str and v.strip() for v in
                (reviewer_id, review_ref))):
        raise SafetyError("mock reviewer and review reference required")
    now = time.time_ns()
    new_total = _validate(proof, now)
    order = proof.order
    if order.mode is not Mode.DEMO:
        raise SafetyError("late-fill revision is DEMO-only; LIVE is disabled")
    raw=json.dumps(asdict(proof),sort_keys=True,separators=(",",":"),
                   default=lambda v:v.value)
    digest=hashlib.sha256(raw.encode("utf-8")).hexdigest()

    with ledger._tx() as db:
        old=db.execute("""SELECT strategy_id,run_id,mode,account_id,quantity,
            status,filled_quantity FROM orders WHERE client_id=?""",
            (order.client_id,)).fetchone()
        if (not old or old[:6] != (
                order.strategy_id,order.run_id,order.mode.value,
                order.account_id,order.requested_quantity,order.status.value)
                or old[5] not in ("filled","canceled")):
            raise SafetyError("final order identity or state changed")
        initial=db.execute("""SELECT broker_order_id,final_status,
            filled_quantity FROM reconciliation_audit
            WHERE client_id=?""",(order.client_id,)).fetchone()
        if (not initial or initial[0]!=order.broker_order_id
                or initial[1]!=order.status.value
                or old[6]<initial[2]):
            raise SafetyError("original final evidence is missing or inconsistent")
        previous=old[6]
        # Fail closed if earlier entry/close accounting updates were not
        # journaled. The original event time cannot be guessed from totals.
        verify_position_journal(db,order.client_id)
        if new_total<=previous:
            raise SafetyError("new fill quantity must strictly exceed recorded total")

        held=db.execute("""SELECT 1 FROM mock_event_holds WHERE
            client_id=? AND account_id=? AND mode=?""",
            (order.client_id,order.account_id,order.mode.value)).fetchone()
        if not held:
            raise SafetyError("no quarantined event requiring revision")
        if db.execute("""SELECT 1 FROM mock_fill_inbox
            WHERE client_id=? AND
              (account_id!=? OR mode!=?) LIMIT 1""",
            (order.client_id,order.account_id,order.mode.value)).fetchone():
            raise SafetyError("cross-account or cross-mode event attribution conflict")
        if db.execute("""SELECT 1 FROM mock_event_conflicts c
            WHERE c.client_id=? OR EXISTS (
                SELECT 1 FROM mock_fill_inbox e
                WHERE e.client_id=? AND e.account_id=c.account_id
                  AND e.mode=c.mode AND e.execution_id=c.execution_id
            ) LIMIT 1""",
            (order.client_id,order.client_id)).fetchone():
            raise SafetyError("conflicting execution ID requires external review")

        events=db.execute("""SELECT execution_id,broker_order_id,quantity,
            occurred_at_ns,received_at_ns FROM mock_fill_inbox
            WHERE client_id=? AND account_id=? AND mode=?""",
            (order.client_id,order.account_id,order.mode.value)).fetchall()
        if not events:
            raise SafetyError("no received incremental fill event")
        fills={item.execution_id:(item.broker_order_id,item.quantity)
               for item in proof.fills}
        for execution_id,broker_id,qty,occurred,received in events:
            if (fills.get(execution_id)!=(broker_id,qty)
                    or broker_id!=order.broker_order_id
                    or occurred>proof.position.observed_at_ns
                    or received>proof.position.observed_at_ns):
                raise SafetyError("snapshot fails to cover the received event")

        # Original fill IDs were NOT stored in the first P0 audit, only a
        # fingerprint and filled total. The full-history flag is a mock-only
        # assertion and cannot establish real broker completeness.
        if db.execute("""SELECT 1 FROM position_close_batches
            WHERE client_id=? LIMIT 1""",(order.client_id,)).fetchone():
            raise SafetyError("recorded closes require separate full-life reconciliation")
        position=db.execute("""SELECT account_id,strategy_id,run_id,mode,
            broker_entry_order_id,opened_quantity,remaining_quantity
            FROM position_balances WHERE client_id=?""",
            (order.client_id,)).fetchone()
        if previous>0:
            if (not position or position != (
                    order.account_id,order.strategy_id,order.run_id,
                    order.mode.value,order.broker_order_id,previous,previous)):
                raise SafetyError("owned position was altered or is inconsistent")
            db.execute("""UPDATE position_balances
                SET opened_quantity=?,remaining_quantity=?
                WHERE client_id=? AND opened_quantity=? AND remaining_quantity=?""",
                (new_total,new_total,order.client_id,previous,previous))
        else:
            if position:
                raise SafetyError("unexpected old position for zero prior fill")
            try:
                db.execute("""INSERT INTO position_balances
                    (client_id,account_id,strategy_id,run_id,mode,
                     broker_entry_order_id,opened_quantity,remaining_quantity)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    (order.client_id,order.account_id,order.strategy_id,
                     order.run_id,order.mode.value,order.broker_order_id,
                     new_total,new_total))
            except sqlite3.IntegrityError as exc:
                raise SafetyError("position identity conflict") from exc

        record_late_entry(
            db,proof,expected_previous=previous,evidence_hash=digest,
            kind="late-no-close",recorded_at_ns=now)
        result=db.execute("""UPDATE orders SET filled_quantity=?
            WHERE client_id=? AND filled_quantity=? AND status=?""",
            (new_total,order.client_id,previous,order.status.value)).rowcount
        if result!=1:
            raise SafetyError("order fill record changed while revising")

        db.execute("""CREATE TABLE IF NOT EXISTS mock_terminal_revisions (
            revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL REFERENCES orders(client_id),
            account_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            previous_filled_quantity INTEGER NOT NULL,
            revised_filled_quantity INTEGER NOT NULL,
            reviewer_id TEXT NOT NULL,
            review_ref TEXT NOT NULL,
            evidence_sha256 TEXT NOT NULL,
            reviewed_at_ns INTEGER NOT NULL
        )""")
        db.execute("""INSERT INTO mock_terminal_revisions
            (client_id,account_id,mode,previous_filled_quantity,
             revised_filled_quantity,reviewer_id,review_ref,
             evidence_sha256,reviewed_at_ns)
            VALUES(?,?,?,?,?,?,?,?,?)""",
            (order.client_id,order.account_id,order.mode.value,
             previous,new_total,reviewer_id,review_ref,digest,now))
        db.execute("""DELETE FROM mock_event_holds
            WHERE account_id=? AND mode=? AND client_id=?""",
            (order.account_id,order.mode.value,order.client_id))
        record_entry_journal(db,proof,evidence_hash=digest,
                             recorded_at_ns=now)
        # Never reopen the runtime gate. It was revoked on inbox arrival.
        # An explicit *mock DEMO* approval is still required for re-admission.
    return digest
