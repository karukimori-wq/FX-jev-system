"""Append-only *mock accounting* journal; deliberately not broker event time.

The journal records the order of locally COMMITTED accounting updates. Broker
execution timestamps are retained as data but are unauthenticated, and delayed
evidence is not retroactively inserted into the earlier accounting sequence.
A SHA-256 link is a consistency check, NOT an external tamper-proof signature.
"""
import hashlib
import json

from .ledger import SafetyError
from .execution_history import verify_entry_lineage


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",",":"))


def _hash(value):
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _executions(items):
    # (execution_id,broker_or_close_order_id,quantity,executed_at_ns)
    return _canonical(sorted([list(item) for item in items]))


def _row_digest(row):
    (client_id,account_id,mode,seq,kind,ref_id,entry_total,
     closed_total,remaining,executions_json,evidence_sha256,
     previous_sha256,recorded_at_ns)=row
    return _hash({
        "client_id":client_id,"account_id":account_id,"mode":mode,
        "seq":seq,"kind":kind,"ref_id":ref_id,"entry_total":entry_total,
        "closed_total":closed_total,"remaining":remaining,
        "executions_json":executions_json,
        "evidence_sha256":evidence_sha256,
        "previous_sha256":previous_sha256,"recorded_at_ns":recorded_at_ns,
    })


def _all(db,client_id):
    return db.execute("""SELECT client_id,account_id,mode,seq,kind,ref_id,
        entry_total,closed_total,remaining,executions_json,evidence_sha256,
        previous_sha256,recorded_at_ns,row_sha256
        FROM mock_position_journal WHERE client_id=? ORDER BY seq""",
        (client_id,)).fetchall()


def _entry_items(db,client_id,version):
    return db.execute("""SELECT execution_id,broker_order_id,quantity,executed_at_ns
        FROM entry_executions WHERE client_id=? AND first_version=?
        ORDER BY execution_id""",(client_id,version)).fetchall()


def _close_items(db,account_id,client_id,close_id):
    return db.execute("""SELECT execution_id,close_order_id,quantity,executed_at_ns
        FROM position_close_executions WHERE account_id=? AND client_id=?
        AND close_order_id=? ORDER BY execution_id""",
        (account_id,client_id,close_id)).fetchall()


def verify_position_journal(db,client_id):
    """Reconstruct ledger position from all previously committed mock updates.

    A legacy finalized order without a journal is NOT backfilled.
    Returns (seq, hash, entry_total, closed_total, remaining).
    """
    history=_all(db,client_id)
    if not history or history[0][4]!="entry_initial":
        raise SafetyError("missing chronological accounting history; legacy review required")
    entry_version_count,_,history_total=verify_entry_lineage(
        db,client_id)
    row=db.execute("""SELECT account_id,mode,filled_quantity FROM orders
        WHERE client_id=?""",(client_id,)).fetchone()
    if not row:
        raise SafetyError("missing original order for accounting history")
    account,mode,quantity=row
    previous="0"*64
    entry_total=closed_total=remaining=0
    version=0
    seen_closes=set()
    for expected_seq,entry in enumerate(history,1):
        (*details,digest)=entry
        (cid,acct,md,seq,kind,ref_id,new_entry,new_closed,new_remaining,
         executions_json,evidence,linked,recorded)=details
        if (cid!=client_id or acct!=account or md!=mode
                or seq!=expected_seq or linked!=previous
                or digest!=_row_digest(details)
                or type(recorded) is not int or recorded<=0):
            raise SafetyError("accounting journal chain or owner corrupted")
        if kind in ("entry_initial","entry_revision"):
            version+=1
            if (ref_id!=f"v{version}" or
                    kind!=("entry_initial" if version==1 else "entry_revision")):
                raise SafetyError("unexpected entry history version")
            chain=db.execute("""SELECT filled_quantity,evidence_sha256
                FROM entry_revision_chain WHERE client_id=? AND version=?""",
                (client_id,version)).fetchone()
            items=_entry_items(db,client_id,version)
            if (not chain or not items and
                    (kind!="entry_initial" or new_entry!=0)
                    or _executions(items)!=executions_json
                    or chain!=(new_entry,evidence)
                    or new_closed!=closed_total
                    or new_entry!=entry_total+sum(item[2] for item in items)
                    or (version>1 and not items)
                    or new_remaining!=remaining+sum(item[2] for item in items)):
                raise SafetyError("entry journal differs from execution lineage")
            entry_total=new_entry
            remaining=new_remaining
        elif kind=="close":
            if ref_id in seen_closes:
                raise SafetyError("close appears twice in accounting journal")
            seen_closes.add(ref_id)
            close=db.execute("""SELECT quantity,remaining_quantity,evidence_sha256,
                recorded_at_ns FROM position_close_batches
                WHERE client_id=? AND account_id=? AND close_order_id=?""",
                (client_id,account,ref_id)).fetchone()
            items=_close_items(db,account,client_id,ref_id)
            if (not close or any(item[3] is None for item in items)
                    or not items or _executions(items)!=executions_json
                    or close[0]!=sum(item[2] for item in items)
                    or close[1]!=new_remaining or close[2]!=evidence
                    or close[3]!=recorded
                    or new_entry!=entry_total
                    or new_closed!=closed_total+close[0]
                    or new_remaining!=remaining-close[0]
                    or new_remaining<0):
                raise SafetyError("close accounting chronology conflicts with immutable close audit")
            closed_total=new_closed
            remaining=new_remaining
        else:
            raise SafetyError("unrecognized accounting journal record")
        if entry_total-closed_total!=remaining:
            raise SafetyError("accounting quantities do not conserve position")
        previous=digest
    close_count=db.execute("""SELECT COUNT(*) FROM position_close_batches
        WHERE client_id=?""",(client_id,)).fetchone()[0]
    position=db.execute("""SELECT opened_quantity,remaining_quantity FROM
        position_balances WHERE client_id=?""",(client_id,)).fetchone()
    if (version!=entry_version_count or close_count!=len(seen_closes)
            or history_total!=entry_total or quantity!=entry_total
            or (position is None and (entry_total!=0 or remaining!=0))
            or (position is not None and position!=(entry_total,remaining))):
        raise SafetyError("stored position or revision count differs from journal replay")
    return len(history),previous,entry_total,closed_total,remaining


def _append(db,client_id,account,mode,kind,ref_id,entry_total,
            closed_total,remaining,items,evidence,recorded_at_ns):
    latest=db.execute("""SELECT seq,row_sha256 FROM mock_position_journal
        WHERE client_id=? ORDER BY seq DESC LIMIT 1""",(client_id,)).fetchone()
    seq,previous=(latest[0]+1,latest[1]) if latest else (1,"0"*64)
    detail=(client_id,account,mode,seq,kind,ref_id,entry_total,
            closed_total,remaining,_executions(items),evidence,previous,
            recorded_at_ns)
    digest=_row_digest(detail)
    db.execute("""INSERT INTO mock_position_journal
        (client_id,account_id,mode,seq,kind,ref_id,entry_total,closed_total,
         remaining,executions_json,evidence_sha256,previous_sha256,
         recorded_at_ns,row_sha256)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",detail+(digest,))
    return digest


def record_initial_journal(db,proof,*,evidence_hash,recorded_at_ns):
    order=proof.order
    if _all(db,order.client_id):
        raise SafetyError("initial accounting journal already exists")
    count=sum(fill.quantity for fill in proof.fills)
    return _append(db,order.client_id,order.account_id,order.mode.value,
        "entry_initial","v1",count,0,count,
        _entry_items(db,order.client_id,1),evidence_hash,recorded_at_ns)


def record_entry_journal(db,proof,*,evidence_hash,recorded_at_ns):
    order=proof.order
    latest=db.execute("""SELECT seq,entry_total,closed_total,remaining
        FROM mock_position_journal WHERE client_id=?
        ORDER BY seq DESC LIMIT 1""",(order.client_id,)).fetchone()
    if not latest:
        raise SafetyError("no original journal for incremental entry")
    nversions=db.execute("""SELECT MAX(version) FROM entry_revision_chain
        WHERE client_id=?""",(order.client_id,)).fetchone()[0]
    total=sum(fill.quantity for fill in proof.fills)
    if total<=latest[1]:
        raise SafetyError("entry revision must increase total")
    return _append(db,order.client_id,order.account_id,order.mode.value,
        "entry_revision",f"v{nversions}",total,latest[2],
        latest[3]+total-latest[1],
        _entry_items(db,order.client_id,nversions),evidence_hash,
        recorded_at_ns)


def record_close_journal(db,proof,*,evidence_hash,recorded_at_ns):
    latest=db.execute("""SELECT entry_total,closed_total,remaining
        FROM mock_position_journal WHERE client_id=?
        ORDER BY seq DESC LIMIT 1""",(proof.client_id,)).fetchone()
    if not latest:
        raise SafetyError("original entry journal missing")
    quantity=sum(x.quantity for x in proof.executions)
    if latest[2]-quantity!=proof.remaining_quantity:
        raise SafetyError("close does not match journal remaining")
    return _append(db,proof.client_id,proof.account_id,proof.mode.value,
        "close",proof.close_order_id,latest[0],latest[1]+quantity,
        proof.remaining_quantity,
        _close_items(db,proof.account_id,proof.client_id,
                     proof.close_order_id),evidence_hash,recorded_at_ns)
