"""SQLite mock entry execution lineage and tamper-evident revision chain.

A SHA-256 chain detects accidental/local changes; it is NOT signed, immutable,
authenticated broker evidence, or appropriate proof for real FX accounts.
Missing legacy execution IDs are NEVER reconstructed or inferred.
"""
import hashlib
import json
import sqlite3

from .ledger import SafetyError


def _hash(value):
    return hashlib.sha256(json.dumps(
        value,sort_keys=True,separators=(",",":")).encode("utf-8")).hexdigest()


def _rows(db, client_id, through=None):
    if through is None:
        return db.execute("""SELECT execution_id,broker_order_id,quantity,
            executed_at_ns,first_version FROM entry_executions
            WHERE client_id=? ORDER BY execution_id""",(client_id,)).fetchall()
    return db.execute("""SELECT execution_id,broker_order_id,quantity,
        executed_at_ns,first_version FROM entry_executions
        WHERE client_id=? AND first_version<=? ORDER BY execution_id""",
        (client_id,through)).fetchall()


def _version_hash(client_id, version, kind, total, evidence_hash,
                  snapshot_hash, previous_hash):
    return _hash({
        "client_id":client_id,"version":version,"kind":kind,
        "filled_quantity":total,"evidence_sha256":evidence_hash,
        "execution_snapshot_sha256":snapshot_hash,
        "previous_sha256":previous_hash,
    })


def verify_entry_lineage(db, client_id, *, expected_total=None):
    """Verify every revision, all recorded IDs, and the current snapshot.

    Returns (last_version, last_hash, current_total). Missing lineage is
    considered UNKNOWN even if an old aggregate reconciliation audit exists.
    """
    chain=db.execute("""SELECT version,kind,filled_quantity,evidence_sha256,
        snapshot_sha256,previous_sha256,chain_sha256 FROM entry_revision_chain
        WHERE client_id=? ORDER BY version""",(client_id,)).fetchall()
    if not chain or chain[0][0]!=1 or chain[0][1]!="initial":
        raise SafetyError("legacy or missing entry execution history: manual quarantine required")
    prev="0"*64
    for expected_version,(version,kind,total,evidence,snapshot,previous,digest) in enumerate(chain,1):
        if (version!=expected_version or previous!=prev or
                kind not in ("initial","late-no-close","late-with-closes") or
                type(total) is not int or total<0 or
                _hash(_rows(db,client_id,version))!=snapshot or
                sum(row[2] for row in _rows(db,client_id,version))!=total or
                _version_hash(client_id,version,kind,total,evidence,snapshot,prev)!=digest):
            raise SafetyError("entry execution revision chain is inconsistent")
        prev=digest
    current=chain[-1][2]
    all_rows=_rows(db,client_id)
    if (any(x[4]<1 or x[4]>len(chain) for x in all_rows)
            or sum(x[2] for x in all_rows)!=current
            or (expected_total is not None and current!=expected_total)):
        raise SafetyError("entry execution ledger does not match cumulative position")
    return len(chain),prev,current


def _insert_fills(db, proof, *, version):
    order=proof.order
    existing={row[0]:row for row in _rows(db,order.client_id)}
    supplied={fill.execution_id:fill for fill in proof.fills}
    if not set(existing).issubset(supplied):
        raise SafetyError("historical execution ID missing from revision evidence")
    for execution_id,row in existing.items():
        # The original execution timestamp is immutable in SQLite. Synthetic
        # mock proofs can have a later timestamp for an already-known ID; do
        # not overwrite it. Real broker timestamp authenticity is unverified.
        fill=supplied[execution_id]
        if fill.broker_order_id!=row[1] or fill.quantity!=row[2]:
            raise SafetyError("historical execution identity or quantity changed")
    for execution_id,fill in supplied.items():
        if execution_id in existing:
            continue
        if db.execute("""SELECT 1 FROM position_close_executions
            WHERE account_id=? AND execution_id=? LIMIT 1""",
            (order.account_id,execution_id)).fetchone():
            raise SafetyError("entry execution ID collides with recorded close execution")
        try:
            db.execute("""INSERT INTO entry_executions
                (client_id,account_id,mode,broker_order_id,execution_id,
                 quantity,executed_at_ns,first_version)
                VALUES(?,?,?,?,?,?,?,?)""",
                (order.client_id,order.account_id,order.mode.value,
                 order.broker_order_id,execution_id,fill.quantity,
                 fill.executed_at_ns,version))
        except sqlite3.IntegrityError as exc:
            raise SafetyError("execution ID already belongs to another order") from exc
    return _rows(db,order.client_id)


def record_initial_entry(db, proof, *, evidence_hash, recorded_at_ns):
    client_id=proof.order.client_id
    if db.execute("""SELECT 1 FROM entry_revision_chain WHERE client_id=?""",
                  (client_id,)).fetchone():
        raise SafetyError("initial execution history already exists")
    # This function runs inside the parent reconciliation transaction.
    rows=_insert_fills(db,proof,version=1)
    total=sum(x[2] for x in rows)
    snapshot=_hash(rows)
    digest=_version_hash(client_id,1,"initial",total,evidence_hash,snapshot,"0"*64)
    db.execute("""INSERT INTO entry_revision_chain
        (client_id,version,kind,filled_quantity,evidence_sha256,
         snapshot_sha256,previous_sha256,chain_sha256,recorded_at_ns)
        VALUES(?,1,'initial',?,?,?,?,?,?)""",
        (client_id,total,evidence_hash,snapshot,"0"*64,digest,recorded_at_ns))
    return digest


def record_late_entry(db, proof, *, expected_previous, evidence_hash,
                      kind, recorded_at_ns):
    if kind not in ("late-no-close","late-with-closes"):
        raise SafetyError("unrecognized mock revision kind")
    client_id=proof.order.client_id
    version,previous_digest,previous=verify_entry_lineage(
        db,client_id,expected_total=expected_previous)
    # If a prior execution is removed, modified or aliased, reject the entire
    # revision instead of inventing its missing identity.
    rows=_insert_fills(db,proof,version=version+1)
    total=sum(x[2] for x in rows)
    if total<=previous:
        raise SafetyError("late revision must have new distinct execution IDs and positive quantity")
    if total!=sum(fill.quantity for fill in proof.fills):
        raise SafetyError("execution history quantity differs from full mock proof")
    snapshot=_hash(rows)
    digest=_version_hash(client_id,version+1,kind,total,
                         evidence_hash,snapshot,previous_digest)
    db.execute("""INSERT INTO entry_revision_chain
        (client_id,version,kind,filled_quantity,evidence_sha256,
         snapshot_sha256,previous_sha256,chain_sha256,recorded_at_ns)
        VALUES(?,?,?,?,?,?,?,?,?)""",
        (client_id,version+1,kind,total,evidence_hash,
         snapshot,previous_digest,digest,recorded_at_ns))
    return digest
