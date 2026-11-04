"""Broker-free sequenced mock fill feed with persistent gap alarms.

This is an UNAUTHENTICATED, CALLER-DECLARED sequence, NOT a real broker
sequence number or a proof that all events were received. Reconciliation and
operator authorization are separate. No API for automatically clearing alarms.
"""
from dataclasses import dataclass
import time

from .events import FillNotice, _digest_notice, _hold, _ingest_mock_fill_tx
from .ledger import Ledger, Mode, SafetyError


@dataclass(frozen=True)
class MockSequencedFill:
    stream_id: str
    sequence: int
    notice: FillNotice


def _valid_stream(stream_id, sequence):
    return (type(stream_id) is str and bool(stream_id.strip())
            and type(sequence) is int and sequence>=1)


def _alert(db, ledger, account, mode, stream, seq, kind, digest, now):
    db.execute("""INSERT INTO mock_feed_alerts
        (account_id,mode,stream_id,sequence,kind,payload_sha256,detected_at_ns)
        VALUES(?,?,?,?,?,?,?)""",
        (account,mode.value,stream,seq,kind,digest,now))
    db.execute("""UPDATE mock_feed_streams SET has_alarm=1
        WHERE account_id=? AND mode=?""",(account,mode.value))
    _hold(db,ledger,account,mode,"__feed__:"+stream,
          "mock feed "+kind+"; requires independent review",now)


def _state(db, account, mode, stream):
    found=db.execute("""SELECT stream_id,next_expected,high_watermark,has_alarm
        FROM mock_feed_streams WHERE account_id=? AND mode=?""",
        (account,mode.value)).fetchone()
    if found:
        return found
    db.execute("""INSERT INTO mock_feed_streams
        (account_id,mode,stream_id,next_expected,high_watermark,has_alarm)
        VALUES(?,?,?,1,0,0)""",(account,mode.value,stream))
    return (stream,1,0,0)


def _advance(db,account,mode,next_expected):
    while db.execute("""SELECT 1 FROM mock_feed_messages WHERE
        account_id=? AND mode=? AND sequence=?""",
        (account,mode.value,next_expected)).fetchone():
        next_expected+=1
    db.execute("""UPDATE mock_feed_streams SET next_expected=?
        WHERE account_id=? AND mode=?""",
        (next_expected,account,mode.value))
    return next_expected


def ingest_mock_sequenced_fill(ledger: Ledger, envelope: MockSequencedFill) -> str:
    """Atomically record transport sequence + fill, retaining every alarm.

    An exact message replay is idempotent. Same sequence with different
    payload, stream replacement, duplicate execution ID under a second
    sequence, and missing prefix/intermediate/tail all quarantine an account.
    """
    if (type(envelope) is not MockSequencedFill
            or not _valid_stream(envelope.stream_id,envelope.sequence)):
        raise SafetyError("invalid mock stream identifier or sequence")
    notice=envelope.notice
    digest=_digest_notice(notice)
    account,mode=notice.account_id,notice.mode
    stream,seq=envelope.stream_id,envelope.sequence
    now=time.time_ns()
    failure=None
    status="duplicate"
    with ledger._tx() as db:
        active,next_expected,watermark,alarm=_state(db,account,mode,stream)
        if active!=stream:
            _alert(db,ledger,account,mode,stream,seq,"unexpected-stream",digest,now)
            failure="sequenced stream replacement requires manual review"
        else:
            prior=db.execute("""SELECT payload_sha256 FROM mock_feed_messages
                WHERE account_id=? AND mode=? AND sequence=?""",
                (account,mode.value,seq)).fetchone()
            if prior:
                if prior[0]!=digest:
                    _alert(db,ledger,account,mode,stream,seq,
                           "conflicting-sequence",digest,now)
                    failure="conflicting mock sequence payload"
            else:
                reused=db.execute("""SELECT sequence FROM mock_feed_messages
                    WHERE account_id=? AND mode=? AND execution_id=?
                    LIMIT 1""",(account,mode.value,notice.execution_id)).fetchone()
                db.execute("""INSERT INTO mock_feed_messages
                    (account_id,mode,stream_id,sequence,execution_id,
                     payload_sha256,received_at_ns) VALUES(?,?,?,?,?,?,?)""",
                    (account,mode.value,stream,seq,notice.execution_id,digest,now))
                if seq>next_expected:
                    _alert(db,ledger,account,mode,stream,seq,
                           "sequence-gap",digest,now)
                elif seq<next_expected:
                    _alert(db,ledger,account,mode,stream,seq,
                           "old-unrecorded-sequence",digest,now)
                if reused:
                    _alert(db,ledger,account,mode,stream,seq,
                           "execution-reused-at-new-sequence",digest,now)
                status,conflict=_ingest_mock_fill_tx(
                    db,ledger,notice,digest,now)
                if conflict:
                    _alert(db,ledger,account,mode,stream,seq,
                           "conflicting-execution",digest,now)
                    failure="conflicting mock fill execution ID"
                _advance(db,account,mode,next_expected)
                current=db.execute("""SELECT next_expected,high_watermark
                    FROM mock_feed_streams WHERE account_id=? AND mode=?""",
                    (account,mode.value)).fetchone()
                if current[1]>=current[0] and current[1]>0:
                    _alert(db,ledger,account,mode,stream,seq,
                           "watermark-gap",digest,now)
    if failure:
        raise SafetyError(failure)
    return status


def observe_mock_watermark(ledger: Ledger, *,
                           account_id: str, mode: Mode, stream_id: str,
                           last_sequence: int) -> bool:
    """Observe caller-declared highest sequence; return True if a gap remains.

    Watermarks are monotonic assertions made by an UNTRUSTED caller.
    A gap alarm is permanently retained, even after missing messages arrive.
    There is intentionally no automatic restart/acknowledgement API.
    """
    if (type(account_id) is not str or not account_id.strip()
            or type(mode) is not Mode
            or type(stream_id) is not str or not stream_id.strip()
            or type(last_sequence) is not int or last_sequence<0):
        raise SafetyError("invalid mock feed watermark")
    now=time.time_ns()
    with ledger._tx() as db:
        active,next_expected,watermark,alarm=_state(
            db,account_id,mode,stream_id)
        if active!=stream_id:
            _alert(db,ledger,account_id,mode,stream_id,last_sequence,
                   "unexpected-stream-watermark",None,now)
            return True
        if last_sequence<watermark:
            _alert(db,ledger,account_id,mode,stream_id,last_sequence,
                   "regressed-watermark",None,now)
            return True
        db.execute("""UPDATE mock_feed_streams SET high_watermark=?
            WHERE account_id=? AND mode=?""",
            (last_sequence,account_id,mode.value))
        if last_sequence>=next_expected:
            _alert(db,ledger,account_id,mode,stream_id,last_sequence,
                   "watermark-gap",None,now)
            return True
        return bool(alarm)
