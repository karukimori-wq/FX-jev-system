"""Read-only inspection of broker-free simulated sequence coverage.

This report is an operator diagnostic, NOT a source of broker-authenticated
completeness or authorization to clear holds or restart trading.
"""
from contextlib import closing
from dataclasses import dataclass
import sqlite3

from .ledger import Ledger, Mode, SafetyError


@dataclass(frozen=True)
class MockFeedInspection:
    account_id: str
    mode: Mode
    stream_id: str
    next_expected: int
    high_watermark: int
    observed_max_sequence: int
    observed_count: int
    missing_total: int
    missing_ranges: tuple[tuple[int, int], ...]
    ranges_truncated: bool
    checkpoint_consistent: bool
    message_stream_consistent: bool
    has_alarm: bool
    alert_count: int
    event_hold_count: int


def inspect_mock_feed(
    ledger: Ledger, *,
    account_id: str,
    mode: Mode,
    max_ranges: int = 128,
) -> MockFeedInspection:
    """Analyze recorded holes without changing the DB or making risk decisions.

    Watermarks and sequences are caller-supplied mock values. An empty
    gap list is never proof of event completeness; a past alarm stays set.
    """
    if (type(ledger) is not Ledger
            or type(account_id) is not str or not account_id.strip()
            or type(mode) is not Mode
            or type(max_ranges) is not int or not 1<=max_ranges<=1024):
        raise SafetyError("invalid mock feed inspection parameters")
    with closing(sqlite3.connect(ledger.filename, timeout=5)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        state=db.execute("""SELECT stream_id,next_expected,high_watermark,
            has_alarm FROM mock_feed_streams
            WHERE account_id=? AND mode=?""",
            (account_id,mode.value)).fetchone()
        if not state:
            raise SafetyError("mock feed not initialized; no sequence completeness evidence")
        stream,next_expected,watermark,alarm=state
        rows=db.execute("""SELECT sequence,stream_id FROM mock_feed_messages
            WHERE account_id=? AND mode=? ORDER BY sequence""",
            (account_id,mode.value)).fetchall()
        alerts=db.execute("""SELECT COUNT(*) FROM mock_feed_alerts
            WHERE account_id=? AND mode=?""",
            (account_id,mode.value)).fetchone()[0]
        holds=db.execute("""SELECT COUNT(*) FROM mock_event_holds
            WHERE account_id=? AND mode=?""",
            (account_id,mode.value)).fetchone()[0]
        observed_max=rows[-1][0] if rows else 0
        ceiling=max(observed_max,watermark)
        missing=[]
        missing_total=0
        next_missing=1
        # The final missing tail is visible only when the caller explicitly
        # supplies a watermark; otherwise future unseen events are unknowable.
        for seq,_source_stream in rows:
            if seq>next_missing:
                gap=(next_missing,seq-1)
                missing_total+=gap[1]-gap[0]+1
                if len(missing)<max_ranges:
                    missing.append(gap)
            next_missing=max(next_missing,seq+1)
        if next_missing<=ceiling:
            missing_total+=ceiling-next_missing+1
            if len(missing)<max_ranges:
                missing.append((next_missing,ceiling))
        # A derived checkpoint can expose a tampered saved checkpoint but
        # does not establish authenticity of the event sequence.
        derived=1
        for seq,_source_stream in rows:
            if seq!=derived:
                break
            derived+=1
        return MockFeedInspection(
            account_id,mode,stream,next_expected,watermark,observed_max,
            len(rows),missing_total,tuple(missing),
            missing_total>sum(end-start+1 for start,end in missing),
            next_expected==derived,
            all(row_stream==stream for _,row_stream in rows),
            bool(alarm),alerts,holds,
        )
