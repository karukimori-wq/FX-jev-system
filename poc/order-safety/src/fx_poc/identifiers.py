"""Deterministic IDs for new callers; existing ledger IDs are never rewritten."""
import json
from .models import Mode


def scoped_id(mode: Mode, run_id: str, kind: str, source_id: str) -> str:
    """Encode the complete identity without delimiter ambiguity or hash loss."""
    mode = Mode(mode)
    if kind not in ('decision', 'close', 'execution', 'position'):
        raise ValueError('unsupported identifier kind')
    if any(not isinstance(value, str) or not value.strip() for value in (run_id, source_id)):
        raise ValueError('nonempty run and source identifiers required')
    return 'fxid:v1:' + json.dumps([mode.value, run_id, kind, source_id], ensure_ascii=True, separators=(',', ':'))
