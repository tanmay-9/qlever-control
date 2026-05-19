"""Pure primitives for reading the query metrics log.

The producer escapes every quote in user content, so a bare `"` is
always structural. That is what lets us slice scalars out of a line by
fixed key order instead of paying json.loads on the bulk path. The keys
below assume the producer's fixed field order; a contract change is
caught by the startup self-check, not here.
"""

import json

TS_PREFIX = b'{"ts-ms":'
EVENT_KEY = b'"event":"'
QID_KEY = b'"qid":"'
STATUS_KEY = b'"status":"'

STATUS_SET = frozenset({"ok", "failed", "cancelled", "timeout", "unknown"})


def slice_string_value(line_bytes: bytes, key: bytes) -> str | None:
    """Return the string value following `key`, up to the next quote.

    `key` includes the value's opening quote (for example b'"qid":"').
    Returns None if the key or its closing quote is absent.
    """
    start = line_bytes.find(key)
    if start == -1:
        return None
    start += len(key)
    end = line_bytes.find(b'"', start)
    if end == -1:
        return None
    return line_bytes[start:end].decode()


def parse_line(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Byte-slice one log line into (ts_ms, event, qid, status).

    The fast path: no json.loads, the query blob is never scanned.
    `status` is None on start lines. Returns None on any sanity-check
    miss so the caller can fall back to parse_line_fallback. Never
    raises.
    """
    if not line_bytes.startswith(TS_PREFIX):
        return None
    comma = line_bytes.find(b",", len(TS_PREFIX))
    if comma == -1:
        return None
    try:
        ts_ms = int(line_bytes[len(TS_PREFIX):comma])
    except ValueError:
        return None

    event = slice_string_value(line_bytes, EVENT_KEY)
    if event not in ("start", "end"):
        return None

    qid = slice_string_value(line_bytes, QID_KEY)
    if qid is None:
        return None

    if event == "start":
        return (ts_ms, event, qid, None)

    status = slice_string_value(line_bytes, STATUS_KEY)
    if status not in STATUS_SET:
        return None
    return (ts_ms, event, qid, status)


def parse_line_fallback(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Real json.loads for a line parse_line rejected.

    Same 4-tuple shape as parse_line so callers treat both paths
    alike. Returns None on a malformed line or a contract miss; never
    raises.
    """
    try:
        obj = json.loads(line_bytes)
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None

    ts_ms = obj.get("ts-ms")
    event = obj.get("event")
    qid = obj.get("qid")
    if not isinstance(ts_ms, int):
        return None
    if event not in ("start", "end"):
        return None
    if not isinstance(qid, str):
        return None

    if event == "start":
        return (ts_ms, event, qid, None)

    status = obj.get("status")
    if status not in STATUS_SET:
        return None
    return (ts_ms, event, qid, status)

