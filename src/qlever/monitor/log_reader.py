"""Pure primitives for reading the query metrics log"""

import json
from collections.abc import Iterator
from typing import BinaryIO

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


def peek_ts_ms(line_bytes: bytes) -> int | None:
    """Read only the leading ts-ms timestamp from a log line.

    The producer always writes ts-ms first, so the integer sits between
    the fixed prefix and the next comma. Lets navigation compare
    timestamps without parsing the rest of the line. Returns None if the
    prefix is absent or the value is not an integer.
    """
    if not line_bytes.startswith(TS_PREFIX):
        return None
    comma = line_bytes.find(b",", len(TS_PREFIX))
    if comma == -1:
        return None
    try:
        return int(line_bytes[len(TS_PREFIX) : comma])
    except ValueError:
        return None


def parse_line(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Byte-slice one log line into (ts_ms, event, qid, status).

    The fast path: no json.loads, the query blob is never scanned.
    `status` is None on start lines. Returns None on any sanity-check
    miss so the caller can fall back to parse_line_fallback. Never
    raises.
    """
    ts_ms = peek_ts_ms(line_bytes)
    if ts_ms is None:
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


def next_whole_line(
    log_stream: BinaryIO, probe: int, file_size: int
) -> tuple[int, int] | None:
    """Find the first complete line starting at or after `probe`.

    A raw probe usually lands mid-line, so we align to the start of the
    next whole line. Returns (line_start, ts_ms), or None when `probe`
    falls in the file's trailing partial line or past EOF.
    """
    if probe >= file_size:
        return None
    if probe == 0:
        line_start = 0
        log_stream.seek(0)
    else:
        log_stream.seek(probe)
        # Discard the partial line the probe landed in; the next read
        # then starts on a whole line.
        log_stream.readline()
        line_start = log_stream.tell()

    line = log_stream.readline()
    # An empty read or a line with no terminating newline is the file's
    # trailing partial line, not a complete record.
    if not line.endswith(b"\n"):
        return None
    ts_ms = peek_ts_ms(line)
    if ts_ms is None:
        return None
    return (line_start, ts_ms)


GALLOP_START = 128 * 1024


def offset_for_ts(log_stream: BinaryIO, target_ms: int, file_size: int) -> int:
    """Find where to start reading so a forward read sees every line
    at or after target_ms.

    The log is only roughly time-ordered, so this aims a little early
    on purpose rather than risk skipping lines; the caller ignores the
    few extra older lines. Returns 0 if target_ms is at or before the
    first line, and a spot near the end if it is past the last line.
    """
    first = next_whole_line(log_stream, 0, file_size)
    if first is None:
        return 0
    first_start, first_ts = first
    if first_ts >= target_ms:
        return 0

    # Gallop backward from the end, doubling the step until a probed
    # line is old enough (ts <= target). before_target brackets the
    # search from the old side, after_target from the new side.
    before_target = first_start
    after_target = file_size
    step = GALLOP_START
    while True:
        probe = file_size - step
        if probe <= before_target:
            break
        found = next_whole_line(log_stream, probe, file_size)
        if found is None:
            step *= 2
            continue
        start, ts = found
        if ts <= target_ms:
            before_target = start
            break
        after_target = start
        step *= 2

    # Narrow the bracket. Bias every step toward the older side so the
    # result never sits past the first matching line.
    while after_target - before_target > 1:
        mid = (before_target + after_target) // 2
        found = next_whole_line(log_stream, mid, file_size)
        if found is None:
            after_target = mid
            continue
        start, ts = found
        # The probe sat inside one long line spanning the bracket edge,
        # so it gave no usable interior line. Tighten the new side to
        # keep the search making progress.
        if start <= before_target or start >= after_target:
            after_target = mid
            continue
        if ts < target_ms:
            before_target = start
        else:
            after_target = start
    return before_target


def scan_range(
    log_stream: BinaryIO, lo_offset: int, hi_bound: int
) -> Iterator:
    """Yield (parsed, line_offset) for whole lines in [lo_offset, hi_bound].

    parsed is the (ts_ms, event, qid, status) tuple. lo_offset must be
    line-aligned; it always comes from offset_for_ts. The line straddling
    hi_bound is included; a trailing line without a newline is left for a
    later read. Malformed lines are skipped.
    """
    log_stream.seek(lo_offset)
    offset = lo_offset
    for line in log_stream:
        if offset > hi_bound:
            return
        if not line.endswith(b"\n"):
            return
        parsed = parse_line(line) or parse_line_fallback(line)
        if parsed is not None:
            yield (parsed, offset)
        offset += len(line)
