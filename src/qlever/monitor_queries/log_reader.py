"""Pure primitives for reading the query metrics log"""

import json
from collections.abc import Callable, Iterator
from typing import BinaryIO, NamedTuple

TS_PREFIX = b'{"ts-ms":'
EVENT_KEY = b'"event":"'
QID_KEY = b'"qid":"'
CLIENT_IP_KEY = b'"client-ip":"'
STATUS_KEY = b'"status":"'

# Statuses the server writes; anything else maps to UNKNOWN_STATUS.
STATUS_SET = frozenset({"ok", "failed", "cancelled", "timeout"})

UNKNOWN_STATUS = "unknown"


class CompletedQuery(NamedTuple):
    """One start event paired with its matching end event.

    start_line_offset is the byte offset of the start line in the log,
    kept so load_sparql_at can fetch the SPARQL text later. None when
    the pair was built without scanning the file (the live tailer),
    since those completions feed metrics only and never need their
    SPARQL text re-read.
    """

    start_ms: int
    end_ms: int
    duration_ms: int
    status: str
    start_line_offset: int | None


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

    Every log line begins with ts-ms, so the integer sits between
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


def normalize_status(status: str) -> str:
    """Pass a known status through, map anything else to unknown."""
    return status if status in STATUS_SET else UNKNOWN_STATUS


def parse_line(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Byte-slice one log line into (ts_ms, event, qid, status).

    Avoids json.loads and never scans the query blob, so the common
    line stays cheap to parse. `status` is None on start lines. Returns
    None on anything unexpected so the caller can fall back to
    parse_line_fallback. Never raises.
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
    if status is None:
        return None
    return (ts_ms, event, qid, normalize_status(status))


def parse_line_fallback(
    line_bytes: bytes,
) -> tuple[int, str, str, str | None] | None:
    """Full json.loads for a line parse_line rejected.

    Same 4-tuple shape as parse_line so callers treat both alike.
    Returns None on a malformed line or a missing/wrong field; never
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
    if not isinstance(status, str):
        return None
    return (ts_ms, event, qid, normalize_status(status))


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


def read_first_timestamp(log_stream: BinaryIO, file_size: int) -> int | None:
    """Return the ts_ms of the first complete line, or None.

    Scans forward from the start of the file, skipping malformed
    lines, and returns the first ts_ms that parses.
    """
    if file_size == 0:
        return None
    log_stream.seek(0)
    while True:
        line = log_stream.readline()
        # No newline means EOF or a partial line: nothing complete left.
        if not line.endswith(b"\n"):
            return None
        ts_ms = peek_ts_ms(line)
        if ts_ms is not None:
            return ts_ms


def read_last_timestamp(log_stream: BinaryIO, file_size: int) -> int | None:
    """Return the ts_ms of the last complete line, or None.

    Reads a 32KB tail buffer and walks its lines backward, returning
    the first ts_ms that parses. A trailing partial line is skipped by
    its missing newline; a leading mid-line fragment is skipped because
    it does not start with the ts_ms prefix. Doubles the buffer and
    retries if nothing parses, capped at the whole file.
    """
    if file_size == 0:
        return None
    tail_bytes = 32 * 1024
    while True:
        probe = max(0, file_size - tail_bytes)
        log_stream.seek(probe)
        for line in reversed(log_stream.readlines()):
            if not line.endswith(b"\n"):
                continue
            ts_ms = peek_ts_ms(line)
            if ts_ms is not None:
                return ts_ms
        if probe == 0:
            return None
        tail_bytes *= 2


GALLOP_START = 128 * 1024

# Re-check should_cancel after this many scanned bytes.
CANCEL_CHECK_BYTES = 8 * 1024 * 1024


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
    log_stream: BinaryIO,
    lo_offset: int,
    hi_bound: int,
    should_cancel: Callable[[], bool] | None = None,
) -> Iterator:
    """Yield (parsed, line_offset) for whole lines in [lo_offset, hi_bound].

    parsed is the (ts_ms, event, qid, status) tuple. lo_offset must be
    line-aligned; it always comes from offset_for_ts. The line straddling
    hi_bound is included; a trailing line without a newline is left for a
    later read. Malformed lines are skipped.

    When should_cancel is given, it is polled every CANCEL_CHECK_BYTES and
    the scan returns early if it is true. That yields a partial stream, so
    a caller that cancels must re-check before trusting the result.
    """
    log_stream.seek(lo_offset)
    offset = lo_offset
    next_cancel_check = lo_offset + CANCEL_CHECK_BYTES
    for line in log_stream:
        if offset > hi_bound:
            return
        if not line.endswith(b"\n"):
            return
        if should_cancel is not None and offset >= next_cancel_check:
            if should_cancel():
                return
            next_cancel_check = offset + CANCEL_CHECK_BYTES
        parsed = parse_line(line) or parse_line_fallback(line)
        if parsed is not None:
            yield (parsed, offset)
        offset += len(line)


def pair_start_end_events(
    events: Iterator,
) -> tuple[list[CompletedQuery], dict[str, tuple[int, int]]]:
    """Pair start and end events from a scan into completed queries.

    Walks the events once. Each end pops its matching start by qid into
    completed_queries; whatever remains unmatched is still_open. Unmatched
    ends are dropped (their start was outside the scanned range).

    events: yields ((ts_ms, event, qid, status), line_offset) from
    scan_range.

    Returns (completed_queries, still_open). still_open maps qid to
    (start_ms, start_line_offset) for queries with no end event seen yet.
    """
    completed_queries = []
    still_open = {}
    for (ts_ms, event, qid, status), line_offset in events:
        if event == "start":
            still_open[qid] = (ts_ms, line_offset)
            continue
        # event == "end"
        matched_start = still_open.pop(qid, None)
        if matched_start is None:
            continue
        start_ms, start_line_offset = matched_start
        completed_queries.append(
            CompletedQuery(
                start_ms=start_ms,
                end_ms=ts_ms,
                duration_ms=ts_ms - start_ms,
                status=status,
                start_line_offset=start_line_offset,
            )
        )
    return (completed_queries, still_open)


def extract_qid_ip_query(line_bytes: bytes) -> tuple[str, str, str]:
    """Return (qid, client_ip, query) from a start line, or ("", "", "").

    All three fields come from one json.loads. The caller has already
    validated this is a start line via parse_line, so qid and query are
    guaranteed present strings. client-ip falls back to "" so log lines
    written before the field existed still produce usable rows.
    """
    try:
        obj = json.loads(line_bytes)
    except (ValueError, TypeError):
        return ("", "", "")
    return (obj["qid"], obj.get("client-ip", ""), obj["query"])


def load_sparql_at(
    log_stream: BinaryIO, line_offset: int
) -> tuple[str, str, str]:
    """Return (qid, client_ip, sparql) for the start line at line_offset.

    Used by callers that have only the offset, not the line bytes:
    find_active_queries on survivors (qid already known, ignored), and
    Historic on displayed rows (needs all three for the SparqlPane).
    """
    log_stream.seek(line_offset)
    return extract_qid_ip_query(log_stream.readline())
