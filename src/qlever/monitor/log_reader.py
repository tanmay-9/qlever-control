"""Parsing and time-windowed scanning of qlever server logs."""

import json
import os
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import BinaryIO

# Length of "2026-05-06 16:35:49.815".
TS_LEN = 23
TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
# Initial backward jump for the gallop phase.
GALLOP_INITIAL_STEP = 128 * 1024
# Marker that precedes the JSON payload on a metric line.
METRIC_MARKER = b"METRIC: "


def extract_timestamp(line: bytes) -> bytes:
    """Return the 23-byte timestamp prefix of a log line, suitable for lexical comparison."""
    return line[:TS_LEN]


def is_timestamped(line: bytes) -> bool:
    """Cheap structural check: line begins with 'YYYY-MM-DD HH:...' (the log-record header format)."""
    return (
        len(line) >= TS_LEN
        and line[4] == 0x2D
        and line[7] == 0x2D
        and line[10] == 0x20
    )


def read_line_after(
    log: BinaryIO, probe_offset: int
) -> tuple[bytes, int] | None:
    """Seek near probe_offset and return (timestamp_bytes, line_start_offset) of the next timestamped line, or None at EOF."""
    log.seek(probe_offset)
    if probe_offset > 0:
        # Discard the partial line we landed inside.
        log.readline()
    while True:
        line_offset = log.tell()
        line = log.readline()
        if not line:
            return None
        if is_timestamped(line):
            return extract_timestamp(line), line_offset


def read_last_timestamp(log: BinaryIO, file_size: int) -> datetime:
    """Read the last timestamped line's timestamp as a datetime, growing the tail buffer until one is found."""
    tail = 32 * 1024
    while True:
        log.seek(max(0, file_size - tail))
        lines = log.readlines()
        for line in reversed(lines):
            if is_timestamped(line):
                return datetime.strptime(
                    extract_timestamp(line).decode(), TS_FORMAT
                )
        if tail >= file_size:
            raise ValueError("log file has no timestamped lines")
        tail *= 2


def read_first_timestamp(log: BinaryIO) -> datetime:
    """Read the first timestamped line's timestamp as a datetime."""
    log.seek(0)
    for line in log:
        if is_timestamped(line):
            return datetime.strptime(
                extract_timestamp(line).decode(), TS_FORMAT
            )
    raise ValueError("log file has no timestamped lines")


def log_time_span(log_file: Path) -> tuple[datetime, datetime]:
    """Return (first, last) timestamped-line times in a single file open."""
    file_size = log_file.stat().st_size
    with log_file.open("rb") as log:
        first_dt = read_first_timestamp(log)
        last_dt = read_last_timestamp(log, file_size)
    return first_dt, last_dt


def format_log_timestamp(dt: datetime) -> bytes:
    """Render a datetime as a 23-byte log-format timestamp ('2026-05-06 16:35:49.815')."""
    # strftime gives 6-digit microseconds; truncate the last 3 to get milliseconds.
    return dt.strftime(TS_FORMAT)[:-3].encode()


def find_offset_in_window(
    log: BinaryIO, oldest_target: bytes, newest_target: bytes
) -> int:
    """Return a byte offset whose line timestamp lies in [oldest_target, newest_target] (lexical bytes).

    Falls back to an offset older than oldest_target when no in-window sample is found,
    so callers always over-include rather than under-include.
    """
    log.seek(0, os.SEEK_END)
    file_size = log.tell()

    # Phase 1: gallop backward from EOF until we land in-window or overshoot it.
    step = GALLOP_INITIAL_STEP
    probe_offset = file_size
    newer_bound = file_size
    older_bound = None
    while probe_offset > 0:
        probe_offset = max(0, probe_offset - step)
        sample = read_line_after(log, probe_offset)
        if sample is None:
            step *= 2
            continue
        timestamp, line_offset = sample
        if oldest_target <= timestamp <= newest_target:
            return line_offset
        if timestamp < oldest_target:
            older_bound = line_offset
            break
        newer_bound = line_offset
        step *= 2

    # Whole file is within the window.
    if older_bound is None:
        return 0

    # Phase 2: binary-search the bracket [older_bound, newer_bound].
    while older_bound < newer_bound:
        mid = (older_bound + newer_bound) // 2
        sample = read_line_after(log, mid)
        # Probe sat inside a long line that crosses past newer_bound; tighten left.
        if sample is None or sample[1] >= newer_bound:
            newer_bound = mid
            continue
        timestamp, line_offset = sample
        if oldest_target <= timestamp <= newest_target:
            return line_offset
        if timestamp < oldest_target:
            older_bound = line_offset + 1
        else:
            newer_bound = line_offset
    return older_bound


def parse_metric_line(line: bytes) -> dict | None:
    """Parse a single log line as a METRIC event, or return None if it is not one."""
    # Continuation lines from multi-line non-METRIC entries lack a timestamp.
    if not is_timestamped(line):
        return None
    # Partial line at the tail of an actively-appended log.
    if not line.endswith(b"\n"):
        return None
    # rfind so that lines corrupted by concurrent writes (two entries colliding
    # into one) still yield the surviving JSON.
    marker_idx = line.rfind(METRIC_MARKER)
    if marker_idx < 0:
        return None
    try:
        return json.loads(line[marker_idx + len(METRIC_MARKER):])
    except json.JSONDecodeError:
        return None


def iter_metric_events(
    log: BinaryIO, start_offset: int, end_ts: bytes | None = None
) -> Iterator[dict]:
    """Yield parsed METRIC events forward from start_offset, stopping at EOF
    or when a line's timestamp exceeds end_ts.
    """
    log.seek(start_offset)
    for line in log:
        if not is_timestamped(line):
            continue
        if end_ts is not None and extract_timestamp(line) > end_ts:
            return
        event = parse_metric_line(line)
        if event is not None:
            yield event


def group_queries(
    events: Iterator[dict],
) -> tuple[dict[str, dict], list[tuple[dict, dict]]]:
    """Group query events by lifecycle; return (active_by_qid, completed_pairs).

    active_by_qid holds starts that never saw a matching end inside the stream.
    completed_pairs is a list of (start_event, end_event) tuples.
    End events without a preceding start (i.e. their start fell outside the window) are dropped.
    """
    active = {}
    completed = []
    for event in events:
        qid = event["query-id"]
        if event["event"] == "start":
            active[qid] = event
        else:
            start = active.pop(qid, None)
            if start is not None:
                completed.append((start, event))
    return active, completed


def get_live_active_queries(
    log_file: Path, timeout_seconds: float
) -> list[dict]:
    """Return start events for queries that began but did not end within the
    last ~2*timeout near EOF.
    """
    with log_file.open("rb") as log:
        log.seek(0, os.SEEK_END)
        file_size = log.tell()
        eof_dt = read_last_timestamp(log, file_size)
        oldest_target = format_log_timestamp(
            eof_dt - timedelta(seconds=2 * timeout_seconds)
        )
        newest_target = format_log_timestamp(
            eof_dt - timedelta(seconds=1.5 * timeout_seconds)
        )
        start_offset = find_offset_in_window(log, oldest_target, newest_target)
        active, _ = group_queries(iter_metric_events(log, start_offset))
        return list(active.values())


def get_metrics_history(
    log_file: Path, max_lookback_seconds: int = 3600
) -> tuple[int, int, list[tuple[int, int]]]:
    """Return (eof_ms, coverage_start_ms, finish_events) anchored at EOF.

    finish_events is (ended_at_ms, duration_ms) for queries completed within
    the covered range. coverage_start_ms is the epoch ms of the earliest
    line scanned, capped at eof_ms - max_lookback_seconds so callers know
    how far back the data actually reaches.
    """
    with log_file.open("rb") as log:
        log.seek(0, os.SEEK_END)
        file_size = log.tell()
        eof_dt = read_last_timestamp(log, file_size)
        eof_ms = int(eof_dt.timestamp() * 1000)
        target = eof_dt - timedelta(seconds=max_lookback_seconds)
        tolerance = max(60.0, max_lookback_seconds * 0.05)
        oldest_target = format_log_timestamp(
            target - timedelta(seconds=tolerance)
        )
        newest_target = format_log_timestamp(target)
        start_offset = find_offset_in_window(log, oldest_target, newest_target)
        first_sample = read_line_after(log, start_offset)
        if first_sample is None:
            return eof_ms, eof_ms, []
        first_dt = datetime.strptime(first_sample[0].decode(), TS_FORMAT)
        first_ms = int(first_dt.timestamp() * 1000)
        # Coverage reaches whichever is later: the first scanned line, or the
        # nominal max-lookback edge.
        coverage_start_ms = max(first_ms, eof_ms - max_lookback_seconds * 1000)
        _, completed = group_queries(iter_metric_events(log, start_offset))

    finish_events = [
        (end["ended-at"], end["ended-at"] - start["started-at"])
        for start, end in completed
        if end["ended-at"] >= coverage_start_ms
    ]
    return eof_ms, coverage_start_ms, finish_events


def get_historic_window(
    log_file: Path,
    start_dt: datetime,
    end_dt: datetime,
    timeout_seconds: float,
) -> tuple[list[dict], list[tuple[dict, dict]], int]:
    """Return (active, completed, last_scanned_ms) for the window [start_dt, end_dt].

    Scans an extended range [start_dt - 2*timeout, end_dt + 2*timeout] so that queries
    whose starts precede the window and whose ends follow it are captured. 'active' are
    starts whose end was never seen even within the extended scan (genuinely stuck or
    lost end event); 'completed' are (start, end) pairs that overlap the window.
    last_scanned_ms is the largest event timestamp encountered during the scan; it
    bounds how far our "no end seen" claim is verified for active rows.
    """
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    last_scanned_ms = 0
    with log_file.open("rb") as log:
        oldest_target = format_log_timestamp(
            start_dt - timedelta(seconds=2 * timeout_seconds)
        )
        newest_target = format_log_timestamp(
            start_dt - timedelta(seconds=1.5 * timeout_seconds)
        )
        start_offset = find_offset_in_window(log, oldest_target, newest_target)
        scan_end_ts = format_log_timestamp(
            end_dt + timedelta(seconds=2 * timeout_seconds)
        )

        def tracked_events() -> Iterator[dict]:
            nonlocal last_scanned_ms
            for event in iter_metric_events(log, start_offset, scan_end_ts):
                ts = event.get("started-at") or event.get("ended-at") or 0
                if ts > last_scanned_ms:
                    last_scanned_ms = ts
                yield event

        active_by_qid, completed = group_queries(tracked_events())

    overlapping_completed = [
        (start, end)
        for start, end in completed
        if start["started-at"] <= end_ms and end["ended-at"] >= start_ms
    ]
    overlapping_active = [
        start
        for start in active_by_qid.values()
        if start["started-at"] <= end_ms
    ]
    return overlapping_active, overlapping_completed, last_scanned_ms
