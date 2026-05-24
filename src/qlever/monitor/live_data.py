"""Data layer for the Live screen.

Owns the in-memory state the Live screen renders: the queries currently
running on the server and the rolling 1h history of completed queries
that feeds the 5m, 15m, and 1h metric rows. The tailer is the only
steady-state writer; the screen's refresh callbacks read under the same
lock and return the frozen `models.py` dataclasses the widgets consume.
"""

import json
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from qlever.monitor.log_reader import (
    STATUS_SET,
    CompletedQuery,
    load_sparql_at,
    offset_for_ts,
    pair_start_end_events,
    read_last_timestamp,
    scan_range,
)
from qlever.monitor.metrics import (
    EMPTY_FIELDS,
    MetricsSnapshot,
    metrics_for_ranges,
)
from qlever.monitor.models import LiveQueryRow, LiveSubtitle, MetricsCounts

LIVE_METRIC_WINDOWS_MS = [5 * 60_000, 15 * 60_000, 60 * 60_000]
LIVE_HORIZON_MS = LIVE_METRIC_WINDOWS_MS[2]
LIVE_METRIC_LABELS = ["last 5m", "last 15m", "last 1h"]


def current_ms() -> int:
    """Wall-clock now in epoch milliseconds."""
    return time.time_ns() // 1_000_000


class CompletedQueries:
    """Rolling 1h history of completed queries.

    Storage is a deque ordered by insertion, which is the tailer's
    forward read order. Wall-clock is monotonic in steady state so
    older entries sit at the left and trimming is one popleft loop.
    """

    def __init__(self) -> None:
        self.entries = deque()

    def add(self, query: CompletedQuery) -> None:
        """Append a completed query to the history.

        Caller writes in tailer order, which is wall-clock order in
        steady state; entries remain sorted by end_ms for trimming.
        """
        self.entries.append(query)

    def drop_older_than(self, cutoff_ms: int) -> None:
        """Discard completions that ended before cutoff_ms.

        Called on each tailer tick with cutoff_ms = now_ms - 1h so the
        history stays bounded even during quiet periods.
        """
        while self.entries and self.entries[0].end_ms < cutoff_ms:
            self.entries.popleft()

    def metrics_for_windows(
        self,
        now_ms: int,
        windows_ms: list[int],
        slow_threshold_ms: int,
        data_start_ms: int,
    ) -> list[MetricsSnapshot | None]:
        """Compute one snapshot per window in a single deque walk.

        Each window is the interval [now_ms - width, now_ms].
        data_start_ms is the oldest timestamp the caller has loaded
        complete data for; a window starting before that gets None
        instead of a snapshot, so the widget can render the row as
        not-yet-ready. Returns snapshots in the same order as windows_ms.
        """
        ranges = [(now_ms - width, now_ms) for width in windows_ms]
        snapshots = metrics_for_ranges(
            self.entries, ranges, slow_threshold_ms
        )
        return [
            None if lo_ms < data_start_ms else snapshot
            for (lo_ms, _), snapshot in zip(ranges, snapshots)
        ]


@dataclass
class LiveState:
    """Shared in-memory state for the Live screen.

    The tailer is the steady-state writer; the boot-time loader prepends
    historical completions once. UI refresh callbacks read under the
    same lock and build metric rows on the fly.
    """

    completed: CompletedQueries = field(default_factory=CompletedQueries)
    active: dict[str, tuple[int, str]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


def find_active_queries(
    log_path: Path,
    window_pad_ms: int,
) -> tuple[LiveState, int, int]:
    """Scan the log's tail to determine queries still running.

    Reads [eof_ts - window_pad_ms, EOF], pairs start/end events, and
    records the unmatched starts as the active set. The completed
    pairs found in this window are discarded; backfill rediscovers
    them. Loads SPARQL text for each survivor by stored offset.

    Returns (state, cut_offset, eof_ts). cut_offset is where the tailer
    starts reading forward; eof_ts is the timestamp of the log's last
    complete line, or 0 when the log holds none.
    """
    state = LiveState()
    with log_path.open("rb") as log_stream:
        file_size = log_path.stat().st_size
        eof_ts = read_last_timestamp(log_stream, file_size)
        if eof_ts is None:
            return (state, file_size, 0)

        lo_offset = offset_for_ts(
            log_stream, eof_ts - window_pad_ms, file_size
        )
        events = scan_range(log_stream, lo_offset, file_size)
        _, still_open = pair_start_end_events(events)

        for qid, (start_ms, start_line_offset) in still_open.items():
            _, sparql = load_sparql_at(log_stream, start_line_offset)
            state.active[qid] = (start_ms, sparql)

    return (state, file_size, eof_ts)


def load_completed_history(
    log_path: Path,
    state: LiveState,
    cut_offset: int,
    now_ms: Callable[[], int] = current_ms,
) -> None:
    """Scan the hour before cut_offset and seed completed history.

    One-shot: callers spawn this on a daemon thread at startup if they
    want it off the main loop. After it returns, the tailer is the only
    writer of state.completed.
    """
    with log_path.open("rb") as log_stream:
        oldest_wanted_ms = now_ms() - LIVE_HORIZON_MS
        scan_start_offset = offset_for_ts(
            log_stream, oldest_wanted_ms, cut_offset
        )
        events = scan_range(log_stream, scan_start_offset, cut_offset)
        older_completions, _ = pair_start_end_events(events)

    with state.lock:
        tailer_entries = state.completed.entries
        state.completed.entries = deque(older_completions)
        state.completed.entries.extend(tailer_entries)


class LiveLogReader:
    """Tail cursor and per-poll logic for the live query log.

    Sole writer of LiveState in steady state. Each poll() reads any
    whole lines appended since the last cursor, dispatches them into
    active / completed under the state lock, and trims stale entries.
    The run loop is owned by the caller.
    """

    def __init__(
        self,
        log_path: Path,
        state: LiveState,
        cut_offset: int,
        window_pad_ms: int,
        poll_interval: float = 0.2,
        now_ms: Callable[[], int] = current_ms,
    ) -> None:
        self.log_path = log_path
        self.state = state
        self.cursor = cut_offset
        self.window_pad_ms = window_pad_ms
        self.poll_interval = poll_interval
        self.now_ms = now_ms

    def poll(self, log_stream: BinaryIO) -> None:
        """Process any whole lines appended since the last poll."""
        log_stream.seek(self.cursor)
        for line in log_stream:
            if not line.endswith(b"\n"):
                break
            self.cursor += len(line)
            self.handle_line(line)
        self.evict_stale()

    def evict_stale(self) -> None:
        """Drop completions older than 1h and actives older than 2t.

        Active eviction guards against a missing end event: any query
        whose start is older than the 2t safety horizon is treated as
        gone, since the server contract says no real query lives that
        long.
        """
        now = self.now_ms()
        completed_cutoff = now - LIVE_HORIZON_MS
        active_cutoff = now - self.window_pad_ms
        with self.state.lock:
            self.state.completed.drop_older_than(completed_cutoff)
            stale = [
                qid
                for qid, (start_ms, _) in self.state.active.items()
                if start_ms < active_cutoff
            ]
            for qid in stale:
                del self.state.active[qid]

    def handle_line(self, line: bytes) -> None:
        """Parse one whole line and dispatch it into LiveState."""
        try:
            obj = json.loads(line)
        except (ValueError, TypeError):
            return
        if not isinstance(obj, dict):
            return
        ts_ms = obj.get("ts-ms")
        event = obj.get("event")
        qid = obj.get("qid")
        if not isinstance(ts_ms, int) or not isinstance(qid, str):
            return

        if event == "start":
            sparql = obj.get("query")
            if not isinstance(sparql, str):
                sparql = ""
            with self.state.lock:
                self.state.active[qid] = (ts_ms, sparql)
            return

        if event == "end":
            status = obj.get("status")
            if status not in STATUS_SET:
                return
            with self.state.lock:
                start = self.state.active.pop(qid, None)
                if start is None:
                    return
                start_ms, _ = start
                self.state.completed.add(
                    CompletedQuery(
                        start_ms=start_ms,
                        end_ms=ts_ms,
                        duration_ms=ts_ms - start_ms,
                        status=status,
                        start_line_offset=None,
                    )
                )


def get_live_subtitle(
    state: LiveState, server_status: str, endpoint: str
) -> LiveSubtitle:
    """Subtitle line: server-reachable status plus current active count."""
    return LiveSubtitle(
        endpoint=endpoint, state=server_status, n_active=len(state.active)
    )


def get_live_query_rows(state: LiveState) -> list[LiveQueryRow]:
    """Snapshot the active set as a list of UI rows; no sort."""
    with state.lock:
        active_snapshot = list(state.active.items())
    return [
        LiveQueryRow(qid=qid, ts_ms=start_ms, sparql=sparql)
        for qid, (start_ms, sparql) in active_snapshot
    ]


def get_live_metrics(
    state: LiveState,
    slow_threshold_ms: int,
    now_ms: int,
) -> list[MetricsCounts]:
    """Three rolling-window metric rows for the Live screen.

    Takes a short-locked deque copy so the metric scan and percentile
    work runs lock-free while the tailer keeps appending. Windows whose
    cutoff predates the oldest entry render as an all-None row so the
    widget shows "..." until the deque covers that range.
    """
    with state.lock:
        history = CompletedQueries()
        history.entries = deque(state.completed.entries)
    oldest_ms = (
        history.entries[0].start_ms if history.entries else now_ms
    )
    snapshots = history.metrics_for_windows(
        now_ms=now_ms,
        windows_ms=LIVE_METRIC_WINDOWS_MS,
        slow_threshold_ms=slow_threshold_ms,
        data_start_ms=oldest_ms,
    )
    return [
        MetricsCounts(
            label=label,
            **(snap._asdict() if snap is not None else EMPTY_FIELDS),
        )
        for label, snap in zip(LIVE_METRIC_LABELS, snapshots)
    ]
