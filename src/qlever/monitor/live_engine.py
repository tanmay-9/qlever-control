"""In-memory state for the Live tab.

Holds the queries currently running on the server and the rolling
history of completed queries that feeds the 5m, 15m, and 1h metric
rows. A single writer thread mutates the state; readers take a short
locked snapshot. Pure data layer: no Textual imports.
"""

import json
import statistics
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, NamedTuple

from qlever.monitor.log_reader import (
    STATUS_SET,
    CompletedQuery,
    load_sparql_at,
    offset_for_ts,
    pair_start_end_events,
    read_last_timestamp,
    scan_range,
)

LIVE_METRIC_WINDOWS_MS = [5 * 60_000, 15 * 60_000, 60 * 60_000]
LIVE_HORIZON_MS = LIVE_METRIC_WINDOWS_MS[2]


def current_ms() -> int:
    """Wall-clock now in epoch milliseconds."""
    return time.time_ns() // 1_000_000


class MetricsSnapshot(NamedTuple):
    """Counts and percentiles for completed queries in one time window.

    The status counts sum to seen by construction:
        seen == ok + failed + timeout + cancelled + unknown
    slow is independent: a query can be ok and slow. p50 and p95 are
    duration_ms values across all completions in the window, None when
    the window is empty.
    """

    seen: int
    ok: int
    failed: int
    timeout: int
    cancelled: int
    unknown: int
    slow: int
    p50: int | None
    p95: int | None


def percentiles(durations_ms: list[int]) -> tuple[int | None, int | None]:
    """Return (p50, p95) over durations_ms, or (None, None) if empty."""
    if not durations_ms:
        return (None, None)
    if len(durations_ms) == 1:
        only = durations_ms[0]
        return (only, only)
    cuts = statistics.quantiles(durations_ms, n=100)
    return (round(cuts[49]), round(cuts[94]))


def build_snapshot(
    counts: dict[str, int],
    slow: int,
    durations_ms: list[int],
) -> MetricsSnapshot:
    """Wrap one set of accumulators into a MetricsSnapshot."""
    p50, p95 = percentiles(durations_ms)
    return MetricsSnapshot(
        seen=len(durations_ms),
        ok=counts["ok"],
        failed=counts["failed"],
        timeout=counts["timeout"],
        cancelled=counts["cancelled"],
        unknown=counts["unknown"],
        slow=slow,
        p50=p50,
        p95=p95,
    )


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

        data_start_ms is the oldest timestamp the caller has loaded
        complete data for. A window whose cutoff falls before that gets
        None instead of a snapshot, so the widget can render the row as
        not-yet-ready. Returns snapshots in the same order as windows_ms.
        """
        cutoffs = [now_ms - w for w in windows_ms]
        counts = [
            {"ok": 0, "failed": 0, "timeout": 0, "cancelled": 0, "unknown": 0}
            for _ in windows_ms
        ]
        slow = [0] * len(windows_ms)
        durations = [[] for _ in windows_ms]

        for entry in self.entries:
            if entry.end_ms > now_ms:
                continue
            for i, cutoff in enumerate(cutoffs):
                if entry.end_ms >= cutoff:
                    counts[i][entry.status] += 1
                    if entry.duration_ms >= slow_threshold_ms:
                        slow[i] += 1
                    durations[i].append(entry.duration_ms)

        return [
            None
            if cutoffs[i] < data_start_ms
            else build_snapshot(counts[i], slow[i], durations[i])
            for i in range(len(windows_ms))
        ]


@dataclass
class LiveState:
    """Shared in-memory state for the Live tab.

    The tailer is the steady-state writer; the boot-time loader prepends
    historical completions once. The UI render callback reads under the
    same lock and computes metrics on the fly via compute_live_metrics.
    """

    completed: CompletedQueries = field(default_factory=CompletedQueries)
    active: dict[str, tuple[int, str]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


def compute_live_metrics(
    state: LiveState,
    slow_threshold_ms: int,
    now_ms: int,
) -> list[MetricsSnapshot | None]:
    """Compute one snapshot per live window over the completed history.

    Takes a short-locked deque copy so the actual scan and percentile
    work runs lock-free while the tailer keeps appending. Windows whose
    cutoff predates the oldest entry come back as None so the UI can
    render a "not yet covered" sentinel.
    """
    with state.lock:
        history_snapshot = CompletedQueries()
        history_snapshot.entries = deque(state.completed.entries)

    oldest_ms = (
        history_snapshot.entries[0].start_ms
        if history_snapshot.entries
        else now_ms
    )
    return history_snapshot.metrics_for_windows(
        now_ms=now_ms,
        windows_ms=LIVE_METRIC_WINDOWS_MS,
        slow_threshold_ms=slow_threshold_ms,
        data_start_ms=oldest_ms,
    )


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
            sparql = load_sparql_at(log_stream, start_line_offset)
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
