"""Data layer for the Historic screen.

Reads one time window of the log into a cached `WindowData`, filters
that cache by display mode, and maps the survivors into the
`models.py` dataclasses the screen renders. A window change reruns
the scan; a mode change reuses the cache.
"""

from dataclasses import replace
from pathlib import Path
from typing import NamedTuple

from qlever.monitor.log_reader import (
    load_sparql_at,
    offset_for_ts,
    pair_start_end_events,
    scan_range,
)
from qlever.monitor.metrics import MetricsSnapshot, metrics_for_ranges
from qlever.monitor.models import (
    ControlsState,
    HistoricQueryRow,
    MetricsCounts,
)

# Used as the duration of orphaned queries. Negative so they sort last.
DURATION_UNKNOWN = -1


class LoggedQuery(NamedTuple):
    """One SPARQL query observed in the log over the current window.

    `end_ms` is `None` when the query started but has not ended yet,
    either because it is still running or the server crashed before
    writing the end event. `status` carries the raw end status, or
    `"running"` for a still-open survivor. `start_line_offset` is the
    byte offset of the start line, used to read the query's text only
    once it is about to be displayed.
    """

    start_ms: int
    end_ms: int | None
    status: str
    start_line_offset: int


class WindowData(NamedTuple):
    """Cached scan of one time window, reused across mode flips.

    `queries` is every `LoggedQuery` overlapping the window in any
    mode (the ACTIVE superset), so a mode change is a pure in-memory
    filter. Each query carries its `start_line_offset` but not its
    text; the text is read for the visible rows only. `metrics` is
    computed over completions whose `end_ms` lies inside the window,
    so it is the same for every mode.
    """

    queries: list[LoggedQuery]
    metrics: MetricsSnapshot


def read_window(
    log_path: Path,
    window_start_ms: int,
    window_end_ms: int,
    pad_ms: int,
    slow_threshold_ms: int,
    log_end_ms: int,
    now_ms: int,
) -> WindowData:
    """Scan one time window of the log into a `WindowData` snapshot.

    The byte range scanned is the window padded by `pad_ms` on each
    side so both events of a query straddling the window edge are
    recovered. Pairs that lie entirely inside the pad are dropped, so
    the cache holds only rows a mode predicate could keep. The query
    text is not read here; `load_query_details_for_rows` reads it for
    the visible rows. Metrics count completions whose `end_ms` lies
    inside `[window_start_ms, window_end_ms]`.

    A still-open query is `"running"` only if its start is within
    `pad_ms` of `log_end_ms` and the log itself is fresh
    (`now_ms - log_end_ms <= pad_ms`); otherwise `"orphaned"`.
    """
    with log_path.open("rb") as log_stream:
        file_size = log_path.stat().st_size
        lo_offset = offset_for_ts(
            log_stream, window_start_ms - pad_ms, file_size
        )
        hi_bound = offset_for_ts(log_stream, window_end_ms + pad_ms, file_size)
        events = scan_range(log_stream, lo_offset, hi_bound)
        completed, still_open = pair_start_end_events(events)

        queries = []
        for pair in completed:
            if pair.start_ms > window_end_ms or pair.end_ms < window_start_ms:
                continue
            queries.append(
                LoggedQuery(
                    start_ms=pair.start_ms,
                    end_ms=pair.end_ms,
                    status=pair.status,
                    start_line_offset=pair.start_line_offset,
                )
            )
        log_is_fresh = now_ms - log_end_ms <= pad_ms
        running_cutoff_ms = log_end_ms - pad_ms
        for start_ms, start_line_offset in still_open.values():
            if start_ms > window_end_ms:
                continue
            status = (
                "running"
                if log_is_fresh and start_ms >= running_cutoff_ms
                else "orphaned"
            )
            queries.append(
                LoggedQuery(
                    start_ms=start_ms,
                    end_ms=None,
                    status=status,
                    start_line_offset=start_line_offset,
                )
            )

    metrics = metrics_for_ranges(
        completed, [(window_start_ms, window_end_ms)], slow_threshold_ms
    )[0]
    return WindowData(queries=queries, metrics=metrics)


def filter_queries(
    window_data: WindowData,
    mode: str,
    window_start_ms: int,
    window_end_ms: int,
) -> list[LoggedQuery]:
    """Select queries from a cached `WindowData` for the given mode.

    The cache is already the ACTIVE set (every query overlaps the
    window), so ACTIVE is the fallthrough that returns it unchanged.
    STARTS narrows to queries that began inside the window; ENDS
    narrows to queries that finished inside it. Still-running queries
    (`end_ms is None`) can satisfy STARTS but never ENDS.
    """
    if mode == "STARTS":
        return [
            query
            for query in window_data.queries
            if window_start_ms <= query.start_ms <= window_end_ms
        ]
    if mode == "ENDS":
        return [
            query
            for query in window_data.queries
            if query.end_ms is not None
            and window_start_ms <= query.end_ms <= window_end_ms
        ]
    return window_data.queries


def display_duration_ms(query: LoggedQuery, log_end_ms: int) -> int:
    """Duration to show for a query row; DURATION_UNKNOWN for orphans."""
    if query.status == "orphaned":
        return DURATION_UNKNOWN
    return (query.end_ms or log_end_ms) - query.start_ms


def render_window(
    window_data: WindowData,
    controls: ControlsState,
    log_end_ms: int,
) -> tuple[list[HistoricQueryRow], MetricsCounts]:
    """Map a cached `WindowData` into the rows and metrics the UI consumes.

    Runs `filter_queries` for the current mode and translates each
    surviving `LoggedQuery` into a `HistoricQueryRow`. `duration_ms`
    is measured against `log_end_ms` for still-running queries,
    against the recorded `end_ms` for completed ones, and reported as
    DURATION_UNKNOWN for crash orphans. The rows carry empty text;
    `load_query_details_for_rows` fills it for the visible slice.
    Metrics come straight from `window_data.metrics`, labelled with
    the current window size.
    """
    queries = filter_queries(
        window_data, controls.mode, controls.start_ms, controls.end_ms
    )
    historic_rows = [
        HistoricQueryRow(
            qid="",
            start_line_offset=query.start_line_offset,
            started_at_ms=query.start_ms,
            duration_ms=display_duration_ms(query, log_end_ms),
            status=query.status,
            sparql="",
            client_ip="",
        )
        for query in queries
    ]
    metrics = MetricsCounts(
        label=controls.window_size, **window_data.metrics._asdict()
    )
    return historic_rows, metrics


def load_query_details_for_rows(
    log_path: Path,
    rows: list[HistoricQueryRow],
    query_details_cache: dict[int, tuple[str, str, str]],
) -> list[HistoricQueryRow]:
    """Fill the deferred start-line fields on the given rows.

    Each row's `qid`, `client_ip`, and SPARQL text live on its start
    line, which `read_window` did not read. Offsets already in
    `query_details_cache` are reused; the rest are read in one pass,
    opening the file only when something is missing. The cache is
    scoped to one window so a sort or mode change repaints from memory.
    Returns filled copies; the input rows are left unchanged.
    """
    missing = [
        row.start_line_offset
        for row in rows
        if row.start_line_offset not in query_details_cache
    ]
    if missing:
        with log_path.open("rb") as log_stream:
            for offset in missing:
                query_details_cache[offset] = load_sparql_at(
                    log_stream, offset
                )
    filled_rows = []
    for row in rows:
        qid, client_ip, sparql = query_details_cache[row.start_line_offset]
        filled_rows.append(
            replace(row, qid=qid, client_ip=client_ip, sparql=sparql)
        )
    return filled_rows
