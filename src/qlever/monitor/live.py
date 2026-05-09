from __future__ import annotations

import os
import threading
import time
from collections import deque
from pathlib import Path

from rich.text import Text
from textual.containers import Container
from textual.widgets import DataTable, Static

from qlever.monitor.log_reader import (
    get_live_active_queries,
    get_metrics_history,
    parse_metric_line,
)
from qlever.monitor.metrics import (
    LABEL_WIDTH,
    MAX_WINDOW_S,
    WINDOWS,
    format_top_metrics,
    format_window_line,
)
from qlever.monitor.widgets import (
    QueryTable,
    SparqlPane,
    duration_sort_key,
)


class LiveScreen(Container):
    """Live view: tails the log and shows currently-active queries plus metrics."""

    BINDINGS = [
        ("c", "clear_detail", "Clear SPARQL"),
        ("y", "copy_sparql", "Copy SPARQL"),
        ("f", "freeze", "Freeze/Unfreeze table"),
    ]
    DEFAULT_CSS = """
    LiveScreen #metrics { padding-left: 1; }
    LiveScreen Static.window-row { padding-left: 1; }
    LiveScreen #window-1h { margin-bottom: 1; }
    """

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: float,
        warning_log: Path,
        system: str,
        is_stale: bool = False,
    ) -> None:
        """Initialize the live view and its per-session state."""
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        self.warning_log = warning_log
        self.system = system
        self.is_stale = is_stale
        # File handle and offset for the live reader. Set in on_mount
        # before Phase A so the reader resumes at the same EOF Phase A
        # reads up to.
        self.live_log = None
        self.live_offset = 0
        # Shutdown signal for the live loop and its partial-line buffer.
        self.stopping = threading.Event()
        self.pending = b""
        # Source of truth for currently-running queries: qid -> start event.
        # The DataTable is a view of this.
        self.active_queries = {}
        # (ended_at_ms, duration_ms) per finished query, for percentile metrics.
        self.finish_events = deque()
        # Metrics state seeded by Phase B; metrics_loaded gates rendering.
        self.eof_ms = 0
        self.coverage_start_ms = 0
        self.metrics_loaded = False

    def compose(self):
        """Build the live-view widget tree."""
        if self.is_stale:
            yield Static(
                "Log is stale: no recent server activity. Use the Historic tab.",
                id="stale-notice",
            )
            return
        yield Static("", id="metrics")
        for key in ["5m", "15m", "1h"]:
            yield Static("", id=f"window-{key}", classes="window-row")
        yield QueryTable()
        yield Static("", id="status")
        yield SparqlPane()

    def on_mount(self) -> None:
        """Open the live reader, seed Phase A, and start workers."""
        if self.is_stale:
            return
        # Placeholders shown until Phase B applies its results.
        self.query_one("#metrics", Static).update(
            Text.from_markup("[dim]loading metrics...[/dim]")
        )
        for key, _, _ in WINDOWS:
            self.query_one(f"#window-{key}", Static).update(
                Text.from_markup(
                    f"[bold yellow]{key:<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
                    f"[dim]loading metrics...[/dim]"
                )
            )

        # Open the live reader before Phase A so its recorded offset is
        # the same EOF Phase A reads up to.
        self.live_log = self.log_file.open("rb")
        self.live_log.seek(0, os.SEEK_END)
        self.live_offset = self.live_log.tell()

        self.seed_active_queries()
        self.run_worker(self.live_loop, thread=True)
        self.run_worker(self.seed_metrics_history, thread=True)
        self.set_interval(0.5, self.repaint)

    def seed_active_queries(self) -> None:
        """Phase A: blocking populate of currently-active queries."""
        for event in get_live_active_queries(self.log_file, self.timeout):
            self.active_queries[event["query-id"]] = event
        now_ms = int(time.time() * 1000)
        table = self.query_one(QueryTable)
        for qid, event in self.active_queries.items():
            table.add_query_row(qid, event, now_ms)

    def live_loop(self) -> None:
        """Phase C: read appended bytes and dispatch METRIC events to the UI."""
        while not self.stopping.is_set():
            chunk = self.live_log.read()
            if not chunk:
                # Wait briefly, but wake immediately on shutdown.
                self.stopping.wait(0.2)
                continue
            self.pending += chunk
            while True:
                nl = self.pending.find(b"\n")
                if nl < 0:
                    break
                line = self.pending[: nl + 1]
                self.pending = self.pending[nl + 1 :]
                event = parse_metric_line(line)
                if event is None:
                    continue
                kind = event.get("event")
                if kind == "start":
                    self.app.call_from_thread(self.handle_start_event, event)
                elif kind == "end":
                    self.app.call_from_thread(self.handle_end_event, event)

    def handle_start_event(self, event: dict) -> None:
        """Record a started query and add its row."""
        qid = event["query-id"]
        if qid in self.active_queries:
            return
        self.active_queries[qid] = event
        now_ms = int(time.time() * 1000)
        self.query_one(QueryTable).add_query_row(qid, event, now_ms)

    def handle_end_event(self, event: dict) -> None:
        """Record a finished query, append to finish_events, drop its row."""
        qid = event["query-id"]
        start = self.active_queries.pop(qid, None)
        if start is not None:
            ended_at = event["ended-at"]
            duration_ms = ended_at - start["started-at"]
            self.finish_events.append((ended_at, duration_ms))
        table = self.query_one(QueryTable)
        existing = {rk.value for rk in table.rows}
        if qid in existing:
            table.remove_row(qid)

    def seed_metrics_history(self) -> None:
        """Phase B: read completed-query history from the log on a worker thread."""
        eof_ms, coverage_start_ms, finish_events = get_metrics_history(
            self.log_file, MAX_WINDOW_S
        )
        self.app.call_from_thread(
            self.apply_metrics_history,
            eof_ms,
            coverage_start_ms,
            finish_events,
        )

    def apply_metrics_history(
        self,
        eof_ms: int,
        coverage_start_ms: int,
        finish_events: list[tuple[int, int]],
    ) -> None:
        """Merge Phase B history into self.finish_events and unblock rendering."""
        self.eof_ms = eof_ms
        self.coverage_start_ms = coverage_start_ms
        # History entries are older than anything the live tailer captured
        # during Phase B, so prepend them in reverse order.
        for entry in reversed(finish_events):
            self.finish_events.appendleft(entry)
        self.metrics_loaded = True
        self.repaint()

    def repaint(self) -> None:
        """Refresh active-query duration cells and the metrics rows."""
        now_ms = int(time.time() * 1000)
        self.refresh_duration_cells(now_ms)
        # Drop entries older than 1h so windowed sums stay bounded.
        cutoff_ms = now_ms - MAX_WINDOW_S * 1000
        while self.finish_events and self.finish_events[0][0] < cutoff_ms:
            self.finish_events.popleft()
        if not self.metrics_loaded:
            return
        coverage_s = (now_ms - self.coverage_start_ms) / 1000
        slow_count = sum(
            1 for _, d in self.finish_events if d >= self.warn_after * 1000
        )
        self.query_one("#metrics", Static).update(
            format_top_metrics(
                active=len(self.active_queries),
                slow_count=slow_count,
                warn_after=self.warn_after,
                coverage_s=coverage_s,
            )
        )
        for key, label, window_s in WINDOWS:
            self.query_one(f"#window-{key}", Static).update(
                format_window_line(
                    label=label,
                    window_s=window_s,
                    coverage_s=coverage_s,
                    finish_events=self.finish_events,
                    warn_after=self.warn_after,
                    now_ms=now_ms,
                )
            )

    def refresh_duration_cells(self, now_ms: int) -> None:
        """Update each row's duration cell and resort by duration descending."""
        table = self.query_one(QueryTable)
        for row_key in table.rows:
            qid = row_key.value
            info = self.active_queries.get(qid)
            if info is None:
                continue
            duration_s = (now_ms - info["started-at"]) / 1000
            if duration_s >= self.warn_after:
                cell = Text.from_markup(
                    f"[red]{duration_s:.1f}s[/red]", justify="right"
                )
            else:
                cell = Text(f"{duration_s:.1f}s", justify="right")
            table.update_cell(row_key, "duration", cell)
        table.sort("duration", key=duration_sort_key, reverse=True)

    def on_data_table_row_selected(
        self, event: DataTable.RowSelected
    ) -> None:
        """Cache and render the selected query's full SPARQL."""
        qid = event.row_key.value if event.row_key else None
        if qid is None:
            return
        pane = self.query_one(SparqlPane)
        if qid == pane.selected_qid:
            return
        start = self.active_queries.get(qid)
        if start is None:
            return
        pane.show(qid, start.get("query", ""))

    def action_clear_detail(self) -> None:
        """Clear the detail pane and restore the hint."""
        self.query_one(SparqlPane).clear()

    def action_copy_sparql(self) -> None:
        """Copy the selected SPARQL to the system clipboard."""
        pane = self.query_one(SparqlPane)
        if not pane.has_selection:
            self.app.notify("No query selected", severity="warning")
            return
        ok = pane.copy()
        self.app.notify("Copied" if ok else "Copy failed")

    def on_unmount(self) -> None:
        """Stop the live loop and release the live reader."""
        self.stopping.set()
        if self.live_log is not None:
            self.live_log.close()
