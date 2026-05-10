from __future__ import annotations

import os
import threading
import time
from collections import deque
from pathlib import Path

from textual.app import ComposeResult
from textual.widgets import Static

from qlever.monitor.log_reader import (
    get_live_active_queries,
    get_metrics_history,
    parse_metric_line,
)
from qlever.monitor.metrics import (
    MAX_WINDOW_S,
    WINDOWS,
    format_top_line,
    format_window_line,
)
from qlever.monitor.views.base_view import BaseView
from qlever.monitor.widgets.metrics_pane import (
    LiveMetricsPane,
    MetricsPane,
)
from qlever.monitor.widgets.query_table import QueryTable
from qlever.monitor.widgets.sparql_pane import SparqlPane


class LiveView(BaseView):
    """Live view: tails the log, shows currently-active queries + rolling metrics."""

    BINDINGS = [("f", "toggle_freeze", "Freeze/Unfreeze")]

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: int,
        warning_log: Path,
        repaint_interval: float,
    ) -> None:
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        self.warning_log = warning_log
        self.repaint_interval = repaint_interval
        self.active_queries = {}
        self.finish_events = deque()
        self.coverage_start_ms = 0
        self.metrics_loaded = False
        self.frozen = False
        self.live_log = None
        self.pending = b""
        self.stopping = threading.Event()

    def compose(self) -> ComposeResult:
        yield self.build_metrics_pane()
        yield self.build_query_table()
        yield Static("", id="freeze-banner")
        yield SparqlPane()

    def build_metrics_pane(self) -> MetricsPane:
        return LiveMetricsPane()

    def build_query_table(self) -> QueryTable:
        return QueryTable(warn_after=self.warn_after)

    def get_query_text(self, qid: str) -> str | None:
        event = self.active_queries.get(qid)
        return event.get("query") if event else None

    def on_mount(self) -> None:
        # Open the live reader BEFORE Phase A so its recorded offset is at
        # or before Phase A's EOF. The live tail may then re-emit events
        # Phase A already saw; handle_start_event dedupes by qid.
        self.live_log = self.log_file.open("rb")
        self.live_log.seek(0, os.SEEK_END)
        self.seed_active_queries()
        self.run_worker(self.live_loop, thread=True)
        self.run_worker(self.seed_metrics_history, thread=True)
        self.set_interval(self.repaint_interval, self.repaint)

    def on_unmount(self) -> None:
        self.stopping.set()
        if self.live_log is not None:
            self.live_log.close()

    def seed_active_queries(self) -> None:
        events = get_live_active_queries(self.log_file, self.timeout)
        for event in events:
            self.active_queries[event["query-id"]] = event
        now_ms = int(time.time() * 1000)
        table = self.query_one(QueryTable)
        for qid, event in self.active_queries.items():
            duration_s = (now_ms - event["started-at"]) / 1000
            table.add_query_row(qid, event.get("query", ""), duration_s)
        table.sort_by_duration()

    def live_loop(self) -> None:
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
        qid = event["query-id"]
        if qid in self.active_queries:
            return
        self.active_queries[qid] = event

    def handle_end_event(self, event: dict) -> None:
        qid = event["query-id"]
        start = self.active_queries.pop(qid, None)
        if start is not None:
            ended_at = event["ended-at"]
            duration_ms = ended_at - start["started-at"]
            self.finish_events.append((ended_at, duration_ms))

    def seed_metrics_history(self) -> None:
        _, coverage_start_ms, finish_events = get_metrics_history(
            self.log_file, MAX_WINDOW_S
        )
        self.app.call_from_thread(
            self.apply_metrics_history, coverage_start_ms, finish_events
        )

    def apply_metrics_history(
        self,
        coverage_start_ms: int,
        finish_events: list[tuple[int, int]],
    ) -> None:
        self.coverage_start_ms = coverage_start_ms
        # History entries are older than anything Phase C captured during
        # Phase B; prepend in reverse to preserve chronological order.
        for entry in reversed(finish_events):
            self.finish_events.appendleft(entry)
        self.metrics_loaded = True

    def repaint(self) -> None:
        if self.frozen:
            return
        table = self.query_one(QueryTable)
        table_qids = {rk.value for rk in table.rows}
        active_qids = set(self.active_queries.keys())

        for qid in table_qids - active_qids:
            table.remove_row(qid)

        now_ms = int(time.time() * 1000)
        for qid in active_qids - table_qids:
            event = self.active_queries[qid]
            duration_s = (now_ms - event["started-at"]) / 1000
            table.add_query_row(qid, event.get("query", ""), duration_s)

        for row_key in table.rows:
            qid = row_key.value
            event = self.active_queries[qid]
            duration_s = (now_ms - event["started-at"]) / 1000
            table.update_duration_cell(qid, duration_s)
        table.sort_by_duration()

        cutoff_ms = now_ms - MAX_WINDOW_S * 1000
        while self.finish_events and self.finish_events[0][0] < cutoff_ms:
            self.finish_events.popleft()

        pane = self.query_one(LiveMetricsPane)
        pane.set_top_line(format_top_line(active=len(self.active_queries)))
        if self.metrics_loaded:
            coverage_s = (now_ms - self.coverage_start_ms) / 1000
            for key, window_s in WINDOWS:
                pane.set_window_line(
                    key,
                    format_window_line(
                        label=key,
                        window_s=window_s,
                        coverage_s=coverage_s,
                        finish_events=self.finish_events,
                        warn_after=self.warn_after,
                        now_ms=now_ms,
                    ),
                )

    def action_toggle_freeze(self) -> None:
        self.frozen = not self.frozen
        banner = self.query_one("#freeze-banner", Static)
        banner.update("FROZEN — press 'f' to resume" if self.frozen else "")
