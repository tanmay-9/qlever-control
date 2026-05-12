from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from textual.app import ComposeResult
from textual.widgets import Static

from qlever.monitor.log_reader import (
    get_historic_window,
    read_log_bounds,
)
from qlever.monitor.metrics import (
    format_duration_ms,
    format_metrics_line,
    format_window_duration,
    nearest_rank_percentile,
)
from qlever.monitor.views.base_view import BaseView
from qlever.monitor.widgets.metrics_pane import (
    HistoricMetricsPane,
    MetricsPane,
)
from qlever.monitor.widgets.query_table import (
    HistoricQueryTable,
    QueryTable,
)
from qlever.monitor.widgets.sparql_pane import SparqlPane
from qlever.monitor.widgets.timeline import Timeline
from qlever.monitor.widgets.window_controls import PRESETS, WindowControls

PRESET_SECONDS = dict(PRESETS)

DEFAULT_WINDOW = timedelta(minutes=5)
DEFAULT_PRESET_KEY = "5m"
MAX_ROWS = 200


class HistoricView(BaseView):
    """Static snapshot view: scan the log for an explicit [from, to] window."""

    BINDINGS = BaseView.BINDINGS + [
        ("w", "cycle_window(1)", "Window +"),
        ("W", "cycle_window(-1)", "Window -"),
        ("m", "cycle_mode", "Mode"),
        ("shift+left", "page(-1)", "Page left"),
        ("shift+right", "page(1)", "Page right"),
        ("g", "snap_start", "Snap start"),
        ("G", "snap_end", "Snap end"),
    ]

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: int,
    ) -> None:
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        self.last_size = self.log_file.stat().st_size
        log_start, log_end = read_log_bounds(log_file)
        self.log_start_dt = log_start
        self.log_end_dt = log_end
        # Pick a default preset that actually fits the log span; otherwise
        # the pill would advertise a size larger than what's selectable.
        if log_end - log_start >= DEFAULT_WINDOW:
            self.default_preset_key = DEFAULT_PRESET_KEY
            self.window_from_dt = log_end - DEFAULT_WINDOW
        else:
            self.default_preset_key = "all"
            self.window_from_dt = log_start
        self.window_to_dt = log_end
        self.events_by_qid = {}
        # Generation guard: discard scan results queued before a newer
        # apply_window() invalidated them.
        self.scan_generation = 0
        # True when the window is parked at log_end and should grow with it.
        # Toggled automatically by apply_window based on whether to_dt hits
        # the current log_end.
        self.follow_tail = True

    def compose(self) -> ComposeResult:
        yield WindowControls()
        yield Timeline()
        yield self.build_metrics_pane()
        yield self.build_query_table()
        yield Static("", id="historic-help")
        yield SparqlPane()

    def build_metrics_pane(self) -> MetricsPane:
        return HistoricMetricsPane()

    def build_query_table(self) -> QueryTable:
        return HistoricQueryTable(warn_after=self.warn_after)

    def get_query_text(self, qid: str) -> str | None:
        event = self.events_by_qid.get(qid)
        return event.get("query") if event else None
    
    def refresh_log_end(self) -> None:
        size = self.log_file.stat().st_size
        if size == self.last_size:
            return
        self.last_size = size

        log_start, log_end = read_log_bounds(self.log_file)
        self.log_start_dt = log_start
        self.log_end_dt = log_end
        if self.follow_tail and self.window_to_dt is not None:
            # Slide the window forward to keep its right edge on the new
            # log_end while preserving its size. Triggers a re-scan.
            size = self.window_to_dt - self.window_from_dt
            new_to = self.log_end_dt
            new_from = max(self.log_start_dt, new_to - size)
            self.apply_window(new_from, new_to)
        else:
            # Window stays anchored on its absolute timestamps; only the
            # bar's right edge advances.
            self.query_one(Timeline).set_state(
                self.log_start_dt,
                self.log_end_dt,
                self.window_from_dt,
                self.window_to_dt,
            )

    def on_mount(self) -> None:
        self.query_one(WindowControls).preset_key = self.default_preset_key
        self.apply_window(self.window_from_dt, self.window_to_dt)
        self.set_interval(5, self.refresh_log_end)

    def action_cycle_window(self, delta: int) -> None:
        controls = self.query_one(WindowControls)
        keys = controls.available_presets()
        try:
            idx = keys.index(controls.preset_key)
        except ValueError:
            idx = 0
        new_key = keys[(idx + delta) % len(keys)]
        if new_key == "all":
            from_dt = self.log_start_dt
            to_dt = self.log_end_dt
        else:
            # Pivot around the current right edge so a resize doesn't yank
            # the user back to log_end; follow_tail wins when on the tail.
            new_size = timedelta(seconds=PRESET_SECONDS[new_key])
            anchor_to = (
                self.log_end_dt if self.follow_tail else self.window_to_dt
            )
            from_dt = anchor_to - new_size
            if from_dt < self.log_start_dt:
                from_dt = self.log_start_dt
                to_dt = min(self.log_end_dt, from_dt + new_size)
            else:
                to_dt = anchor_to
        controls.preset_key = new_key
        self.apply_window(from_dt, to_dt)

    def action_page(self, direction: int) -> None:
        if self.window_from_dt is None:
            return
        size = self.window_to_dt - self.window_from_dt
        step = size / 2
        new_from = self.window_from_dt + step * direction
        new_to = self.window_to_dt + step * direction
        # Clamp into [log_start, log_end] while preserving window size, so
        # paging into a boundary lands flush against it rather than shrinking.
        if new_from < self.log_start_dt:
            new_from = self.log_start_dt
            new_to = new_from + size
        if new_to > self.log_end_dt:
            new_to = self.log_end_dt
            new_from = max(self.log_start_dt, new_to - size)
        if (new_from, new_to) == (self.window_from_dt, self.window_to_dt):
            return
        self.apply_window(new_from, new_to)

    def action_snap_start(self) -> None:
        if self.window_from_dt is None:
            return
        size = self.window_to_dt - self.window_from_dt
        new_from = self.log_start_dt
        new_to = min(self.log_end_dt, new_from + size)
        self.apply_window(new_from, new_to)

    def action_snap_end(self) -> None:
        if self.window_from_dt is None:
            return
        size = self.window_to_dt - self.window_from_dt
        new_to = self.log_end_dt
        new_from = max(self.log_start_dt, new_to - size)
        self.apply_window(new_from, new_to)

    def action_cycle_mode(self) -> None:
        # Dummy: rotates the chip highlight; no effect on the scan yet.
        controls = self.query_one(WindowControls)
        controls.mode_index = (controls.mode_index + 1) % 3
        controls.repaint()

    def apply_window(self, from_dt: datetime, to_dt: datetime) -> None:
        # Single source of truth for "new window selected": mutates state,
        # pushes it to the controls + timeline, and kicks the scan worker.
        self.window_from_dt = from_dt
        self.window_to_dt = to_dt
        self.follow_tail = to_dt == self.log_end_dt
        self.scan_generation += 1
        generation = self.scan_generation
        self.query_one(Timeline).set_state(
            self.log_start_dt, self.log_end_dt, from_dt, to_dt
        )
        self.query_one(WindowControls).set_state(
            self.log_start_dt, self.log_end_dt, from_dt, to_dt
        )
        self.query_one(HistoricQueryTable).clear()
        self.events_by_qid.clear()
        self.query_one(HistoricMetricsPane).set_summary("Loading…")
        self.query_one("#historic-help", Static).update("")
        self.run_worker(
            lambda: self.scan_window(from_dt, to_dt, generation),
            thread=True,
            exclusive=True,
        )

    def scan_window(
        self, from_dt: datetime, to_dt: datetime, generation: int
    ) -> None:
        active, completed, last_scanned_ms = get_historic_window(
            self.log_file, from_dt, to_dt, self.timeout
        )
        self.app.call_from_thread(
            self.apply_window_data,
            active,
            completed,
            last_scanned_ms,
            generation,
        )

    def apply_window_data(
        self,
        active: list[dict],
        completed: list[tuple[dict, dict]],
        last_scanned_ms: int,
        generation: int,
    ) -> None:
        # Drop results queued before a newer apply_window() invalidated them.
        # Without this, fast cycling can land stale rows in the freshly-cleared
        # table and trigger DuplicateKey on qid collisions.
        if generation != self.scan_generation:
            return
        table = self.query_one(HistoricQueryTable)
        rows = []
        for start, end in completed:
            qid = start["query-id"]
            self.events_by_qid[qid] = start
            rows.append(
                (qid, end["ended-at"] - start["started-at"], False, start)
            )
        for start in active:
            qid = start["query-id"]
            self.events_by_qid[qid] = start
            rows.append(
                (qid, last_scanned_ms - start["started-at"], True, start)
            )
        rows.sort(key=lambda r: r[1], reverse=True)
        for qid, duration_ms, is_active, start in rows[:MAX_ROWS]:
            table.add_query_row(
                qid,
                start.get("query", ""),
                duration_ms / 1000,
                is_active=is_active,
            )

        completed_durations = sorted(d for _, d, is_a, _ in rows if not is_a)
        warn_ms = self.warn_after * 1000
        slow_completed = sum(1 for d in completed_durations if d >= warn_ms)
        slow_active = sum(1 for _, d, is_a, _ in rows if is_a and d >= warn_ms)
        if completed_durations:
            p50 = format_duration_ms(
                nearest_rank_percentile(completed_durations, 0.50)
            )
            p95 = format_duration_ms(
                nearest_rank_percentile(completed_durations, 0.95)
            )
        else:
            p50 = p95 = "—"
        self.query_one(HistoricMetricsPane).set_summary(
            format_metrics_line(
                label=format_window_duration(
                    self.window_from_dt, self.window_to_dt
                ),
                completed_count=len(completed_durations),
                p50=p50,
                p95=p95,
                slow_count=slow_completed + slow_active,
            )
        )
        total = len(rows)
        shown = min(total, MAX_ROWS)
        self.query_one("#historic-help", Static).update(
            f"Showing {shown} of {total} queries (sorted by duration desc)."
        )
