from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

from rich.text import Text
from textual.containers import Container
from textual.widgets import DataTable, Static

from qlever.monitor.log_reader import get_historic_window
from qlever.monitor.metrics import format_duration_ms, nearest_rank_percentile
from qlever.monitor.widgets import (
    QID_COL_WIDTH,
    SPARQL_PREVIEW_LEN,
    QueryTable,
    SparqlPane,
    truncate,
)

MAX_TABLE_ROWS = 200

# (label, seconds). 'All' is the special whole-log preset; its second item is None.
WINDOW_PRESETS: list[tuple[str, int | None]] = [
    ("5m", 300),
    ("15m", 900),
    ("1h", 3600),
    ("6h", 21600),
    ("24h", 86400),
    ("All", None),
]
DEFAULT_PRESET_IDX = 2  # 1h


def format_window_duration(start_dt: datetime, end_dt: datetime) -> str:
    """Render the window length as 'Xh Ym' or 'Ym'."""
    total_s = int((end_dt - start_dt).total_seconds())
    hours, rem = divmod(total_s, 3600)
    minutes = rem // 60
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


class HistoricScreen(Container):
    """Historic view: scan a chosen time window in the log."""

    BINDINGS = [
        ("c", "clear_detail", "Clear SPARQL"),
        ("y", "copy_sparql", "Copy SPARQL"),
        ("w", "cycle_window", "Window size"),
        ("[", "slide_back", "Earlier"),
        ("]", "slide_forward", "Later"),
        ("g", "jump_start", "Log start"),
        ("G", "jump_end", "Log end"),
    ]
    DEFAULT_CSS = """
    HistoricScreen #range-bar { padding-left: 1; }
    HistoricScreen #historic-metrics { padding-left: 1; }
    HistoricScreen #table-info { padding-left: 1; margin-bottom: 1; }
    """

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: float,
        log_start_dt: datetime,
        log_end_dt: datetime,
    ) -> None:
        """Initialize the historic view with the default last-1h window."""
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        self.log_start_dt = log_start_dt
        self.log_end_dt = log_end_dt
        # Anchor at log end with the default preset; clamped to log span.
        self.preset_idx = DEFAULT_PRESET_IDX
        self.window_end_dt = log_end_dt
        self.window_start_dt = log_end_dt
        self.set_window_from_end(log_end_dt)
        # qid -> start event, used for SPARQL lookup on row selection.
        self.events_by_qid: dict[str, dict] = {}

    def current_preset_seconds(self) -> int | None:
        """Return the duration of the current preset, or None for 'All'."""
        return WINDOW_PRESETS[self.preset_idx][1]

    def set_window_from_end(self, end_dt: datetime) -> None:
        """Anchor the window at end_dt and size it from the current preset, clamped to the log."""
        size_s = self.current_preset_seconds()
        if size_s is None:
            self.window_start_dt = self.log_start_dt
            self.window_end_dt = self.log_end_dt
            return
        end = min(end_dt, self.log_end_dt)
        start = max(end - timedelta(seconds=size_s), self.log_start_dt)
        self.window_start_dt = start
        self.window_end_dt = end

    def set_window_from_start(self, start_dt: datetime) -> None:
        """Anchor the window at start_dt and size it from the current preset, clamped to the log."""
        size_s = self.current_preset_seconds()
        if size_s is None:
            self.window_start_dt = self.log_start_dt
            self.window_end_dt = self.log_end_dt
            return
        start = max(start_dt, self.log_start_dt)
        end = min(start + timedelta(seconds=size_s), self.log_end_dt)
        self.window_start_dt = start
        self.window_end_dt = end

    def compose(self):
        """Build the historic-view widget tree."""
        yield Static("", id="range-bar")
        yield Static("", id="historic-metrics")
        yield Static("", id="table-info")
        yield QueryTable()
        yield SparqlPane()

    def on_mount(self) -> None:
        """Render initial range bar and kick off the window load."""
        self.reload()

    def reload(self) -> None:
        """Refresh the range bar, clear stale data, and kick off a new load worker."""
        self.update_range_bar()
        self.query_one(QueryTable).clear()
        self.query_one("#historic-metrics", Static).update(
            Text.from_markup("[dim]loading...[/dim]")
        )
        self.query_one("#table-info", Static).update("")
        self.run_worker(self.load_window, thread=True, exclusive=True)

    def update_range_bar(self) -> None:
        """Render the From/To/Window line at the top."""
        start_str = self.window_start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = self.window_end_dt.strftime("%Y-%m-%d %H:%M:%S")
        preset_label = WINDOW_PRESETS[self.preset_idx][0]
        actual = format_window_duration(
            self.window_start_dt, self.window_end_dt
        )
        # Show the preset label, plus the clamped span when it differs from
        # the preset's nominal size (e.g. '24h' on a 52m log).
        size_s = self.current_preset_seconds()
        if size_s is None:
            window_str = preset_label
        else:
            actual_s = int(
                (self.window_end_dt - self.window_start_dt).total_seconds()
            )
            if abs(actual_s - size_s) <= 1:
                window_str = preset_label
            else:
                window_str = f"{preset_label} (clamped: {actual})"
        self.query_one("#range-bar", Static).update(
            Text.from_markup(
                f"[bold]From:[/] {start_str}   "
                f"[bold]To:[/] {end_str}   "
                f"[bold]Window:[/] {window_str}"
            )
        )

    def load_window(self) -> None:
        """Worker: scan the current window, then post results to the UI."""
        active, completed, last_scanned_ms = get_historic_window(
            self.log_file,
            self.window_start_dt,
            self.window_end_dt,
            self.timeout,
        )
        self.app.call_from_thread(
            self.apply_window_data, active, completed, last_scanned_ms
        )

    def apply_window_data(
        self,
        active: list[dict],
        completed: list[tuple[dict, dict]],
        last_scanned_ms: int,
    ) -> None:
        """Populate table and metrics from the loaded window's events."""
        table = self.query_one(QueryTable)
        table.clear()
        self.events_by_qid.clear()

        # For active rows, the scan verified no end up to the timestamp of
        # the last event it read. Use that as the lower bound on runtime.
        active_floor_ms = last_scanned_ms

        # Combine active and completed into a single sortable list.
        # For active rows, "duration" is the lower bound described above;
        # the table renders those dim with a '>=' prefix.
        rows = []
        for start, end in completed:
            qid = start["query-id"]
            self.events_by_qid[qid] = start
            duration_ms = end["ended-at"] - start["started-at"]
            rows.append((qid, duration_ms, start, False))
        for start in active:
            qid = start["query-id"]
            self.events_by_qid[qid] = start
            duration_ms = active_floor_ms - start["started-at"]
            rows.append((qid, duration_ms, start, True))

        rows.sort(key=lambda r: r[1], reverse=True)
        capped = rows[:MAX_TABLE_ROWS]
        for qid, duration_ms, start, is_active in capped:
            duration_s = duration_ms / 1000
            sparql = truncate(
                re.sub(r"\s+", " ", start.get("query", "")).strip(),
                SPARQL_PREVIEW_LEN,
            )
            duration_cell = build_duration_cell(duration_s, is_active)
            table.add_row(
                truncate(qid, QID_COL_WIDTH),
                duration_cell,
                sparql,
                key=qid,
            )

        self.update_metrics(active, completed, active_floor_ms)
        self.update_table_info(len(rows))

    def update_metrics(
        self,
        active: list[dict],
        completed: list[tuple[dict, dict]],
        active_floor_ms: int,
    ) -> None:
        """Render the one-line metrics summary for the current window."""
        completed_durations_ms = sorted(
            end["ended-at"] - start["started-at"] for start, end in completed
        )
        warn_after_ms = self.warn_after * 1000
        slow_completed = sum(
            1 for d in completed_durations_ms if d >= warn_after_ms
        )
        # Active rows count as slow when their lower bound on runtime
        # already exceeds the threshold.
        slow_active = sum(
            1
            for s in active
            if active_floor_ms - s["started-at"] >= warn_after_ms
        )
        slow_count = slow_completed + slow_active
        if completed_durations_ms:
            p50 = format_duration_ms(
                nearest_rank_percentile(completed_durations_ms, 0.50)
            )
            p95 = format_duration_ms(
                nearest_rank_percentile(completed_durations_ms, 0.95)
            )
        else:
            p50 = p95 = "—"
        threshold = int(self.warn_after)
        total = len(active) + len(completed)
        self.query_one("#historic-metrics", Static).update(
            Text.from_markup(
                f"Queries active during window: [cyan]{total}[/cyan]   "
                f"p50: [cyan]{p50}[/cyan]   "
                f"p95: [cyan]{p95}[/cyan]   "
                f"Slow (>={threshold}s): [yellow]{slow_count}[/yellow]"
            )
        )

    def update_table_info(self, total: int) -> None:
        """Render the always-visible table-info hint, with cap line when needed."""
        base = (
            "Sorted by duration desc. Active rows (dim, '>= Xs') were still "
            "running at the last log line scanned; their duration is a "
            "lower bound, not the final runtime."
        )
        if total > MAX_TABLE_ROWS:
            base += f"  Showing {MAX_TABLE_ROWS} slowest of {total}."
        self.query_one("#table-info", Static).update(
            Text.from_markup(f"[dim]{base}[/dim]")
        )

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
        start = self.events_by_qid.get(qid)
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

    def action_cycle_window(self) -> None:
        """Cycle to the next window-size preset and reload."""
        self.preset_idx = (self.preset_idx + 1) % len(WINDOW_PRESETS)
        self.set_window_from_end(self.window_end_dt)
        self.reload()

    def action_slide_back(self) -> None:
        """Shift the window earlier by one window-width and reload."""
        size_s = self.current_preset_seconds()
        if size_s is None:
            return
        self.set_window_from_end(
            self.window_end_dt - timedelta(seconds=size_s)
        )
        self.reload()

    def action_slide_forward(self) -> None:
        """Shift the window later by one window-width and reload."""
        size_s = self.current_preset_seconds()
        if size_s is None:
            return
        self.set_window_from_end(
            self.window_end_dt + timedelta(seconds=size_s)
        )
        self.reload()

    def action_jump_start(self) -> None:
        """Anchor the window at the log start and reload."""
        self.set_window_from_start(self.log_start_dt)
        self.reload()

    def action_jump_end(self) -> None:
        """Anchor the window at the log end and reload."""
        self.set_window_from_end(self.log_end_dt)
        self.reload()


def build_duration_cell(duration_s: float, is_active: bool) -> Text:
    """Return the rendered duration cell, dim cyan '>= Xs' for active or default 'Xs' for completed."""
    if is_active:
        return Text(f">{duration_s:.1f}s", justify="right", style="dim cyan")
    return Text(f"{duration_s:.1f}s", justify="right")
