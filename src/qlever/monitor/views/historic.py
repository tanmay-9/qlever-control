from __future__ import annotations

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer, Static

from qlever.monitor.historic_data import (
    load_query_details_for_rows,
    read_window,
    render_window,
)
from qlever.monitor.live_data import current_ms
from qlever.monitor.metrics import EMPTY_FIELDS
from qlever.monitor.models import (
    ControlsState,
    HistoricQueryRow,
    MetricsCounts,
    SparqlContent,
    TimelineBounds,
)
from qlever.monitor.widgets.controls_row import HistoricControlsRow
from qlever.monitor.widgets.header_row import HeaderRow
from qlever.monitor.widgets.metrics_row import MetricsRow
from qlever.monitor.widgets.mode_picker import MODES, ModePicker
from qlever.monitor.widgets.nav_pill import NavPill
from qlever.monitor.widgets.query_table import HistoricQueryTable
from qlever.monitor.widgets.selected_window import SelectedWindow
from qlever.monitor.widgets.sparql_pane import SparqlPane
from qlever.monitor.widgets.timeline import Timeline
from qlever.monitor.widgets.window_stepper import (
    WindowStepper,
    available_presets,
    preset_ms,
)

TITLE = "QLever monitor-queries: Historic"

MODE_PHRASES = {
    "ACTIVE": "were active during",
    "STARTS": "started in",
    "ENDS": "ended in",
}

SORT_COLUMNS = ["Started", "Duration", "Status"]

SORT_KEYS = {
    "Started": lambda row: row.started_at_ms,
    "Duration": lambda row: row.duration_ms,
    "Status": lambda row: row.status,
}

# Each column's (descending, ascending) wording for the status line.
SORT_PHRASES = {
    "Started": ("newest first", "oldest first"),
    "Duration": ("longest first", "shortest first"),
    "Status": ("Z to A", "A to Z"),
}

# Sort the whole window but paint at most this many rows, so a huge
# window does not freeze the table.
MAX_VISIBLE_ROWS = 1000

# Collapse a fast window scrub into a single scan once the user settles.
RESCAN_DEBOUNCE_S = 0.1


class HistoricScreen(Screen, inherit_bindings=False):
    """Historic view: shows active queries parsed from the log over a time window."""

    BINDINGS = [
        Binding("tab", "app.swap_screen", "<Live", priority=True),
        Binding("w", "cycle_window", "Window size"),
        Binding("m", "cycle_mode", "Mode"),
        Binding(
            "shift+left",
            "shift_earlier",
            "Shift window",
            key_display="⇧ ←→",
        ),
        Binding("shift+right", "shift_later", "Shift window", show=False),
        Binding("g", "snap_start", "Window start/end", key_display="g/G"),
        Binding("G", "snap_end", "Window end", show=False),
        Binding(
            "less_than_sign",
            "sort_prev_column",
            "Sort column",
            key_display="< >",
        ),
        Binding("greater_than_sign", "sort_next_column", "", show=False),
        Binding("i", "invert_sort", "Invert sort"),
        Binding("ctrl+c,super+c", "screen.copy_text", "Copy selection"),
    ]

    def compose(self) -> ComposeResult:
        self.log_start_ms = self.app.log_start_ms
        self.log_end_ms = self.read_log_end()
        # Presets longer than the log span are pointless; `all` covers it.
        self.available_presets = available_presets(
            self.log_end_ms - self.log_start_ms
        )
        self.window_size = self.available_presets[0]
        self.mode = "ACTIVE"
        self.sort_column = "Duration"
        self.sort_reverse = True
        width = preset_ms(self.window_size)
        self.window_start_ms = (
            self.log_start_ms if width is None else self.log_end_ms - width
        )
        self.window_end_ms = self.log_end_ms
        self.window_queries = None
        self.all_rows = []
        self.query_details_cache = {}
        self.rescan_timer = None
        self.cached_window = None
        controls = ControlsState(
            window_size=self.window_size,
            mode=self.mode,
            start_ms=self.window_start_ms,
            end_ms=self.window_end_ms,
        )
        bounds = TimelineBounds(
            log_start_ms=self.log_start_ms,
            log_end_ms=self.log_end_ms,
            window_start_ms=self.window_start_ms,
            window_end_ms=self.window_end_ms,
        )

        yield HeaderRow(
            left=NavPill("< Live", target="live"),
            center=Static(TITLE),
        )
        yield HistoricControlsRow(controls)
        yield Timeline(bounds)
        yield MetricsRow(
            [MetricsCounts(label=self.window_size, **EMPTY_FIELDS)],
            self.app.slow_threshold,
        )
        yield HistoricQueryTable([])
        yield Static("", id="table-status")
        yield SparqlPane()
        yield Footer(show_command_palette=False, compact=True)

    def on_screen_resume(self) -> None:
        """Catch up on log growth, then push state and rescan."""
        self.log_end_ms = self.read_log_end()
        self.available_presets = available_presets(
            self.log_end_ms - self.log_start_ms
        )
        self.clamp_window()
        self.refresh_view(rescan=True)

    def read_log_end(self) -> int:
        """Return the freshest log timestamp the tailer has seen."""
        state = self.app.live_state
        with state.lock:
            return state.latest_event_ms

    def show_loading_state(self) -> None:
        """Blank the table and metrics row while a rescan is in flight."""
        self.query_one(HistoricQueryTable).set_rows([])
        self.query_one(MetricsRow).rows = [
            MetricsCounts(
                label=self.window_size,
                **EMPTY_FIELDS,
                not_ready_message="loading…",
            )
        ]
        self.query_one("#table-status", Static).update("Loading window…")

    def refresh_view(self, rescan: bool) -> None:
        """Rebuild the controls/timeline widgets and kick the data refresh.

        The single update path: every key/click action mutates the
        screen's window fields and then calls this. `rescan=True` reads
        a fresh window from the log; `rescan=False` reuses the cached
        scan and only re-filters by the current mode.
        """
        controls = ControlsState(
            window_size=self.window_size,
            mode=self.mode,
            start_ms=self.window_start_ms,
            end_ms=self.window_end_ms,
        )
        bounds = TimelineBounds(
            log_start_ms=self.log_start_ms,
            log_end_ms=self.log_end_ms,
            window_start_ms=self.window_start_ms,
            window_end_ms=self.window_end_ms,
        )
        self.query_one(WindowStepper).window_size = self.window_size
        self.query_one(ModePicker).selected = self.mode
        self.query_one(SelectedWindow).state = controls
        self.query_one(Timeline).bounds = bounds
        if rescan:
            if (
                self.window_start_ms,
                self.window_end_ms,
            ) == self.cached_window:
                return
            self.show_loading_state()
            self.schedule_rescan()
        else:
            self.refresh_data(rescan=False)

    def schedule_rescan(self) -> None:
        """Collapse a fast window scrub into one scan of where the user lands."""
        if self.rescan_timer is not None:
            self.rescan_timer.stop()
        self.rescan_timer = self.set_timer(
            RESCAN_DEBOUNCE_S, lambda: self.refresh_data(rescan=True)
        )

    @work(thread=True, exclusive=True, group="refresh_data")
    def refresh_data(self, rescan: bool) -> None:
        """Scan and/or re-filter the window, push rows + metrics + status.

        On `rescan` the log is read into a fresh list of window queries
        cached on the screen and the query-details cache is emptied,
        since a new window is a new set of queries. Render always runs,
        so a mode change pays only an in-memory filter. The visible
        rows' text is read here, off the UI thread, reusing the cache
        across sorts and mode changes within the window.
        """
        if rescan:
            self.window_queries = read_window(
                self.app.log_file,
                self.window_start_ms,
                self.window_end_ms,
                self.app.window_pad_ms,
                self.log_end_ms,
                current_ms(),
            )
            self.query_details_cache = {}
            self.cached_window = (self.window_start_ms, self.window_end_ms)
        controls = ControlsState(
            window_size=self.window_size,
            mode=self.mode,
            start_ms=self.window_start_ms,
            end_ms=self.window_end_ms,
        )
        rows, metrics = render_window(
            self.window_queries,
            controls,
            self.app.slow_threshold * 1000,
            self.log_end_ms,
        )
        self.all_rows = rows
        visible_rows = load_query_details_for_rows(
            self.app.log_file, self.visible_rows(), self.query_details_cache
        )
        self.app.call_from_thread(
            self.apply_window_result, visible_rows, metrics
        )

    def apply_window_result(
        self,
        rows: list[HistoricQueryRow],
        metrics: MetricsCounts,
    ) -> None:
        """Push fresh rows, metrics, and status line into the widgets."""
        self.query_one(HistoricQueryTable).set_rows(rows)
        self.query_one(MetricsRow).rows = [metrics]
        self.query_one("#table-status", Static).update(
            self.status_text(len(self.all_rows))
        )
        self.refresh_sort_indicator()

    def sorted_rows(
        self, rows: list[HistoricQueryRow]
    ) -> list[HistoricQueryRow]:
        """Order rows by the active sort column and direction."""
        return sorted(
            rows,
            key=SORT_KEYS[self.sort_column],
            reverse=self.sort_reverse,
        )

    def visible_rows(self) -> list[HistoricQueryRow]:
        """The sorted window capped to the rows the table will paint."""
        return self.sorted_rows(self.all_rows)[:MAX_VISIBLE_ROWS]

    def sort_phrase(self) -> str:
        """Describe the active sort and direction for the status line."""
        descending, ascending = SORT_PHRASES[self.sort_column]
        direction = descending if self.sort_reverse else ascending
        return f"{self.sort_column}, {direction}"

    def status_text(self, total: int) -> str:
        """Status line describing the window mode, row count, and sort."""
        phrase = MODE_PHRASES[self.mode]
        if total > MAX_VISIBLE_ROWS:
            count = f"top {MAX_VISIBLE_ROWS:,} of {total:,}"
        else:
            count = f"{total:,}"
        return (
            f"Showing {count} queries that {phrase} the time window "
            f"(sorted by {self.sort_phrase()})"
        )

    def refresh_sort_indicator(self) -> None:
        """Point the table's header arrow at the active sort column."""
        self.query_one(HistoricQueryTable).set_sort_indicator(
            SORT_COLUMNS.index(self.sort_column), self.sort_reverse
        )

    def clamp_window_start(self, start: int, width: int) -> int:
        """Keep a window of `width` fully inside the log span."""
        return min(max(start, self.log_start_ms), self.log_end_ms - width)

    def center_window_at(self, ms: int) -> None:
        """Center the window on `ms`; no-op when `all` (unbounded)."""
        width = preset_ms(self.window_size)
        if width is None:
            return
        self.window_start_ms = self.clamp_window_start(ms - width // 2, width)
        self.window_end_ms = self.window_start_ms + width
        self.refresh_view(rescan=True)

    def clamp_window(self) -> None:
        """Fit the window into the log span, anchored to its right edge.

        `all` spans the whole log; any fixed size keeps the current
        `window_end_ms` put and walks `window_start_ms` left, clamped
        so the window stays inside the log span.
        """
        width = preset_ms(self.window_size)
        if width is None:
            self.window_start_ms = self.log_start_ms
            self.window_end_ms = self.log_end_ms
            return
        start = self.clamp_window_start(self.window_end_ms - width, width)
        self.window_start_ms = start
        self.window_end_ms = start + width

    def step_window(self, direction: int) -> None:
        """Move the window size one preset in `direction` (wraps)."""
        presets = self.available_presets
        index = presets.index(self.window_size)
        self.window_size = presets[(index + direction) % len(presets)]
        self.clamp_window()
        self.refresh_view(rescan=True)

    def set_mode(self, mode: str) -> None:
        """Select an exact match mode, then refresh the view (no rescan)."""
        self.mode = mode
        self.refresh_view(rescan=False)

    def action_cycle_window(self) -> None:
        """Step to the next window-size preset (wraps)."""
        self.step_window(1)

    def action_cycle_mode(self) -> None:
        """Step to the next match mode (wraps)."""
        index = MODES.index(self.mode)
        self.set_mode(MODES[(index + 1) % len(MODES)])

    def cycle_sort_column(self, direction: int) -> None:
        """Move the sort one column in `direction` (wraps)."""
        index = SORT_COLUMNS.index(self.sort_column)
        self.sort_column = SORT_COLUMNS[
            (index + direction) % len(SORT_COLUMNS)
        ]
        self.refresh_data(rescan=False)

    def action_sort_next_column(self) -> None:
        """Sort by the next column (wraps)."""
        self.cycle_sort_column(1)

    def action_sort_prev_column(self) -> None:
        """Sort by the previous column (wraps)."""
        self.cycle_sort_column(-1)

    def action_invert_sort(self) -> None:
        """Flip the sort direction."""
        self.sort_reverse = not self.sort_reverse
        self.refresh_data(rescan=False)

    def shift_window(self, direction: int) -> None:
        """Move the window by its own width; clamp; no-op when `all`."""
        width = preset_ms(self.window_size)
        if width is None:
            return
        start = self.window_start_ms + direction * width
        self.window_start_ms = self.clamp_window_start(start, width)
        self.window_end_ms = self.window_start_ms + width
        self.refresh_view(rescan=True)

    def action_shift_earlier(self) -> None:
        """Shift the window one width toward the log start."""
        self.shift_window(-1)

    def action_shift_later(self) -> None:
        """Shift the window one width toward the log end."""
        self.shift_window(1)

    def action_snap_start(self) -> None:
        """Snap the window to the log start."""
        width = preset_ms(self.window_size)
        self.window_start_ms = self.log_start_ms
        self.window_end_ms = (
            self.log_end_ms if width is None else self.log_start_ms + width
        )
        self.refresh_view(rescan=True)

    def action_snap_end(self) -> None:
        """Snap the window to the log end."""
        width = preset_ms(self.window_size)
        self.window_end_ms = self.log_end_ms
        self.window_start_ms = (
            self.log_start_ms if width is None else self.log_end_ms - width
        )
        self.refresh_view(rescan=True)

    def on_nav_pill_clicked(self, message: NavPill.Clicked) -> None:
        """Switch to the screen named by the clicked pill."""
        self.app.switch_screen(message.target)

    def on_window_stepper_stepped(
        self, message: WindowStepper.Stepped
    ) -> None:
        """Resize the window when a stepper arrow is clicked."""
        self.step_window(message.direction)

    def on_mode_picker_selected(self, message: ModePicker.Selected) -> None:
        """Switch the match mode when a segment is clicked."""
        self.set_mode(message.mode)

    def on_resize(self) -> None:
        """Re-evaluate the conditional scroll bindings after a resize."""
        self.call_after_refresh(self.refresh_bindings)

    def on_timeline_recentered(self, message: Timeline.Recentered) -> None:
        """Recenter the window on the clicked timeline position."""
        self.center_window_at(message.center_ms)

    def on_data_table_header_selected(
        self, message: HistoricQueryTable.HeaderSelected
    ) -> None:
        """Sort by the clicked column; click the active one again to invert."""
        if message.column_index >= len(SORT_COLUMNS):
            return
        column = SORT_COLUMNS[message.column_index]
        if column == self.sort_column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
        self.refresh_data(rescan=False)

    def on_data_table_row_selected(
        self, message: HistoricQueryTable.RowSelected
    ) -> None:
        """Show the selected finished query's SPARQL in the pane."""
        row = message.data_table.query_rows[message.cursor_row]
        self.query_one(SparqlPane).content = SparqlContent(
            qid=row.qid,
            started_at_ms=row.started_at_ms,
            status=row.status,
            sparql_text=row.sparql,
            client_ip=row.client_ip,
        )
