from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer, Static

from qlever.monitor.historic_stubs import (
    get_controls_state,
    get_historic_metrics,
    get_historic_query_rows,
    get_timeline_bounds,
)
from qlever.monitor.models import (
    ControlsState,
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
    ]

    def compose(self) -> ComposeResult:
        controls = get_controls_state()
        bounds = get_timeline_bounds()
        # Fixed once: the log span never changes during a session.
        self.log_start_ms = bounds.log_start_ms
        self.log_end_ms = bounds.log_end_ms
        # Presets longer than the log span are pointless; `all` covers it.
        self.available_presets = available_presets(
            self.log_end_ms - self.log_start_ms
        )
        # The screen owns the mutable window state from here on.
        self.window_size = controls.window_size
        self.mode = controls.mode
        self.window_start_ms = controls.start_ms
        self.window_end_ms = controls.end_ms

        yield HeaderRow(
            left=NavPill("< Live", target="live"),
            center=Static(TITLE),
        )
        yield HistoricControlsRow(controls)
        yield Timeline(bounds)
        yield MetricsRow([get_historic_metrics()])
        rows = sorted(
            get_historic_query_rows(),
            key=lambda r: r.duration_ms,
            reverse=True,
        )
        yield HistoricQueryTable(rows)
        yield Static(
            f"Showing {len(rows)} queries sorted by duration desc",
            id="table-status",
        )
        yield SparqlPane()
        yield Footer(show_command_palette=False)

    def push_state(self) -> None:
        """Rebuild the controls/timeline models and push them to the widgets.

        The single update path: every key/click action mutates the
        screen's window fields and then calls this.
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
        self.push_state()

    def reframe_window(self) -> None:
        """Recompute the window range from its size, then push state.

        `all` spans the whole log; any fixed size keeps the current
        start but is clamped so the window stays inside the log span.
        """
        width = preset_ms(self.window_size)
        if width is None:
            self.window_start_ms = self.log_start_ms
            self.window_end_ms = self.log_end_ms
        else:
            start = self.clamp_window_start(self.window_start_ms, width)
            self.window_start_ms = start
            self.window_end_ms = start + width
        self.push_state()

    def step_window(self, direction: int) -> None:
        """Move the window size one preset in `direction` (wraps)."""
        presets = self.available_presets
        index = presets.index(self.window_size)
        self.window_size = presets[(index + direction) % len(presets)]
        self.reframe_window()

    def set_mode(self, mode: str) -> None:
        """Select an exact match mode, then push state."""
        self.mode = mode
        self.push_state()

    def action_cycle_window(self) -> None:
        """Step to the next window-size preset (wraps)."""
        self.step_window(1)

    def action_cycle_mode(self) -> None:
        """Step to the next match mode (wraps)."""
        index = MODES.index(self.mode)
        self.set_mode(MODES[(index + 1) % len(MODES)])

    def shift_window(self, direction: int) -> None:
        """Move the window by its own width; clamp; no-op when `all`."""
        width = preset_ms(self.window_size)
        if width is None:
            return
        start = self.window_start_ms + direction * width
        self.window_start_ms = self.clamp_window_start(start, width)
        self.window_end_ms = self.window_start_ms + width
        self.push_state()

    def action_shift_earlier(self) -> None:
        """Shift the window one width toward the log start."""
        self.shift_window(-1)

    def action_shift_later(self) -> None:
        """Shift the window one width toward the log end."""
        self.shift_window(1)

    def action_snap_start(self) -> None:
        """Snap the window to the log start."""
        self.window_start_ms = self.log_start_ms
        self.reframe_window()

    def action_snap_end(self) -> None:
        """Snap the window to the log end."""
        self.window_start_ms = self.log_end_ms
        self.reframe_window()

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
        )
