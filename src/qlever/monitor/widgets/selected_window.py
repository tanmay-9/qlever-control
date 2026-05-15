from __future__ import annotations

from textual.widgets import Static

from qlever.monitor.models import ControlsState
from qlever.monitor.util import format_clock


def format_selected_window(state: ControlsState) -> str:
    """Read-only summary of the current window's range and width."""
    start = format_clock(state.start_ms)
    end = format_clock(state.end_ms)
    return f"[b]SELECTED[/b]  {start} → {end}  ([b]{state.window_size}[/b])"


class SelectedWindow(Static):
    """Read-only window range under the HeaderRow on the Historic screen."""

    can_focus = False

    def __init__(self, state: ControlsState) -> None:
        """Render the selected window range computed from `state`."""
        super().__init__(format_selected_window(state))
