from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Static


class WindowStepper(Horizontal):
    """Window-size stepper: WINDOW ◄ 15m ►.

    The arrows are separate Statics so each can be made independently
    clickable without restructuring the widget.
    """

    can_focus = False

    def __init__(self, window_size: str) -> None:
        """Hold the current window size to paint once mounted."""
        super().__init__()
        self.window_size = window_size

    def compose(self) -> ComposeResult:
        yield Static("WINDOW", classes="stepper-caption")
        yield Static("◄", id="window-prev", classes="stepper-arrow")
        yield Static(self.window_size, id="window-size")
        yield Static("►", id="window-next", classes="stepper-arrow")
