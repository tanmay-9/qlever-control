from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Static

MODES = ("STARTS", "ACTIVE", "ENDS")


class ModePicker(Horizontal):
    """Segmented mode control: MODE [ STARTS │ ACTIVE │ ENDS ].

    Each mode is its own Static so a segment can be made independently
    clickable without restructuring the widget. The selected segment
    carries the `-selected` state class for styling.
    """

    can_focus = False

    def __init__(self, selected: str) -> None:
        """Hold the selected mode to paint once mounted."""
        super().__init__()
        self.selected = selected

    def compose(self) -> ComposeResult:
        yield Static("MODE", classes="picker-caption")
        yield Static("[", classes="picker-bracket")
        for index, mode in enumerate(MODES):
            if index > 0:
                yield Static("│", classes="picker-sep")
            classes = "picker-segment"
            if mode == self.selected:
                classes += " -selected"
            yield Static(mode, id=f"mode-{mode.lower()}", classes=classes)
        yield Static("]", classes="picker-bracket")
