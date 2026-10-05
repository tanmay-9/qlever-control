"""The window's range on one line, with the arrows that move it.

The full-screen plots have no room for the Historic timeline, so this
gives the same shift and jump controls around the range they move.
"""

from __future__ import annotations

from textual import events
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.message import Message
from textual.widgets import Static

from qlever.monitor_queries.live_data import current_ms
from qlever.monitor_queries.util import format_range


class WindowRange(Horizontal):
    """The range as `« ◄ 10:02:00 → 10:17:00 ► »`, arrows optional.

    A window that cannot move, as on Live, shows the range alone.
    """

    can_focus = False

    class Shifted(Message):
        """Posted when an arrow is clicked; -1 earlier, +1 later."""

        def __init__(self, direction: int) -> None:
            super().__init__()
            self.direction = direction

    class Jumped(Message):
        """Posted when a jump arrow is clicked; -1 to start, +1 to end."""

        def __init__(self, direction: int) -> None:
            super().__init__()
            self.direction = direction

    def __init__(self, movable: bool) -> None:
        """Remember whether to draw the arrows."""
        super().__init__()
        self.movable = movable

    def compose(self) -> ComposeResult:
        if self.movable:
            yield Static("", id="jump-start-key", classes="key-pill")
            jump_start = Static("«", id="jump-start", classes="range-arrow")
            jump_start.tooltip = "Jump to the start of the log."
            yield jump_start
            yield Static("", id="shift-earlier-key", classes="key-pill")
            shift_earlier = Static(
                "◄", id="shift-earlier", classes="range-arrow"
            )
            shift_earlier.tooltip = "Move the window backward."
            yield shift_earlier
        range_text = Static("", id="range-text")
        range_text.tooltip = "Start and end of the time window the plots show."
        yield range_text
        if self.movable:
            shift_later = Static("►", id="shift-later", classes="range-arrow")
            shift_later.tooltip = "Move the window forward."
            yield shift_later
            yield Static("", id="shift-later-key", classes="key-pill")
            jump_end = Static("»", id="jump-end", classes="range-arrow")
            jump_end.tooltip = "Jump to the end of the log."
            yield jump_end
            yield Static("", id="jump-end-key", classes="key-pill")

    def show_range(self, start_ms: int, end_ms: int) -> None:
        """Write the window's start and end, dated unless both are today."""
        self.query_one("#range-text", Static).update(
            format_range(start_ms, end_ms, current_ms())
        )

    def set_edges(self, at_start: bool, at_end: bool) -> None:
        """Grey both arrows on a side the window cannot move toward."""
        if not self.movable:
            return
        for arrow in ("#jump-start", "#shift-earlier"):
            self.query_one(arrow, Static).disabled = at_start
        for arrow in ("#shift-later", "#jump-end"):
            self.query_one(arrow, Static).disabled = at_end

    def set_help_keys(
        self, shift: tuple[str, str], jump: tuple[str, str]
    ) -> None:
        """Name the keys on the shift arrows and on the jump arrows."""
        if not self.movable:
            return
        self.query_one("#shift-earlier-key", Static).update(shift[0])
        self.query_one("#shift-later-key", Static).update(shift[1])
        self.query_one("#jump-start-key", Static).update(jump[0])
        self.query_one("#jump-end-key", Static).update(jump[1])

    def on_click(self, event: events.Click) -> None:
        """Translate an arrow click into a Shifted or Jumped message."""
        if event.widget is None or event.widget.disabled:
            return
        clicked = event.widget.id
        if clicked == "shift-earlier":
            self.post_message(self.Shifted(-1))
        elif clicked == "shift-later":
            self.post_message(self.Shifted(1))
        elif clicked == "jump-start":
            self.post_message(self.Jumped(-1))
        elif clicked == "jump-end":
            self.post_message(self.Jumped(1))
