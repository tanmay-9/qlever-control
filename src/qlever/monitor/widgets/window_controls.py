from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Static

# (label, seconds). "all" resolves to the log span at render time.
PRESETS = (
    ("5m", 300),
    ("15m", 900),
    ("30m", 1800),
    ("1h", 3600),
    ("6h", 21600),
    ("24h", 86400),
    ("all", None),
)

MODES = ("STARTS", "ACTIVE", "ENDS")


class WindowControls(Horizontal):
    """Top control row: window-size pill, mode chips, selected-range readout.

    State is pushed in via set_state(); rendering and key handling are filled
    in by later sub-chunks. The pill is the source of truth for the chosen
    preset; mode is dummy UI for now.
    """

    def __init__(self) -> None:
        super().__init__()
        self.log_start = None
        self.log_end = None
        self.window_from = None
        self.window_to = None
        # Default to "1h" so the pill matches HistoricView's default window
        # until the first set_state arrives.
        self.preset_key = "1h"
        self.mode_index = 1  # ACTIVE

    def compose(self) -> ComposeResult:
        yield Static("", id="window-pill")
        yield Static("", id="mode-chips")
        yield Static("", id="selected-pill")

    def set_state(
        self,
        log_start,
        log_end,
        window_from,
        window_to,
        preset_key=None,
        mode_index=None,
    ) -> None:
        self.log_start = log_start
        self.log_end = log_end
        self.window_from = window_from
        self.window_to = window_to
        if preset_key is not None:
            self.preset_key = preset_key
        if mode_index is not None:
            self.mode_index = mode_index
        self.repaint()

    def repaint(self) -> None:
        # set_state may fire before mount; defer until the Statics exist.
        if not self.is_mounted or self.window_from is None:
            return

        header = "bold cyan"

        pill = Text()
        pill.append("WINDOW   ", style=header)
        pill.append("◀ ", style="dim")
        pill.append(f" {self.preset_key} ", style="reverse bold")
        pill.append(" ▶", style="dim")
        self.query_one("#window-pill", Static).update(pill)

        chips = Text()
        chips.append("│   ", style="dim")
        chips.append("MODE   ", style=header)
        # Segmented control: brackets on the outside, thin pipes between.
        chips.append("[", style="dim")
        for i, mode in enumerate(MODES):
            if i > 0:
                chips.append("│", style="dim")
            style = "reverse bold" if i == self.mode_index else None
            chips.append(f" {mode} ", style=style)
        chips.append("]", style="dim")
        self.query_one("#mode-chips", Static).update(chips)

        sel = Text()
        sel.append("│   ", style="dim")
        sel.append("SELECTED   ", style=header)
        sel.append(
            f"{self.window_from.strftime('%H:%M:%S')} → "
            f"{self.window_to.strftime('%H:%M:%S')}",
            style="bold",
        )
        sel.append(f"   ({self.preset_key})", style="dim")
        self.query_one("#selected-pill", Static).update(sel)

    def available_presets(self):
        # Hide presets that don't fit the current log span; "all" is always
        # offered so a tiny log still has a way to pick the full range.
        if self.log_start is None or self.log_end is None:
            return [key for key, _ in PRESETS]
        span = (self.log_end - self.log_start).total_seconds()
        keys = [key for key, secs in PRESETS if secs is None or secs <= span]
        if "all" not in keys:
            keys.append("all")
        return keys

    def on_mount(self) -> None:
        # State may have been pushed before mount; repaint once now that the
        # child Statics exist.
        self.repaint()
