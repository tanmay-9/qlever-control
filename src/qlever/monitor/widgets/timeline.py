from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Container
from textual.widgets import Static

TRACK_CHAR = "─"
SELECTED_CHAR = "▓"
LEFT_CAP = "├"
RIGHT_CAP = "┤"
CAPTION_FORMAT = "%Y-%m-%d %H:%M:%S"
MIN_BAR_CELLS = 4


@dataclass(frozen=True)
class TickStride:
    seconds: int
    fmt: str
    date_on_day_boundary: bool


# Pick a stride that yields roughly 6-10 visible labels across the typical
# terminal width, regardless of whether the log covers seconds or weeks.
def pick_stride(total_seconds):
    if total_seconds < 120:
        return TickStride(10, "%H:%M:%S", False)
    if total_seconds < 1800:
        return TickStride(60, "%H:%M", False)
    if total_seconds < 21600:
        return TickStride(900, "%H:%M", False)
    if total_seconds < 172800:
        return TickStride(3600, "%H:%M", True)
    if total_seconds < 1209600:
        return TickStride(21600, "%m-%d %H:%M", False)
    return TickStride(86400, "%m-%d", False)


class Timeline(Container):
    """Horizontal log-span bar with a highlighted selection window.

    State (log bounds + selection) is pushed in via set_state(); the
    widget repaints itself, adapting to its own column width.
    """

    def __init__(self) -> None:
        super().__init__()
        self.log_start = None
        self.log_end = None
        self.window_from = None
        self.window_to = None

    def compose(self) -> ComposeResult:
        yield Static("", id="timeline-bar")
        yield Static("", id="timeline-ticks")

    def set_state(
        self,
        log_start: datetime,
        log_end: datetime,
        window_from: datetime,
        window_to: datetime,
    ) -> None:
        self.log_start = log_start
        self.log_end = log_end
        self.window_from = window_from
        self.window_to = window_to
        self.repaint()

    def on_resize(self) -> None:
        self.repaint()

    def repaint(self) -> None:
        # set_state can be called before mount (e.g. from HistoricView.__init__);
        # defer the first paint until on_resize fires after mount.
        if (
            self.log_start is None
            or self.log_end is None
            or not self.is_mounted
        ):
            return
        width = self.size.width
        if width <= 0:
            return
        total_seconds = (self.log_end - self.log_start).total_seconds()
        if total_seconds <= 0:
            return

        # Row 1 holds the bar with the absolute timestamps flanking it;
        # row 2 holds the human labels and the tick labels. Reserve the wider
        # of (label, timestamp) on each side so the bar and ticks line up.
        left_label = "Log start"
        right_label = "Log end"
        left_ts = self.log_start.strftime(CAPTION_FORMAT)
        right_ts = self.log_end.strftime(CAPTION_FORMAT)
        left_width = max(len(left_label), len(left_ts))
        right_width = max(len(right_label), len(right_ts))
        bar_cells = max(
            MIN_BAR_CELLS,
            width - left_width - right_width - 4,
        )

        # Map absolute time to a column in [0, bar_cells). Multiply by
        # bar_cells - 1 so log_start lands on col 0 and log_end on the last col.
        def t_to_col(t):
            frac = (t - self.log_start).total_seconds() / total_seconds
            return round(frac * (bar_cells - 1))

        sel_a = max(0, min(bar_cells - 1, t_to_col(self.window_from)))
        sel_b = max(0, min(bar_cells, t_to_col(self.window_to)))
        # Keep tiny selections (e.g. 5m on a multi-day log) visible as one cell
        # rather than collapsing to nothing.
        if sel_b <= sel_a:
            sel_b = min(bar_cells, sel_a + 1)

        bar = Text()
        bar.append(left_ts.ljust(left_width) + " ", style="dim")
        bar.append(LEFT_CAP)
        for c in range(bar_cells):
            if sel_a <= c < sel_b:
                bar.append(SELECTED_CHAR, style="bold cyan")
            else:
                bar.append(TRACK_CHAR, style="dim")
        bar.append(RIGHT_CAP)
        bar.append(" " + right_ts.rjust(right_width), style="dim")
        self.query_one("#timeline-bar", Static).update(bar)

        # Tick labels live at arbitrary columns, so build the line as a flat
        # char array and stamp labels in by position, then wrap once.
        stride = pick_stride(total_seconds)
        prefix_len = left_width + 2  # label/ts col + space + LEFT_CAP
        chars = [" "] * width
        # Anchor the row with the human labels; ticks slot between them.
        for i, ch in enumerate(left_label):
            chars[i] = ch
        for i, ch in enumerate(right_label):
            chars[width - len(right_label) + i] = ch
        # Snap to the first stride-aligned timestamp strictly after log_start
        # so we don't try to draw a label under the LEFT_CAP.
        first_sec = (
            int(self.log_start.timestamp()) // stride.seconds + 1
        ) * stride.seconds
        last_sec = int(self.log_end.timestamp())
        prev_day = None
        for sec in range(first_sec, last_sec + 1, stride.seconds):
            t = datetime.fromtimestamp(sec)
            label = t.strftime(stride.fmt)
            # Prefix the date when an intra-day stride crosses midnight so the
            # reader can tell which day a "06:00" tick belongs to.
            if (
                stride.date_on_day_boundary
                and prev_day is not None
                and t.day != prev_day
            ):
                label = t.strftime("%m-%d ") + label
            prev_day = t.day
            pos = prefix_len + t_to_col(t)
            end = pos + len(label)
            if end >= prefix_len + bar_cells:
                continue
            # Skip labels that would collide with one already placed; this is
            # what lets a narrow bar thin out automatically.
            if pos > 0 and chars[pos - 1] != " ":
                continue
            if any(chars[i] != " " for i in range(pos, end)):
                continue
            for i, ch in enumerate(label):
                chars[pos + i] = ch
        self.query_one("#timeline-ticks", Static).update(
            Text("".join(chars), style="dim")
        )
