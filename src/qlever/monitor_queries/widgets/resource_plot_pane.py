from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from textual_plotext import PlotextPlot

from qlever.monitor_queries.models import ResourcePlot

# Saturated line colors, one pair per theme background: deeper on a
# light background, brighter on a dark one, so both stay legible.
CPU_COLOR_LIGHT = (0, 150, 130)
RSS_COLOR_LIGHT = (176, 25, 127)
CPU_COLOR_DARK = (34, 211, 200)
RSS_COLOR_DARK = (255, 105, 190)

# Restart markers, one pair per theme like the series colors: orange
# marks the server going down, green it coming back. Kept clear of the
# RSS and CPU line colors and legible on either background.
STOP_COLOR_LIGHT = (200, 110, 20)
START_COLOR_LIGHT = (30, 140, 70)
STOP_COLOR_DARK = (240, 160, 60)
START_COLOR_DARK = (90, 210, 130)


def plot_colors(
    dark: bool,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Pick the (RSS, CPU) line colors for the active theme background."""
    if dark:
        return RSS_COLOR_DARK, CPU_COLOR_DARK
    return RSS_COLOR_LIGHT, CPU_COLOR_LIGHT


def marker_colors(
    dark: bool,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Pick the (stop, start) marker colors for the active theme."""
    if dark:
        return STOP_COLOR_DARK, START_COLOR_DARK
    return STOP_COLOR_LIGHT, START_COLOR_LIGHT


# A plot column holds 2 braille dots across, so 2 points per usable
# column is the most the plot can resolve; more just overplots. Reserve
# columns for the two y-axis label gutters.
Y_AXIS_CHROME = 16
MIN_PLOT_POINTS = 60


def point_budget(width: int) -> int:
    """Points worth plotting for a pane this wide (2 per braille column)."""
    usable_cols = max(10, width - Y_AXIS_CHROME)
    return max(MIN_PLOT_POINTS, usable_cols * 2)


def clock_ticks(
    start_s: float, end_s: float, count: int = 5
) -> tuple[list[float], list[str]]:
    """Evenly spaced x positions across the window with HH:MM:SS labels.

    Returns the tick positions in epoch seconds and their clock-time
    labels, so the x-axis reads as wall-clock time for both a rolling
    live window and a fixed historic span.
    """
    if end_s <= start_s:
        return [start_s], [
            datetime.fromtimestamp(start_s).strftime("%H:%M:%S")
        ]
    span = end_s - start_s
    positions = [
        start_s + span * index / (count - 1) for index in range(count)
    ]
    labels = [
        datetime.fromtimestamp(position).strftime("%H:%M:%S")
        for position in positions
    ]
    return positions, labels


def break_at_starts(
    times: tuple[float, ...],
    values: tuple[float, ...],
    start_times: tuple[float, ...],
) -> tuple[list[float], list[float]]:
    """Insert a gap at each restart so the line is not drawn across it.

    Breaking at the start time leaves the downtime, from the stop to the
    start, empty: before the first point at or after a start time, add a
    NaN point at that time; plotext leaves a NaN unconnected. A start
    before the first point or after the last adds no gap.
    """
    out_times = []
    out_values = []
    starts = list(start_times)
    idx = 0
    for time_s, value in zip(times, values):
        while idx < len(starts) and time_s >= starts[idx]:
            if out_times:
                out_times.append(starts[idx])
                out_values.append(float("nan"))
            idx += 1
        out_times.append(time_s)
        out_values.append(value)
    return out_times, out_values


class ResourcePlotPane(PlotextPlot):
    """Dual-axis RSS and CPU plot over a time window.

    Takes a source that returns the points to draw and an optional
    refresh interval. With an interval the plot replots on a timer and
    rolls forward, for the Live window; without one it draws once and
    stays fixed, for a historic span.
    """

    can_focus = False

    def __init__(
        self,
        source: Callable[[int], ResourcePlot],
        refresh_interval: float | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.refresh_interval = refresh_interval

    def on_mount(self) -> None:
        """Draw once; with an interval, also replot on a timer to roll."""
        self.replot()
        if self.refresh_interval is not None:
            self.set_interval(self.refresh_interval, self.replot)
        self.app.theme_changed_signal.subscribe(
            self, lambda theme: self.replot()
        )

    def on_resize(self) -> None:
        """Redraw so the y-tick count re-fits the new height."""
        self.replot()

    def replot(self) -> None:
        """Draw the current window: RSS on the left axis, CPU on the right.

        Frames the window and axes regardless of data, then either plots
        the two series or, when the window holds no samples, draws a
        centered note in place of a blank box. An orange line marks each
        stop and a green one each start, with the series broken across
        the downtime between them.
        """
        max_points = point_budget(self.size.width)
        data = self.source(max_points)
        dark = self.app.current_theme.dark
        rss_color, cpu_color = plot_colors(dark)
        stop_color, start_color = marker_colors(dark)
        has_restarts = bool(data.stop_times_s or data.start_times_s)
        plt = self.plt
        plt.clear_figure()
        plt.xlim(data.start_s, data.end_s)
        plt.ylim(0, data.rss_total, yside="left")
        plt.ylim(0, data.cpu_total, yside="right")
        positions, labels = clock_ticks(data.start_s, data.end_s)
        plt.xticks(positions, labels)
        # Name each series in its own top corner, colored to match its
        # line, so the reader maps line to axis without a stacked legend.
        # A bottom label row would sit under the footer keys.
        plt.text(
            "RSS (GB)",
            data.start_s,
            data.rss_total,
            yside="left",
            color=rss_color,
            background="default",
            alignment="left",
        )
        # The right axis auto-scales when the core count is unknown, so
        # anchor its label at the tallest CPU point rather than a fixed top.
        cpu_label_y = (
            data.cpu_total
            if data.cpu_total is not None
            else max(data.cpu_cores, default=0)
        )
        plt.text(
            "CPU (cores)",
            data.end_s,
            cpu_label_y,
            yside="right",
            color=cpu_color,
            background="default",
            alignment="right",
        )
        # Legend for the restart markers, each drawn in its own color
        # with the vertical-bar glyph so it reads as "this line".
        if has_restarts:
            mid = (data.start_s + data.end_s) / 2
            offset = (data.end_s - data.start_s) * 0.12
            plt.text(
                "│ Server stopped",
                mid - offset,
                data.rss_total,
                yside="left",
                color=stop_color,
                background="default",
                alignment="center",
            )
            plt.text(
                "│ Server started",
                mid + offset,
                data.rss_total,
                yside="left",
                color=start_color,
                background="default",
                alignment="center",
            )
        if data.times_s:
            rss_times, rss_values = break_at_starts(
                data.times_s, data.rss_gb, data.start_times_s
            )
            cpu_times, cpu_values = break_at_starts(
                data.times_s, data.cpu_cores, data.start_times_s
            )
            plt.plot(
                rss_times,
                rss_values,
                yside="left",
                marker="braille",
                color=rss_color,
            )
            plt.plot(
                cpu_times,
                cpu_values,
                yside="right",
                marker="braille",
                color=cpu_color,
            )
        else:
            # plotext only draws a y-axis for a side that has data, so an
            # empty window would show the RSS axis but not the CPU one.
            # Anchor an invisible point on each side to keep both framed.
            plt.plot([data.start_s], [0], yside="left", marker=" ")
            plt.plot([data.start_s], [0], yside="right", marker=" ")
            plt.text(
                "No samples in this window",
                (data.start_s + data.end_s) / 2,
                data.rss_total / 2,
                yside="left",
                background="default",
                alignment="center",
            )
        for stop_s in data.stop_times_s:
            plt.vline(stop_s, color=stop_color)
        for start_s in data.start_times_s:
            plt.vline(start_s, color=start_color)
        self.refresh()
