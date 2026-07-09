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

# Restart marker: neutral grey so it reads on either background and does
# not compete with the two series colors.
RESTART_COLOR = (150, 150, 150)


def plot_colors(
    dark: bool,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Pick the (RSS, CPU) line colors for the active theme background."""
    if dark:
        return RSS_COLOR_DARK, CPU_COLOR_DARK
    return RSS_COLOR_LIGHT, CPU_COLOR_LIGHT


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


def break_at_restarts(
    times: tuple[float, ...],
    values: tuple[float, ...],
    restart_times: tuple[float, ...],
) -> tuple[list[float], list[float]]:
    """Insert a gap at each restart so the line is not drawn across it.

    Before the first point at or after a restart time, add a NaN point
    at that time; plotext leaves a NaN unconnected. A restart before the
    first point or after the last adds no gap, only its vline shows.
    """
    out_times = []
    out_values = []
    restarts = list(restart_times)
    idx = 0
    for time_s, value in zip(times, values):
        while idx < len(restarts) and time_s >= restarts[idx]:
            if out_times:
                out_times.append(restarts[idx])
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
        centered note in place of a blank box. A grey vertical line marks
        each server restart, where the series is also broken.
        """
        max_points = point_budget(self.size.width)
        data = self.source(max_points)
        rss_color, cpu_color = plot_colors(self.app.current_theme.dark)
        has_restarts = bool(data.restart_times_s)
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
        plt.text(
            "CPU (cores)",
            data.end_s,
            data.cpu_total,
            yside="right",
            color=cpu_color,
            background="default",
            alignment="right",
        )
        # Legend for the restart marker, drawn in the marker's own grey
        # with the same vertical-bar glyph so it reads as "this line".
        if has_restarts:
            plt.text(
                "│ = server restart",
                (data.start_s + data.end_s) / 2,
                data.rss_total,
                yside="left",
                color=RESTART_COLOR,
                background="default",
                alignment="center",
            )
        if data.times_s:
            rss_times, rss_values = break_at_restarts(
                data.times_s, data.rss_gb, data.restart_times_s
            )
            cpu_times, cpu_values = break_at_restarts(
                data.times_s, data.cpu_cores, data.restart_times_s
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
        for restart_s in data.restart_times_s:
            plt.vline(restart_s, color=RESTART_COLOR)
        self.refresh()
