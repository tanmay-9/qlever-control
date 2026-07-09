from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from textual_plotext import PlotextPlot

from qlever.monitor_queries.models import ResourcePlot

# Saturated line colors
RSS_COLOR = (204, 0, 0)
CPU_COLOR = (31, 119, 180)


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
        source: Callable[[], ResourcePlot],
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

    def replot(self) -> None:
        """Draw the current window: RSS on the left axis, CPU on the right.

        Frames the window and axes regardless of data, then either plots
        the two series or, when the window holds no samples, draws a
        centered note in place of a blank box.
        """
        data = self.source()
        plt = self.plt
        plt.clear_figure()
        plt.xlim(data.start_s, data.end_s)
        plt.ylim(0, data.rss_total, yside="left")
        plt.ylim(0, data.cpu_total, yside="right")
        plt.ylabel("RSS (GB)", yside="left")
        plt.ylabel("CPU (cores)", yside="right")
        positions, labels = clock_ticks(data.start_s, data.end_s)
        plt.xticks(positions, labels)
        if data.times_s:
            plt.plot(
                data.times_s,
                data.rss_gb,
                yside="left",
                marker="fhd",
                color=RSS_COLOR,
                label="RSS (GB)",
            )
            plt.plot(
                data.times_s,
                data.cpu_cores,
                yside="right",
                marker="fhd",
                color=CPU_COLOR,
                label="CPU (cores)",
            )
        else:
            plt.text(
                "No samples in this window",
                (data.start_s + data.end_s) / 2,
                data.rss_total / 2,
                yside="left",
                alignment="center",
            )
        self.refresh()
