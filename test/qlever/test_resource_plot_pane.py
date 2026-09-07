"""Tests for the plot widget's scaling, series picking, and colors.

These are the parts a plot definition drives: how far up an axis goes,
which of a plot's columns the window actually has, and which color a
line is drawn in. Drawing itself is left to the widget.
"""

from qlever.monitor_queries.models import ResourceSeries, ResourceWindow
from qlever.monitor_queries.widgets.resource_plot_pane import (
    axis_top,
    line_color,
    series_for_keys,
)

NAN = float("nan")


def series(key, values, total=None):
    """One series with the label and unit the tests never look at."""
    return ResourceSeries(
        key=key, label=key, unit="u", values=values, total=total
    )


def window(*all_series):
    """A window over ten seconds holding the given series."""
    return ResourceWindow(
        start_s=0.0,
        end_s=10.0,
        times_s=tuple(float(step) for step in range(10)),
        series={one.key: one for one in all_series},
        events=(),
    )


def test_axis_top_uses_the_capacity_over_the_readings():
    win = window(series("rss", (7.0, 8.0), total=32.9))
    assert axis_top(win, ("rss",)) == 32.9


def test_axis_top_without_a_capacity_uses_the_largest_reading():
    win = window(series("read_bytes_per_s", (1.0, 4.5, 2.0)))
    assert axis_top(win, ("read_bytes_per_s",)) == 4.5


def test_axis_top_skips_the_buckets_that_reported_nothing():
    win = window(series("read_bytes_per_s", (NAN, 3.0, NAN)))
    assert axis_top(win, ("read_bytes_per_s",)) == 3.0


def test_axis_top_of_a_column_that_never_reported_is_zero():
    win = window(series("read_bytes_per_s", (NAN, NAN)))
    assert axis_top(win, ("read_bytes_per_s",)) == 0.0


def test_axis_top_spans_every_series_on_the_side():
    win = window(
        series("read_bytes_per_s", (1.0, 2.0)),
        series("write_bytes_per_s", (0.5, 6.0)),
    )
    keys = ("read_bytes_per_s", "write_bytes_per_s")
    assert axis_top(win, keys) == 6.0


def test_axis_top_ignores_a_key_the_window_does_not_have():
    win = window(series("read_bytes_per_s", (1.0, 2.0)))
    keys = ("read_bytes_per_s", "io_stall_percent")
    assert axis_top(win, keys) == 2.0


def test_axis_top_of_an_absent_side_is_zero():
    win = window(series("rss", (1.0,), total=32.9))
    assert axis_top(win, ("io_stall_percent",)) == 0.0


def test_series_for_keys_keeps_the_plot_order():
    win = window(
        series("write_bytes_per_s", (1.0,)),
        series("read_bytes_per_s", (2.0,)),
    )
    keys = ("read_bytes_per_s", "write_bytes_per_s")
    assert [one.key for one in series_for_keys(win, keys)] == list(keys)


def test_series_for_keys_drops_the_absent_ones():
    win = window(series("read_bytes_per_s", (1.0,)))
    keys = ("read_bytes_per_s", "write_bytes_per_s")
    found = series_for_keys(win, keys)
    assert [one.key for one in found] == ["read_bytes_per_s"]


def test_series_for_keys_of_an_empty_window_is_empty():
    assert series_for_keys(window(), ("rss",)) == []


def test_left_axis_gives_each_series_its_own_color():
    first = line_color("left", 0, dark=True)
    second = line_color("left", 1, dark=True)
    assert first != second


def test_right_axis_is_grey_and_not_a_left_hue():
    red, green, blue = line_color("right", 0, dark=True)
    assert red == green == blue
    assert (red, green, blue) != line_color("left", 0, dark=True)


def test_line_colors_differ_between_the_theme_backgrounds():
    assert line_color("left", 0, dark=True) != line_color(
        "left", 0, dark=False
    )
    assert line_color("right", 0, dark=True) != line_color(
        "right", 0, dark=False
    )
