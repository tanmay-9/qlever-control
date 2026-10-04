"""Tests for the plot widget's scaling, series picking, and colors.

These are the parts a plot definition drives: how far up an axis goes,
which ticks it gets, which of a plot's columns the window actually has,
which color a line is drawn in, and which plots a log can carry.
Drawing itself is left to the widget.
"""

from math import isnan

import pytest

from qlever.monitor_queries.models import ResourceSeries, ResourceWindow
from qlever.monitor_queries.widgets.resource_plot_pane import (
    PLOTS,
    Axis,
    available_plots,
    axis_ticks,
    axis_top,
    clamp,
    empty_note,
    line_color,
    percentile,
    round_step,
    series_for_keys,
    tick_layout,
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


def axis(*keys, min_top=0.0, adjustable=False):
    """An axis over the given columns, unbounded unless asked."""
    return Axis(keys=keys, min_top=min_top, adjustable=adjustable)


def test_axis_top_uses_the_capacity_over_the_readings():
    win = window(series("rss", (7.0, 8.0), total=32.9))
    assert axis_top(win, axis("rss")) == 32.9


def test_axis_top_without_a_capacity_uses_the_largest_reading():
    win = window(series("read_bytes_per_s", (1.0, 4.5, 2.0)))
    assert axis_top(win, axis("read_bytes_per_s")) == 4.5


def test_axis_top_skips_the_buckets_that_reported_nothing():
    win = window(series("read_bytes_per_s", (NAN, 3.0, NAN)))
    assert axis_top(win, axis("read_bytes_per_s")) == 3.0


def test_axis_top_of_a_column_that_never_reported_is_zero():
    win = window(series("read_bytes_per_s", (NAN, NAN)))
    assert axis_top(win, axis("read_bytes_per_s")) == 0.0


def test_axis_top_spans_every_series_on_the_side():
    win = window(
        series("read_bytes_per_s", (1.0, 2.0)),
        series("write_bytes_per_s", (0.5, 6.0)),
    )
    both = axis("read_bytes_per_s", "write_bytes_per_s")
    assert axis_top(win, both) == 6.0


def test_axis_top_ignores_a_key_the_window_does_not_have():
    win = window(series("read_bytes_per_s", (1.0, 2.0)))
    both = axis("read_bytes_per_s", "io_stall_percent")
    assert axis_top(win, both) == 2.0


def test_axis_top_of_an_absent_side_is_zero():
    win = window(series("rss", (1.0,), total=32.9))
    assert axis_top(win, axis("io_stall_percent")) == 0.0


def test_axis_top_never_scales_below_the_min_top():
    win = window(series("io_stall_percent", (1.0, 2.0)))
    bounded = axis("io_stall_percent", min_top=20.0)
    assert axis_top(win, bounded) == 20.0


def test_axis_top_grows_past_the_min_top_with_the_readings():
    win = window(series("io_stall_percent", (1.0, 65.0)))
    bounded = axis("io_stall_percent", min_top=20.0)
    assert axis_top(win, bounded) == 65.0


def test_axis_top_of_an_absent_side_ignores_the_min_top():
    # The zero is what tells the plot this side has nothing to draw, so
    # a bound must not raise an axis for a column the log never has.
    win = window(series("rss", (1.0,), total=32.9))
    bounded = axis("io_stall_percent", min_top=20.0)
    assert axis_top(win, bounded) == 0.0


def test_the_disk_plot_bounds_a_small_io_stall():
    win = window(
        series("read_bytes_per_s", (1.0,)),
        series("io_stall_percent", (2.0,)),
    )
    assert axis_top(win, PLOTS[1].right) == 20.0


def test_only_the_disk_plot_offers_the_step():
    assert PLOTS[1].adjustable
    assert not PLOTS[0].adjustable


def test_percentile_of_a_hundred_is_the_largest_reading():
    assert percentile([3.0, 1.0, 2.0], 100) == 3.0


def test_percentile_leaves_out_the_readings_above_it():
    assert percentile([float(step) for step in range(1, 11)], 75) == 8.0


def test_percentile_of_one_reading_is_that_reading():
    assert percentile([4.0], 90) == 4.0


def test_axis_top_steps_down_past_a_spike():
    readings = tuple(float(step) for step in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    stepped = axis("read_bytes_per_s", adjustable=True)
    assert axis_top(win, stepped) == 100.0
    assert axis_top(win, stepped, step=2) == 9.0


def test_axis_top_ignores_the_step_on_a_fixed_axis():
    readings = tuple(float(step) for step in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    assert axis_top(win, axis("read_bytes_per_s"), step=3) == 100.0


def test_axis_top_gives_each_series_its_own_percentile():
    # Pooling the two would answer 80, because the low write readings
    # push the taller read line's own cut-off down the sorted list.
    win = window(
        series(
            "read_bytes_per_s",
            tuple(float(step * 10) for step in range(1, 11)),
        ),
        series("write_bytes_per_s", (1.0,) * 10),
    )
    both = axis("read_bytes_per_s", "write_bytes_per_s", adjustable=True)
    assert axis_top(win, both, step=2) == 90.0


def test_the_disk_plot_steps_its_rate_axis():
    readings = tuple(float(step) for step in range(1, 10)) + (100.0,)
    win = window(series("read_bytes_per_s", readings))
    assert axis_top(win, PLOTS[1].left, step=2) == 9.0


def test_clamp_pulls_a_reading_above_the_ceiling_onto_it():
    assert clamp((1.0, 9.0, 2.0), 5.0) == (1.0, 5.0, 2.0)


def test_clamp_leaves_the_readings_under_the_ceiling_alone():
    assert clamp((1.0, 5.0), 5.0) == (1.0, 5.0)


def test_clamp_keeps_the_buckets_that_reported_nothing_empty():
    # A gap means the server was down, so clamping must not fill it.
    clamped = clamp((NAN, 9.0), 5.0)
    assert isnan(clamped[0])
    assert clamped[1] == 5.0


def test_clamp_without_a_ceiling_changes_nothing():
    assert clamp((1.0, 9.0), None) == (1.0, 9.0)


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


def offered_names(has_new_columns, has_operation_types):
    """Plot names offered for one combination of log formats."""
    return [
        plot.name
        for plot in available_plots(has_new_columns, has_operation_types)
    ]


def test_old_logs_offer_only_the_plot_every_log_can_fill():
    assert offered_names(False, False) == ["Memory and CPU"]


def test_new_logs_offer_every_plot():
    assert available_plots(True, True) == list(PLOTS)


def test_an_old_resource_log_still_offers_the_operation_plot():
    # It reads CPU, which every resource log has, and its two other
    # series come from the metrics log.
    assert offered_names(False, True) == ["Memory and CPU", "Operation time"]


def test_a_metrics_log_without_types_hides_the_operation_plot():
    assert offered_names(True, False) == ["Memory and CPU", "Disk I/O"]


def test_an_empty_window_says_it_has_no_samples():
    win = ResourceWindow(
        start_s=0.0, end_s=10.0, times_s=(), series={}, events=()
    )
    assert empty_note(win, PLOTS[0]) == "No samples in this window"


def test_a_window_missing_only_this_plot_names_the_plot():
    win = window(series("rss", (1.0,), total=32.9))
    assert empty_note(win, PLOTS[1]) == "No Disk I/O readings in this window"


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


def test_round_step_rounds_up_to_the_next_round_number():
    assert round_step(752.25) == 800
    assert round_step(23) == 25


def test_round_step_keeps_a_value_that_is_already_round():
    assert round_step(1000) == 1000
    assert round_step(3) == 3


def test_round_step_moves_to_the_next_power_of_ten_past_eight():
    assert round_step(900) == 1000


def test_round_step_never_picks_a_fraction():
    # 2.5 is round at every power of ten but the first.
    assert round_step(2.2) == 3
    assert round_step(0.3) == 1


def test_axis_ticks_label_round_numbers_to_at_least_the_top():
    axis_max, positions, labels = axis_ticks(3009, count=5, gaps=16)
    assert labels == ["0", "800", "1600", "2400", "3200"]
    assert positions == [0, 800, 1600, 2400, 3200]
    assert axis_max == 3200


def test_axis_ticks_print_a_step_of_25_without_a_decimal():
    _, _, labels = axis_ticks(70, count=4, gaps=12)
    assert labels == ["0", "25", "50", "75"]


def test_axis_ticks_print_a_large_label_in_full():
    _, _, labels = axis_ticks(3_600_000, count=5, gaps=16)
    assert labels[-1] == "4000000"


def test_axis_ticks_raise_the_axis_over_a_spare_row():
    # Ten rows in three gaps leaves one row above the top tick, worth a
    # third of a step.
    axis_max, _, labels = axis_ticks(40, count=4, gaps=10)
    assert labels[-1] == "60"
    assert axis_max == pytest.approx(200 / 3)


def test_axis_ticks_of_an_empty_axis_is_a_lone_zero():
    assert axis_ticks(0, count=5, gaps=16) == (1.0, [0], ["0"])


def test_tick_layout_picks_the_count_that_wastes_the_least():
    # Five ticks end RAM at 160 and CPU at exactly 32.
    count, gaps = tick_layout(20, (135.0, 32.0))
    assert (count, gaps) == (5, 16)


def test_tick_layout_keeps_four_ticks_when_they_fit():
    # Three ticks, 0 20 40 and 0 8 16, would waste a little less here.
    count, _ = tick_layout(14, (32.88, 16.0))
    assert count >= 4


def test_tick_layout_uses_fewer_ticks_on_a_short_pane():
    count, gaps = tick_layout(8, (32.88, 16.0))
    assert (count, gaps) == (3, 4)


def test_tick_layout_skips_a_side_with_nothing_to_draw():
    assert tick_layout(20, (135.0, 0.0)) == tick_layout(20, (135.0,))


def test_tick_layout_of_two_empty_sides_uses_the_most_ticks():
    count, _ = tick_layout(20, (0.0, 0.0))
    assert count == 8
