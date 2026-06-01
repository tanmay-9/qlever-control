"""Tests for read_window, filter_queries, render_window, and detail loading."""

from qlever.monitor.historic_data import (
    DURATION_UNKNOWN,
    LoggedQuery,
    filter_queries,
    load_query_details_for_rows,
    read_window,
    render_window,
)
from qlever.monitor.models import ControlsState, HistoricQueryRow

PAD_MS = 10_000
SLOW_MS = 60_000


def start_line(ts_ms, qid, query="SELECT 1"):
    return (
        f'{{"ts-ms":{ts_ms},"event":"start","qid":"{qid}",'
        f'"client-ip":"x","query":"{query}"}}\n'
    ).encode()


def end_line(ts_ms, qid, status="ok"):
    return (
        f'{{"ts-ms":{ts_ms},"event":"end","qid":"{qid}",'
        f'"status":"{status}"}}\n'
    ).encode()


def test_read_window_empty_log_returns_no_queries(write_log):
    path = write_log(b"")
    queries = read_window(path, 0, 1000, PAD_MS, log_end_ms=0, now_ms=0)
    assert queries == []


def test_read_window_keeps_pair_entirely_inside_window(write_log):
    path = write_log(
        start_line(1_000_000, "q1", "SELECT a") + end_line(1_000_500, "q1")
    )
    queries = read_window(
        path,
        900_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_000_500,
        now_ms=1_000_500,
    )
    assert queries == [
        LoggedQuery(
            start_ms=1_000_000,
            end_ms=1_000_500,
            status="ok",
            start_line_offset=0,
        )
    ]


def test_read_window_drops_pair_inside_pad_before_window(write_log):
    path = write_log(
        start_line(900_000, "pad", "SELECT pad")
        + end_line(900_500, "pad")
        + start_line(1_000_000, "q1", "SELECT a")
        + end_line(1_000_500, "q1")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_000_500,
        now_ms=1_000_500,
    )
    starts = [query.start_ms for query in queries]
    assert starts == [1_000_000]


def test_read_window_keeps_pair_straddling_window_start(write_log):
    path = write_log(
        start_line(995_000, "edge", "SELECT edge")
        + end_line(1_005_000, "edge")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_005_000,
        now_ms=1_005_000,
    )
    assert len(queries) == 1
    assert queries[0].start_ms == 995_000
    assert queries[0].end_ms == 1_005_000


def test_read_window_keeps_pair_straddling_window_end(write_log):
    path = write_log(
        start_line(1_095_000, "edge", "SELECT edge")
        + end_line(1_105_000, "edge")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_105_000,
        now_ms=1_105_000,
    )
    assert len(queries) == 1
    assert queries[0].start_ms == 1_095_000


def test_read_window_includes_still_open_with_running_status(write_log):
    path = write_log(start_line(1_050_000, "live", "SELECT live"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_050_000,
        now_ms=1_050_000,
    )
    assert queries == [
        LoggedQuery(
            start_ms=1_050_000,
            end_ms=None,
            status="running",
            start_line_offset=0,
        )
    ]


def test_read_window_marks_still_open_as_orphaned_when_log_advanced_past_pad(
    write_log,
):
    path = write_log(start_line(1_050_000, "ghost", "SELECT ghost"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_200_000,
        now_ms=1_200_000,
    )
    assert queries == [
        LoggedQuery(
            start_ms=1_050_000,
            end_ms=None,
            status="orphaned",
            start_line_offset=0,
        )
    ]


def test_read_window_marks_still_open_as_orphaned_when_log_is_stale(write_log):
    """A still-open query is orphaned once the log has been silent past 2t.

    The start is within pad of log_end_ms (gate (a) passes), but wall
    clock has moved past log_end_ms by more than pad (gate (b) fails),
    so the server must be dead and the survivor cannot be running.
    """
    path = write_log(start_line(1_050_000, "ghost", "SELECT ghost"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_050_000,
        now_ms=1_050_000 + PAD_MS + 1,
    )
    [query] = queries
    assert query.status == "orphaned"


def test_read_window_drops_still_open_started_after_window(write_log):
    path = write_log(start_line(1_200_000, "later", "SELECT later"))
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_200_000,
        now_ms=1_200_000,
    )
    assert queries == []


def test_read_window_keeps_all_overlapping_pairs(write_log):
    path = write_log(
        start_line(995_000, "edge_start", "SELECT a")
        + end_line(1_005_000, "edge_start")
        + start_line(1_050_000, "inside", "SELECT b")
        + end_line(1_060_000, "inside")
        + start_line(1_095_000, "edge_end", "SELECT c")
        + end_line(1_105_000, "edge_end")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_105_000,
        now_ms=1_105_000,
    )
    assert {query.start_ms for query in queries} == {
        995_000,
        1_050_000,
        1_095_000,
    }


def test_read_window_carries_real_start_line_offset(write_log):
    first = start_line(1_010_000, "q1", "SELECT a") + end_line(1_020_000, "q1")
    path = write_log(
        first
        + start_line(1_050_000, "q2", "SELECT b")
        + end_line(1_060_000, "q2")
    )
    queries = read_window(
        path,
        1_000_000,
        1_100_000,
        PAD_MS,
        log_end_ms=1_060_000,
        now_ms=1_060_000,
    )
    offsets = {query.start_ms: query.start_line_offset for query in queries}
    assert offsets[1_010_000] == 0
    assert offsets[1_050_000] == len(first)


def make_query(start_ms, end_ms, status="ok", start_line_offset=0):
    return LoggedQuery(
        start_ms=start_ms,
        end_ms=end_ms,
        status=status,
        start_line_offset=start_line_offset,
    )


def test_filter_queries_active_returns_list_unchanged():
    queries = [
        make_query(100, 200),
        make_query(300, None, status="running"),
    ]
    assert filter_queries(queries, "ACTIVE", 0, 1000) is queries


def test_filter_queries_starts_keeps_only_starts_in_window():
    queries = [
        make_query(50, 150),
        make_query(200, 400),
        make_query(1100, 1200),
    ]
    starts = [
        query.start_ms for query in filter_queries(queries, "STARTS", 100, 1000)
    ]
    assert starts == [200]


def test_filter_queries_starts_includes_running_queries():
    queries = [make_query(500, None, status="running")]
    starts = [
        query.start_ms for query in filter_queries(queries, "STARTS", 100, 1000)
    ]
    assert starts == [500]


def test_filter_queries_ends_excludes_running_queries():
    queries = [
        make_query(200, 500),
        make_query(300, None, status="running"),
    ]
    ends = [
        query.end_ms for query in filter_queries(queries, "ENDS", 100, 1000)
    ]
    assert ends == [500]


def test_filter_queries_ends_drops_ends_outside_window():
    queries = [
        make_query(50, 90),
        make_query(200, 500),
        make_query(800, 1200),
    ]
    ends = [
        query.end_ms for query in filter_queries(queries, "ENDS", 100, 1000)
    ]
    assert ends == [500]


def make_controls(mode="ACTIVE"):
    return ControlsState(
        window_size="15m",
        mode=mode,
        start_ms=1_000_000,
        end_ms=2_000_000,
    )


def test_render_window_uses_recorded_end_for_completed_query():
    queries = [make_query(1_100_000, 1_150_000)]
    rows, _ = render_window(
        queries, make_controls(), SLOW_MS, log_end_ms=2_500_000
    )
    assert rows[0].duration_ms == 50_000
    assert rows[0].status == "ok"


def test_render_window_uses_log_end_for_running_query():
    queries = [make_query(1_100_000, None, status="running")]
    rows, _ = render_window(
        queries, make_controls(), SLOW_MS, log_end_ms=2_500_000
    )
    assert rows[0].duration_ms == 1_400_000
    assert rows[0].status == "running"


def test_render_window_marks_orphan_duration_as_unknown():
    queries = [make_query(1_100_000, None, status="orphaned")]
    rows, _ = render_window(
        queries, make_controls(), SLOW_MS, log_end_ms=2_500_000
    )
    assert rows[0].duration_ms == DURATION_UNKNOWN
    assert rows[0].status == "orphaned"


def test_render_window_passes_window_size_as_metric_label():
    queries = [make_query(100 + index, 200 + index) for index in range(7)]
    controls = ControlsState(
        window_size="1h",
        mode="ACTIVE",
        start_ms=0,
        end_ms=1_000_000,
    )
    _, metrics = render_window(queries, controls, SLOW_MS, log_end_ms=1_000_000)
    assert metrics.label == "1h"
    assert metrics.seen == 7
    assert metrics.ok == 7


def test_render_window_carries_start_line_offset_onto_row():
    queries = [make_query(1_100_000, 1_200_000, start_line_offset=512)]
    rows, _ = render_window(
        queries, make_controls(), SLOW_MS, log_end_ms=2_500_000
    )
    assert rows[0].start_line_offset == 512


def test_render_window_leaves_text_empty():
    queries = [make_query(1_100_000, 1_200_000)]
    rows, _ = render_window(
        queries, make_controls(), SLOW_MS, log_end_ms=2_500_000
    )
    assert rows[0].qid == ""
    assert rows[0].sparql == ""
    assert rows[0].client_ip == ""


def test_render_window_respects_mode_filter():
    queries = [
        make_query(1_100_000, 1_200_000),
        make_query(1_500_000, None, status="running"),
    ]
    rows, _ = render_window(
        queries, make_controls(mode="ENDS"), SLOW_MS, log_end_ms=2_500_000
    )
    assert [row.started_at_ms for row in rows] == [1_100_000]


# Queries used by the mode-aware metric tests. A is fully inside the
# window, B starts inside but ends after it, C starts before but ends
# inside. So the metric count must change with the selected mode.
def mode_metric_queries():
    return [
        make_query(1_100_000, 1_900_000),
        make_query(1_500_000, 2_200_000),
        make_query(800_000, 1_300_000),
    ]


def test_render_window_metrics_active_counts_all_overlapping():
    _, metrics = render_window(
        mode_metric_queries(),
        make_controls("ACTIVE"),
        SLOW_MS,
        log_end_ms=2_200_000,
    )
    assert metrics.seen == 3


def test_render_window_metrics_starts_counts_started_in_window():
    _, metrics = render_window(
        mode_metric_queries(),
        make_controls("STARTS"),
        SLOW_MS,
        log_end_ms=2_200_000,
    )
    assert metrics.seen == 2


def test_render_window_metrics_ends_counts_ended_in_window():
    _, metrics = render_window(
        mode_metric_queries(),
        make_controls("ENDS"),
        SLOW_MS,
        log_end_ms=2_200_000,
    )
    assert metrics.seen == 2


def test_render_window_metrics_exclude_running_and_orphaned():
    queries = [
        make_query(1_100_000, 1_900_000),
        make_query(1_500_000, None, status="running"),
        make_query(1_200_000, None, status="orphaned"),
    ]
    _, metrics = render_window(
        queries, make_controls("ACTIVE"), SLOW_MS, log_end_ms=2_500_000
    )
    assert metrics.seen == 1


def make_row(start_line_offset):
    return HistoricQueryRow(
        qid="",
        start_line_offset=start_line_offset,
        started_at_ms=1_000_000,
        duration_ms=0,
        status="ok",
        sparql="",
        client_ip="",
    )


def test_load_query_details_fills_text_from_offsets(write_log):
    first = start_line(1_000_000, "q1", "SELECT a")
    path = write_log(first + start_line(1_050_000, "q2", "SELECT b"))
    cache = {}
    rows = [make_row(0), make_row(len(first))]
    filled = load_query_details_for_rows(path, rows, cache)
    assert [(row.qid, row.sparql) for row in filled] == [
        ("q1", "SELECT a"),
        ("q2", "SELECT b"),
    ]
    assert filled[0].client_ip == "x"
    assert cache[0] == ("q1", "x", "SELECT a")


def test_load_query_details_reuses_cache_without_reading(write_log):
    path = write_log(start_line(1_000_000, "q1", "SELECT a"))
    cache = {0: ("cached-qid", "cached-ip", "CACHED")}
    [filled] = load_query_details_for_rows(path, [make_row(0)], cache)
    assert filled.qid == "cached-qid"
    assert filled.client_ip == "cached-ip"
    assert filled.sparql == "CACHED"
