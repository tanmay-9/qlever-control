"""Tests for read_window, filter_queries, and render_window."""

from qlever.monitor.historic_data import (
    DURATION_UNKNOWN,
    LoggedQuery,
    WindowData,
    filter_queries,
    read_window,
    render_window,
)
from qlever.monitor.metrics import MetricsSnapshot
from qlever.monitor.models import ControlsState


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
    data = read_window(path, 0, 1000, PAD_MS, SLOW_MS, log_end_ms=0)
    assert data.queries == []
    assert data.metrics.seen == 0


def test_read_window_keeps_pair_entirely_inside_window(write_log):
    path = write_log(
        start_line(1_000_000, "q1", "SELECT a")
        + end_line(1_000_500, "q1")
    )
    data = read_window(
        path, 900_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_000_500
    )
    assert data.queries == [
        LoggedQuery(
            start_ms=1_000_000,
            end_ms=1_000_500,
            status="ok",
            qid="q1",
            sparql="SELECT a",
        )
    ]
    assert data.metrics.seen == 1
    assert data.metrics.ok == 1


def test_read_window_drops_pair_inside_pad_before_window(write_log):
    path = write_log(
        start_line(900_000, "pad", "SELECT pad")
        + end_line(900_500, "pad")
        + start_line(1_000_000, "q1", "SELECT a")
        + end_line(1_000_500, "q1")
    )
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_000_500
    )
    qids = [query.qid for query in data.queries]
    assert qids == ["q1"]
    assert data.metrics.seen == 1


def test_read_window_keeps_pair_straddling_window_start(write_log):
    path = write_log(
        start_line(995_000, "edge", "SELECT edge")
        + end_line(1_005_000, "edge")
    )
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_005_000
    )
    assert [query.qid for query in data.queries] == ["edge"]
    assert data.queries[0].start_ms == 995_000
    assert data.queries[0].end_ms == 1_005_000
    assert data.metrics.seen == 1


def test_read_window_keeps_pair_straddling_window_end(write_log):
    path = write_log(
        start_line(1_095_000, "edge", "SELECT edge")
        + end_line(1_105_000, "edge")
    )
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_105_000
    )
    assert [query.qid for query in data.queries] == ["edge"]
    assert data.metrics.seen == 0


def test_read_window_includes_still_open_with_running_status(write_log):
    path = write_log(start_line(1_050_000, "live", "SELECT live"))
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_050_000
    )
    assert data.queries == [
        LoggedQuery(
            start_ms=1_050_000,
            end_ms=None,
            status="running",
            qid="live",
            sparql="SELECT live",
        )
    ]


def test_read_window_marks_still_open_as_orphaned_when_log_advanced_past_pad(
    write_log,
):
    path = write_log(start_line(1_050_000, "ghost", "SELECT ghost"))
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_200_000
    )
    assert data.queries == [
        LoggedQuery(
            start_ms=1_050_000,
            end_ms=None,
            status="orphaned",
            qid="ghost",
            sparql="SELECT ghost",
        )
    ]


def test_read_window_drops_still_open_started_after_window(write_log):
    path = write_log(start_line(1_200_000, "later", "SELECT later"))
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_200_000
    )
    assert data.queries == []


def test_read_window_metrics_count_only_ends_inside_window(write_log):
    path = write_log(
        start_line(995_000, "edge_start", "SELECT a")
        + end_line(1_005_000, "edge_start")
        + start_line(1_050_000, "inside", "SELECT b")
        + end_line(1_060_000, "inside")
        + start_line(1_095_000, "edge_end", "SELECT c")
        + end_line(1_105_000, "edge_end")
    )
    data = read_window(
        path, 1_000_000, 1_100_000, PAD_MS, SLOW_MS, log_end_ms=1_105_000
    )
    assert {query.qid for query in data.queries} == {
        "edge_start", "inside", "edge_end",
    }
    assert data.metrics.seen == 2


def make_query(qid, start_ms, end_ms, status="ok"):
    return LoggedQuery(
        start_ms=start_ms, end_ms=end_ms, status=status,
        qid=qid, sparql=f"SELECT {qid}",
    )


def make_cache(queries):
    empty_metrics = MetricsSnapshot(
        seen=0, ok=0, failed=0, timeout=0, cancelled=0, unknown=0,
        slow=0, p50=None, p95=None,
    )
    return WindowData(queries=queries, metrics=empty_metrics)


def test_filter_queries_active_returns_cache_unchanged():
    cache = make_cache([
        make_query("a", 100, 200),
        make_query("b", 300, None, status="running"),
    ])
    assert filter_queries(cache, "ACTIVE", 0, 1000) is cache.queries


def test_filter_queries_starts_keeps_only_starts_in_window():
    cache = make_cache([
        make_query("early", 50, 150),
        make_query("inside", 200, 400),
        make_query("late", 1100, 1200),
    ])
    qids = [query.qid for query in filter_queries(cache, "STARTS", 100, 1000)]
    assert qids == ["inside"]


def test_filter_queries_starts_includes_running_queries():
    cache = make_cache([make_query("live", 500, None, status="running")])
    qids = [query.qid for query in filter_queries(cache, "STARTS", 100, 1000)]
    assert qids == ["live"]


def test_filter_queries_ends_excludes_running_queries():
    cache = make_cache([
        make_query("done", 200, 500),
        make_query("live", 300, None, status="running"),
    ])
    qids = [query.qid for query in filter_queries(cache, "ENDS", 100, 1000)]
    assert qids == ["done"]


def test_filter_queries_ends_drops_ends_outside_window():
    cache = make_cache([
        make_query("before", 50, 90),
        make_query("inside", 200, 500),
        make_query("after", 800, 1200),
    ])
    qids = [query.qid for query in filter_queries(cache, "ENDS", 100, 1000)]
    assert qids == ["inside"]


def make_controls(mode="ACTIVE"):
    return ControlsState(
        window_size="15m", mode=mode, start_ms=1_000_000, end_ms=2_000_000,
    )


def make_render_cache(queries, **metric_fields):
    snapshot_fields = dict(
        seen=0, ok=0, failed=0, timeout=0, cancelled=0, unknown=0,
        slow=0, p50=None, p95=None,
    )
    snapshot_fields.update(metric_fields)
    return WindowData(queries=queries, metrics=MetricsSnapshot(**snapshot_fields))


def test_render_window_uses_recorded_end_for_completed_query():
    cache = make_render_cache([make_query("done", 1_100_000, 1_150_000)])
    rows, _ = render_window(cache, make_controls(), log_end_ms=2_500_000)
    assert rows[0].duration_ms == 50_000
    assert rows[0].status == "ok"


def test_render_window_uses_log_end_for_running_query():
    cache = make_render_cache(
        [make_query("live", 1_100_000, None, status="running")]
    )
    rows, _ = render_window(cache, make_controls(), log_end_ms=2_500_000)
    assert rows[0].duration_ms == 1_400_000
    assert rows[0].status == "running"


def test_render_window_marks_orphan_duration_as_unknown():
    cache = make_render_cache(
        [make_query("ghost", 1_100_000, None, status="orphaned")]
    )
    rows, _ = render_window(cache, make_controls(), log_end_ms=2_500_000)
    assert rows[0].duration_ms == DURATION_UNKNOWN
    assert rows[0].status == "orphaned"


def test_render_window_passes_window_size_as_metric_label():
    cache = make_render_cache([], seen=7, ok=7)
    controls = ControlsState(
        window_size="1h", mode="ACTIVE", start_ms=0, end_ms=1,
    )
    _, metrics = render_window(cache, controls, log_end_ms=1)
    assert metrics.label == "1h"
    assert metrics.seen == 7
    assert metrics.ok == 7


def test_render_window_respects_mode_filter():
    cache = make_render_cache([
        make_query("done", 1_100_000, 1_200_000),
        make_query("live", 1_500_000, None, status="running"),
    ])
    rows, _ = render_window(cache, make_controls(mode="ENDS"), log_end_ms=2_500_000)
    assert [row.qid for row in rows] == ["done"]
