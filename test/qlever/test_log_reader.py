import pytest

from qlever.monitor import log_reader
from qlever.monitor.log_reader import (
    next_whole_line,
    offset_for_ts,
    parse_line,
    parse_line_fallback,
    peek_ts_ms,
    scan_range,
)

START = (
    b'{"ts-ms":1716000000000,"event":"start","qid":"q-8a4f",'
    b'"client-ip":"1.2.3.4","query":"SELECT * WHERE { ?s ?p ?o }"}'
)
END = b'{"ts-ms":1716000000050,"event":"end","qid":"q-8a4f","status":"ok"}'

VALID_STATUSES = ["ok", "failed", "cancelled", "timeout", "unknown"]


def test_parse_line_start_has_no_status_and_ignores_query():
    assert parse_line(START) == (1716000000000, "start", "q-8a4f", None)


@pytest.mark.parametrize("status", VALID_STATUSES)
def test_parse_line_end_carries_each_valid_status(status):
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"%s"}' % (
        status.encode()
    )
    assert parse_line(line) == (1, "end", "q1", status)


def test_parse_line_trailing_newline_tolerated():
    assert parse_line(END + b"\n") == (1716000000050, "end", "q-8a4f", "ok")


def test_parse_line_escaped_quote_in_query_does_not_break_qid():
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"client-ip":"::1","query":"FILTER(?x = \\"a\\")"}'
    )
    assert parse_line(line) == (1, "start", "q1", None)


def test_parse_line_wrong_first_key_is_a_miss():
    assert parse_line(b'{"event":"end","ts-ms":1,"qid":"q1"}') is None


def test_parse_line_non_integer_ts_is_a_miss():
    assert parse_line(b'{"ts-ms":1.5,"event":"end","qid":"q1"}') is None


def test_parse_line_unknown_event_is_a_miss():
    assert parse_line(b'{"ts-ms":1,"event":"ping","qid":"q1"}') is None


def test_parse_line_end_status_outside_closed_set_is_a_miss():
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"weird"}'
    assert parse_line(line) is None


def test_parse_line_missing_qid_is_a_miss():
    assert parse_line(b'{"ts-ms":1,"event":"end","status":"ok"}') is None


def test_fallback_recovers_a_line_the_fast_path_rejects():
    # Leading space after `{` defeats the byte prefix but is valid
    # JSON, so the fallback still extracts the fields.
    line = b'{ "ts-ms":1716000000000,"event":"end","qid":"q1","status":"ok"}'
    assert parse_line(line) is None
    assert parse_line_fallback(line) == (1716000000000, "end", "q1", "ok")


def test_fallback_start_returns_none_status():
    assert parse_line_fallback(START) == (
        1716000000000,
        "start",
        "q-8a4f",
        None,
    )


def test_fallback_malformed_json_returns_none():
    assert parse_line_fallback(b'{"ts-ms":1,') is None


def test_fallback_non_object_json_returns_none():
    assert parse_line_fallback(b"[1,2,3]") is None


def test_fallback_non_integer_ts_returns_none():
    line = b'{"ts-ms":1.0,"event":"end","qid":"q1","status":"ok"}'
    assert parse_line_fallback(line) is None


def test_fallback_status_outside_closed_set_returns_none():
    line = b'{"ts-ms":1,"event":"end","qid":"q1","status":"weird"}'
    assert parse_line_fallback(line) is None


FIRST_LINE = b'{"ts-ms":1000,"event":"start","qid":"q1","query":"SELECT 1"}\n'
SECOND_LINE = b'{"ts-ms":2000,"event":"end","qid":"q1","status":"ok"}\n'
THIRD_LINE = b'{"ts-ms":3000,"event":"start","qid":"q2","query":"SELECT 2"}\n'
SECOND_OFFSET = len(FIRST_LINE)
THIRD_OFFSET = len(FIRST_LINE) + len(SECOND_LINE)


@pytest.fixture
def open_log(tmp_path):
    """Write bytes to a log file and hand back an open (handle, size).

    Closes every handle it produced when the test finishes.
    """
    handles = []

    def make(data):
        path = tmp_path / "q.log"
        path.write_bytes(data)
        handle = path.open("rb")
        handles.append(handle)
        return handle, len(data)

    yield make
    for handle in handles:
        handle.close()


def test_next_whole_line_probe_zero_returns_first_line(open_log):
    log, size = open_log(FIRST_LINE + SECOND_LINE + THIRD_LINE)
    assert next_whole_line(log, 0, size) == (0, 1000)


def test_next_whole_line_mid_line_aligns_to_following_line(open_log):
    log, size = open_log(FIRST_LINE + SECOND_LINE + THIRD_LINE)
    # A probe inside the first line lands on the second.
    assert next_whole_line(log, 10, size) == (SECOND_OFFSET, 2000)
    assert next_whole_line(log, SECOND_OFFSET - 1, size) == (
        SECOND_OFFSET,
        2000,
    )


def test_next_whole_line_on_a_boundary_advances_to_the_next_line(open_log):
    log, size = open_log(FIRST_LINE + SECOND_LINE + THIRD_LINE)
    # probe > 0 always discards one line, so a probe on the second
    # line's start returns the third.
    assert next_whole_line(log, SECOND_OFFSET, size) == (THIRD_OFFSET, 3000)


def test_next_whole_line_past_eof_returns_none(open_log):
    log, size = open_log(FIRST_LINE + SECOND_LINE)
    assert next_whole_line(log, size, size) is None


def test_next_whole_line_trailing_partial_line_returns_none(open_log):
    partial = b'{"ts-ms":4000,"event":"end","qid":"q3"'
    log, size = open_log(FIRST_LINE + partial)
    # The only line after the probe has no newline, so it is incomplete.
    assert next_whole_line(log, 5, size) is None


def build_log(timestamps):
    """Bytes for an ascending-ts log plus each line's start offset."""
    data = bytearray()
    offsets = []
    for i, ts in enumerate(timestamps):
        offsets.append(len(data))
        data += b'{"ts-ms":%d,"event":"end","qid":"q%d","status":"ok"}\n' % (
            ts,
            i,
        )
    return bytes(data), offsets


def first_ts_at_or_after(log, offset, target):
    """ts of the first line at/after offset, or None if every line older."""
    log.seek(offset)
    for line in log:
        ts = peek_ts_ms(line)
        if ts >= target:
            return ts
    return None


def test_offset_for_ts_empty_file_returns_zero(open_log):
    log, size = open_log(b"")
    assert offset_for_ts(log, 100, size) == 0


def test_offset_for_ts_target_before_first_line_returns_zero(open_log):
    data, _ = build_log([1000, 1010, 1020])
    log, size = open_log(data)
    assert offset_for_ts(log, 500, size) == 0


def test_offset_for_ts_target_equal_to_first_line_returns_zero(open_log):
    data, _ = build_log([1000, 1010, 1020])
    log, size = open_log(data)
    # first_ts >= target means every line qualifies; start at 0.
    assert offset_for_ts(log, 1000, size) == 0


@pytest.mark.parametrize("target", [1015, 1020, 1099, 1500, 1900])
def test_offset_for_ts_lands_at_or_before_the_boundary(
    open_log, monkeypatch, target
):
    # Tiny gallop step so the backward gallop actually iterates here.
    monkeypatch.setattr(log_reader, "GALLOP_START", 16)
    timestamps = list(range(1000, 2000, 10))
    data, offsets = build_log(timestamps)
    log, size = open_log(data)

    result = offset_for_ts(log, target, size)

    # Never overshoots: the returned line is not newer than target, so
    # no line with ts >= target was skipped.
    boundary = next(o for o, ts in zip(offsets, timestamps) if ts >= target)
    assert result <= boundary
    assert first_ts_at_or_after(log, result, target) == next(
        ts for ts in timestamps if ts >= target
    )


def test_offset_for_ts_target_past_last_line_yields_empty_scan(
    open_log, monkeypatch
):
    monkeypatch.setattr(log_reader, "GALLOP_START", 16)
    data, _ = build_log(list(range(1000, 2000, 10)))
    log, size = open_log(data)

    result = offset_for_ts(log, 9999, size)

    # Near EOF, not necessarily file_size, but a forward scan from it
    # finds nothing at or after the target: the same empty result.
    assert first_ts_at_or_after(log, result, 9999) is None


def test_scan_range_yields_each_whole_line_with_its_offset(open_log):
    data, offsets = build_log([1000, 1010, 1020])
    log, size = open_log(data)
    assert list(scan_range(log, 0, size)) == [
        ((1000, "end", "q0", "ok"), offsets[0]),
        ((1010, "end", "q1", "ok"), offsets[1]),
        ((1020, "end", "q2", "ok"), offsets[2]),
    ]


def test_scan_range_includes_the_line_straddling_hi_bound(open_log):
    data, offsets = build_log([1000, 1010, 1020])
    log, size = open_log(data)
    # hi_bound falls inside the second line. The second line starts
    # before it so it is still emitted; the third starts past it.
    result = list(scan_range(log, 0, offsets[1] + 3))
    assert [ts for (ts, _, _, _), _ in result] == [1000, 1010]


def test_scan_range_stops_before_a_trailing_partial_line(open_log):
    data, _ = build_log([1000])
    log, size = open_log(data + b'{"ts-ms":2000,"event":"end","qid":"q9"')
    assert list(scan_range(log, 0, size)) == [
        ((1000, "end", "q0", "ok"), 0),
    ]


def test_scan_range_skips_a_malformed_line_and_continues(open_log):
    good = b'{"ts-ms":1000,"event":"end","qid":"q0","status":"ok"}\n'
    junk = b"not json at all\n"
    tail = b'{"ts-ms":1020,"event":"end","qid":"q2","status":"ok"}\n'
    log, size = open_log(good + junk + tail)
    assert list(scan_range(log, 0, size)) == [
        ((1000, "end", "q0", "ok"), 0),
        ((1020, "end", "q2", "ok"), len(good) + len(junk)),
    ]


def test_scan_range_recovers_a_line_via_the_json_fallback(open_log):
    # Leading space defeats the fast byte path but is valid JSON.
    line = b'{ "ts-ms":2000,"event":"end","qid":"q1","status":"ok"}\n'
    log, size = open_log(line)
    assert list(scan_range(log, 0, size)) == [
        ((2000, "end", "q1", "ok"), 0),
    ]


def test_scan_range_from_eof_yields_nothing(open_log):
    data, _ = build_log([1000, 1010])
    log, size = open_log(data)
    assert list(scan_range(log, size, size)) == []
