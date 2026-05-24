import pytest

from qlever.monitor import log_reader
from qlever.monitor.log_reader import (
    CompletedQuery,
    extract_qid_and_query,
    load_sparql_at,
    next_whole_line,
    offset_for_ts,
    pair_start_end_events,
    parse_line,
    parse_line_fallback,
    peek_ts_ms,
    read_first_timestamp,
    read_last_timestamp,
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


def test_pair_start_end_events_empty_input_yields_empty_outputs():
    completed, still_open = pair_start_end_events(iter([]))
    assert completed == []
    assert still_open == {}


def test_pair_start_end_events_clean_pair():
    events = [
        ((1000, "start", "q1", None), 0),
        ((1050, "end", "q1", "ok"), 100),
    ]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == [
        CompletedQuery(
            start_ms=1000,
            end_ms=1050,
            duration_ms=50,
            status="ok",
            start_line_offset=0,
        )
    ]
    assert still_open == {}


def test_pair_start_end_events_start_without_end_stays_open():
    events = [((1000, "start", "q1", None), 42)]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == []
    assert still_open == {"q1": (1000, 42)}


def test_pair_start_end_events_end_without_start_is_dropped():
    events = [((1050, "end", "q1", "ok"), 0)]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == []
    assert still_open == {}


def test_pair_start_end_events_interleaved_queries_pair_in_end_order():
    events = [
        ((1000, "start", "qA", None), 0),
        ((1010, "start", "qB", None), 100),
        ((1020, "end", "qB", "ok"), 200),
        ((1030, "end", "qA", "failed"), 300),
    ]
    completed, still_open = pair_start_end_events(iter(events))
    assert completed == [
        CompletedQuery(
            start_ms=1010,
            end_ms=1020,
            duration_ms=10,
            status="ok",
            start_line_offset=100,
        ),
        CompletedQuery(
            start_ms=1000,
            end_ms=1030,
            duration_ms=30,
            status="failed",
            start_line_offset=0,
        ),
    ]
    assert still_open == {}


def test_pair_start_end_events_pairs_by_qid_not_file_order():
    # qA's end is written before qB's start (timestamps out of order),
    # but pairing is by qid so each query gets its own duration.
    events = [
        ((1000, "start", "qA", None), 0),
        ((1100, "start", "qB", None), 100),
        ((1050, "end", "qA", "ok"), 200),
        ((1150, "end", "qB", "ok"), 300),
    ]
    completed, _ = pair_start_end_events(iter(events))
    assert completed == [
        CompletedQuery(
            start_ms=1000,
            end_ms=1050,
            duration_ms=50,
            status="ok",
            start_line_offset=0,
        ),
        CompletedQuery(
            start_ms=1100,
            end_ms=1150,
            duration_ms=50,
            status="ok",
            start_line_offset=100,
        ),
    ]


@pytest.mark.parametrize("status", VALID_STATUSES)
def test_pair_start_end_events_status_flows_through(status):
    events = [
        ((1000, "start", "q1", None), 0),
        ((1050, "end", "q1", status), 100),
    ]
    completed, _ = pair_start_end_events(iter(events))
    assert completed[0].status == status


def test_extract_qid_and_query_returns_both_fields():
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"query":"SELECT * WHERE { ?s ?p ?o }"}\n'
    )
    assert extract_qid_and_query(line) == (
        "q1",
        "SELECT * WHERE { ?s ?p ?o }",
    )


def test_extract_qid_and_query_handles_escaped_quote_in_query():
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"query":"SELECT ?x WHERE { ?x ?p \\"a\\" }"}\n'
    )
    assert extract_qid_and_query(line) == (
        "q1",
        'SELECT ?x WHERE { ?x ?p "a" }',
    )


def test_extract_qid_and_query_missing_field_returns_empty():
    assert extract_qid_and_query(END + b"\n") == ("", "")


def test_extract_qid_and_query_malformed_json_returns_empty():
    assert extract_qid_and_query(b"this is not json\n") == ("", "")


def test_load_sparql_at_returns_qid_and_query(open_log):
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"client-ip":"1.2.3.4","query":"SELECT * WHERE { ?s ?p ?o }"}\n'
    )
    log, _ = open_log(line)
    assert load_sparql_at(log, 0) == (
        "q1",
        "SELECT * WHERE { ?s ?p ?o }",
    )


def test_load_sparql_at_handles_escaped_quotes_and_backslashes(open_log):
    # The JSON value encodes a literal " and a literal \ in the query.
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"query":"SELECT ?x WHERE { ?x ?p \\"a\\\\b\\" }"}\n'
    )
    log, _ = open_log(line)
    assert load_sparql_at(log, 0) == (
        "q1",
        'SELECT ?x WHERE { ?x ?p "a\\b" }',
    )


def test_load_sparql_at_handles_unicode(open_log):
    line = (
        b'{"ts-ms":1,"event":"start","qid":"q1",'
        b'"query":"SELECT ?s WHERE { ?s ?p \xe2\x98\x83 }"}\n'
    )
    log, _ = open_log(line)
    assert load_sparql_at(log, 0) == ("q1", "SELECT ?s WHERE { ?s ?p ☃ }")


def test_load_sparql_at_end_line_has_no_query_returns_empty(open_log):
    log, _ = open_log(END + b"\n")
    assert load_sparql_at(log, 0) == ("", "")


def test_load_sparql_at_malformed_json_returns_empty(open_log):
    log, _ = open_log(b"this is not json\n")
    assert load_sparql_at(log, 0) == ("", "")


def test_load_sparql_at_top_level_non_object_returns_empty(open_log):
    log, _ = open_log(b"42\n")
    assert load_sparql_at(log, 0) == ("", "")


def test_load_sparql_at_reads_the_line_at_the_given_offset(open_log):
    first = (
        b'{"ts-ms":1,"event":"start","qid":"q1","query":"FIRST"}\n'
    )
    second = (
        b'{"ts-ms":2,"event":"start","qid":"q2","query":"SECOND"}\n'
    )
    log, _ = open_log(first + second)
    assert load_sparql_at(log, len(first)) == ("q2", "SECOND")


def test_read_last_timestamp_empty_file_returns_none(open_log):
    log, size = open_log(b"")
    assert read_last_timestamp(log, size) is None


def test_read_last_timestamp_returns_last_complete_line_ts(open_log):
    log, size = open_log(FIRST_LINE + SECOND_LINE)
    assert read_last_timestamp(log, size) == 2000


def test_read_last_timestamp_skips_trailing_partial_line(open_log):
    partial = b'{"ts-ms":3000,"event":"end","qid":"q2","status'
    log, size = open_log(FIRST_LINE + SECOND_LINE + partial)
    assert read_last_timestamp(log, size) == 2000


def test_read_last_timestamp_grows_buffer_for_long_lines(open_log):
    long_query = b"x" * (40 * 1024)
    line = (
        b'{"ts-ms":1000,"event":"start","qid":"q1",'
        b'"query":"' + long_query + b'"}\n'
    )
    log, size = open_log(line)
    assert read_last_timestamp(log, size) == 1000


def test_read_first_timestamp_empty_file_returns_none(open_log):
    log, size = open_log(b"")
    assert read_first_timestamp(log, size) is None


def test_read_first_timestamp_returns_first_complete_line_ts(open_log):
    log, size = open_log(FIRST_LINE + SECOND_LINE)
    assert read_first_timestamp(log, size) == 1000


def test_read_first_timestamp_single_line(open_log):
    log, size = open_log(FIRST_LINE)
    assert read_first_timestamp(log, size) == 1000


def test_read_first_timestamp_skips_malformed_first_line(open_log):
    log, size = open_log(b"not a log line\n" + SECOND_LINE)
    assert read_first_timestamp(log, size) == 2000


def test_read_first_timestamp_unterminated_only_line_returns_none(open_log):
    partial = b'{"ts-ms":1000,"event":"start","qid":"q1"'
    log, size = open_log(partial)
    assert read_first_timestamp(log, size) is None
