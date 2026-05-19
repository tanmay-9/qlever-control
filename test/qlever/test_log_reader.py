import pytest

from qlever.monitor.log_reader import parse_line, parse_line_fallback

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
