"""Convert the query metrics log into RDF for offline querying.

Walks the whole log once, pairs each start event with its end, and
emits one record per query. Loading those records into a triple store
lets you run arbitrary stat queries that go beyond the live app's
filters. The query text can be massive, so instead of storing it we
store a `blake2b` fingerprint of it plus the byte offset of its start
line, which is enough to group identical queries and to seek back to
the full text in the original log.
"""

from collections.abc import Iterator
from datetime import datetime, timezone
from hashlib import blake2b
from pathlib import Path
from typing import NamedTuple

from qlever.monitor_queries.log_reader import (
    CLIENT_IP_KEY,
    EVENT_KEY,
    QID_KEY,
    QUERY_KEY,
    STATUS_KEY,
    STATUS_SET,
    UNKNOWN_STATUS,
    LogBuffer,
    normalize_status,
    peek_ts_ms,
    slice_string_value,
)


class PairedQuery(NamedTuple):
    """One query with its start and end joined.

    `end_ms` is None when no end event arrived. For those, `status` is
    "running" if the query started within `pad_ms` of the last event in
    the log, otherwise "orphaned". For completed queries `status` is the
    real terminal status. `query` stays raw bytes.
    `start_line_offset` is the byte offset of the start event's line,
    kept so a caller can report progress by file position.
    """

    qid: str
    start_ms: int
    end_ms: int | None
    status: str
    client_ip: str
    query: bytes
    start_line_offset: int


def pair_queries(buf: LogBuffer, pad_ms: int) -> Iterator[PairedQuery]:
    """Walk the whole log buffer once, yielding one query per pair.

    A start event is held in `open_queries` until its end arrives; the
    end yields the completed pair. Lines that fail to parse are skipped.
    Whatever never gets an end is yielded after the walk: "running" if
    it started within `pad_ms` of the last event in the log, otherwise
    "orphaned", so its start facts are not lost either way.
    """
    open_queries = {}
    last_event_ms = 0
    offset = 0
    size = len(buf)
    while offset < size:
        newline = buf.find(b"\n", offset)
        # No newline left means a trailing partial line; leave it.
        if newline == -1:
            break
        line = buf[offset:newline]
        line_offset = offset
        offset = newline + 1
        parsed = parse_event_line(line)
        if parsed is None:
            continue
        if parsed.ts_ms > last_event_ms:
            last_event_ms = parsed.ts_ms
        if parsed.event == "start":
            previous = open_queries.get(parsed.qid)
            if previous is not None:
                # The qid is reused while an earlier start is still
                # unpaired, so that earlier query never got an end.
                # Emit it as orphaned before the new start replaces it.
                prev_event, prev_offset = previous
                yield unpaired_query(prev_event, prev_offset, "orphaned")
            open_queries[parsed.qid] = (parsed, line_offset)
            continue
        start = open_queries.pop(parsed.qid, None)
        if start is None:
            continue
        start_event, start_offset = start
        yield PairedQuery(
            qid=parsed.qid,
            start_ms=start_event.ts_ms,
            end_ms=parsed.ts_ms,
            status=parsed.status,
            client_ip=start_event.client_ip,
            query=start_event.query,
            start_line_offset=start_offset,
        )
    running_cutoff_ms = last_event_ms - pad_ms
    for start_event, start_offset in open_queries.values():
        status = (
            "running" if start_event.ts_ms >= running_cutoff_ms else "orphaned"
        )
        yield unpaired_query(start_event, start_offset, status)


class QueryEvent(NamedTuple):
    """One parsed log line, holding every field the converter writes.

    `event` is "start" or "end". On a start line `status` is None and
    `client_ip` and `query` are filled; on an end line `status` is
    filled and `client_ip`/`query` are empty. `query` stays raw bytes,
    exactly as written in the log, so it can go straight between
    N-Triples quotes without JSON decoding.
    """

    ts_ms: int
    event: str
    qid: str
    status: str | None
    client_ip: str
    query: bytes


def parse_event_line(line_bytes: bytes) -> QueryEvent | None:
    """Slice one whole log line into a QueryEvent, or None if malformed.

    Reads the fields by byte position rather than json.loads. The query
    is the last field, so its value runs from just after the query key
    to the line's closing quote and brace, and is kept as raw bytes.
    Returns None on anything unexpected so the caller can skip the line.
    """
    ts_ms = peek_ts_ms(line_bytes)
    if ts_ms is None:
        return None
    event = slice_string_value(line_bytes, EVENT_KEY)
    if event not in ("start", "end"):
        return None
    qid = slice_string_value(line_bytes, QID_KEY)
    if qid is None:
        return None

    if event == "end":
        status = slice_string_value(line_bytes, STATUS_KEY)
        if status is None:
            return None
        return QueryEvent(
            ts_ms=ts_ms,
            event=event,
            qid=qid,
            status=normalize_status(status),
            client_ip="",
            query=b"",
        )

    client_ip = slice_string_value(line_bytes, CLIENT_IP_KEY) or ""
    query_start = line_bytes.find(QUERY_KEY)
    if query_start == -1 or not line_bytes.endswith(b'"}'):
        return None
    query_start += len(QUERY_KEY)
    # Drop the query's closing quote and the object's closing brace.
    query = line_bytes[query_start:-2]
    return QueryEvent(
        ts_ms=ts_ms,
        event=event,
        qid=qid,
        status=None,
        client_ip=client_ip,
        query=query,
    )


def unpaired_query(
    start: QueryEvent, line_offset: int, status: str
) -> PairedQuery:
    """Build a record for a start event that never got an end.

    Used mid-stream, when a qid is reused before its earlier start
    ended, and at EOF for whatever is still open. The caller picks
    `status`: "orphaned" for a reused qid, or the time-based
    running/orphaned decision at EOF. `line_offset` is the start
    line's byte offset.
    """
    return PairedQuery(
        qid=start.qid,
        start_ms=start.ts_ms,
        end_ms=None,
        status=status,
        client_ip=start.client_ip,
        query=start.query,
        start_line_offset=line_offset,
    )


# Every status the writer can emit: the terminal statuses log_reader
# recognises, plus the two the converter assigns to queries with no end.
# STATUS_SET is a frozenset, so it is sorted for a stable header.
STATUS_VALUES = (*sorted(STATUS_SET), UNKNOWN_STATUS, "running", "orphaned")


def header(log_path: Path, created: str) -> bytes:
    """Build the Turtle prologue, written once before any query block.

    Holds the prefixes, the dataset descriptor, and the status
    vocabulary so each status IRI is self-describing. `log_path` is
    recorded as the dataset source and `created` is an xsd:dateTime
    string for when the conversion ran.
    """
    status_lines = "".join(
        f'ml:{status} a ml:Status ; rdfs:label "{status}" .\n'
        for status in STATUS_VALUES
    )
    text = (
        "@prefix ml:      <https://qlever.cs.uni-freiburg.de/metrics-log/> .\n"
        "@prefix q:       "
        "<https://qlever.cs.uni-freiburg.de/metrics-log/query/> .\n"
        "@prefix void:    <http://rdfs.org/ns/void#> .\n"
        "@prefix dcterms: <http://purl.org/dc/terms/> .\n"
        "@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .\n"
        "\n"
        "ml:dataset a void:Dataset ;\n"
        '    dcterms:title "QLever metrics log" ;\n'
        f'    dcterms:source "{log_path}" ;\n'
        f'    dcterms:created "{created}"^^xsd:dateTime .\n'
        "\n"
        f"{status_lines}"
    )
    return text.encode()


def iso_datetime(ms: int) -> str:
    """Format epoch milliseconds as a UTC `xsd:dateTime` string.

    Splits into whole seconds and leftover milliseconds so the
    fractional part stays exact, with no float division.
    """
    seconds, millis = divmod(ms, 1000)
    stamp = datetime.fromtimestamp(seconds, timezone.utc)
    return (
        stamp.replace(microsecond=millis * 1000)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def triples_for(query: PairedQuery, n: int) -> bytes:
    """Build the Turtle block for one query, subject `q:<n>`.

    `qid` and `clientIp` are escaped for the literal. The query text is
    not stored: `queryHash` is a `blake2b` fingerprint of it, so
    identical queries share a value, and `startLineOffset` is the byte
    offset of its start line, so the full text can be seeked out of the
    original log. `startTime`/`endTime` are `xsd:dateTime`; `durationMs`
    stays a plain integer for numeric filtering. `clientIp` is left out
    when empty, and `endTime`/`durationMs` only appear for a query that
    completed. The status is always a known token, so it needs no
    escaping and is written as the IRI `ml:<status>`.
    """
    parts = [
        b"a ml:Query",
        b'ml:qid "' + escape_literal(query.qid).encode() + b'"',
        b'ml:startTime "'
        + iso_datetime(query.start_ms).encode()
        + b'"^^xsd:dateTime',
    ]
    if query.client_ip:
        parts.append(
            b'ml:clientIp "' + escape_literal(query.client_ip).encode() + b'"'
        )
    query_hash = blake2b(query.query, digest_size=16).hexdigest()
    parts.append(b'ml:queryHash "' + query_hash.encode() + b'"')
    parts.append(
        b"ml:startLineOffset " + str(query.start_line_offset).encode()
    )
    parts.append(b"ml:status ml:" + query.status.encode())
    if query.end_ms is not None:
        parts.append(
            b'ml:endTime "'
            + iso_datetime(query.end_ms).encode()
            + b'"^^xsd:dateTime'
        )
        duration_ms = query.end_ms - query.start_ms
        parts.append(b"ml:durationMs " + str(duration_ms).encode())

    body = b" ;\n    ".join(parts)
    return b"q:" + str(n).encode() + b" " + body + b" .\n"


def escape_literal(text: str) -> str:
    """Escape a string for use inside a Turtle double-quoted literal."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


def footer(count: int) -> bytes:
    """Build the closing line recording how many queries were written."""
    return b"\nml:dataset void:entities " + str(count).encode() + b" .\n"
