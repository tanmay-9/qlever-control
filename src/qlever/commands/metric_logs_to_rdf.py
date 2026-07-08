from __future__ import annotations

import sys
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from qlever.command import QleverCommand
from qlever.log import log
from qlever.monitor_queries.log_reader import LogBuffer, open_log_buffer
from qlever.monitor_queries.log_to_rdf import (
    PairedQuery,
    footer,
    header,
    iso_datetime,
    pair_queries,
    triples_for,
)


def write_turtle(
    buf: LogBuffer,
    out: BinaryIO,
    log_path: Path,
    created: str,
    pad_ms: int,
) -> Iterator[PairedQuery]:
    """Write the whole Turtle document to `out`, yielding each query.

    Emits the header, one block per query from `pair_queries` numbered
    from 1 (subjects q:1, q:2, ...), then the footer with the final
    count. Each query is yielded after its block is written, so a caller
    can drive a progress bar; a caller that wants none just exhausts it.
    """
    out.write(header(log_path, created))
    count = 0
    for n, query in enumerate(pair_queries(buf, pad_ms), start=1):
        out.write(triples_for(query, n))
        count = n
        yield query
    out.write(footer(count))


def convert_with_progress(
    buf: LogBuffer,
    ttl_path: Path,
    log_path: Path,
    created: str,
    pad_ms: int,
) -> int:
    """Write the Turtle file while showing a progress bar.

    Completion is measured by how far into the log file the converter
    has read: each query advances the bar to the byte offset of its
    start line, against the total file size. Returns the number of
    queries written.
    """
    total_bytes = len(buf)
    processed = 0
    seen_bytes = 0
    with (
        ttl_path.open("wb") as out,
        Progress(
            SpinnerColumn(spinner_name="dots"),
            TextColumn("[bold blue]Converting metrics log to RDF"),
            TextColumn(" "),
            BarColumn(bar_width=50),
            TextColumn(" "),
            TaskProgressColumn(),
            TextColumn(" "),
            TextColumn("{task.fields[queries]} queries"),
            TextColumn(" "),
            TimeRemainingColumn(),
        ) as progress,
    ):
        task = progress.add_task("convert", total=total_bytes, queries=0)
        for query in write_turtle(buf, out, log_path, created, pad_ms):
            processed += 1
            seen_bytes = max(seen_bytes, query.start_line_offset)
            if processed % 1000 == 0:
                progress.update(task, completed=seen_bytes, queries=processed)
        progress.update(task, completed=total_bytes, queries=processed)
    return processed


class MetricLogsToRdfCommand(QleverCommand):
    """
    Class for executing the `metric-logs-to-rdf` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Convert QLever's `metrics-log.jsonl` into RDF (Turtle), written "
            "to a `.ttl` file (or stdout), so the queries can be indexed and "
            "analysed with SPARQL"
        )

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["timeout"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--metrics-log",
            type=Path,
            help=(
                "QLever's `metrics-log.jsonl` log file "
                "(default = {name}.metrics-log.jsonl)"
            ),
        )
        subparser.add_argument(
            "--stdout",
            action="store_true",
            default=False,
            help="Write the raw Turtle to stdout instead of a `.ttl` file; "
            "no progress bar is shown in this mode",
        )

    def execute(self, args) -> bool:
        if not args.metrics_log:
            args.metrics_log = Path.cwd() / f"{args.name}.metrics-log.jsonl"
        ttl_path = args.metrics_log.with_suffix(".ttl")

        try:
            timeout_s = int(args.timeout.rstrip("s"))
        except ValueError:
            log.error(f"Could not parse server timeout {args.timeout!r}")
            return False
        pad_ms = 2000 * timeout_s

        destination = "stdout" if args.stdout else str(ttl_path)
        show_msg = (
            f"Converting {args.metrics_log} to RDF (Turtle), writing to "
            f"{destination}."
        )
        self.show(show_msg, only_show=args.show)
        if args.show:
            return True

        if not args.metrics_log.is_file():
            log.error(f"Log file not found: {args.metrics_log}")
            return False

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        created = iso_datetime(now_ms)
        with open_log_buffer(args.metrics_log) as buf:
            if buf is None:
                log.error(f"Log file is empty: {args.metrics_log}")
                return False
            if args.stdout:
                for _ in write_turtle(
                    buf, sys.stdout.buffer, args.metrics_log, created, pad_ms
                ):
                    pass
            else:
                count = convert_with_progress(
                    buf, ttl_path, args.metrics_log, created, pad_ms
                )
                log.info(f"Wrote {count} queries to {ttl_path}")
        return True
