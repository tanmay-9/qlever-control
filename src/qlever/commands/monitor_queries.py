from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime

from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import pretty_printed_query

MAX_CONSECUTIVE_FAILURES = 5


def fetch_queries(monitor_queries_cmd: str) -> dict | None:
    try:
        output = subprocess.check_output(monitor_queries_cmd, shell=True)
    except Exception as e:
        log.error(f"Failed to fetch active queries: {e}")
        return None
    output = output.strip()
    if not output:
        return {}
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError as e:
        log.error(f"Server returned unexpected response: {e}")
        return None
    return parsed if isinstance(parsed, dict) else {}


def server_supports_duration(queries_dict: dict) -> bool:
    return any(isinstance(v, dict) for v in queries_dict.values())


def append_slow_log(
    path: str, event: str, qid: str, duration_s: int, sparql: str = ""
) -> None:
    """Append a single TSV-formatted slow-query event to the warning log."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a") as f:
        f.write(f"{ts}\t{event}\t{qid}\t{duration_s}\t{sparql}\n")


def detect_slow_queries(
    queries_dict: dict,
    slow_seen: dict,
    warn_after: float,
    log_path: str,
) -> None:
    """Log start/finish events for queries that cross the slow threshold.

    Mutates slow_seen in place (qid -> started_at). Appends a 'start' event
    when a query first exceeds warn_after, and a 'finish' event when a
    previously logged slow query is no longer active.
    """
    now_ms = int(time.time() * 1000)

    # Queries that finished since last poll: log with final duration.
    for qid in list(slow_seen):
        if qid not in queries_dict:
            final_s = (now_ms - slow_seen[qid]) // 1000
            append_slow_log(log_path, "finish", qid, final_s)
            del slow_seen[qid]

    # Queries that just crossed the threshold: log start event.
    for qid, info in queries_dict.items():
        if not isinstance(info, dict) or qid in slow_seen:
            continue
        started_at = info.get("started_at")
        if started_at is None:
            continue
        duration_s = (now_ms - started_at) // 1000
        if duration_s >= warn_after:
            slow_seen[qid] = started_at
            sparql = re.sub(r"\s+", " ", info["query"]).strip()
            append_slow_log(log_path, "start", qid, duration_s, sparql)


def compact_slow_log(path: str) -> None:
    """Collapse start/finish event pairs in the log into one row per qid.

    Reads the append-only log, pairs each 'start' with its matching 'finish'
    by qid, and rewrites the file with a single row per query showing the
    final duration (or the last-known duration if the query was still
    running at compaction time). No-op if the log does not exist or has
    no start events.
    """
    starts = {}
    finals = {}
    try:
        with open(path) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t", maxsplit=4)
                if len(parts) < 5:
                    continue
                ts, event, qid, duration_str, sparql = parts
                try:
                    duration_s = int(duration_str)
                except ValueError:
                    continue
                if event == "start":
                    starts[qid] = (ts, duration_s, sparql)
                elif event == "finish":
                    finals[qid] = duration_s
    except FileNotFoundError:
        return
    if not starts:
        return
    with open(path, "w") as f:
        for qid, (logged_at, start_duration, sparql) in starts.items():
            duration = finals.get(qid, start_duration)
            status = "finished" if qid in finals else "unfinished"
            f.write(
                f"{logged_at}\t{status}\t{qid}\t{duration}\t{sparql}\n"
            )


def build_table(
    queries_dict: dict, has_duration: bool, warn_after: float
) -> Table:
    table = Table(
        show_header=True,
        header_style="bold",
        expand=True,
        padding=(0, 1),
    )

    table.add_column("#", width=3, justify="right", no_wrap=True)
    table.add_column(
        "Query ID", min_width=12, max_width=18, overflow="ellipsis"
    )
    if has_duration:
        table.add_column("Duration", width=8, justify="right", no_wrap=True)
    table.add_column(
        "SPARQL",
        ratio=1,
        overflow="ellipsis",
        no_wrap=True,
    )

    now_ms = int(time.time() * 1000)
    for i, (qid, info) in enumerate(queries_dict.items(), 1):
        query_text = info["query"] if isinstance(info, dict) else info
        sparql = re.sub(r"\s+", " ", query_text).strip()
        if has_duration:
            started_at = (
                info.get("started_at") if isinstance(info, dict) else None
            )
            if started_at is not None:
                duration_s = (now_ms - started_at) // 1000
                duration = f"{duration_s}s"
                if duration_s >= warn_after:
                    duration = f"[red]{duration}[/red]"
            else:
                duration = "N/A"
            table.add_row(str(i), qid, duration, sparql)
        else:
            table.add_row(str(i), qid, sparql)
    return table


class MonitorQueriesCommand(QleverCommand):
    """
    Class for executing the `monitor-queries` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return "Show the currently active queries on the server"

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["access_token", "host_name", "port", "timeout"],
            "runtime": ["system"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint",
            help="URL of the SPARQL endpoint, default is {host_name}:{port}",
        )
        subparser.add_argument(
            "--query-id",
            help="Show the full SPARQL text for a specific query,"
            " either by its index (#) or server query ID",
        )
        subparser.add_argument(
            "--interval",
            type=float,
            default=2.0,
            help="Refresh interval in seconds (default = 2.0)",
        )
        subparser.add_argument(
            "--warn-after",
            type=float,
            default=None,
            help="Duration in seconds after which an active query is logged"
            " as slow (default = server timeout - 10s)",
        )
        subparser.add_argument(
            "--warning-log",
            type=str,
            default=None,
            help="File to append slow-query warnings to"
            " (default = {name}.slow-queries.log)",
        )

    def execute(self, args) -> bool:
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )
        monitor_queries_cmd = (
            f'curl -s {sparql_endpoint} --data-urlencode "cmd=dump-active-queries" '
            f'--data-urlencode access-token="{args.access_token}"'
        )

        self.show(monitor_queries_cmd, only_show=args.show)
        if args.show:
            return True

        if args.interval < 0.5:
            log.error("--interval must be at least 0.5 seconds")
            return False

        if args.warn_after is None:
            try:
                timeout_s = float(args.timeout.rstrip("s"))
            except ValueError:
                log.error(
                    f"Could not parse server timeout {args.timeout!r};"
                    " pass --warn-after explicitly"
                )
                return False
            args.warn_after = max(1.0, timeout_s - 10)
        if args.warning_log is None:
            args.warning_log = f"{args.name}.slow-queries.log"

        console = Console()

        # Show full SPARQL for a specific query.
        if args.query_id:
            queries_dict = fetch_queries(monitor_queries_cmd)
            if queries_dict is None:
                return False
            queries = list(queries_dict.items())
            try:
                # When user passes row index as query id
                idx = int(args.query_id)
                info = (
                    queries[idx - 1][1] if 1 <= idx <= len(queries) else None
                )
            except ValueError:
                # When user passes server query id directly
                info = queries_dict.get(args.query_id)
            if not info:
                log.error("No active query found for the given ID")
                return False
            query_text = info["query"] if isinstance(info, dict) else info
            log.info(pretty_printed_query(query_text, False, args.system))
            return True

        has_duration = None
        failures = 0
        slow_seen = {}
        try:
            with Live(console=console, refresh_per_second=4) as live:
                while True:
                    queries_dict = fetch_queries(monitor_queries_cmd)
                    if queries_dict is None:
                        failures += 1
                        if failures > MAX_CONSECUTIVE_FAILURES:
                            log.error(
                                f"Failed to fetch active queries more than "
                                f"{MAX_CONSECUTIVE_FAILURES} times. Exiting..."
                            )
                            compact_slow_log(args.warning_log)
                            return False
                        live.update(
                            "Failed to fetch active queries, retrying..."
                        )
                    else:
                        # Reset the failures on successful fetch
                        failures = 0
                        # Lock in the format on the first non-empty fetch.
                        if has_duration is None and queries_dict:
                            has_duration = server_supports_duration(
                                queries_dict
                            )
                        if has_duration:
                            detect_slow_queries(
                                queries_dict,
                                slow_seen,
                                args.warn_after,
                                args.warning_log,
                            )
                        table = build_table(
                            queries_dict,
                            bool(has_duration),
                            args.warn_after,
                        )
                        # Approx. rows rich can show: terminal height minus
                        # top/bottom border, header row, header separator,
                        # and one line for the caption above.
                        max_rows = max(1, console.size.height - 5)
                        hidden = max(0, len(queries_dict) - max_rows)
                        if hidden > 0:
                            caption = Text(
                                f"{len(queries_dict)} active queries, "
                                f"{hidden} not shown",
                                justify="center",
                                style="bold",
                            )
                            live.update(Group(caption, table))
                        else:
                            live.update(table)
                    time.sleep(args.interval)
        except KeyboardInterrupt:
            compact_slow_log(args.warning_log)
            return True
