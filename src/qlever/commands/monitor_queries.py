from __future__ import annotations

import json
import re
import subprocess
import time

from rich.console import Console
from rich.live import Live
from rich.table import Table

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import pretty_printed_query


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


def build_table(queries_dict: dict, has_duration: bool) -> Table:
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
            duration = (
                f"{(now_ms - started_at) // 1000}s"
                if started_at is not None
                else "N/A"
            )
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
            "server": ["access_token", "host_name", "port"],
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
            "--watch",
            action="store_true",
            default=False,
            help="Continuously refresh the list of active queries"
            " until interrupted with Ctrl-C",
        )
        subparser.add_argument(
            "--interval",
            type=float,
            default=2.0,
            help="Refresh interval in seconds when using --watch"
            " (default: 2.0)",
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

        if args.watch and args.interval < 0.5:
            log.error("--interval must be at least 0.5 seconds")
            return False
        if args.watch and args.query_id:
            log.error("--watch cannot be combined with --query-id")
            return False

        console = Console()

        # One-shot: show full SPARQL for a specific query.
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

        # Watch mode: refresh the table in place.
        if args.watch:
            has_duration = None
            try:
                with Live(console=console, refresh_per_second=4) as live:
                    while True:
                        queries_dict = fetch_queries(monitor_queries_cmd)
                        if queries_dict is None:
                            live.update(
                                "(failed to fetch active queries, retrying...)"
                            )
                        else:
                            # Lock in the format on the first non-empty fetch.
                            if has_duration is None and queries_dict:
                                has_duration = server_supports_duration(
                                    queries_dict
                                )
                            live.update(
                                build_table(queries_dict, bool(has_duration))
                            )
                        time.sleep(args.interval)
            except KeyboardInterrupt:
                return True

        # One-shot: print the table once.
        queries_dict = fetch_queries(monitor_queries_cmd)
        if queries_dict is None:
            return False
        has_duration = server_supports_duration(queries_dict)
        console.print(build_table(queries_dict, has_duration))
        return True
