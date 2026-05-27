from __future__ import annotations

from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.monitor.app import MonitorQueriesApp


class MonitorQueriesCommand(QleverCommand):
    """
    Class for executing the `monitor-queries-tui` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Show the currently active and historically all the active queries "
            "on the server (interactive TUI)"
        )

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["host_name", "port", "timeout"],
            "runtime": ["system"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint",
            type=str,
            help="URL of the SPARQL endpoint (default = {host_name}:{port})",
        )
        subparser.add_argument(
            "--metrics-log",
            type=Path,
            help=(
                "QLever's `metrics-log.jsonl` log file "
                "(default = {name}.metrics-log.jsonl)"
            ),
        )
        subparser.add_argument(
            "--slow-threshold",
            type=int,
            default=None,
            help="Duration in seconds above which a query (active or"
            " completed) is counted as slow in the metrics"
            " (default = server timeout - 10s)",
        )

    def execute(self, args) -> bool:
        if not args.metrics_log:
            args.metrics_log = Path.cwd() / f"{args.name}.metrics-log.jsonl"
        show_msg = (
            f"Reading server logs from {args.metrics_log} to display the "
            "currently active queries on the server"
        )
        self.show(show_msg, only_show=args.show)
        if args.show:
            return True

        if not args.metrics_log.is_file():
            log.error(f"Log file not found: {args.metrics_log}")
            return False

        timeout_s = 30
        if args.slow_threshold is None:
            try:
                timeout_s = int(args.timeout.rstrip("s"))
            except ValueError:
                log.error(
                    f"Could not parse server timeout {args.timeout!r};"
                    " pass --slow-threshold explicitly"
                )
                return False
            args.slow_threshold = max(1, timeout_s - 10)

        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )

        MonitorQueriesApp(
            log_file=args.metrics_log,
            sparql_endpoint=sparql_endpoint,
            timeout=timeout_s,
            slow_threshold=args.slow_threshold,
            system=args.system,
        ).run()
        return True
