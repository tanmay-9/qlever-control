from __future__ import annotations

from qlever.command import QleverCommand
from qlever.log import log
from qlever.monitor_queries_app import MonitorApp, compact_slow_log


class MonitorQueriesTuiCommand(QleverCommand):
    """
    Class for executing the `monitor-queries-tui` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Show the currently active queries on the server (interactive TUI)"
        )

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
            " (default = {name}.slow-queries.tsv)",
        )

    def execute(self, args) -> bool:
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )
        if args.interval < 1:
            log.error("--interval must be at least 1 second")
            return False
        # --max-time bounds end-to-end fetch time. Lower bound 5s so a
        # busy server doesn't get classified as failed; upper cap 30s
        # so failure detection stays responsive at long intervals.
        max_time = max(5, min(int(args.interval), 30))
        monitor_queries_cmd = (
            f'curl -s --max-time {max_time} {sparql_endpoint} '
            f'--data-urlencode "cmd=dump-active-queries" '
            f'--data-urlencode access-token="{args.access_token}"'
        )

        self.show(monitor_queries_cmd, only_show=args.show)
        if args.show:
            return True

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
            args.warning_log = f"{args.name}.slow-queries.tsv"

        try:
            MonitorApp(
                monitor_queries_cmd,
                args.interval,
                args.warn_after,
                args.warning_log,
                args.system,
            ).run()
        finally:
            compact_slow_log(args.warning_log)
        return True
