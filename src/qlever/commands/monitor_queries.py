from __future__ import annotations

from pathlib import Path

from qlever.command import QleverCommand
from qlever.log import log
from qlever.monitor.app import MonitorLogApp


class MonitorQueriesCommand(QleverCommand):
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
            "server": ["host_name", "port", "timeout"],
            "runtime": ["system"],
        }

    def additional_arguments(self, subparser) -> None:
        # subparser.add_argument(
        #     "--sparql-endpoint",
        #     help="URL of the SPARQL endpoint, default is {host_name}:{port}",
        # )
        subparser.add_argument(
            "--qlever-server-log",
            type=Path,
            help="The `qlever-server` log file (default = {name}.server-log.txt)",
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
            type=Path,
            help="File to append slow-query warnings to"
            " (default = {name}.slow-queries.tsv)",
        )

    def execute(self, args) -> bool:
        # sparql_endpoint = (
        #     args.sparql_endpoint
        #     if args.sparql_endpoint
        #     else f"{args.host_name}:{args.port}"
        # )

        if not args.qlever_server_log:
            args.qlever_server_log = Path.cwd() / f"{args.name}.server-log.txt"
        show_msg = (
            f"Reading server logs from {args.qlever_server_log} to display the "
            "currently active queries on the server"
        )
        self.show(show_msg, only_show=args.show)
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
            args.warning_log = Path.cwd() / f"{args.name}.slow-queries.tsv"

        MonitorLogApp(
            args.qlever_server_log,
            timeout_s,
            args.warn_after,
            args.warning_log,
            args.system,
        ).run()
        return True
