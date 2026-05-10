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
            "server": ["timeout"],
            "runtime": ["system"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--qlever-server-log",
            type=Path,
            help="The `qlever-server` log file (default = {name}.server-log.txt)",
        )
        subparser.add_argument(
            "--warn-after",
            type=int,
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
        subparser.add_argument(
            "--screen_refresh_s",
            type=float,
            default=0.5,
            help="Duration in seconds after which the live view is refreshed "
            "with the new queries from server log file. This only affects the "
            "screen refresh and not the interval at which the queries are "
            "read from the log file (default = 0.5s)",
        )

    def execute(self, args) -> bool:
        if not args.qlever_server_log:
            args.qlever_server_log = Path.cwd() / f"{args.name}.server-log.txt"
        show_msg = (
            f"Reading server logs from {args.qlever_server_log} to display the "
            "currently active queries on the server"
        )
        self.show(show_msg, only_show=args.show)
        if args.show:
            return True

        timeout_s = 30
        if args.warn_after is None:
            try:
                timeout_s = int(args.timeout.rstrip("s"))
            except ValueError:
                log.error(
                    f"Could not parse server timeout {args.timeout!r};"
                    " pass --warn-after explicitly"
                )
                return False
            args.warn_after = max(1, timeout_s - 10)
        if args.warning_log is None:
            args.warning_log = Path.cwd() / f"{args.name}.slow-queries.tsv"
        
        repaint_interval = max(0.5, args.screen_refresh_s)

        MonitorQueriesApp(
            log_file=args.qlever_server_log,
            timeout=timeout_s,
            warn_after=args.warn_after,
            warning_log=args.warning_log,
            repaint_interval=repaint_interval,
            system=args.system,
        ).run()
        return True
