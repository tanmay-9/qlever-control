from __future__ import annotations

from qlever.command import QleverCommand
from qlever.resource_usage.resource_monitor import ResourceMonitor
from qlever.util import process_cmdline_regex

# Seconds between resource-usage samples of the running server.
SAMPLE_INTERVAL_S = 1.0


class ServerResourceMonitorCommand(QleverCommand):
    """
    Class for executing the `server-resource-monitor` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Sample the running server's resource usage (memory, CPU) to "
            "the resource-usage log; spawned by `qlever start`"
        )

    def show_in_help(self) -> bool:
        return False

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["server_binary"],
            "runtime": ["system", "server_container"],
        }

    def additional_arguments(self, subparser) -> None:
        pass

    def execute(self, args) -> bool:
        if not args.server_container:
            args.server_container = f"qlever.server.{args.name}"
        cmdline_regex = process_cmdline_regex(args.server_binary, args.name)
        show_msg = (
            f"Sampling resource usage of the `{args.name}` server every "
            f"{SAMPLE_INTERVAL_S}s until it is gone"
        )
        self.show(show_msg, only_show=args.show)
        if args.show:
            return True

        ResourceMonitor(
            dataset=args.name,
            cmdline_regex=cmdline_regex,
            container=args.server_container,
            system=args.system,
            interval=SAMPLE_INTERVAL_S,
            log_name=f"{args.name}.server.resource-usage-log.tsv",
            append_mode=True,
            use_epoch_ms=True,
        ).run_until_server_gone()
        return True
