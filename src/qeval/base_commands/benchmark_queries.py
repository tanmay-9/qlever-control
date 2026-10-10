from __future__ import annotations

from qlever.commands.benchmark_queries import (
    BenchmarkQueriesCommand as QleverBenchmarkQueriesCommand,
)


class BaseBenchmarkQueriesCommand(QleverBenchmarkQueriesCommand):
    """
    Run benchmark queries against the SPARQL endpoint of an engine.
    """

    def execute(self, args) -> bool:
        args.sparql_endpoint = (
            args.sparql_endpoint
            or f"{args.host_name}:{args.port}{args.endpoint_path}"
        )
        return super().execute(args)
