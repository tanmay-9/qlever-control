from __future__ import annotations

from qlever.commands.query import QueryCommand as QleverQueryCommand


class QueryCommand(QleverQueryCommand):
    """
    Send a query to the SPARQL endpoint of an engine. QLever's option to pin
    the result to its cache, and the access token that this needs, are left
    out.
    """

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {"server": ["host_name", "port"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "query",
            type=str,
            nargs="?",
            default="SELECT * WHERE { ?s ?p ?o } LIMIT 10",
            help="SPARQL query to send",
        )
        subparser.add_argument(
            "--predefined-query",
            type=str,
            choices=self.predefined_queries.keys(),
            help="Use a predefined query",
        )
        subparser.add_argument(
            "--sparql-endpoint", type=str, help="URL of the SPARQL endpoint"
        )
        subparser.add_argument(
            "--accept",
            type=str,
            choices=[
                "text/tab-separated-values",
                "text/csv",
                "application/sparql-results+json",
                "application/sparql-results+xml",
            ],
            default="text/tab-separated-values",
            help="Accept header for the SPARQL query",
        )
        subparser.add_argument(
            "--no-time",
            action="store_true",
            default=False,
            help="Do not print the (end-to-end) time taken",
        )

    def execute(self, args) -> bool:
        args.sparql_endpoint = (
            args.sparql_endpoint
            or f"{args.host_name}:{args.port}{args.endpoint_path}"
        )
        # QLever's `execute` reads this option, which is left out above.
        args.pin_to_cache = False
        return super().execute(args)
