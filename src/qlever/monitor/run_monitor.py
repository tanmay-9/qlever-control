from pathlib import Path

from qlever.monitor.app import MonitorQueriesApp

app = MonitorQueriesApp(
    log_file=Path.cwd() / "wikidata.server-log.txt",
    timeout=800,
    warn_after=5000,
    warning_log=Path("/tmp/qlever_warnings.tsv"),
    system="docker",
)
app.run()
