from __future__ import annotations

from pathlib import Path

from textual.app import App, ComposeResult
from textual.widgets import Footer, TabbedContent, TabPane

from qlever.monitor.views.historic_view import HistoricView
from qlever.monitor.views.live_view import LiveView


class MonitorQueriesApp(App):
    """
    Textual app for the interactive `monitor-queries` TUI

    The app has two tabs:
    1. Live view which tails an active `qlever-server` log file to show
       currently-active queries on the server plus rolling metrics
    2. Historic view which parses the log file in a given time window for
       active queries and metrics.

    In both tabs, the user has the option of selecting a particular query and
    view the full SPARQL query with syntax highlighting plus copy it.
    Slow queries that cross the `--warn-after` threshold are logged in a TSV
    file for future inspection.
    """

    CSS_PATH = "monitor.tcss"

    BINDINGS = [
        ("q", "quit", "Quit/Exit"),
        ("d", "toggle_dark", "Toggle dark mode"),
    ]

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: int,
        warning_log: Path,
        repaint_interval: float = 0.5,
        system: str = "docker",
    ) -> None:
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        self.warning_log = warning_log
        self.repaint_interval = repaint_interval
        self.system = system

    def compose(self) -> ComposeResult:
        with TabbedContent(initial="live-view"):
            with TabPane("Live", id="live-view"):
                yield LiveView(
                    self.log_file,
                    self.timeout,
                    self.warn_after,
                    self.warning_log,
                    self.repaint_interval,
                )
            with TabPane("Historic", id="historic-view"):
                yield HistoricView(
                    self.log_file, self.timeout, self.warn_after
                )
        yield Footer()
