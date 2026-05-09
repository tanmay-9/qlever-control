from __future__ import annotations

import time
from pathlib import Path

from textual.app import App, ComposeResult
from textual.widgets import Footer, TabbedContent, TabPane

from qlever.monitor.historic import HistoricScreen
from qlever.monitor.live import LiveScreen
from qlever.monitor.log_reader import log_time_span

STALE_LOG_THRESHOLD_S = 120


class MonitorLogApp(App):
    """
    Textual app for the interactive monitor-queries TUI.

    Two tabs: Live (tails the log to show currently-active queries plus
    rolling metrics) and Historic (scans a chosen window for completed
    queries; placeholder for now).

    The app reads from `qlever-server` log file and looks for loglevel
    `METRIC` lines. Each query on the server outputs a `started-at` and
    `ended-at` log line with a timestamp and the epoch ms value.

    Double-clicking a row or pressing Enter on a highlighted row shows the
    pretty-printed SPARQL with syntax highlighting in a detailed pane and
    allows the user to copy the pretty-printed query. Slow queries that
    cross the `--warn-after` threshold are logged in a TSV file.
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("d", "toggle_dark", "Toggle dark mode"),
    ]
    CSS = """
    TabbedContent Tabs #tabs-scroll { align-horizontal: center; }
    TabbedContent Tabs #tabs-list-bar { min-width: 0; }
    TabbedContent Tabs #tabs-list { min-width: 0; }
    TabbedContent Tab {
        padding: 0 4;
        text-style: bold;
    }
    TabbedContent Tab.-active {
        color: $text;
    }
    """

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: float,
        warning_log: Path,
        system: str,
    ) -> None:
        """Initialize the app with the args needed by each tab."""
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        self.warning_log = warning_log
        self.system = system
        # Decide once at startup whether the log is fresh enough for Live.
        self.log_start_dt, self.eof_dt = log_time_span(log_file)
        self.is_stale = (
            time.time() - self.eof_dt.timestamp() > STALE_LOG_THRESHOLD_S
        )

    def compose(self) -> ComposeResult:
        """Build the tabbed widget tree."""
        initial = "historic-tab" if self.is_stale else "live-tab"
        with TabbedContent(initial=initial):
            with TabPane("Live", id="live-tab"):
                yield LiveScreen(
                    log_file=self.log_file,
                    timeout=self.timeout,
                    warn_after=self.warn_after,
                    warning_log=self.warning_log,
                    system=self.system,
                    is_stale=self.is_stale,
                )
            with TabPane("Historic", id="historic-tab"):
                yield HistoricScreen(
                    log_file=self.log_file,
                    timeout=self.timeout,
                    warn_after=self.warn_after,
                    log_start_dt=self.log_start_dt,
                    log_end_dt=self.eof_dt,
                )
        yield Footer()
