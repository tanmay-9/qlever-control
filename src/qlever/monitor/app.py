from __future__ import annotations

import time
from pathlib import Path

from textual import work
from textual.app import App
from textual.binding import Binding
from textual.css.query import NoMatches
from textual.worker import get_current_worker

from qlever.monitor.live_data import (
    LIVE_HORIZON_MS,
    LiveLogReader,
    LiveState,
    current_ms,
    find_active_queries,
    load_completed_history,
)
from qlever.monitor.log_reader import read_first_timestamp
from qlever.monitor.util import clipboard_install_hint, copy_text
from qlever.monitor.views.historic import HistoricScreen
from qlever.monitor.views.live import LiveScreen
from qlever.monitor.widgets.sparql_pane import SparqlPane, SparqlScroll
from qlever.util import pretty_printed_query


class MonitorQueriesApp(App):
    """
    Textual app for the interactive `monitor-queries` TUI

    The app has two tabs:
    1. Live view which tails an active `qlever-server` metrics.log file to
       show currently-active queries on the server plus rolling metrics
    2. Historic view which parses the log file in a given time window for
       active queries and metrics.

    In both tabs, the user has the option of selecting a particular query and
    view the full SPARQL query with syntax highlighting plus copy it.
    """

    CSS_PATH = "monitor.tcss"

    SCREENS = {"live": LiveScreen, "historic": HistoricScreen}

    BINDINGS = [
        ("q", "quit", "Quit/Exit"),
        Binding("t", "cycle_themes", "Change theme", key_display="T/t"),
        Binding("T", "cycle_themes(-1)", "Previous theme", show=False),
        ("y", "copy_query", "Copy SPARQL"),
        ("p", "pretty_print", "Pretty print"),
        ("c", "clear_query", "Clear SPARQL"),
        Binding(
            "shift+up",
            "scroll_sparql_up",
            "Scroll SPARQL",
            key_display="⇧ ↑↓",
        ),
        Binding(
            "shift+down", "scroll_sparql_down", "Scroll SPARQL", show=False
        ),
    ]

    def __init__(
        self,
        log_file: Path,
        sparql_endpoint: str,
        timeout: int,
        slow_threshold: int,
        repaint_interval: float = 0.5,
        system: str = "docker",
    ) -> None:
        super().__init__()
        self.log_file = log_file
        self.sparql_endpoint = sparql_endpoint
        self.timeout = timeout
        self.window_pad_ms = 2000 * timeout
        self.slow_threshold = slow_threshold
        self.repaint_interval = repaint_interval
        self.system = system
        self.server_status = "checking"
        self.live_state = LiveState()

    def on_mount(self) -> None:
        """Boot the live engine, then open the Live screen."""
        self.boot_time_ms = current_ms()
        state, cut_offset, _ = find_active_queries(
            self.log_file, self.window_pad_ms
        )
        self.live_state = state
        self.cut_offset = cut_offset
        with self.log_file.open("rb") as log_stream:
            self.log_start_ms = read_first_timestamp(
                log_stream, self.log_file.stat().st_size
            )
        self.tail_live_log()
        self.load_metrics_history()
        self.push_screen("live")

    @work(thread=True, exclusive=True)
    def tail_live_log(self) -> None:
        """Poll the log forever, dispatching new events into LiveState."""
        tailer = LiveLogReader(
            self.log_file,
            self.live_state,
            self.cut_offset,
            self.window_pad_ms,
        )
        worker = get_current_worker()
        with self.log_file.open("rb") as log_stream:
            while not worker.is_cancelled:
                tailer.poll(log_stream)
                time.sleep(tailer.poll_interval)

    @work(thread=True)
    def load_metrics_history(self) -> None:
        """Prepend the hour before cut_offset into completed history so metrics aren't empty at boot."""
        load_completed_history(self.log_file, self.live_state, self.cut_offset)
        # Backfill is done; record how far back metrics have data.
        log_start_or_boot = self.log_start_ms or self.boot_time_ms
        self.live_state.metrics_known_from_ms = max(
            log_start_or_boot, self.boot_time_ms - LIVE_HORIZON_MS
        )

    def action_swap_screen(self) -> None:
        """Toggle between Live and Historic (bound to Tab on each screen)."""
        target = "historic" if isinstance(self.screen, LiveScreen) else "live"
        self.switch_screen(target)

    def action_copy_query(self) -> None:
        """Copy the displayed query's SPARQL to the system clipboard."""
        pane = self.screen.query_one(SparqlPane)
        if pane.displayed_text is None:
            self.notify("No query selected", severity="warning")
            return
        result = copy_text(pane.displayed_text)
        if result is True:
            self.notify("SPARQL copied to clipboard")
        elif result is None:
            self.notify(
                f"No clipboard tool found: {clipboard_install_hint()}",
                severity="warning",
            )
        else:
            self.notify("Clipboard tool failed", severity="error")

    def action_pretty_print(self) -> None:
        """Toggle pretty-printed SPARQL in the pane for the selected query."""
        pane = self.screen.query_one(SparqlPane)
        if pane.content is None:
            self.notify("No query selected", severity="warning")
            return
        if pane.show_pretty:
            pane.show_pretty = False
            return
        pane.show_pretty = True
        # A cached result means the formatter already ran for this query.
        if pane.pretty_text is None:
            self.pretty_print_worker(pane, pane.content.sparql_text)

    @work(thread=True, exclusive=True)
    def pretty_print_worker(self, pane: SparqlPane, raw: str) -> None:
        """Run the blocking sparql-formatter off the UI thread."""
        result = pretty_printed_query(
            raw, show_prefixes=True, system=self.system
        )
        self.call_from_thread(self.apply_pretty_result, pane, raw, result)

    def apply_pretty_result(
        self, pane: SparqlPane, raw: str, result: str | None
    ) -> None:
        """Apply the formatter output, unless the selection moved on."""
        if pane.content is None or pane.content.sparql_text != raw:
            return
        if result is None:
            pane.show_pretty = False
            self.notify("Could not pretty-print this query", severity="error")
            return
        pane.pretty_text = result

    def action_clear_query(self) -> None:
        """Drop the displayed query, restoring the empty-state hint."""
        pane = self.screen.query_one(SparqlPane)
        if pane.content is None:
            return
        pane.content = None

    def action_scroll_sparql_up(self) -> None:
        """Scroll the overflowing SPARQL pane up one line."""
        self.screen.query_one(SparqlScroll).scroll_up()

    def action_scroll_sparql_down(self) -> None:
        """Scroll the overflowing SPARQL pane down one line."""
        self.screen.query_one(SparqlScroll).scroll_down()

    def check_action(
        self, action: str, parameters: tuple[object, ...]
    ) -> bool | None:
        """Show the scroll bindings only when the query overflows the pane.

        Returns False (hidden) rather than None (grayed) so the footer
        entry disappears entirely until there is something to scroll.
        """
        if action in ("scroll_sparql_up", "scroll_sparql_down"):
            try:
                scroll = self.screen.query_one(SparqlScroll)
            except NoMatches:
                return False
            return scroll.max_scroll_y > 0
        return True

    def action_cycle_themes(self, direction: int = 1) -> None:
        """Step through themes; `direction` is +1 for `t`, -1 for `T`."""
        themes = list(self.available_themes)
        selected_theme_idx = themes.index(self.theme)
        self.theme = themes[(selected_theme_idx + direction) % len(themes)]
