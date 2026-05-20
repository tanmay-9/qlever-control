from __future__ import annotations

from pathlib import Path

from textual import work
from textual.app import App
from textual.binding import Binding
from textual.css.query import NoMatches

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
        ("t", "cycle_themes", "Change theme"),
        ("y", "copy_query", "Copy SPARQL"),
        ("p", "pretty_print", "Pretty print"),
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

    def on_mount(self) -> None:
        """Open the Live screen on startup."""
        self.push_screen("live")

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

    def action_cycle_themes(self) -> None:
        """Select the next theme on press of `t` binding"""
        themes = [t for t in self.available_themes if "ansi" not in t]
        selected_theme_idx = themes.index(self.theme)
        self.theme = themes[(selected_theme_idx + 1) % len(themes)]
