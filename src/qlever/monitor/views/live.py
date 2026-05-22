from __future__ import annotations

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Footer, Static

from qlever.monitor.live_data import (
    get_live_metrics,
    get_live_query_rows,
    get_live_subtitle,
)
from qlever.monitor.live_engine import current_ms
from qlever.monitor.models import SparqlContent
from qlever.monitor.widgets.header_row import HeaderRow
from qlever.monitor.widgets.metrics_row import MetricsRow
from qlever.monitor.widgets.nav_pill import NavPill
from qlever.monitor.widgets.query_table import LiveQueryTable
from qlever.monitor.widgets.sparql_pane import SparqlPane
from qlever.monitor.widgets.status_row import LiveStatusRow
from qlever.util import is_qlever_server_alive

TITLE = "QLever monitor-queries: Live"


class LiveScreen(Screen, inherit_bindings=False):
    """Live view: shows currently active queries tailed from the server log."""

    BINDINGS = [
        Binding("tab", "app.swap_screen", "Historic>", priority=True),
        Binding("f", "toggle_freeze", "Freeze/Unfreeze"),
    ]

    frozen = reactive(False, init=False)

    def compose(self) -> ComposeResult:
        yield HeaderRow(
            center=Static(TITLE),
            right=NavPill("Historic >", target="historic"),
        )
        state = self.app.live_state
        slow_ms = self.app.slow_threshold * 1000
        yield LiveStatusRow(
            get_live_subtitle(
                state, self.app.server_status, self.app.sparql_endpoint
            )
        )
        yield MetricsRow(get_live_metrics(state, slow_ms, current_ms()))
        rows = sorted(get_live_query_rows(state), key=lambda row: row.ts_ms)
        yield LiveQueryTable(rows)
        yield Static("", id="table-status")
        yield SparqlPane()
        yield Footer()

    def on_mount(self) -> None:
        """Start the periodic refresh of table, metrics, and server ping."""
        self.table_timer = self.set_interval(
            self.app.repaint_interval, self.refresh_table
        )
        self.metrics_timer = self.set_interval(2.0, self.refresh_metrics)
        self.live_server_check()
        self.ping_timer = self.set_interval(5.0, self.live_server_check)

    def on_screen_suspend(self) -> None:
        """Pause periodic UI work while Live isn't the active screen."""
        self.table_timer.pause()
        self.metrics_timer.pause()
        self.ping_timer.pause()

    def on_screen_resume(self) -> None:
        """Resume periodic UI work and refresh server status immediately."""
        self.table_timer.resume()
        self.metrics_timer.resume()
        self.ping_timer.resume()
        self.live_server_check()

    @work(thread=True, exclusive=True)
    def live_server_check(self) -> None:
        """Ping the server off the UI thread and push the result back."""
        server_alive = is_qlever_server_alive(
            self.app.sparql_endpoint, max_time=2
        )
        self.app.call_from_thread(self.apply_server_status, server_alive)

    def apply_server_status(self, server_alive: bool) -> None:
        """Cache the latest ping result, then redraw the subtitle once."""
        self.app.server_status = (
            "reachable" if server_alive else "unreachable"
        )
        self.refresh_subtitle()

    def refresh_subtitle(self) -> None:
        """Rebuild the subtitle from the currently cached app state."""
        self.query_one(LiveStatusRow).subtitle = get_live_subtitle(
            self.app.live_state,
            self.app.server_status,
            self.app.sparql_endpoint,
        )

    def refresh_table(self) -> None:
        """Push the current active set into the table; no-op when frozen."""
        if self.frozen:
            return
        state = self.app.live_state
        rows = sorted(get_live_query_rows(state), key=lambda row: row.ts_ms)
        self.query_one(LiveQueryTable).set_rows(rows)
        self.refresh_subtitle()

    def refresh_metrics(self) -> None:
        """Push fresh metric snapshots into MetricsRow; no-op when frozen."""
        if self.frozen:
            return
        state = self.app.live_state
        slow_ms = self.app.slow_threshold * 1000
        self.query_one(MetricsRow).rows = get_live_metrics(
            state, slow_ms, current_ms()
        )

    def watch_frozen(self, frozen: bool) -> None:
        """Reflect the frozen state in the table status line."""
        status = "Frozen - press f to resume" if frozen else ""
        self.query_one("#table-status", Static).update(status)

    def action_toggle_freeze(self) -> None:
        """Toggle the frozen state of the live view."""
        self.frozen = not self.frozen

    def on_resize(self) -> None:
        """Re-evaluate the conditional scroll bindings after a resize."""
        self.call_after_refresh(self.refresh_bindings)

    def on_nav_pill_clicked(self, message: NavPill.Clicked) -> None:
        """Switch to the screen named by the clicked pill."""
        self.app.switch_screen(message.target)

    def on_data_table_row_selected(
        self, message: LiveQueryTable.RowSelected
    ) -> None:
        """Show the selected active query's SPARQL in the pane."""
        row = message.data_table.query_rows[message.cursor_row]
        self.query_one(SparqlPane).content = SparqlContent(
            qid=row.qid,
            started_at_ms=row.ts_ms,
            status=None,
            sparql_text=row.sparql,
        )
