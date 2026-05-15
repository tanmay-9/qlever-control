from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer, Static

from qlever.monitor.live_stubs import (
    get_live_metrics,
    get_live_query_rows,
    get_live_subtitle,
)
from qlever.monitor.models import SparqlContent
from qlever.monitor.widgets.header_row import HeaderRow
from qlever.monitor.widgets.metrics_row import MetricsRow
from qlever.monitor.widgets.nav_pill import NavPill
from qlever.monitor.widgets.query_table import LiveQueryTable
from qlever.monitor.widgets.sparql_pane import SparqlPane
from qlever.monitor.widgets.status_row import LiveStatusRow

TITLE = "QLever monitor-queries: Live"


class LiveScreen(Screen, inherit_bindings=False):
    """Live view: shows currently active queries tailed from the server log."""

    BINDINGS = [Binding("tab", "app.swap_screen", "Historic>", priority=True)]

    def compose(self) -> ComposeResult:
        yield HeaderRow(
            center=Static(TITLE),
            right=NavPill("Historic >", target="historic"),
        )
        yield LiveStatusRow(get_live_subtitle(), self.app.sparql_endpoint)
        yield MetricsRow(get_live_metrics())
        rows = sorted(get_live_query_rows(), key=lambda r: r.ts_ms)
        yield LiveQueryTable(rows)
        yield Static("", id="table-status")
        yield SparqlPane()
        yield Footer()

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
