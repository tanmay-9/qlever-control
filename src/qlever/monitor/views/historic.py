from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer, Static

from qlever.monitor.historic_stubs import (
    get_controls_state,
    get_historic_metrics,
    get_historic_query_rows,
    get_timeline_bounds,
)
from qlever.monitor.models import SparqlContent
from qlever.monitor.widgets.controls_row import HistoricControlsRow
from qlever.monitor.widgets.header_row import HeaderRow
from qlever.monitor.widgets.metrics_row import MetricsRow
from qlever.monitor.widgets.nav_pill import NavPill
from qlever.monitor.widgets.query_table import HistoricQueryTable
from qlever.monitor.widgets.sparql_pane import SparqlPane
from qlever.monitor.widgets.timeline import Timeline

TITLE = "QLever monitor-queries: Historic"


class HistoricScreen(Screen, inherit_bindings=False):
    """Historic view: shows active queries parsed from the log over a time window."""

    BINDINGS = [Binding("tab", "app.swap_screen", "<Live", priority=True)]

    def compose(self) -> ComposeResult:
        yield HeaderRow(
            left=NavPill("< Live", target="live"),
            center=Static(TITLE),
        )
        yield HistoricControlsRow(get_controls_state())
        yield Timeline(get_timeline_bounds())
        yield MetricsRow([get_historic_metrics()])
        rows = sorted(
            get_historic_query_rows(),
            key=lambda r: r.duration_ms,
            reverse=True,
        )
        yield HistoricQueryTable(rows)
        yield Static(
            f"Showing {len(rows)} queries sorted by duration desc",
            id="table-status",
        )
        yield SparqlPane()
        yield Footer()

    def on_nav_pill_clicked(self, message: NavPill.Clicked) -> None:
        """Switch to the screen named by the clicked pill."""
        self.app.switch_screen(message.target)

    def on_data_table_row_selected(
        self, message: HistoricQueryTable.RowSelected
    ) -> None:
        """Show the selected finished query's SPARQL in the pane."""
        row = message.data_table.query_rows[message.cursor_row]
        self.query_one(SparqlPane).content = SparqlContent(
            qid=row.qid,
            started_at_ms=row.started_at_ms,
            status=row.status,
            sparql_text=row.sparql,
        )
