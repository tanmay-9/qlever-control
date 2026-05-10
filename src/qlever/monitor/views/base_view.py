from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Container
from textual.widgets import DataTable

from qlever.monitor.util import clipboard_install_hint
from qlever.monitor.widgets.metrics_pane import MetricsPane
from qlever.monitor.widgets.query_table import QueryTable
from qlever.monitor.widgets.sparql_pane import SparqlPane


class BaseView(Container):
    """Composes [metrics, table, sparql] and wires row-selection + copy/clear."""

    BINDINGS = [
        ("c", "clear_detail", "Clear SPARQL"),
        ("y", "copy_sparql", "Copy SPARQL"),
        ("p", "pprint_sparql", "Pretty-print SPARQL"),
    ]

    def compose(self) -> ComposeResult:
        yield self.build_metrics_pane()
        yield self.build_query_table()
        yield SparqlPane()

    def build_metrics_pane(self) -> MetricsPane:
        raise NotImplementedError

    def build_query_table(self) -> QueryTable:
        raise NotImplementedError

    def get_query_text(self, qid: str) -> str | None:
        raise NotImplementedError

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        qid = event.row_key.value if event.row_key else None
        if qid is None:
            return
        pane = self.query_one(SparqlPane)
        if qid == pane.selected_qid:
            return
        query_text = self.get_query_text(qid)
        if query_text is None:
            return
        pane.show(qid, query_text)

    def action_clear_detail(self) -> None:
        self.query_one(SparqlPane).clear()

    def action_copy_sparql(self) -> None:
        pane = self.query_one(SparqlPane)
        if not pane.has_selection:
            self.app.notify("No query selected", severity="warning")
            return
        result = pane.copy()
        if result is True:
            self.app.notify("Copied")
        elif result is None:
            self.app.notify(
                f"No clipboard tool found — {clipboard_install_hint()}",
                severity="error",
            )
        else:
            self.app.notify("Copy failed", severity="error")

    def action_pprint_sparql(self) -> None:
        pane = self.query_one(SparqlPane)
        if not pane.has_selection:
            self.app.notify("No query selected", severity="warning")
            return
        self.run_worker(self.pprint_worker, thread=True, exclusive=True)

    def pprint_worker(self) -> None:
        pane = self.query_one(SparqlPane)
        if not pane.pretty_print(self.app.system):
            self.app.call_from_thread(
                self.app.notify,
                "Failed to pretty-print the query",
                severity="error",
            )
