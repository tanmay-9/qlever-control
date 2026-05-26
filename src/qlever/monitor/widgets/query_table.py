from __future__ import annotations

from rich.text import Text
from textual.widgets import DataTable

from qlever.monitor.models import HistoricQueryRow, LiveQueryRow
from qlever.monitor.util import format_clock

QID_WIDTH = 10
SPARQL_WIDTH = 280


def oneline(text: str) -> str:
    """Collapse all runs of whitespace (incl. newlines) to single spaces."""
    return " ".join(text.split())


def truncate(text: str, width: int) -> str:
    """Clip `text` to `width` characters, ending with an ellipsis if cut."""
    if len(text) <= width:
        return text
    return text[: width - 1] + "…"


def format_duration(ms: int) -> str:
    """Render a duration in seconds to one decimal, or '?' when unknown."""
    if ms < 0:
        return "?"
    return f"{ms / 1000:.1f}s"


class QueryTable(DataTable):
    """Shared base for the Live and Historic query tables.

    Holds only what both screens duplicate: the row cursor, the stored
    rows, and (via module helpers) SPARQL clipping. Each subclass owns
    its own columns, since the two column sets differ.
    """

    def __init__(
        self, rows: list[LiveQueryRow] | list[HistoricQueryRow]
    ) -> None:
        """Hold the rows to paint once the table is mounted."""
        super().__init__(cursor_type="row")
        self.query_rows = rows

    def set_rows(
        self, rows: list[LiveQueryRow] | list[HistoricQueryRow]
    ) -> None:
        """Replace the table rows, preserving the cursor position by qid."""
        cursor_qid = None
        old_index = self.cursor_row
        if self.query_rows and 0 <= old_index < len(self.query_rows):
            cursor_qid = self.query_rows[old_index].qid

        self.query_rows = rows
        self.clear(columns=False)
        self.repaint_rows()

        if not self.query_rows:
            return
        if cursor_qid is not None:
            for index, row in enumerate(self.query_rows):
                if row.qid == cursor_qid:
                    self.move_cursor(row=index)
                    return
        self.move_cursor(row=min(old_index, len(self.query_rows) - 1))


class LiveQueryTable(QueryTable):
    """Active queries on the Live screen; the only focusable widget."""

    def on_mount(self) -> None:
        """Add the columns and one table row per active query."""
        self.add_column("Query ID")
        self.add_column("Duration", width=8)
        self.add_column("SPARQL")
        self.repaint_rows()

    def repaint_rows(self) -> None:
        """Add one DataTable row per entry in self.query_rows."""
        for row in self.query_rows:
            self.add_row(
                truncate(row.qid, QID_WIDTH),
                Text(format_duration(row.duration_ms), justify="right"),
                truncate(oneline(row.sparql), SPARQL_WIDTH),
            )


class HistoricQueryTable(QueryTable):
    """Finished queries in the current window on the Historic screen."""

    def on_mount(self) -> None:
        """Add the columns and one table row per finished query."""
        self.add_column("Started")
        self.add_column("Duration", width=8)
        self.add_column("Status")
        self.add_column("SPARQL")
        self.repaint_rows()

    def repaint_rows(self) -> None:
        """Add one DataTable row per entry in self.query_rows."""
        for row in self.query_rows:
            self.add_row(
                format_clock(row.started_at_ms),
                Text(format_duration(row.duration_ms), justify="right"),
                row.status,
                truncate(oneline(row.sparql), SPARQL_WIDTH),
            )
