from __future__ import annotations

import time

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
    """Render a duration as whole seconds."""
    return f"{max(ms, 0) // 1000}s"


class QueryTable(DataTable):
    """Shared base for the Live and Historic query tables.

    Holds only what both screens duplicate: the row cursor, the stored
    rows, and (via module helpers) SPARQL clipping. Each subclass owns
    its own columns, since the two column sets differ.
    """

    def __init__(self, rows: list) -> None:
        """Hold the rows to paint once the table is mounted."""
        super().__init__(cursor_type="row")
        self.query_rows = rows


class LiveQueryTable(QueryTable):
    """Active queries on the Live screen; the only focusable widget."""

    def __init__(self, rows: list[LiveQueryRow]) -> None:
        super().__init__(rows)

    def on_mount(self) -> None:
        """Add the columns and one table row per active query."""
        self.add_column("Query ID")
        self.add_column("Duration", width=8)
        self.add_column("SPARQL")
        now_ms = int(time.time() * 1000)
        for row in self.query_rows:
            self.add_row(
                truncate(row.qid, QID_WIDTH),
                Text(format_duration(now_ms - row.ts_ms), justify="right"),
                truncate(oneline(row.sparql), SPARQL_WIDTH),
            )


class HistoricQueryTable(QueryTable):
    """Finished queries in the current window on the Historic screen."""

    def __init__(self, rows: list[HistoricQueryRow]) -> None:
        super().__init__(rows)

    def on_mount(self) -> None:
        """Add the columns and one table row per finished query."""
        self.add_column("Started")
        self.add_column("Duration", width=8)
        self.add_column("Status")
        self.add_column("SPARQL")
        for row in self.query_rows:
            self.add_row(
                format_clock(row.started_at_ms),
                Text(format_duration(row.duration_ms), justify="right"),
                row.status,
                truncate(oneline(row.sparql), SPARQL_WIDTH),
            )
