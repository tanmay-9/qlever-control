from __future__ import annotations

import re

from rich.text import Text
from textual.widgets import DataTable

from qlever.monitor.util import truncate

QID_COL_WIDTH = 15
SPARQL_PREVIEW_LEN = 300


def duration_sort_key(cell) -> float:
    """Parse a duration cell ('5.5s' or '>5.5s') into a float for sort."""
    text = cell.plain if hasattr(cell, "plain") else str(cell)
    try:
        return float(text.lstrip(">").rstrip("s"))
    except ValueError:
        return -1.0


class QueryTable(DataTable):
    """DataTable with qid / duration / sparql columns. Renders durations
    >= warn_after in red when warn_after is set.
    """

    def __init__(self, warn_after: float | None = None, **kwargs) -> None:
        super().__init__(cursor_type="row", **kwargs)
        self.warn_after = warn_after

    def on_mount(self) -> None:
        self.add_column("Query ID", width=QID_COL_WIDTH, key="qid")
        self.add_column(
            Text("Duration", justify="right"), width=10, key="duration"
        )
        self.add_column("SPARQL", key="sparql")

    def duration_style(self, duration_s: float) -> str:
        if self.warn_after is not None and duration_s >= self.warn_after:
            return "red"
        return ""

    def format_duration_cell(self, duration_s: float) -> Text:
        return Text(
            f"{duration_s:.1f}s",
            style=self.duration_style(duration_s),
            justify="right",
        )

    def format_sparql_preview(self, query: str) -> str:
        return truncate(re.sub(r"\s+", " ", query).strip(), SPARQL_PREVIEW_LEN)

    def add_query_row(
        self, qid: str, query_text: str, duration_s: float
    ) -> None:
        self.add_row(
            truncate(qid, QID_COL_WIDTH),
            self.format_duration_cell(duration_s),
            self.format_sparql_preview(query_text),
            key=qid,
        )

    def update_duration_cell(self, qid: str, duration_s: float) -> None:
        self.update_cell(
            qid, "duration", self.format_duration_cell(duration_s)
        )

    def sort_by_duration(self) -> None:
        self.sort("duration", key=duration_sort_key, reverse=True)


class HistoricQueryTable(QueryTable):
    """Query table for the historic view: active rows render '>Xs' to mark
    their duration as a lower bound; coloring is inherited from the base.
    """

    def format_duration_cell(
        self, duration_s: float, is_active: bool = False
    ) -> Text:
        prefix = ">" if is_active else ""
        return Text(
            f"{prefix}{duration_s:.1f}s",
            style=self.duration_style(duration_s),
            justify="right",
        )

    def add_query_row(
        self,
        qid: str,
        query_text: str,
        duration_s: float,
        is_active: bool = False,
    ) -> None:
        self.add_row(
            truncate(qid, QID_COL_WIDTH),
            self.format_duration_cell(duration_s, is_active=is_active),
            self.format_sparql_preview(query_text),
            key=qid,
        )
