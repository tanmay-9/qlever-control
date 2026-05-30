from __future__ import annotations

from rich.text import Text
from textual.widgets import DataTable

from qlever.monitor.models import HistoricQueryRow, LiveQueryRow
from qlever.monitor.util import format_clock

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
        # qid -> the cell tuple currently shown for that row.
        self.shown_cells = {}
        # Column keys in display order, recorded when columns are added.
        self.column_keys = ()

    def fill_rows(self) -> None:
        """Add one keyed DataTable row per entry in self.query_rows.

        Records each rendered cell tuple in shown_cells so a later diff
        can compare against what is on screen.
        """
        self.shown_cells = {}
        for row in self.query_rows:
            cells = self.render_row(row)
            self.add_row(*cells, key=row.qid)
            self.shown_cells[row.qid] = cells

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
        self.fill_rows()

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
        self.column_keys = (
            self.add_column("Started"),
            self.add_column("Duration", width=8),
            self.add_column("SPARQL"),
        )
        self.fill_rows()

    def render_row(self, row: LiveQueryRow) -> tuple:
        """Render one active query to its Started/Duration/SPARQL cells."""
        return (
            format_clock(row.started_at_ms),
            Text(format_duration(row.duration_ms), justify="right"),
            truncate(oneline(row.sparql), SPARQL_WIDTH),
        )

    def cursor_qid(self) -> str | None:
        """The qid under the row cursor, or None when nothing valid is selected."""
        index = self.cursor_row
        if self.query_rows and 0 <= index < len(self.query_rows):
            return self.query_rows[index].qid
        return None

    def repin_cursor(self, qid: str, fallback_index: int) -> None:
        """Move the cursor back to qid after a layout change.

        Run after query_rows holds the new list. If qid has left the
        table, clamp to the nearest remaining row.
        """
        for index, row in enumerate(self.query_rows):
            if row.qid == qid:
                self.move_cursor(row=index)
                return
        if self.query_rows:
            self.move_cursor(row=min(fallback_index, len(self.query_rows) - 1))

    def set_rows(self, rows: list[LiveQueryRow]) -> None:
        """Bring the table to the new sorted rows by an incremental diff.

        Removes gone rows, updates only changed cells in place, and
        appends new rows at the bottom. A steady tick touches neither
        scroll nor highlight. Falls back to a full rebuild if the sorted
        order cannot be reproduced by append-at-bottom alone.
        """
        new_cells = {row.qid: self.render_row(row) for row in rows}
        new_order = [row.qid for row in rows]
        old_order = [row.qid for row in self.query_rows]
        new_set, old_set = set(new_order), set(old_order)

        survivors = [qid for qid in old_order if qid in new_set]
        additions = [qid for qid in new_order if qid not in old_set]
        if survivors + additions != new_order:
            super().set_rows(rows)
            return

        cursor_qid = self.cursor_qid()
        old_cursor_row = self.cursor_row
        layout_changed = bool(additions) or len(survivors) != len(old_order)

        for qid in old_order:
            if qid not in new_set:
                self.remove_row(qid)
        for qid in survivors:
            old_row, new_row = self.shown_cells[qid], new_cells[qid]
            for index, column_key in enumerate(self.column_keys):
                if str(old_row[index]) != str(new_row[index]):
                    self.update_cell(qid, column_key, new_row[index])
        for qid in additions:
            self.add_row(*new_cells[qid], key=qid)

        self.query_rows = rows
        self.shown_cells = new_cells
        if layout_changed and cursor_qid is not None:
            self.repin_cursor(cursor_qid, old_cursor_row)


class HistoricQueryTable(QueryTable):
    """Finished queries in the current window on the Historic screen."""

    # Wide enough to fit a sortable header plus its sort arrow.
    SORTABLE_COLUMN_WIDTH = 10

    def on_mount(self) -> None:
        """Add the columns and one table row per finished query."""
        self.column_keys = (
            self.add_column("Started", width=self.SORTABLE_COLUMN_WIDTH),
            self.add_column("Duration", width=self.SORTABLE_COLUMN_WIDTH),
            self.add_column("Status", width=self.SORTABLE_COLUMN_WIDTH),
            self.add_column("SPARQL"),
        )
        self.fill_rows()

    def render_row(self, row: HistoricQueryRow) -> tuple:
        """Render one finished query to Started/Duration/Status/SPARQL cells."""
        return (
            format_clock(row.started_at_ms),
            Text(format_duration(row.duration_ms), justify="right"),
            row.status,
            truncate(oneline(row.sparql), SPARQL_WIDTH),
        )

    def set_sort_indicator(self, column_index: int, descending: bool) -> None:
        """Show a sort arrow on one column header and clear the rest."""
        arrow = " ▼" if descending else " ▲"
        for index, column in enumerate(self.ordered_columns):
            base = column.label.plain.rstrip(" ▲▼")
            label = base + arrow if index == column_index else base
            column.label = Text(label)
        # Invalidate the render cache so the new labels paint, the way
        # update_cell does; Textual has no public column relabel.
        self._update_count += 1
        self.refresh()
