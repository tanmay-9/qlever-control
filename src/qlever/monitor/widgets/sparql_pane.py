from __future__ import annotations

from rich.console import Group
from rich.syntax import Syntax
from rich.text import Text
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static

from qlever.monitor.util import copy_text
from qlever.util import try_pretty_print_query

HINT = (
    "Double-click a row (or press Enter on a highlighted row) to view its "
    "full SPARQL text. Arrow keys move the cursor without triggering the print."
)


class SparqlPane(VerticalScroll):
    """Scrollable pane that displays the selected query's full SPARQL."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.selected_qid = None
        self.selected_query_text = None

    def compose(self) -> ComposeResult:
        yield Static(HINT, id="sparql-body")

    @property
    def has_selection(self) -> bool:
        return self.selected_query_text is not None

    def show(self, qid: str, query_text: str) -> None:
        self.selected_qid = qid
        self.selected_query_text = query_text
        is_dark = "light" not in self.app.theme
        syntax = Syntax(
            query_text,
            "sparql",
            theme="monokai" if is_dark else "default",
            word_wrap=True,
        )
        body = Group(
            Text(f"Server Query ID: {qid}", style="bold"),
            Text(""),
            syntax,
        )
        self.query_one("#sparql-body", Static).update(body)

    def clear(self) -> None:
        self.selected_qid = None
        self.selected_query_text = None
        self.query_one("#sparql-body", Static).update(HINT)

    def copy(self) -> bool | None:
        if self.selected_query_text is None:
            return False
        return copy_text(self.selected_query_text)

    def pretty_print(self, system: str) -> bool:
        """Run the formatter and update the pane. Blocking — call from a worker thread."""
        if self.selected_qid is None or self.selected_query_text is None:
            return False
        pretty = try_pretty_print_query(self.selected_query_text, True, system)
        if pretty is None:
            return False
        self.app.call_from_thread(self.show, self.selected_qid, pretty)
        return True
