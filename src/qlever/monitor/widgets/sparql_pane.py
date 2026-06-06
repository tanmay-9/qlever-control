from __future__ import annotations

from rich.style import Style
from rich.syntax import Syntax
from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.reactive import reactive
from textual.widgets import Static

from qlever.monitor.models import SparqlContent
from qlever.monitor.util import format_timestamp

LIGHT_SYNTAX_THEME = "friendly"
DARK_SYNTAX_THEME = "monokai"


def format_header(content: SparqlContent) -> str:
    """One-line query identity shown above the SPARQL body."""
    parts = [
        f"[b]Server Query ID:[/b] {content.qid}",
        f"[b]Client IP:[/b] {content.client_ip or '-'}",
        f"[b]Started at:[/b] {format_timestamp(content.started_at_ms)}",
    ]
    if content.status is not None:
        parts.append(f"[b]Status:[/b] {content.status}")
    return "  │  ".join(parts)


class SparqlBody(Static):
    """Syntax-highlighted SPARQL whose theme follows the app theme.

    render() returns highlighted Text rather than a Syntax renderable:
    Text carries the per-character offsets Textual needs for mouse
    selection. The theme's per-character background is stripped, so the
    query renders on the app background, spans the full pane, and lets
    the selection highlight show through.
    """

    code = reactive(None, layout=True)

    def syntax_theme_name(self) -> str:
        """Pygments theme name for the active light or dark app theme."""
        if self.app.current_theme.dark:
            return DARK_SYNTAX_THEME
        return LIGHT_SYNTAX_THEME

    def render(self):
        if self.code is None:
            return ""
        highlighted = Syntax(
            self.code, "sparql", theme=self.syntax_theme_name()
        ).highlight(self.code)
        highlighted.rstrip()
        # highlight() sets a base background plus a per-character one;
        # drop both so the query renders on the app background and the
        # selection highlight is not masked.
        highlighted.style = ""
        highlighted.spans = [
            span._replace(
                style=span.style.without_color + Style(color=span.style.color)
            )
            for span in highlighted.spans
        ]
        return highlighted


class SparqlScroll(VerticalScroll):
    """Scroll region for an overflowing query; never takes focus."""

    can_focus = False


class SparqlPane(Vertical):
    """Identity line plus full SPARQL for the row under the table cursor.

    Shared by Live and Historic. The screen builds the SparqlContent and
    fills `status` (Live: None, Historic: real status); the pane never
    reads stub data itself.
    """

    can_focus = False

    content = reactive(None, init=False)
    show_pretty = reactive(False, init=False)
    pretty_text = reactive(None, init=False)

    def compose(self) -> ComposeResult:
        yield Static(id="sparql-header")
        with SparqlScroll(id="sparql-scroll"):
            yield SparqlBody(id="sparql-body")

    def on_mount(self) -> None:
        """Paint the initial (empty) state once children are mounted."""
        self.refresh_content(self.content)

    def watch_content(self, content: SparqlContent | None) -> None:
        """Reset pretty-print state for the newly selected query, then paint."""
        self.show_pretty = False
        self.pretty_text = None
        self.refresh_content(content)

    def watch_show_pretty(self, show_pretty: bool) -> None:
        """Repaint with raw or pretty text when the toggle flips."""
        self.refresh_content(self.content)

    def watch_pretty_text(self, pretty_text: str | None) -> None:
        """Repaint once the pretty-printed text has been computed."""
        self.refresh_content(self.content)

    @property
    def displayed_text(self) -> str | None:
        """The SPARQL string currently on screen, or None if empty.

        Pretty-printed text only once the formatter has produced it;
        the raw query otherwise.
        """
        if self.content is None:
            return None
        if self.show_pretty and self.pretty_text is not None:
            return self.pretty_text
        return self.content.sparql_text

    def refresh_content(self, content: SparqlContent | None) -> None:
        """Push the current content into the header and body widgets."""
        header = self.query_one("#sparql-header", Static)
        body = self.query_one("#sparql-body", SparqlBody)
        # Body height changes here; re-evaluate the conditional scroll
        # bindings once layout has settled.
        self.call_after_refresh(self.refresh_bindings)
        if content is None:
            header.update(
                "[dim]Press Enter or double-click a row to view its "
                "full SPARQL query and details.[/dim]"
            )
            body.code = None
            return
        header.update(format_header(content))
        body.code = self.displayed_text
