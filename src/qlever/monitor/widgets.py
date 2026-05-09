from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess

from rich.console import Group
from rich.syntax import Syntax
from rich.text import Text
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Static

QID_COL_WIDTH = 15
SPARQL_PREVIEW_LEN = 300

SHOW_SPARQL_HINT = (
    "Double-click a row (or press Enter on a highlighted row) to view its full "
    "pretty-printed SPARQL. Arrow keys move the cursor without triggering "
    "pretty-print."
)


def truncate(text: str, max_len: int) -> str:
    """Trim text to max_len with an ellipsis"""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def copy_text(text: str) -> bool:
    """Copy text to the system clipboard. Returns True on success."""
    try:
        system = platform.system()

        candidates = []
        if system == "Darwin":
            candidates.append(["pbcopy"])
        elif system == "Linux":
            # On Wayland, never fall through to xclip/xsel: they write to
            # the XWayland selection, which Wayland apps don't read.
            on_wayland = bool(os.environ.get("WAYLAND_DISPLAY"))
            if on_wayland and shutil.which("wl-copy"):
                # Force text/plain so wl-copy doesn't tag SPARQL starting
                # with `PREFIX foo: <http://...>` as a URI-ish MIME type.
                candidates.append(["wl-copy", "--type", "text/plain"])
            else:
                if shutil.which("xclip"):
                    candidates.append(
                        [
                            "xclip",
                            "-selection",
                            "clipboard",
                            "-t",
                            "UTF8_STRING",
                        ]
                    )
                if shutil.which("xsel"):
                    candidates.append(["xsel", "--clipboard", "--input"])

        payload = text.encode("utf-8")
        for cmd in candidates:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                proc.communicate(input=payload, timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.communicate(timeout=1)
                except Exception:
                    pass
                continue
            except Exception:
                continue
            if proc.returncode == 0:
                return True
        return False
    except Exception:
        return False


def build_active_row(qid: str, event: dict, now_ms: int) -> tuple:
    """Build the (qid, duration, sparql) cells for a DataTable row."""
    duration_s = (now_ms - event["started-at"]) / 1000
    sparql = truncate(
        re.sub(r"\s+", " ", event.get("query", "")).strip(), SPARQL_PREVIEW_LEN
    )
    return (
        truncate(qid, QID_COL_WIDTH),
        Text(f"{duration_s:.1f}s", justify="right"),
        sparql,
    )


def duration_sort_key(cell) -> float:
    """Parse a duration cell ('5.5s') into a float for descending sort."""
    text = cell.plain if hasattr(cell, "plain") else str(cell)
    try:
        return float(text.rstrip("s"))
    except ValueError:
        return -1.0


class QueryTable(DataTable):
    """DataTable preconfigured with the qid/duration/sparql columns."""

    DEFAULT_CSS = """
    QueryTable { height: 1fr; }
    """

    def __init__(self, **kwargs) -> None:
        """Initialize with row cursor mode."""
        super().__init__(cursor_type="row", **kwargs)

    def on_mount(self) -> None:
        """Configure the standard columns once the table is mounted."""
        self.add_column("Query ID", width=QID_COL_WIDTH, key="qid")
        self.add_column(
            Text("Duration", justify="right"), width=10, key="duration"
        )
        self.add_column("SPARQL", key="sparql")

    def add_query_row(self, qid: str, event: dict, now_ms: int) -> None:
        """Append a row built from a start event."""
        self.add_row(*build_active_row(qid, event, now_ms), key=qid)


class SparqlPane(VerticalScroll):
    """Scrollable pane that displays the selected query's full SPARQL."""

    DEFAULT_CSS = """
    SparqlPane { height: 1fr; }
    SparqlPane > Static { padding-left: 2; }
    """

    def __init__(self, **kwargs) -> None:
        """Initialize with no current selection."""
        super().__init__(**kwargs)
        self.selected_qid: str | None = None
        self.selected_query_text: str | None = None

    def compose(self):
        """Yield the inner Static where the SPARQL is rendered."""
        yield Static(SHOW_SPARQL_HINT, id="detail")

    @property
    def has_selection(self) -> bool:
        """Whether a query is currently displayed."""
        return self.selected_query_text is not None

    def show(self, qid: str, query_text: str) -> None:
        """Cache the selection and render its SPARQL with syntax highlighting."""
        self.selected_qid = qid
        self.selected_query_text = query_text
        is_dark = "light" not in self.app.theme
        syntax = Syntax(
            query_text,
            "sparql",
            theme="monokai" if is_dark else "default",
            word_wrap=True,
        )
        self.query_one("#detail", Static).update(
            Group(
                Text(f"Server Query ID: {qid}", style="bold"),
                Text(""),
                syntax,
            )
        )

    def clear(self) -> None:
        """Drop the selection and restore the hint."""
        self.selected_qid = None
        self.selected_query_text = None
        self.query_one("#detail", Static).update(SHOW_SPARQL_HINT)

    def copy(self) -> bool:
        """Copy the selected SPARQL to the clipboard. Returns True on success."""
        if self.selected_query_text is None:
            return False
        return copy_text(self.selected_query_text)
