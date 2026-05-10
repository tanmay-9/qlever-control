from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Container
from textual.widgets import Static

LOADING = "loading metrics..."

WINDOW_KEYS = ("5m", "15m", "1h")


class MetricsPane(Container):
    """Marker base for top-of-view metrics panes. CSS-only."""


class LiveMetricsPane(MetricsPane):
    """Top metrics line + per-window rolling metrics (5m / 15m / 1h)."""

    def compose(self) -> ComposeResult:
        yield Static(LOADING, id="live-top")
        for key in WINDOW_KEYS:
            yield Static(
                LOADING, id=f"live-window-{key}", classes="window-row"
            )

    def set_top_line(self, content: str | Text) -> None:
        self.query_one("#live-top", Static).update(content)

    def set_window_line(self, key: str, content: str | Text) -> None:
        self.query_one(f"#live-window-{key}", Static).update(content)


class HistoricMetricsPane(MetricsPane):
    """Single-line summary for the historic view's chosen window."""

    def compose(self) -> ComposeResult:
        yield Static(LOADING, id="historic-summary")

    def set_summary(self, content: str | Text) -> None:
        self.query_one("#historic-summary", Static).update(content)
