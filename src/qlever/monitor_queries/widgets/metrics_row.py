from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widgets import Static

from qlever.monitor_queries.models import MetricsCounts

METRIC_COLORS = {
    "ok": "$success",
    "failed": "$error",
    "timeout": "$error",
    "cancelled": "$warning",
    "unknown": "$warning",
    "slow": "$warning",
}

# (MetricsCounts field, kind) in render order; the field name is the label.
COLUMNS = [
    ("seen", "count"),
    ("ok", "count"),
    ("failed", "count"),
    ("timeout", "count"),
    ("cancelled", "count"),
    ("unknown", "count"),
    ("am", "ms"),
    ("gm", "ms"),
    ("p50", "ms"),
    ("p95", "ms"),
    ("slow", "count"),
]


def color_tag(label: str, value: int | None) -> str | None:
    """Return the theme color for a metric, or None if it should render neutral."""
    if value is None or value == 0:
        return None
    return METRIC_COLORS.get(label)


def format_count(n: int | None) -> str:
    """Render a count, or an ellipsis if it is not yet computed."""
    if n is None:
        return "…"
    return f"{n:,}"


def format_ms(ms: int | None) -> str:
    """Render a duration in seconds, ellipsis if not computed.

    Uses 2 decimals under 1 s, 1 decimal otherwise.
    """
    if ms is None:
        return "…"
    seconds = ms / 1000
    return f"{seconds:.2f}s" if seconds < 1 else f"{seconds:.1f}s"


def format_value(value: int | None, kind: str) -> str:
    """Render a raw cell value according to its column kind."""
    if kind == "count":
        return format_count(value)
    return format_ms(value)


def column_widths(rows: list[MetricsCounts]) -> list[int]:
    """Width of each column, sized to its own widest value across all rows.

    Per-column (not one shared width) so every column uses the least
    horizontal space while still aligning across the rolling-window rows.
    """
    widths = []
    for name, kind in COLUMNS:
        cells = (format_value(getattr(row, name), kind) for row in rows)
        widths.append(max((len(cell) for cell in cells), default=1))
    return widths


def column_label(name: str, slow_threshold_s: int) -> str:
    """Display label for a metric column; slow carries its threshold."""
    if name == "slow":
        return f">{slow_threshold_s}s"
    return name


def render_cell(
    name: str, label: str, value: int | None, width: int, kind: str
) -> str:
    """Render one bold-label, right-aligned value pair with optional color.

    `name` is the metric field and drives the color lookup; `label` is
    the text shown, which can differ from it (the slow column shows its
    threshold).
    """
    text = format_value(value, kind).rjust(width)
    color = color_tag(name, value)
    if color:
        return f"[b]{label}[/] [{color}]{text}[/]"
    return f"[b]{label}[/] {text}"


def format_row(
    row: MetricsCounts, widths: list[int], slow_threshold_s: int
) -> str:
    """Format one rolling-window row as a single line with markup."""
    if row.not_ready_message:
        return f"[bold]{row.label:<8}[/] │ [dim]{row.not_ready_message}[/]"
    cells = [
        render_cell(
            name,
            column_label(name, slow_threshold_s),
            getattr(row, name),
            width,
            kind,
        )
        for (name, kind), width in zip(COLUMNS, widths)
    ]
    return f"[bold]{row.label:<8}[/] │ " + " · ".join(cells)


class MetricsRow(Vertical):
    """Stack of rolling-window metric lines (5m, 15m, 1h on Live)."""

    can_focus = False

    rows = reactive(list, init=False)

    def __init__(
        self, rows: list[MetricsCounts], slow_threshold_s: int
    ) -> None:
        """Render each row in `rows` as one formatted text line."""
        super().__init__()
        self.slow_threshold_s = slow_threshold_s
        self.set_reactive(MetricsRow.rows, rows)

    def compose(self) -> ComposeResult:
        widths = column_widths(self.rows)
        for row in self.rows:
            yield Static(format_row(row, widths, self.slow_threshold_s))

    def watch_rows(self, rows: list[MetricsCounts]) -> None:
        """Repaint each line static when the row data changes."""
        widths = column_widths(rows)
        for line, row in zip(self.query(Static), rows):
            line.update(format_row(row, widths, self.slow_threshold_s))
