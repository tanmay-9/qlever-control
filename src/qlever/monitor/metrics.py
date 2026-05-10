from __future__ import annotations

from datetime import datetime

from rich.text import Text

LABEL_WIDTH = 6
MAX_WINDOW_S = 3600
WINDOWS = [
    ("5m", 300),
    ("15m", 900),
    ("1h", 3600),
]

CYAN = "cyan"
RED = "red"
DIM = "dim"
LABEL_STYLE = "bold yellow"
DASH = "—"


def nearest_rank_percentile(sorted_values: list[int], pct: float) -> int:
    """Nearest-rank percentile of an already-sorted list of ints."""
    idx = int(pct * (len(sorted_values) - 1))
    return sorted_values[idx]


def format_duration_ms(ms: int) -> str:
    """Render a millisecond duration as 'Xms', 'X.Ys', or 'Xm Ys'."""
    if ms < 1000:
        return f"{ms}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{int(seconds // 60)}m {int(seconds % 60)}s"


def format_time_left(seconds: float) -> str:
    """Render 'Xs left', 'Xm left', or 'Xm Ys left' for a positive duration."""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s left"
    minutes, secs = divmod(seconds, 60)
    return f"{minutes}m left" if secs == 0 else f"{minutes}m {secs}s left"


def format_window_duration(start_dt: datetime, end_dt: datetime) -> str:
    """Render a [start, end] span as '52m' or '3h 12m'."""
    total_s = int((end_dt - start_dt).total_seconds())
    hours, rem = divmod(total_s, 3600)
    minutes = rem // 60
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def label_part(label: str) -> tuple[str, str]:
    """Left-padded styled label cell shared by every metrics row."""
    return (f"{label:<{LABEL_WIDTH}}", LABEL_STYLE)


SEP = (" │  ", DIM)


def format_top_line(active: int) -> Text:
    """Top row: instantaneous active-query count."""
    return Text.assemble(
        label_part("Now"),
        SEP,
        "Active: ",
        (str(active), CYAN),
    )


def format_metrics_line(
    label: str,
    completed_count: int,
    p50: str,
    p95: str,
    slow_count: int,
) -> Text:
    """Shared 'completed / p50 / p95 / Slow' row used by live and historic."""
    return Text.assemble(
        label_part(label),
        SEP,
        "completed: ",
        (str(completed_count), CYAN),
        "   p50: ",
        (p50, CYAN),
        "   p95: ",
        (p95, CYAN),
        "   Slow: ",
        (str(slow_count), RED),
    )


def format_historic_summary(
    window_label: str,
    completed_count: int,
    p50: str,
    p95: str,
    slow_count: int,
) -> Text:
    """Single-line summary for a historic window."""
    return format_metrics_line(
        window_label, completed_count, p50, p95, slow_count
    )


def format_window_line(
    label: str,
    window_s: int,
    coverage_s: float,
    finish_events,
    warn_after: float,
    now_ms: int,
) -> Text:
    """One windowed row (5m / 15m / 1h)."""
    if coverage_s < window_s:
        remaining = format_time_left(window_s - coverage_s)
        return Text.assemble(
            label_part(label), SEP, (f"collecting ({remaining})", DIM)
        )
    cutoff_ms = now_ms - window_s * 1000
    durations = sorted(d for ts, d in finish_events if ts >= cutoff_ms)
    slow_count = sum(1 for d in durations if d >= warn_after * 1000)
    if durations:
        p50 = format_duration_ms(nearest_rank_percentile(durations, 0.50))
        p95 = format_duration_ms(nearest_rank_percentile(durations, 0.95))
    else:
        p50 = p95 = DASH
    return format_metrics_line(label, len(durations), p50, p95, slow_count)
