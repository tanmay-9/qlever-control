from __future__ import annotations

from rich.text import Text

LABEL_WIDTH = 8
MAX_WINDOW_S = 3600
WINDOWS = [
    ("5m", "Last 5m", 300),
    ("15m", "Last 15m", 900),
    ("1h", "Last 1h", 3600),
]


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


def format_coverage_hint(coverage_s: float, window_s: int) -> str:
    """Return a '(last Nm)' suffix when coverage is shorter than the window."""
    if coverage_s >= window_s:
        return ""
    minutes = int(coverage_s // 60)
    if minutes > 0:
        return f"  [dim](last {minutes}m)[/dim]"
    return f"  [dim](last {int(coverage_s)}s)[/dim]"


def format_top_metrics(
    active: int, slow_count: int, warn_after: float, coverage_s: float
) -> Text:
    """Render the top row: current active count and cumulative slow count."""
    threshold = int(warn_after)
    suffix = format_coverage_hint(coverage_s, MAX_WINDOW_S)
    return Text.from_markup(
        f"Active queries: [cyan]{active}[/cyan]   "
        f"Slow queries logged (>{threshold}s): [red]{slow_count}[/red]"
        f"{suffix}"
    )


def format_window_line(
    label: str,
    window_s: int,
    coverage_s: float,
    finish_events,
    warn_after: float,
    now_ms: int,
) -> Text:
    """Render one windowed-metrics row (5m/15m/1h)."""
    effective_back_s = min(window_s, coverage_s)
    if effective_back_s <= 0:
        return Text.from_markup(
            f"[bold yellow]{label:<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
            f"[dim]warming up...[/dim]"
        )
    cutoff_ms = now_ms - int(effective_back_s * 1000)
    durations = sorted(d for ts, d in finish_events if ts >= cutoff_ms)
    slow_count = sum(1 for d in durations if d >= warn_after * 1000)
    if durations:
        p50 = format_duration_ms(nearest_rank_percentile(durations, 0.50))
        p95 = format_duration_ms(nearest_rank_percentile(durations, 0.95))
    else:
        p50 = p95 = "—"
    threshold = int(warn_after)
    suffix = format_coverage_hint(effective_back_s, window_s)
    return Text.from_markup(
        f"[bold yellow]{label:<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
        f"p50: [cyan]{p50}[/cyan]   "
        f"p95: [cyan]{p95}[/cyan]   "
        f"Slow (>{threshold}s): [red]{slow_count}[/red]"
        f"{suffix}"
    )
