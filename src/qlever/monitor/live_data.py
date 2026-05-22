"""Adapters that translate live engine state into UI models.

The Live screen calls these getters; they read shared state under
the engine's lock when iteration is involved and return frozen
model dataclasses the widgets consume. No Textual imports, no
log I/O.
"""

from qlever.monitor.live_engine import (
    LiveState,
    MetricsSnapshot,
    compute_live_metrics,
)
from qlever.monitor.models import LiveQueryRow, LiveSubtitle, MetricsCounts

LIVE_METRIC_LABELS = ["last 5m", "last 15m", "last 1h"]
EMPTY_FIELDS = dict.fromkeys(MetricsSnapshot._fields)


def get_live_subtitle(
    state: LiveState, server_status: str, endpoint: str
) -> LiveSubtitle:
    """Subtitle line: server-reachable status plus current active count."""
    return LiveSubtitle(
        endpoint=endpoint, state=server_status, n_active=len(state.active)
    )


def get_live_query_rows(state: LiveState) -> list[LiveQueryRow]:
    """Snapshot the active set as a list of UI rows; no sort."""
    with state.lock:
        active_snapshot = list(state.active.items())
    return [
        LiveQueryRow(qid=qid, ts_ms=start_ms, sparql=sparql)
        for qid, (start_ms, sparql) in active_snapshot
    ]


def get_live_metrics(
    state: LiveState,
    slow_threshold_ms: int,
    now_ms: int,
) -> list[MetricsCounts]:
    """Three rolling-window metric rows for the Live screen."""
    snapshots = compute_live_metrics(state, slow_threshold_ms, now_ms)
    return [
        MetricsCounts(
            label=label,
            **(snap._asdict() if snap is not None else EMPTY_FIELDS),
        )
        for label, snap in zip(LIVE_METRIC_LABELS, snapshots)
    ]
