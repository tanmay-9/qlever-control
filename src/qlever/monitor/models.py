"""Shared data models for the monitor-queries TUI.

These frozen dataclasses are the contract between the data layer and the
UI. Widgets render them; the stub modules (and later the real log-reader)
produce them. Neither side imports the other: both depend on this module,
so swapping stubs for the real reader never touches a widget.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class LiveSubtitle:
    state: str
    n_active: int | None


@dataclass(frozen=True)
class MetricsCounts:
    label: str
    seen: int | None
    ok: int | None
    failed: int | None
    timeout: int | None
    cancelled: int | None
    p50: int | None
    p95: int | None
    slow: int | None


@dataclass(frozen=True)
class LiveQueryRow:
    qid: str
    ts_ms: int
    sparql: str


@dataclass(frozen=True)
class HistoricQueryRow:
    """A finished query in the current window.

    Unlike LiveQueryRow these carry a terminal `status` and a stored
    `duration_ms` (not an elapsed `now - ts`).
    """

    qid: str
    started_at_ms: int
    duration_ms: int
    status: str
    sparql: str


@dataclass(frozen=True)
class SparqlContent:
    """What the SparqlPane renders for the row under the table cursor.

    `status` is filled at the screen seam: Live passes None (active
    queries have no terminal status), Historic passes the real status.
    """

    qid: str
    started_at_ms: int
    status: str | None
    sparql_text: str


@dataclass(frozen=True)
class TimelineBounds:
    """The full log span and the slice the window currently covers.

    The Timeline maps these epochs (ms) to bar positions at render.
    """

    log_start_ms: int
    log_end_ms: int
    window_start_ms: int
    window_end_ms: int


@dataclass(frozen=True)
class ControlsState:
    window_size: str
    mode: str
    start_ms: int
    end_ms: int
