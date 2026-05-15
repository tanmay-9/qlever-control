from __future__ import annotations

from textual.widgets import Static

from qlever.monitor.models import LiveSubtitle


def format_subtitle(subtitle: LiveSubtitle, endpoint: str) -> str:
    """Build the Live subtitle text from current state and server endpoint."""
    if subtitle.state == "connecting":
        return f"[$warning]Connecting[/] to server at [b]{endpoint}[/]…"
    if subtitle.state == "unreachable":
        return (
            f"[$error]Can't reach server[/] at [b]{endpoint}[/]"
            f" · active queries unknown"
        )
    if subtitle.n_active is None:
        return f"[$success]Server active[/] at [b]{endpoint}[/]"
    return (
        f"[$success]Server active[/] at [b]{endpoint}[/]"
        f" · [b $success]{subtitle.n_active}[/] active queries"
    )


class LiveStatusRow(Static):
    """One-line subtitle under the HeaderRow on the Live screen."""

    can_focus = False

    def __init__(self, subtitle: LiveSubtitle, endpoint: str) -> None:
        """Render the subtitle text computed from `subtitle` and `endpoint`."""
        super().__init__(format_subtitle(subtitle, endpoint))
