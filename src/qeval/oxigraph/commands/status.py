from __future__ import annotations

from qeval.base_commands.status import BaseStatusCommand


class StatusCommand(BaseStatusCommand):
    """Show Oxigraph processes running on this machine."""

    def default_regex(self) -> str:
        # `serve` also matches `serve-read-only`.
        # `load` and `optimize` are the index-time processes.
        return r"oxigraph\s+(serve|load|optimize)"
