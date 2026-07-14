from __future__ import annotations

from collections.abc import Callable

from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Footer

from qlever.monitor_queries.models import ResourcePlot
from qlever.monitor_queries.widgets.resource_plot_pane import ResourcePlotPane


class ResourcePlotModal(ModalScreen):
    """Full-screen shell around a ResourcePlotPane.

    The pane owns the drawing and the roll timer; this modal only frames
    it and handles closing.
    """

    BINDINGS = [Binding("escape", "close", "Close")]

    def __init__(
        self,
        source: Callable[[int], ResourcePlot],
        refresh_interval: float | None = None,
    ) -> None:
        super().__init__()
        self.source = source
        self.refresh_interval = refresh_interval

    def compose(self) -> ComposeResult:
        with Vertical(id="resource-plot-modal"):
            yield ResourcePlotPane(self.source, self.refresh_interval)
        yield Footer(show_command_palette=False)

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()

    def on_click(self, event: events.Click) -> None:
        """Close when the dimmed area outside the plot is clicked."""
        if event.widget is self:
            self.action_close()
