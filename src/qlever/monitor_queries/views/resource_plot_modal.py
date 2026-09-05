"""Full-screen modal showing the resource plot.

The pane inside owns the drawing; the modal only handles closing. A
historic plot re-reads its window at the modal's wider width, so
maximizing adds detail instead of stretching the inline pane's points.
"""

from __future__ import annotations

from collections.abc import Callable

from textual import events, work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.worker import get_current_worker

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import ResourcePlotPane


class ResourcePlotModal(ModalScreen):
    """Shows the resource plot full screen.

    Opens on the window the inline pane is showing, and only frames it
    and closes it. Historic passes a reader, so the window is read again
    at the bigger size and the plot gains detail. Live keeps this plot
    rolling by handing it fresh readings on its timer.
    """

    BINDINGS = [Binding("escape", "close", "Close")]

    def __init__(
        self,
        window: ResourceWindow,
        reader: Callable[[int, Callable[[], bool]], ResourceWindow]
        | None = None,
    ) -> None:
        super().__init__()
        self.window = window
        self.reader = reader

    def compose(self) -> ComposeResult:
        reload = self.load_plot if self.reader is not None else None
        with Vertical(id="resource-plot-modal"):
            yield ResourcePlotPane(self.window, reload)
        yield Footer(show_command_palette=False)

    @work(thread=True, exclusive=True)
    def load_plot(self, max_points: int) -> None:
        """Re-read the window at this width off the UI thread.

        The pane calls this on resize. Exclusive, so a resize while a read
        is in flight cancels it and only the final width lands. Drops the
        result if the modal closed while reading.
        """
        worker = get_current_worker()
        resource_window = self.reader(max_points, lambda: worker.is_cancelled)
        if worker.is_cancelled or not self.is_current:
            return
        self.app.call_from_thread(self.apply_plot, resource_window)

    def apply_plot(self, resource_window: ResourceWindow) -> None:
        """Draw the readings that were re-read at this modal's width."""
        self.query_one(ResourcePlotPane).window = resource_window

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()

    def on_click(self, event: events.Click) -> None:
        """Close when the dimmed area outside the plot is clicked."""
        if event.widget is self:
            self.action_close()
