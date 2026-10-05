"""Full-screen modal showing every resource plot at once.

The panes inside do the drawing. This modal only stacks them, frames
them and closes them. They all read the same window, so the same moment
in time is at the same place in each, and the whole stack is one read:
a bigger pane fits more buckets and says so when it resizes, so the
screen that holds the readings can send a more detailed window.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import (
    Plot,
    ResourcePlotPane,
)


class ResourcePlotModal(ModalScreen):
    """Shows the plots this log can carry, stacked, full screen.

    Opens on the window and each plot's axis top the inline pane holds,
    so what the reader set is what the modal draws. After that the
    screen keeps every pane up to date: Historic sends the window it
    re-reads at this size, and Live sends fresh readings on its timer.
    """

    BINDINGS = [
        # One footer entry for the pair: the key that opened it closes it.
        Binding("escape", "close", "Close", key_display="esc/z"),
        Binding("z", "close", "Close", show=False),
        # A modal cuts the app's bindings, so quit is repeated here.
        Binding("q", "app.quit", "Quit"),
    ]

    def __init__(
        self,
        window: ResourceWindow,
        plots: list[Plot],
        top_steps: dict[str, int],
    ) -> None:
        super().__init__()
        self.window = window
        self.plots = plots
        self.top_steps = top_steps

    def compose(self) -> ComposeResult:
        with Vertical(id="resource-plot-modal"):
            for plot in self.plots:
                yield ResourcePlotPane(
                    self.window,
                    plot,
                    top_steps=self.top_steps,
                    time_labels=plot is self.plots[-1],
                )
        yield Footer(show_command_palette=False)

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()
