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
    label_width,
    next_step,
)


class ResourcePlotModal(ModalScreen[int]):
    """Shows the plots this log can carry, stacked, full screen.

    Opens on the window and the axis top the inline pane is showing,
    so what the reader set is what the modal draws, and closes with
    the top it was stepped to, so the inline pane takes that over.
    In between, the screen keeps every pane up to date: Historic sends
    the window it re-reads at this size, and Live sends fresh readings
    on its timer.
    """

    BINDINGS = [
        # One footer entry for the pair: the key that opened it closes it.
        Binding("escape", "close", "Close", key_display="esc/z"),
        Binding("z", "close", "Close", show=False),
        # One entry for the scale pair as well; there is no gutter here.
        Binding("minus", "step_top(-1)", "Plot scale", key_display="-/+"),
        Binding("plus", "step_top", "Plot scale", show=False),
        # A modal cuts the app's bindings, so quit is repeated here.
        Binding("q", "app.quit", "Quit"),
    ]

    def __init__(
        self, window: ResourceWindow, plots: list[Plot], top_step: int
    ) -> None:
        super().__init__()
        self.window = window
        self.plots = plots
        self.top_step = top_step

    def compose(self) -> ComposeResult:
        # One width for the whole stack, since a pane only knows its own
        # numbers and the gutters have to come out the same size.
        width = label_width(self.window, self.plots, self.top_step)
        with Vertical(id="resource-plot-modal"):
            for plot in self.plots:
                yield ResourcePlotPane(
                    self.window,
                    plot,
                    top_step=self.top_step,
                    time_labels=plot is self.plots[-1],
                    label_width=width,
                )
        yield Footer(show_command_palette=False)

    def action_step_top(self, direction: int = 1) -> None:
        """Raise or lower the top of the stack's adjustable axis.

        Every pane gets the new step, as at open, and the labels are
        padded afresh, since the digits of the top change with it.
        """
        panes = list(self.query(ResourcePlotPane))
        window = panes[0].window
        steps = (
            next_step(window, plot, self.top_step, direction)
            for plot in self.plots
        )
        wanted = next((step for step in steps if step is not None), None)
        if wanted is None:
            return
        self.top_step = wanted
        width = label_width(window, self.plots, wanted)
        for pane in panes:
            pane.label_width = width
            pane.top_step = wanted

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss(self.top_step)
