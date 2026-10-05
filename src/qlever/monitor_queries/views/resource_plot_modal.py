"""Full-screen modal showing every resource plot at once.

The panes inside do the drawing. This modal only stacks them, frames
them and closes them. They all read the same window, so the same moment
in time is at the same place in each.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen, Screen

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
    screen keeps every pane up to date: Historic sends each window it
    reads, and Live sends fresh readings on its timer.
    """

    BINDINGS = [
        # One footer entry for the pair: the key that opened it closes it.
        Binding("escape", "close", "Close", key_display="esc/z"),
        Binding("z", "close", "Close", show=False),
        # A modal cuts the app's bindings, so quit is repeated here.
        Binding("q", "app.quit", "Quit"),
        # The window belongs to the screen beneath, so its keys run there.
        Binding("w", "forward('cycle_window')", "Window size", show=False),
        Binding(
            "W", "forward('cycle_window_back')", "Window size", show=False
        ),
        Binding(
            "left", "forward('shift_earlier')", "Shift earlier", show=False
        ),
        Binding("right", "forward('shift_later')", "Shift later", show=False),
        Binding(
            "shift+left",
            "forward('snap_start')",
            "Jump to log start",
            key_display="⇧ ←",
            show=False,
        ),
        Binding(
            "shift+right",
            "forward('snap_end')",
            "Jump to log end",
            key_display="⇧ →",
            show=False,
        ),
    ]

    def __init__(
        self,
        owner: Screen,
        window: ResourceWindow,
        plots: list[Plot],
        top_steps: dict[str, int],
    ) -> None:
        super().__init__()
        # The screen that opened the modal, which owns the window.
        self.owner = owner
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

    def check_action(self, action: str, parameters: tuple) -> bool:
        """Keep only the window keys the screen beneath has an action for."""
        if action == "forward":
            return hasattr(self.owner, f"action_{parameters[0]}")
        return True

    async def action_forward(self, name: str) -> None:
        """Run a window action on the screen that opened the modal."""
        await self.owner.run_action(name)
