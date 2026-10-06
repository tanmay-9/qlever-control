"""Full-screen modal showing every resource plot at once.

The panes inside do the drawing. This modal only stacks them, frames
them and closes them. They all read the same window, so the same moment
in time is at the same place in each.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, Static

from qlever.monitor_queries.models import ResourceWindow
from qlever.monitor_queries.util import action_key
from qlever.monitor_queries.widgets.detail_row import (
    GutterControl,
    gutter_control,
)
from qlever.monitor_queries.widgets.footer import Footer
from qlever.monitor_queries.widgets.resource_plot_pane import (
    TOP_PERCENTILES,
    Plot,
    ResourcePlotPane,
)
from qlever.monitor_queries.widgets.window_range import WindowRange
from qlever.monitor_queries.widgets.window_stepper import WindowStepper


def axis_top_controls(plot: Plot) -> list[GutterControl]:
    """The raise and lower arrows for one plot's left axis top.

    The ids come from the plot's name, so each plot's arrows are its own.
    """
    slug = plot.name.lower().replace("/", "").replace(" ", "-")
    return [
        GutterControl(
            name=f"{slug}-raise-top",
            glyph="⇡",
            hint="Raise the left axis top",
            action=f"step_axis_top('{plot.name}', 1)",
        ),
        GutterControl(
            name=f"{slug}-lower-top",
            glyph="⇣",
            hint="Lower the left axis top",
            action=f"step_axis_top('{plot.name}', -1)",
        ),
    ]


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
        # A modal cuts the app's bindings, so quit and help are repeated.
        Binding("q", "app.quit", "Quit"),
        Binding("question_mark", "app.toggle_help", "Help"),
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
            with Horizontal(id="modal-top-row"):
                yield WindowStepper(
                    self.owner.window_size,
                    caption="WINDOW",
                    tooltip="Width of the time window the plots show.",
                )
                # Movable exactly when the arrow keys are, see check_action.
                yield WindowRange(
                    movable=hasattr(self.owner, "action_shift_earlier")
                )
            for plot in self.plots:
                with Horizontal(classes="modal-plot-row"):
                    # A plot with nothing to adjust keeps an empty column,
                    # so every plot starts at the same place in time.
                    with Vertical(classes="modal-gutter"):
                        if plot.adjustable:
                            yield Static("", classes="axis-top-rail")
                            for control in axis_top_controls(plot):
                                yield gutter_control(control, "-modal")
                            yield Static("", classes="axis-top-rail")
                    yield ResourcePlotPane(
                        self.window,
                        plot,
                        top_steps=self.top_steps,
                        time_labels=plot is self.plots[-1],
                    )
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        """Follow the window the panes are handed, and label the arrows."""
        self.watch(
            self.query_one(ResourcePlotPane), "window", self.sync_top_row
        )
        self.sync_axis_top_arrows()
        # Label the controls with the keys that run them; CSS decides
        # when the labels show.
        self.query_one(WindowStepper).set_help_keys(
            action_key(self, "forward('cycle_window_back')"),
            action_key(self, "forward('cycle_window')"),
        )
        self.query_one(WindowRange).set_help_keys(
            shift=(
                action_key(self, "forward('shift_earlier')"),
                action_key(self, "forward('shift_later')"),
            ),
            jump=(
                action_key(self, "forward('snap_start')"),
                action_key(self, "forward('snap_end')"),
            ),
        )

    def sync_top_row(self) -> None:
        """Show the screen beneath's window size, range and log edges."""
        self.query_one(WindowStepper).window_size = self.owner.window_size
        window = self.query_one(ResourcePlotPane).window
        window_range = self.query_one(WindowRange)
        window_range.show_range(
            int(window.start_s * 1000), int(window.end_s * 1000)
        )
        if window_range.movable:
            window_range.set_edges(
                at_start=self.owner.window_start_ms <= self.owner.log_start_ms,
                at_end=self.owner.window_end_ms >= self.owner.log_end_ms,
            )

    def action_step_axis_top(self, plot_name: str, direction: int) -> None:
        """Raise (1) or lower (-1) the named plot's left axis top."""
        for pane in self.query(ResourcePlotPane):
            if pane.plot.name == plot_name:
                pane.step_top(direction)
        self.sync_axis_top_arrows()

    def sync_axis_top_arrows(self) -> None:
        """Grey out an axis top arrow with nowhere left to go."""
        for pane in self.query(ResourcePlotPane):
            if pane.plot.adjustable:
                raise_top, lower_top = axis_top_controls(pane.plot)
                last_step = len(TOP_PERCENTILES) - 1
                self.query_one(f"#{raise_top.name}-glyph", Button).disabled = (
                    pane.top_step == 0
                )
                self.query_one(f"#{lower_top.name}-glyph", Button).disabled = (
                    pane.top_step == last_step
                )

    def action_close(self) -> None:
        """Close the modal, unless a prior event already closed it."""
        if self.is_current:
            self.dismiss()

    async def on_window_stepper_stepped(
        self, message: WindowStepper.Stepped
    ) -> None:
        """Step the window size when a stepper arrow is clicked."""
        if message.direction > 0:
            await self.action_forward("cycle_window")
        else:
            await self.action_forward("cycle_window_back")

    async def on_window_range_shifted(
        self, message: WindowRange.Shifted
    ) -> None:
        """Shift the window when a shift arrow is clicked."""
        if message.direction > 0:
            await self.action_forward("shift_later")
        else:
            await self.action_forward("shift_earlier")

    async def on_window_range_jumped(
        self, message: WindowRange.Jumped
    ) -> None:
        """Jump to a log edge when a jump arrow is clicked."""
        if message.direction > 0:
            await self.action_forward("snap_end")
        else:
            await self.action_forward("snap_start")

    def check_action(self, action: str, parameters: tuple) -> bool:
        """Keep only the window keys the screen beneath has an action for."""
        if action == "forward":
            return hasattr(self.owner, f"action_{parameters[0]}")
        return True

    async def action_forward(self, name: str) -> None:
        """Run a window action on the screen beneath, then show the result.

        The window size and edges change at once, before Historic's read
        lands, so the row is synced here and again when the read does.
        """
        await self.owner.run_action(name)
        self.sync_top_row()
