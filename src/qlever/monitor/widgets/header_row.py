from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widget import Widget
from textual.widgets import Static


class HeaderRow(Horizontal):
    """One-row header layout with three slots: left, center, right.

    The row is screen-agnostic. Each screen decides what goes in each slot.
    Empty slots become an empty Static so the three-slot layout stays stable.
    """

    def __init__(
        self,
        left: Widget | None = None,
        center: Widget | None = None,
        right: Widget | None = None,
    ) -> None:
        """Build a row with the given widgets in the left, center, right slots."""
        super().__init__()
        self.left = left if left is not None else Static("")
        self.center = center if center is not None else Static("")
        self.right = right if right is not None else Static("")
        self.left.add_class("slot-left")
        self.center.add_class("slot-center")
        self.right.add_class("slot-right")

    def compose(self) -> ComposeResult:
        yield self.left
        yield self.center
        yield self.right
