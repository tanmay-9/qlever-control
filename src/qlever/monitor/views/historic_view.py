from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.widgets import Button, Input, Label, Static

from qlever.monitor.log_reader import (
    get_historic_window,
    read_log_bounds,
)
from qlever.monitor.metrics import (
    DASH,
    format_duration_ms,
    format_historic_summary,
    format_window_duration,
    nearest_rank_percentile,
)
from qlever.monitor.views.base_view import BaseView
from qlever.monitor.widgets.metrics_pane import (
    HistoricMetricsPane,
    MetricsPane,
)
from qlever.monitor.widgets.query_table import (
    HistoricQueryTable,
    QueryTable,
)
from qlever.monitor.widgets.sparql_pane import SparqlPane

DEFAULT_WINDOW = timedelta(hours=1)
DISPLAY_FORMAT = "%Y-%m-%d %H:%M:%S"
DISPLAY_FORMAT_HUMAN = "YYYY-MM-DD HH:MM:SS"
MAX_ROWS = 200


class HistoricView(BaseView):
    """Static snapshot view: scan the log for an explicit [from, to] window."""

    def __init__(
        self,
        log_file: Path,
        timeout: int,
        warn_after: int,
    ) -> None:
        super().__init__()
        self.log_file = log_file
        self.timeout = timeout
        self.warn_after = warn_after
        log_start, log_end = read_log_bounds(log_file)
        self.log_start_dt = log_start
        self.log_end_dt = log_end
        default_from = max(log_end - DEFAULT_WINDOW, log_start)
        self.default_from_str = default_from.strftime(DISPLAY_FORMAT)
        self.default_to_str = log_end.strftime(DISPLAY_FORMAT)
        self.events_by_qid = {}

    def compose(self) -> ComposeResult:
        hint_text = (
            f"Log start: {self.log_start_dt.strftime(DISPLAY_FORMAT)}\n"
            f"Log end:   {self.log_end_dt.strftime(DISPLAY_FORMAT)}"
        )
        with Horizontal(id="header"):
            yield Static(hint_text, id="log-range-hint")
            yield Static("│\n│\n│", id="header-sep")
            with Horizontal(id="range-inputs"):
                yield Label("From", classes="input-label")
                yield Input(
                    value=self.default_from_str,
                    id="from-input",
                )
                yield Label("To", classes="input-label")
                yield Input(
                    value=self.default_to_str,
                    id="to-input",
                )
                yield Button(
                    "Load",
                    id="load-button",
                    variant="primary",
                )
        yield self.build_metrics_pane()
        yield Static("", id="historic-help")
        yield self.build_query_table()
        yield SparqlPane()

    def build_metrics_pane(self) -> MetricsPane:
        return HistoricMetricsPane()

    def build_query_table(self) -> QueryTable:
        return HistoricQueryTable(warn_after=self.warn_after)

    def get_query_text(self, qid: str) -> str | None:
        event = self.events_by_qid.get(qid)
        return event.get("query") if event else None

    def on_mount(self) -> None:
        self.reload_window()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "load-button":
            self.reload_window()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.reload_window()

    def reload_window(self) -> None:
        from_value = self.query_one("#from-input", Input).value
        to_value = self.query_one("#to-input", Input).value
        try:
            from_dt = datetime.strptime(from_value, DISPLAY_FORMAT)
        except ValueError:
            self.app.notify(
                f"From: invalid timestamp '{from_value}' "
                f"(expected {DISPLAY_FORMAT_HUMAN})",
                severity="error",
            )
            return
        try:
            to_dt = datetime.strptime(to_value, DISPLAY_FORMAT)
        except ValueError:
            self.app.notify(
                f"To: invalid timestamp '{to_value}' "
                f"(expected {DISPLAY_FORMAT_HUMAN})",
                severity="error",
            )
            return
        if to_dt <= from_dt:
            self.app.notify("To must be later than From", severity="error")
            return

        self.window_from_dt = from_dt
        self.window_to_dt = to_dt
        self.query_one("#load-button", Button).disabled = True
        self.query_one(HistoricQueryTable).clear()
        self.events_by_qid.clear()
        self.query_one(HistoricMetricsPane).set_summary("Loading…")
        self.query_one("#historic-help", Static).update("")
        self.run_worker(
            lambda: self.scan_window(from_dt, to_dt),
            thread=True,
            exclusive=True,
        )

    def scan_window(self, from_dt: datetime, to_dt: datetime) -> None:
        active, completed, last_scanned_ms = get_historic_window(
            self.log_file, from_dt, to_dt, self.timeout
        )
        self.app.call_from_thread(
            self.apply_window_data, active, completed, last_scanned_ms
        )

    def apply_window_data(
        self,
        active: list[dict],
        completed: list[tuple[dict, dict]],
        last_scanned_ms: int,
    ) -> None:
        table = self.query_one(HistoricQueryTable)
        rows = []
        for start, end in completed:
            qid = start["query-id"]
            self.events_by_qid[qid] = start
            rows.append(
                (qid, end["ended-at"] - start["started-at"], False, start)
            )
        for start in active:
            qid = start["query-id"]
            self.events_by_qid[qid] = start
            rows.append(
                (qid, last_scanned_ms - start["started-at"], True, start)
            )
        rows.sort(key=lambda r: r[1], reverse=True)
        for qid, duration_ms, is_active, start in rows[:MAX_ROWS]:
            table.add_query_row(
                qid,
                start.get("query", ""),
                duration_ms / 1000,
                is_active=is_active,
            )

        completed_durations = sorted(d for _, d, is_a, _ in rows if not is_a)
        warn_ms = self.warn_after * 1000
        slow_completed = sum(1 for d in completed_durations if d >= warn_ms)
        slow_active = sum(1 for _, d, is_a, _ in rows if is_a and d >= warn_ms)
        if completed_durations:
            p50 = format_duration_ms(
                nearest_rank_percentile(completed_durations, 0.50)
            )
            p95 = format_duration_ms(
                nearest_rank_percentile(completed_durations, 0.95)
            )
        else:
            p50 = p95 = DASH
        self.query_one(HistoricMetricsPane).set_summary(
            format_historic_summary(
                window_label=format_window_duration(
                    self.window_from_dt, self.window_to_dt
                ),
                completed_count=len(completed_durations),
                p50=p50,
                p95=p95,
                slow_count=slow_completed + slow_active,
            )
        )
        total = len(rows)
        shown = min(total, MAX_ROWS)
        self.query_one("#historic-help", Static).update(
            f"Showing {shown} of {total} queries (sorted by duration desc)."
        )
        self.query_one("#load-button", Button).disabled = False
