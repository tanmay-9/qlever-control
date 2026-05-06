from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
import time
from collections import deque
from datetime import datetime

from rich.console import Group
from rich.syntax import Syntax
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import DataTable, Footer, Static

from qlever.util import pretty_printed_query

MAX_CONSECUTIVE_FAILURES = 5
SLOW_LOG_HEADER = "logged_at\tevent\tqid\tduration_s\tsparql\n"
COMPACTED_HEADER = "logged_at\tstatus\tqid\tduration_s\tsparql\n"
WINDOWS_S = [
    ("5m", "Last 5m", 300),
    ("15m", "Last 15m", 900),
    ("1h", "Last 1h", 3600),
]
MAX_WINDOW_S = 3600
LABEL_WIDTH = 8
HINT_TEXT = (
    "Double-click a row (or press Enter on a highlighted row) to view its full "
    "pretty-printed SPARQL. Arrow keys move the cursor without triggering "
    "pretty-print."
)


def copy_text(text: str) -> bool:
    """Copy text to the system clipboard. Returns True on success."""
    try:
        system = platform.system()

        candidates = []
        if system == "Darwin":
            candidates.append(["pbcopy"])
        elif system == "Linux":
            # On Wayland, never fall through to xclip/xsel: they write to
            # the XWayland selection, which Wayland apps don't read.
            on_wayland = bool(os.environ.get("WAYLAND_DISPLAY"))
            if on_wayland and shutil.which("wl-copy"):
                # Force text/plain so wl-copy doesn't tag SPARQL starting
                # with `PREFIX foo: <http://...>` as a URI-ish MIME type.
                candidates.append(["wl-copy", "--type", "text/plain"])
            else:
                if shutil.which("xclip"):
                    candidates.append(
                        [
                            "xclip",
                            "-selection",
                            "clipboard",
                            "-t",
                            "UTF8_STRING",
                        ]
                    )
                if shutil.which("xsel"):
                    candidates.append(["xsel", "--clipboard", "--input"])

        payload = text.encode("utf-8")
        for cmd in candidates:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                proc.communicate(input=payload, timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.communicate(timeout=1)
                except Exception:
                    pass
                continue
            except Exception:
                continue
            if proc.returncode == 0:
                return True
        return False
    except Exception:
        return False


def fetch_queries(monitor_queries_cmd: str) -> dict | None:
    """Fetch active queries. Returns None on failure, {} if none active."""
    try:
        output = subprocess.check_output(
            monitor_queries_cmd, shell=True, stderr=subprocess.DEVNULL
        )
    except Exception:
        return None
    output = output.strip()
    if not output:
        return {}
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else {}


def server_supports_duration(queries_dict: dict) -> bool:
    """Return True iff the server reports per-query duration metadata."""
    return any(isinstance(v, dict) for v in queries_dict.values())


def append_slow_log(
    path: str, event: str, qid: str, duration_s: int, sparql: str = ""
) -> None:
    """Append one TSV slow-query event, writing the header on first write."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a") as f:
        if f.tell() == 0:
            f.write(SLOW_LOG_HEADER)
        f.write(f"{ts}\t{event}\t{qid}\t{duration_s}\t{sparql}\n")


def detect_slow_queries(
    queries_dict: dict,
    slow_seen: dict,
    warn_after: float,
    log_path: str,
) -> None:
    """Log start/finish events for queries crossing warn_after.

    Mutates slow_seen (qid -> started_at) in place.
    """
    now_ms = int(time.time() * 1000)

    for qid in list(slow_seen):
        if qid not in queries_dict:
            final_s = (now_ms - slow_seen[qid]) // 1000
            append_slow_log(log_path, "finish", qid, final_s)
            del slow_seen[qid]

    for qid, info in queries_dict.items():
        if not isinstance(info, dict) or qid in slow_seen:
            continue
        started_at = info.get("started-at")
        if started_at is None:
            continue
        duration_s = (now_ms - started_at) // 1000
        if duration_s >= warn_after:
            slow_seen[qid] = started_at
            sparql = re.sub(r"\s+", " ", info["query"]).strip()
            append_slow_log(log_path, "start", qid, duration_s, sparql)


def compact_slow_log(path: str) -> None:
    """Rewrite the slow-query log with one row per qid, preserving prior runs.

    Pairs each raw 'start' with its matching 'finish' from the current run;
    queries still running are written with status 'unfinished'. Pre-existing
    compacted rows from previous runs are passed through unchanged.
    No-op if the log is missing or has nothing to write.
    """
    prior_finished = {}
    prior_unfinished = {}
    starts = {}
    finals = {}
    try:
        with open(path) as f:
            for line in f:
                if line == SLOW_LOG_HEADER or line == COMPACTED_HEADER:
                    continue
                parts = line.rstrip("\n").split("\t", maxsplit=4)
                if len(parts) < 5:
                    continue
                ts, kind, qid, duration_str, sparql = parts
                try:
                    duration_s = int(duration_str)
                except ValueError:
                    continue
                if kind == "start":
                    starts[qid] = (ts, duration_s, sparql)
                elif kind == "finish":
                    finals[qid] = duration_s
                elif kind == "finished" and qid not in prior_finished:
                    prior_finished[qid] = (ts, duration_s, sparql)
                elif kind == "unfinished" and qid not in prior_unfinished:
                    prior_unfinished[qid] = (ts, duration_s, sparql)
    except FileNotFoundError:
        return
    if not prior_finished and not prior_unfinished and not starts:
        return
    with open(path, "w") as f:
        f.write(COMPACTED_HEADER)
        for qid, (ts, duration, sparql) in prior_finished.items():
            f.write(f"{ts}\tfinished\t{qid}\t{duration}\t{sparql}\n")
        for qid, (ts, duration, sparql) in prior_unfinished.items():
            if qid in starts or qid in prior_finished:
                continue
            f.write(f"{ts}\tunfinished\t{qid}\t{duration}\t{sparql}\n")
        for qid, (logged_at, start_duration, sparql) in starts.items():
            if qid in prior_finished:
                continue
            duration = finals.get(qid, start_duration)
            status = "finished" if qid in finals else "unfinished"
            f.write(f"{logged_at}\t{status}\t{qid}\t{duration}\t{sparql}\n")


def format_elapsed(seconds: float) -> str:
    """Format a duration as `Xs`, `Xm Ys`, or `Xh Ym`."""
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m {total % 60}s"
    return f"{total // 3600}h {(total % 3600) // 60}m"


def format_metrics_line(
    active: int,
    peak_concurrent: int,
    slow_logged_total: int,
    warn_after: float,
    session_elapsed_s: float,
) -> Text:
    """Render the session metrics row shown above the windowed rows."""
    threshold = int(warn_after)
    elapsed = format_elapsed(session_elapsed_s)
    return Text.from_markup(
        f"[bold green]{'Session':<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
        f"Active queries: [cyan]{active}[/cyan]   "
        f"Peak concurrent: [cyan]{peak_concurrent}[/cyan]   "
        f"Slow queries logged (>{threshold}s): [red]{slow_logged_total}[/red]"
        f"   [dim](running {elapsed})[/dim]"
    )


def nearest_rank_percentile(sorted_values: list[int], pct: float) -> int:
    """Nearest-rank percentile of an already-sorted list of ints."""
    idx = int(pct * (len(sorted_values) - 1))
    return sorted_values[idx]


def format_window_line(
    label: str,
    window_s: int,
    elapsed_s: float,
    finish_events: deque,
    slow_start_events: deque,
) -> Text:
    """Render one windowed-metrics row (5m / 15m / 1h)."""
    if elapsed_s < window_s:
        return Text.from_markup(
            f"[bold yellow]{label:<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
            f"[dim]warming up...[/dim]"
        )
    cutoff = time.monotonic() - window_s
    durations = sorted(d for ts, d in finish_events if ts >= cutoff)
    slow_logged = sum(1 for ts in slow_start_events if ts >= cutoff)
    if durations:
        p50 = f"{nearest_rank_percentile(durations, 0.50)}s"
        p95 = f"{nearest_rank_percentile(durations, 0.95)}s"
    else:
        p50 = p95 = "—"
    return Text.from_markup(
        f"[bold yellow]{label:<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
        f"p50 duration: [cyan]{p50}[/cyan]   "
        f"p95 duration: [cyan]{p95}[/cyan]   "
        f"Slow queries logged: [red]{slow_logged}[/red]"
    )


QID_COL_WIDTH = 14


def truncate_qid(qid: str) -> str:
    """Truncate a qid to fit the Query ID column, with a trailing ellipsis."""
    if len(qid) <= QID_COL_WIDTH:
        return qid
    return qid[: QID_COL_WIDTH - 1] + "…"


def duration_sort_key(cell) -> int:
    """Parse a Duration cell into an int for sorting; "N/A" -> -1."""
    text = cell.plain if hasattr(cell, "plain") else str(cell)
    try:
        return int(text.rstrip("s"))
    except ValueError:
        return -1


class MonitorApp(App):
    """Textual app for the interactive monitor-queries TUI.

    Polls every `interval` seconds and renders active queries in a
    DataTable. Selecting a row (Enter or double-click) shows the
    pretty-printed SPARQL in a detail pane; arrow keys browse without
    triggering the (slow) pretty-printer.

    The cursor re-anchors to the selected qid each refresh so the
    highlight follows the row even when the duration sort reorders.
    On fetch failure the last good rows stay visible while a status
    caption shows the retry count; the app exits after
    MAX_CONSECUTIVE_FAILURES.
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("c", "clear_detail", "Clear SPARQL"),
        ("y", "copy_sparql", "Copy SPARQL"),
        ("f", "freeze", "Freeze/Unfreeze table"),
        ("d", "toggle_dark", "Toggle dark mode"),
    ]
    # Table and detail-scroll both 1fr so they share leftover space
    # evenly and the detail pane doesn't jump as the table resizes.
    CSS = """
    #metrics { padding-left: 1; }
    Static.window-row { padding-left: 1; }
    #window-1h { margin-bottom: 1; }
    DataTable { height: 1fr; }
    #detail-scroll { height: 1fr; }
    #detail { padding-left: 2; }
    """

    def __init__(
        self,
        monitor_queries_cmd: str,
        interval: float,
        warn_after: float,
        warning_log: str,
        system: str,
    ) -> None:
        """Initialize the app and the per-session state."""
        super().__init__()
        self.monitor_queries_cmd = monitor_queries_cmd
        self.interval = interval
        self.warn_after = warn_after
        self.warning_log = warning_log
        self.system = system
        # None until the first non-empty fetch: True for newer servers
        # (dict info with started_at), False for older ones (plain
        # SPARQL string). None also means columns aren't set up yet.
        self.has_duration = None
        self.failures = 0
        self.queries_dict = {}
        # Identify selection by qid, not row index — indices renumber
        # every tick.
        self.selected_qid = None
        # Cached pretty-printed SPARQL so copy keeps working after the
        # query finishes server-side.
        self.selected_query_text = None
        self.freeze = False
        # qid -> started_at for queries already logged as slow.
        self.slow_seen = {}
        self.peak_concurrent = 0
        # Cumulative threshold-crossings this session. Distinct from
        # len(slow_seen), which is currently-active slow queries.
        self.slow_logged_total = 0
        # Caps in-flight fetches at one so a slow server can't pile up
        # worker threads. Reset in apply_fetch on the main thread.
        self.is_fetching = False
        # Monotonic so it's immune to wall-clock jumps.
        self.started_at = time.monotonic()
        # Per-window event log. (ts_monotonic, duration_s) for each
        # observed finish; ts_monotonic for each new slow-threshold
        # crossing. Pruned to MAX_WINDOW_S each fetch.
        self.finish_events = deque()
        self.slow_start_events = deque()

    def compose(self) -> ComposeResult:
        """Build the widget tree: metrics, table, status, detail, footer."""
        yield Static("", id="metrics")
        for key, _, _ in WINDOWS_S:
            yield Static("", id=f"window-{key}", classes="window-row")
        # cursor_type="row" so a click selects the whole row and emits
        # RowSelected (default is "cell").
        yield DataTable(cursor_type="row")
        yield Static("", id="status")
        # Wrap detail in VerticalScroll so long SPARQL scrolls inside
        # the pane instead of squeezing the table.
        with VerticalScroll(id="detail-scroll"):
            yield Static("", id="detail")
        yield Footer()

    def on_mount(self) -> None:
        """Show the hint, dispatch the first fetch, start the timers."""
        # Columns are added lazily once we've seen real data, since
        # DataTable can't insert a column mid-stream and we don't know
        # whether to include Duration until the first fetch.
        # Two timers: a 1s repaint so durations advance smoothly, and
        # a slower fetch timer at the user's interval. Decoupling them
        # avoids the "two repaints clustered, then long pause" jank
        # that came from doing both on a single timer.
        self.theme = "textual-dark"
        self.query_one("#detail", Static).update(HINT_TEXT)
        # Paint a placeholder so the user sees something while the
        # first fetch (which can take up to --max-time) is in flight.
        self.query_one("#metrics", Static).update(
            Text.from_markup(
                f"[bold green]{'Session':<{LABEL_WIDTH}}[/] [dim]│[/dim]  "
                f"[dim]waiting for first fetch...[/dim]"
            )
        )
        self.dispatch_fetch()
        self.set_interval(1.0, self.repaint_timer)
        self.set_interval(self.interval, self.fetch_timer)

    def setup_columns(self) -> None:
        """Add columns once `has_duration` is known. Called once."""
        table = self.query_one(DataTable)
        table.add_column("Query ID", width=QID_COL_WIDTH, key="qid")
        if self.has_duration:
            # Header as Text to match the right-justified cell values.
            table.add_column(
                Text("Duration", justify="right"), width=8, key="duration"
            )
        table.add_column("SPARQL", key="sparql")
        if not self.has_duration:
            self.query_one("#metrics", Static).display = False
            for key, _, _ in WINDOWS_S:
                self.query_one(f"#window-{key}", Static).display = False

    def repaint_timer(self) -> None:
        """1s timer callback: repaint cached durations and metrics."""
        if self.freeze:
            return
        self.rerender_from_cache()

    def fetch_timer(self) -> None:
        """Interval timer callback: dispatch a fetch worker."""
        if self.freeze:
            return
        self.dispatch_fetch()

    def dispatch_fetch(self) -> None:
        """Start a curl worker if none is in flight.

        Only one fetch in flight at a time so a slow server can't pile
        up worker threads. thread=True keeps the blocking subprocess
        off the event loop; the worker calls back via call_from_thread
        so widget mutation stays single-threaded.
        """
        if self.is_fetching:
            return
        self.is_fetching = True
        self.run_worker(self.fetch_in_thread, thread=True)

    def fetch_in_thread(self) -> None:
        """Worker body: fetch off the event loop, hand result back."""
        queries_dict = fetch_queries(self.monitor_queries_cmd)
        self.call_from_thread(self.apply_fetch, queries_dict)

    def apply_fetch(self, queries_dict: dict | None) -> None:
        """Process a fetch result on the main thread."""
        self.is_fetching = False
        status = self.query_one("#status", Static)
        if queries_dict is None:
            self.failures += 1
            if self.failures > MAX_CONSECUTIVE_FAILURES:
                # exit() returns from App.run() so the try/finally in
                # execute() still runs compact_slow_log.
                self.exit(
                    message=(
                        f"Failed to fetch active queries more than "
                        f"{MAX_CONSECUTIVE_FAILURES} times."
                    )
                )
                return
            status.update(
                f"fetch failed, retrying... "
                f"({self.failures}/{MAX_CONSECUTIVE_FAILURES})"
            )
            return
        self.failures = 0
        status.update("")
        prev_queries = self.queries_dict
        self.queries_dict = queries_dict
        # Lock the server format and configure columns on the first
        # non-empty fetch.
        if self.has_duration is None and queries_dict:
            self.has_duration = server_supports_duration(queries_dict)
            self.setup_columns()
        if self.has_duration is None:
            # Empty server, format not yet locked in. Paint metrics so
            # the placeholder is replaced with real zeros.
            self.rerender_from_cache()
            return
        if self.has_duration and len(queries_dict) > self.peak_concurrent:
            self.peak_concurrent = len(queries_dict)
        if self.has_duration:
            # Snapshot slow_seen before detect_slow_queries so the post-call
            # set diff is exactly this tick's new threshold-crossings.
            slow_seen_before = set(self.slow_seen)
            detect_slow_queries(
                queries_dict,
                self.slow_seen,
                self.warn_after,
                self.warning_log,
            )
            new_slow = set(self.slow_seen) - slow_seen_before
            self.slow_logged_total += len(new_slow)
            self.record_window_events(prev_queries, queries_dict, new_slow)

        table = self.query_one(DataTable)
        # Capture cursor's qid before mutation: the cursor is tracked
        # by index, so removing a row above it would shift it to a
        # different qid.
        existing_qids = [rk.value for rk in table.rows]
        cursor_row = (
            table.cursor_coordinate.row
            if table.cursor_coordinate is not None
            else None
        )
        cursor_qid = (
            existing_qids[cursor_row]
            if cursor_row is not None and 0 <= cursor_row < len(existing_qids)
            else None
        )

        new_qid_set = set(queries_dict)
        for qid in existing_qids:
            if qid not in new_qid_set:
                table.remove_row(qid)

        # Duration cells are filled by rerender_from_cache below.
        existing_qid_set = set(existing_qids)
        for qid, info in queries_dict.items():
            if qid in existing_qid_set:
                continue
            query_text = info["query"] if isinstance(info, dict) else info
            sparql = re.sub(r"\s+", " ", query_text).strip()
            display_qid = truncate_qid(qid)
            if self.has_duration:
                table.add_row(display_qid, "", sparql, key=qid)
            else:
                table.add_row(display_qid, sparql, key=qid)

        # Prefer the explicit selection so the highlight tracks it;
        # else restore to wherever the cursor was before the diff.
        target_qid = self.selected_qid or cursor_qid
        if target_qid is not None:
            new_qids = [rk.value for rk in table.rows]
            if target_qid in new_qids:
                table.move_cursor(row=new_qids.index(target_qid))

        self.rerender_from_cache()

    def record_window_events(
        self, prev_queries: dict, queries_dict: dict, new_slow: set
    ) -> None:
        """Append finish/slow-start events to the window deques and prune.

        A "finish" is a qid present in the previous snapshot but absent
        now; duration is derived from its server-reported started-at.
        """
        now_mono = time.monotonic()
        now_ms = int(time.time() * 1000)
        for qid in set(prev_queries) - set(queries_dict):
            info = prev_queries.get(qid)
            started_at = (
                info.get("started-at") if isinstance(info, dict) else None
            )
            if started_at is not None:
                duration_s = (now_ms - started_at) // 1000
                self.finish_events.append((now_mono, duration_s))
        for _ in new_slow:
            self.slow_start_events.append(now_mono)
        cutoff = now_mono - MAX_WINDOW_S
        while self.finish_events and self.finish_events[0][0] < cutoff:
            self.finish_events.popleft()
        while self.slow_start_events and self.slow_start_events[0] < cutoff:
            self.slow_start_events.popleft()

    def rerender_from_cache(self) -> None:
        """Repaint durations and metrics from the cached snapshot.

        No I/O, no row add/remove. Runs every tick (so durations tick
        up while a fetch is in flight) and at the tail of apply_fetch
        (so a fresh snapshot is visible immediately). Metrics paint
        unconditionally so an empty server still shows real zeros
        instead of the "waiting for first fetch" placeholder; the
        old-server path hides those rows via setup_columns.
        """
        if self.has_duration:
            table = self.query_one(DataTable)
            now_ms = int(time.time() * 1000)
            for row_key in table.rows:
                qid = row_key.value
                info = self.queries_dict.get(qid)
                started_at = (
                    info.get("started-at") if isinstance(info, dict) else None
                )
                if started_at is not None:
                    duration_s = (now_ms - started_at) // 1000
                    if duration_s >= self.warn_after:
                        duration_cell = Text.from_markup(
                            f"[red]{duration_s}s[/red]", justify="right"
                        )
                    else:
                        duration_cell = Text(
                            f"{duration_s}s", justify="right"
                        )
                else:
                    duration_cell = Text("N/A", justify="right")
                table.update_cell(row_key, "duration", duration_cell)
            table.sort("duration", key=duration_sort_key, reverse=True)
        elapsed_s = time.monotonic() - self.started_at
        self.query_one("#metrics", Static).update(
            format_metrics_line(
                active=len(self.queries_dict),
                peak_concurrent=self.peak_concurrent,
                slow_logged_total=self.slow_logged_total,
                warn_after=self.warn_after,
                session_elapsed_s=elapsed_s,
            )
        )
        for key, label, window_s in WINDOWS_S:
            self.query_one(f"#window-{key}", Static).update(
                format_window_line(
                    label=label,
                    window_s=window_s,
                    elapsed_s=elapsed_s,
                    finish_events=self.finish_events,
                    slow_start_events=self.slow_start_events,
                )
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Pretty-print (off-thread) and show the SPARQL of the selected row.

        Fires on Enter or click; arrow-key navigation does not fire
        this, by design — pretty_printed_query shells out to
        docker/podman and blocks for hundreds of ms, so it runs in a
        worker thread and the detail pane shows a placeholder while
        it's in flight.
        """
        qid = event.row_key.value if event.row_key else None
        if qid is None or qid == self.selected_qid:
            return
        self.selected_qid = qid
        self.selected_query_text = None
        info = self.queries_dict.get(qid)
        if info is None:
            return
        query_text = info["query"] if isinstance(info, dict) else info
        self.query_one("#detail", Static).update(
            Group(
                Text(f"Server Query ID: {qid}", style="bold"),
                Text(""),
                Text("Pretty-printing...", style="dim italic"),
            )
        )
        self.run_worker(
            lambda: self.pretty_print_in_thread(qid, query_text), thread=True
        )

    def pretty_print_in_thread(self, qid: str, query_text: str) -> None:
        """Worker body: pretty-print off the event loop, hand result back."""
        pretty = pretty_printed_query(query_text, True, self.system)
        self.call_from_thread(self.apply_pretty, qid, pretty)

    def apply_pretty(self, qid: str, pretty: str) -> None:
        """Install pretty-printed SPARQL if the selection hasn't moved on."""
        # Discard stale results: if the user has already selected a
        # different row, the in-flight worker's output would overwrite
        # the newer selection.
        if qid != self.selected_qid:
            return
        self.selected_query_text = pretty
        self.query_one("#detail", Static).update(
            self.render_detail(qid, pretty)
        )

    def render_detail(self, qid: str, pretty: str) -> Group:
        """Render the detail pane: bold qid plus highlighted SPARQL."""
        is_dark = "light" not in self.theme
        syntax_theme = "monokai" if is_dark else "default"
        syntax = Syntax(
            pretty,
            "sparql",
            theme=syntax_theme,
            word_wrap=True,
        )
        return Group(
            Text(f"Server Query ID: {qid}", style="bold"), Text(""), syntax
        )

    def action_freeze(self) -> None:
        """Pause or resume the periodic table refresh."""
        self.freeze = not self.freeze
        status = self.query_one("#status", Static)
        status.update("paused — press 'f' to resume\n" if self.freeze else "")

    def action_toggle_dark(self) -> None:
        """Flip between textual-dark and textual-light themes."""
        self.theme = (
            "textual-light" if self.theme == "textual-dark" else "textual-dark"
        )
        if (
            self.selected_qid is not None
            and self.selected_query_text is not None
        ):
            self.query_one("#detail", Static).update(
                self.render_detail(self.selected_qid, self.selected_query_text)
            )

    def action_clear_detail(self) -> None:
        """Clear the detail pane and restore the hint."""
        self.selected_qid = None
        self.selected_query_text = None
        self.query_one("#detail", Static).update(HINT_TEXT)

    def action_copy_sparql(self) -> None:
        """Copy the selected query's pretty-printed SPARQL to the clipboard."""
        if self.selected_qid is None or self.selected_query_text is None:
            self.notify("No query selected", severity="warning")
            return
        ok = copy_text(self.selected_query_text)
        msg = "Copied" if ok else "Copy failed"
        self.notify(msg)
