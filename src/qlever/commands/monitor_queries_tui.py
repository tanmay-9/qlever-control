from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
import time
from datetime import datetime

from rich.console import Group
from rich.syntax import Syntax
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer, Static

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import pretty_printed_query

MAX_CONSECUTIVE_FAILURES = 5
SLOW_LOG_HEADER = "logged_at\tevent\tqid\tduration_s\tsparql\n"
HINT_TEXT = (
    "Click a row (or press Enter on a cursored row) to view its full"
    " SPARQL. Arrow keys move the cursor without triggering pretty-print."
)


def copy_text(text: str) -> bool:
    """
    Cross-platform clipboard copy for Textual / TUI apps.

    Supports:
      - macOS   : pbcopy
      - Linux   : wl-copy (Wayland) OR xclip/xsel (X11), never both —
                  falling through to xclip on a Wayland session writes
                  to the XWayland selection that Wayland apps don't read.
    """
    try:
        system = platform.system()

        candidates = []
        if system == "Darwin":
            candidates.append(["pbcopy"])
        elif system == "Linux":
            on_wayland = bool(os.environ.get("WAYLAND_DISPLAY"))
            if on_wayland and shutil.which("wl-copy"):
                # Force text/plain so wl-copy doesn't auto-detect a
                # different MIME type from the content. SPARQL queries
                # starting with `PREFIX foo: <http://...>` otherwise
                # get tagged as a URI-ish type and browsers requesting
                # text/plain on paste get nothing.
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
    """Fetch and parse active queries from the SPARQL endpoint.

    Returns None on failure (network error, non-JSON response), {} when
    the server reports no active queries. Stays silent on failure on
    purpose — the Textual app owns the screen, so any log/print here
    would corrupt the rendered display. Callers surface failures via
    the status caption instead.
    """
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
    """Append a single TSV-formatted slow-query event to the warning log.

    Writes a column header if the file is empty or newly created.
    """
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
    """Log start/finish events for queries that cross the slow threshold.

    Mutates slow_seen in place (qid -> started_at). Appends a 'start' event
    when a query first exceeds warn_after, and a 'finish' event when a
    previously logged slow query is no longer active.
    """
    now_ms = int(time.time() * 1000)

    # Queries that finished since last poll: log with final duration.
    for qid in list(slow_seen):
        if qid not in queries_dict:
            final_s = (now_ms - slow_seen[qid]) // 1000
            append_slow_log(log_path, "finish", qid, final_s)
            del slow_seen[qid]

    # Queries that just crossed the threshold: log start event.
    for qid, info in queries_dict.items():
        if not isinstance(info, dict) or qid in slow_seen:
            continue
        started_at = info.get("started_at")
        if started_at is None:
            continue
        duration_s = (now_ms - started_at) // 1000
        if duration_s >= warn_after:
            slow_seen[qid] = started_at
            sparql = re.sub(r"\s+", " ", info["query"]).strip()
            append_slow_log(log_path, "start", qid, duration_s, sparql)


def compact_slow_log(path: str) -> None:
    """Collapse start/finish event pairs in the log into one row per qid.

    Reads the append-only log, pairs each 'start' with its matching 'finish'
    by qid, and rewrites the file with a single row per query showing the
    final duration (or the last-known duration if the query was still
    running at compaction time). No-op if the log does not exist or has
    no start events.
    """
    starts = {}
    finals = {}
    try:
        with open(path) as f:
            for line in f:
                if line == SLOW_LOG_HEADER:
                    continue
                parts = line.rstrip("\n").split("\t", maxsplit=4)
                if len(parts) < 5:
                    continue
                ts, event, qid, duration_str, sparql = parts
                try:
                    duration_s = int(duration_str)
                except ValueError:
                    continue
                if event == "start":
                    starts[qid] = (ts, duration_s, sparql)
                elif event == "finish":
                    finals[qid] = duration_s
    except FileNotFoundError:
        return
    if not starts:
        return
    with open(path, "w") as f:
        f.write("logged_at\tstatus\tqid\tduration_s\tsparql\n")
        for qid, (logged_at, start_duration, sparql) in starts.items():
            duration = finals.get(qid, start_duration)
            status = "finished" if qid in finals else "unfinished"
            f.write(f"{logged_at}\t{status}\t{qid}\t{duration}\t{sparql}\n")


class MonitorApp(App):
    """Textual app for the interactive monitor-queries TUI.

    Polls the server every `interval` seconds and renders active queries
    in a DataTable. Selecting a row (click on the cursored row, or
    Enter while the cursor is on it) shows the full pretty-printed
    SPARQL of that query in a detail pane below the table. Arrow keys
    move the cursor for browsing — they do not trigger the docker-based
    pretty-printer, so navigation stays snappy.

    Until the user selects something the detail pane shows a short
    hint. After a selection the cursor is re-anchored to the selected
    qid on every refresh so the highlight tracks the selected query
    even when the row order shifts. If the selected query disappears
    from the server, the cursor stays where it was and the detail pane
    keeps showing the last seen SPARQL until the user presses `c` or
    selects a different row.

    On fetch failure the last good rows stay visible (frozen) while a
    status caption shows the retry count; after MAX_CONSECUTIVE_FAILURES
    consecutive failures the app exits.
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("c", "clear_detail", "Clear SPARQL"),
        ("y", "copy_sparql", "Copy SPARQL"),
        ("f", "freeze", "Freeze/Unfreeze table"),
        ("d", "toggle_dark", "Toggle dark mode"),
    ]
    # Use Textual's default theme rather than inheriting terminal
    # colors — partial inheritance left the Footer's text invisible
    # (its theme-driven background collided with ANSI defaults).
    CSS = "#detail { padding: 2; padding-top: 0; }"

    def __init__(
        self,
        monitor_queries_cmd: str,
        interval: float,
        warn_after: float,
        warning_log: str,
        system: str,
    ) -> None:
        """Stash the fetch command, refresh interval, slow threshold,
        slow-query log path, and container system.

        State that must survive across ticks (`has_duration`,
        `failures`, the latest `queries_dict`, the sticky selection,
        `slow_seen`) lives on the instance — in the original Live loop
        these were locals, but Textual calls `refresh_table` from a
        timer so each tick is a fresh frame.
        """
        super().__init__()
        self.monitor_queries_cmd = monitor_queries_cmd
        self.interval = interval
        self.warn_after = warn_after
        self.warning_log = warning_log
        self.system = system
        # Locked on the first non-empty fetch: True for newer QLever
        # servers (dict-shaped query info with started_at), False for
        # older servers (plain SPARQL string). None = not yet known,
        # which also means columns haven't been added yet.
        self.has_duration = None
        self.failures = 0
        # Most recent successful fetch; the row-click handler reads from
        # this rather than re-hitting the server.
        self.queries_dict = {}
        # Sticky selection survives table refreshes (we identify by qid
        # rather than row index — indices renumber every tick).
        self.selected_qid = None
        # Raw SPARQL of the selected query, cached at selection time so
        # `y` keeps working even after the query has finished
        # server-side and is no longer in queries_dict.
        self.selected_query_text = None
        self.freeze = False
        # qid -> started_at for queries that have crossed the slow
        # threshold and had a 'start' row appended to the warning log.
        # Mutated in place by detect_slow_queries.
        self.slow_seen = {}

    def compose(self) -> ComposeResult:
        """Build the widget tree: DataTable, status, detail, footer."""
        # cursor_type="row" so a click selects the whole row and emits
        # RowSelected (default cursor_type is "cell").
        yield DataTable(cursor_type="row")
        yield Static("", id="status")
        yield Static("", id="detail")
        # Footer renders the BINDINGS list as labelled key hints,
        # docked to the bottom by Textual's default CSS.
        yield Footer()

    def on_mount(self) -> None:
        """Kick off the first refresh and start the recurring timer.

        Columns are added lazily in `setup_columns` once we've seen
        real data — that's how the Duration column gets hidden on
        servers that don't support it (DataTable can't insert a column
        mid-stream, so we defer the whole column setup until we know).
        """
        self.theme = "textual-dark"
        self.query_one("#detail", Static).update(HINT_TEXT)
        self.refresh_table()
        self.set_interval(self.interval, self.refresh_table)

    def setup_columns(self) -> None:
        """Add columns now that `has_duration` is known.

        Called exactly once, on the first non-empty fetch. The Duration
        column is omitted entirely on older servers.
        """
        table = self.query_one(DataTable)
        table.add_column("#", width=3, key="idx")
        table.add_column("Query ID", width=18, key="qid")
        if self.has_duration:
            table.add_column("Duration", width=8, key="duration")
        table.add_column("SPARQL", key="sparql")

    def refresh_table(self) -> None:
        """Fetch active queries and rerender the table.

        Skipped entirely while `self.freeze` is True — the timer keeps
        ticking but does nothing, so the last-rendered rows stay put.
        On fetch failure the table is left untouched (durations freeze)
        and the status caption shows the retry count; the app exits if
        failures pass the threshold.
        """
        if self.freeze:
            return
        queries_dict = fetch_queries(self.monitor_queries_cmd)
        status = self.query_one("#status", Static)
        if queries_dict is None:
            self.failures += 1
            if self.failures > MAX_CONSECUTIVE_FAILURES:
                # Triggers App.run() to return, so the try/finally in
                # execute() still runs compact_slow_log on the way out.
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
        # Capture the cursor's current qid before we mutate state — used
        # below to preserve cursor position across the clear+repopulate
        # so arrow-key browsing isn't undone on every tick.
        table = self.query_one(DataTable)
        old_qids = list(self.queries_dict.keys())
        cursor_row = (
            table.cursor_coordinate.row
            if table.cursor_coordinate is not None
            else None
        )
        cursor_qid = (
            old_qids[cursor_row]
            if cursor_row is not None and 0 <= cursor_row < len(old_qids)
            else None
        )
        self.queries_dict = queries_dict
        # Lock the server's format and configure columns the first time
        # we actually see data.
        if self.has_duration is None and queries_dict:
            self.has_duration = server_supports_duration(queries_dict)
            self.setup_columns()
        # Still waiting for the first non-empty fetch — no columns yet,
        # nothing to render.
        if self.has_duration is None:
            return
        # Append slow-query start/finish events to the warning log.
        # No-op on servers that don't report started_at.
        if self.has_duration:
            detect_slow_queries(
                queries_dict,
                self.slow_seen,
                self.warn_after,
                self.warning_log,
            )
        table.clear()
        now_ms = int(time.time() * 1000)
        for i, (qid, info) in enumerate(queries_dict.items(), 1):
            query_text = info["query"] if isinstance(info, dict) else info
            sparql = re.sub(r"\s+", " ", query_text).strip()
            if self.has_duration:
                started_at = (
                    info.get("started_at") if isinstance(info, dict) else None
                )
                if started_at is not None:
                    duration_s = (now_ms - started_at) // 1000
                    if duration_s >= self.warn_after:
                        # Text.from_markup keeps the [red]...[/red] markup
                        # working inside DataTable cells.
                        duration_cell = Text.from_markup(
                            f"[red]{duration_s}s[/red]"
                        )
                    else:
                        duration_cell = f"{duration_s}s"
                else:
                    duration_cell = "N/A"
                # Row key = qid so a click can resolve back to a query.
                table.add_row(str(i), qid, duration_cell, sparql, key=qid)
            else:
                table.add_row(str(i), qid, sparql, key=qid)
        # Restore cursor: prefer the user's explicit selection so the
        # highlight tracks it across refreshes; otherwise restore to the
        # qid the cursor was on before the refresh, so plain arrow-key
        # browsing isn't reset every tick. If neither qid is still in
        # the table, leave the cursor at its post-clear default (row 0).
        target_qid = self.selected_qid or cursor_qid
        if target_qid is not None:
            new_qids = list(queries_dict.keys())
            if target_qid in new_qids:
                table.move_cursor(row=new_qids.index(target_qid))

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Show the full pretty-printed SPARQL for the selected row.

        Fires on Enter while the cursor is on a row, or on a click on
        the row that already holds the cursor. Arrow-key navigation
        alone does NOT fire this — that's by design, so the docker-based
        pretty-printer doesn't run on every keystroke.

        `row_key.value` is the qid we passed to `add_row(..., key=qid)`.
        Reads from `self.queries_dict` (last successful fetch) so we
        don't hit the server twice. `pretty_printed_query` shells out
        to docker/podman, so this blocks the UI for a few hundred ms —
        acceptable for an explicit selection.
        """
        qid = event.row_key.value if event.row_key else None
        if qid is None or qid == self.selected_qid:
            return
        self.selected_qid = qid
        info = self.queries_dict.get(qid)
        if info is None:
            return
        query_text = info["query"] if isinstance(info, dict) else info
        # Cache the raw SPARQL for the copy action — keeps copy working
        # if the query finishes server-side after this selection.
        pretty = pretty_printed_query(query_text, True, self.system)
        self.selected_query_text = pretty
        self.query_one("#detail", Static).update(self.render_detail(qid, pretty))

    def render_detail(self, qid: str, pretty: str) -> Group:
        """Build the detail-pane renderable: bold qid + highlighted SPARQL.

        Uses Pygments via rich.syntax. Theme tracks self.theme so the
        highlight stays readable after a light/dark mode toggle. The
        Syntax background is set to match the pane so the highlighter
        doesn't paint its own block of color over the Static.
        """
        is_dark = "light" not in self.theme
        syntax_theme = "monokai" if is_dark else "default"
        syntax = Syntax(
            pretty,
            "sparql",
            theme=syntax_theme,
            word_wrap=True,
        )
        return Group(Text(f"Server Query ID: {qid}", style="bold"), Text(""), syntax)

    def action_freeze(self) -> None:
        """Pause or resume the periodic table refresh."""
        self.freeze = not self.freeze
        status = self.query_one("#status", Static)
        status.update(
            "paused — press 'f' to resume\n" if self.freeze else ""
        )

    def action_toggle_dark(self) -> None:
        """Quick switch between textual-dark and textual-light.

        Independent of the command palette's full theme picker: this
        is just a one-key flip between the two textual theme variants.
        Also re-renders the detail pane so the SPARQL syntax theme
        follows the new mode.
        """
        self.theme = (
            "textual-light" if self.theme == "textual-dark" else "textual-dark"
        )
        if self.selected_qid is not None and self.selected_query_text is not None:
            self.query_one("#detail", Static).update(
                self.render_detail(self.selected_qid, self.selected_query_text)
            )

    def action_clear_detail(self) -> None:
        """Clear the selected-query detail pane (bound to `c`).

        Restores the initial hint text so the user knows what to do
        next; the cursor stays visible because Textual ties cursor
        visibility to interactivity (hiding it disables click/Enter).
        """
        self.selected_qid = None
        self.selected_query_text = None
        self.query_one("#detail", Static).update(HINT_TEXT)

    def action_copy_sparql(self) -> None:
        """Copy the selected query's raw SPARQL to the clipboard (`y`).

        Uses Textual's `copy_to_clipboard` which writes via OSC 52
        escape sequences — the terminal emulator picks it up and routes
        to the system clipboard. Works over SSH if the local terminal
        supports OSC 52 (modern alacritty, kitty, gnome-terminal,
        iterm). Copies the raw SPARQL rather than the pretty-printed
        form so it round-trips cleanly through any SPARQL endpoint.
        """
        if self.selected_qid is None or self.selected_query_text is None:
            self.notify("No query selected", severity="warning")
            return
        self.do_copy(self.selected_query_text)

    @work(thread=True, exclusive=True)
    def do_copy(self, text: str) -> None:
        """Run the clipboard copy on a worker thread.

        Off-thread so a slow clipboard tool can't stall the 2s refresh
        tick. exclusive=True drops the result of any older copy still
        in flight when the user presses 'y' again, so only the latest
        selection produces a toast.
        """
        nbytes = len(text.encode("utf-8"))
        ok = copy_text(text)
        msg = f"Copied ({nbytes} B)" if ok else f"Copy failed ({nbytes} B)"
        self.call_from_thread(self.notify, msg)


class MonitorQueriesTuiCommand(QleverCommand):
    """
    Class for executing the `monitor-queries-tui` command.
    """

    def __init__(self):
        pass

    def description(self) -> str:
        return (
            "Show the currently active queries on the server (interactive TUI)"
        )

    def should_have_qleverfile(self) -> bool:
        return False

    def relevant_qleverfile_arguments(self) -> dict[str, list[str]]:
        return {
            "data": ["name"],
            "server": ["access_token", "host_name", "port", "timeout"],
            "runtime": ["system"],
        }

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "--sparql-endpoint",
            help="URL of the SPARQL endpoint, default is {host_name}:{port}",
        )
        subparser.add_argument(
            "--interval",
            type=float,
            default=2.0,
            help="Refresh interval in seconds (default = 2.0)",
        )
        subparser.add_argument(
            "--warn-after",
            type=float,
            default=None,
            help="Duration in seconds after which an active query is logged"
            " as slow (default = server timeout - 10s)",
        )
        subparser.add_argument(
            "--warning-log",
            type=str,
            default=None,
            help="File to append slow-query warnings to"
            " (default = {name}.slow-queries.tsv)",
        )

    def execute(self, args) -> bool:
        sparql_endpoint = (
            args.sparql_endpoint
            if args.sparql_endpoint
            else f"{args.host_name}:{args.port}"
        )
        monitor_queries_cmd = (
            f'curl -s {sparql_endpoint} --data-urlencode "cmd=dump-active-queries" '
            f'--data-urlencode access-token="{args.access_token}"'
        )

        self.show(monitor_queries_cmd, only_show=args.show)
        if args.show:
            return True

        if args.interval < 1:
            log.error("--interval must be at least 1 second")
            return False

        if args.warn_after is None:
            try:
                timeout_s = float(args.timeout.rstrip("s"))
            except ValueError:
                log.error(
                    f"Could not parse server timeout {args.timeout!r};"
                    " pass --warn-after explicitly"
                )
                return False
            args.warn_after = max(1.0, timeout_s - 10)
        if args.warning_log is None:
            args.warning_log = f"{args.name}.slow-queries.tsv"

        try:
            MonitorApp(
                monitor_queries_cmd,
                args.interval,
                args.warn_after,
                args.warning_log,
                args.system,
            ).run()
        finally:
            compact_slow_log(args.warning_log)
        return True
