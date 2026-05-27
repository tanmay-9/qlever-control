from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path

import psutil

from qlever import engine_name
from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import run_command

GB = 10**9


def format_gb(bytes_val: float) -> str:
    """Format a byte count as decimal GB with two decimals."""
    return f"{bytes_val / GB:.2f} GB"


@dataclass
class Sample:
    """
    One resource measurement. `rss` is populated in native mode (via
    psutil.Process.memory_info — cheap `/proc/<pid>/statm` read) and
    in container mode (via docker/podman stats). Unavailable fields
    stay `None` and are written as empty columns in the TSV.
    """

    elapsed_s: float | None = None
    rss: int | None = None
    cpu_percent: float | None = None


def parse_container_mem_usage(usage: str) -> int:
    """
    Parse a memory usage string from `docker stats` or `podman stats`
    into bytes.  Docker reports binary units (GiB, MiB) while Podman
    reports decimal units (GB, MB).
    """
    usage = usage.strip()
    units = {
        "TIB": 1024**4,
        "TB": 1000**4,
        "GIB": 1024**3,
        "GB": 1000**3,
        "MIB": 1024**2,
        "MB": 1000**2,
        "KIB": 1024,
        "KB": 1000,
        "B": 1,
    }
    for suffix, multiplier in units.items():
        if usage.upper().endswith(suffix):
            number = float(usage[: len(usage) - len(suffix)])
            return int(number * multiplier)
    return 0


def sample_to_tsv_row(sample: Sample) -> str:
    """Format a Sample as a TSV row; None fields become empty columns."""
    values = [getattr(sample, f.name) for f in fields(sample)]
    return "\t".join("" if v is None else str(v) for v in values) + "\n"


def compute_phase_boundaries(
    log_path: Path,
) -> tuple[datetime | None, dict[str, tuple[float, float]]]:
    """
    Parse the index build log for phase timestamps. Returns
    (overall_begin, phases), where `overall_begin` is the datetime
    of the first "INFO: Processing" line, and `phases` maps each
    phase name to (start_s, end_s) elapsed seconds relative to
    `overall_begin`. Returns (None, {}) if the log is missing or
    the first phase cannot be located. Phases with incomplete
    timestamps are skipped.
    """
    try:
        lines = log_path.read_text().splitlines()
    except OSError:
        return None, {}

    ts_regex = r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    ts_format = "%Y-%m-%d %H:%M:%S"

    def find(pattern, start=0):
        for j in range(start, len(lines)):
            if re.search(pattern, lines[j]):
                m = re.match(ts_regex, lines[j])
                if m:
                    try:
                        return datetime.strptime(m.group(1), ts_format), j + 1
                    except ValueError:
                        continue
        return None, len(lines)

    overall_begin, idx = find(r"INFO:\s*Processing")
    if overall_begin is None:
        return None, {}

    merge_begin, idx = find(r"INFO:\s*Merging partial vocab", idx)
    convert_begin, idx = find(r"INFO:\s*Converting triples", idx)

    perms = []
    cursor = idx
    while True:
        perm_begin, cursor = find(
            r"INFO:\s*Creating permutations ([A-Z]+ and [A-Z]+)", cursor
        )
        if perm_begin is None:
            break
        m = re.search(
            r"Creating permutations ([A-Z]+ and [A-Z]+)", lines[cursor - 1]
        )
        name = (
            m.group(1).replace(" and ", " & ") if m else f"#{len(perms) + 1}"
        )
        perms.append((perm_begin, name))

    normal_end, idx = find(r"INFO:\s*Index build completed", idx)
    text_begin, _ = find(r"INFO:\s*Adding text index")
    text_end, _ = find(r"INFO:\s*Text index build comp")

    def rel(ts):
        return (ts - overall_begin).total_seconds() if ts else None

    phases = {}

    def add(name, start, end):
        s, e = rel(start), rel(end)
        if s is not None and e is not None:
            phases[name] = (s, e)

    add("Parse input", overall_begin, merge_begin)
    add("Build vocabularies", merge_begin, convert_begin)
    if perms:
        add("Convert to global IDs", convert_begin, perms[0][0])
        perm_names = set()
        for k, (perm_begin, name) in enumerate(perms):
            perm_end = perms[k + 1][0] if k + 1 < len(perms) else normal_end
            if name in perm_names:
                suffix = 2
                while f"{name} ({suffix})" in perm_names:
                    suffix += 1
                name = f"{name} ({suffix})"
            perm_names.add(name)
            add(f"Permutation {name}", perm_begin, perm_end)
    else:
        add("Convert to global IDs", convert_begin, normal_end)
    add("Text index", text_begin, text_end)

    return overall_begin, phases


def parse_git_hash(log_path: Path) -> str | None:
    """Return the git hash printed on the first line of a QLever index log."""
    try:
        first_line = log_path.read_text().splitlines()[0]
    except (OSError, IndexError):
        return None
    m = re.search(r"git hash ([0-9a-f]+)", first_line)
    return m.group(1) if m else None


def parse_qleverfile(qleverfile_path: Path) -> dict[str, str]:
    """Read STXXL_MEMORY and num-triples-per-batch from a QLeverfile."""
    try:
        text = qleverfile_path.read_text()
    except OSError:
        return {}
    info = {}
    stxxl = re.search(r"^\s*STXXL_MEMORY\s*=\s*(\S+)", text, re.MULTILINE)
    if stxxl:
        info["stxxl"] = stxxl.group(1)
    batch = re.search(r'"num-triples-per-batch"\s*:\s*(\d+)', text)
    if batch:
        n = int(batch.group(1))
        if n >= 1_000_000:
            info["batch"] = f"{n / 1_000_000:g}M"
        elif n >= 1_000:
            info["batch"] = f"{n / 1_000:g}K"
        else:
            info["batch"] = str(n)
    return info


def build_info_line(log_path: Path, qleverfile_path: Path) -> str | None:
    """Assemble a 'batch | git | STXXL' line from the index log and QLeverfile."""
    qf = parse_qleverfile(qleverfile_path)
    git_hash = parse_git_hash(log_path)
    parts = []
    if "batch" in qf:
        parts.append(f"batch = {qf['batch']} triples")
    if git_hash:
        parts.append(f"git = {git_hash}")
    if "stxxl" in qf:
        parts.append(f"STXXL = {qf['stxxl']}")
    return "   |   ".join(parts) if parts else None


def read_tsv(path: Path) -> dict:
    """
    Read a usage-log TSV into a dict of numpy arrays keyed by column
    name. Empty cells become NaN so matplotlib can skip them.
    """
    import csv

    import numpy as np

    cols = {}
    with open(path) as f:
        reader = csv.DictReader(f, delimiter="\t")
        if reader.fieldnames is None:
            return {}
        for name in reader.fieldnames:
            cols[name] = []
        for row in reader:
            for name in reader.fieldnames:
                v = row[name]
                cols[name].append(float(v) if v else np.nan)
    return {name: np.array(vals) for name, vals in cols.items()}


def downsample_for_plot(data: dict, max_points: int) -> dict:
    """
    Bucket consecutive samples and reduce each bucket to one point so
    the plot stays readable on long builds. RSS and CPU are reduced
    with `nanmax` so memory peaks survive; `elapsed_s` uses
    `nanmin` so the x-axis stays monotone. Returns `data` unchanged
    if it already has at most `max_points` rows. Note: `peak_rss`
    is tracked separately at full sampling resolution, so this
    aggregation only affects the rendered plot.
    """
    import warnings

    import numpy as np

    n = len(data["elapsed_s"])
    if n <= max_points:
        return data

    bucket = -(-n // max_points)
    n_buckets = -(-n // bucket)
    pad = n_buckets * bucket - n

    out = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        for name, arr in data.items():
            if pad:
                arr = np.concatenate([arr, np.full(pad, np.nan)])
            reshaped = arr.reshape(n_buckets, bucket)
            reducer = np.nanmin if name == "elapsed_s" else np.nanmax
            out[name] = reducer(reshaped, axis=1)
    return out


def pick_time_unit(max_elapsed_s: float) -> tuple[str, float]:
    """Pick an axis label and divisor for the X axis based on build duration."""
    if max_elapsed_s < 200:
        return "Elapsed (s)", 1.0
    if max_elapsed_s < 3600:
        return "Elapsed (min)", 60.0
    return "Elapsed (h)", 3600.0


def annotate_peak(
    ax, x, y_gb, label: str, color: str, offset: tuple[int, int]
):
    """Draw an arrow pointing at the peak of a GB-valued series."""
    import numpy as np

    if np.all(np.isnan(y_gb)):
        return
    idx = int(np.nanargmax(y_gb))
    ax.annotate(
        f"peak {label}: {y_gb[idx]:.2f} GB",
        xy=(x[idx], y_gb[idx]),
        xytext=offset,
        textcoords="offset points",
        arrowprops=dict(arrowstyle="->", color=color),
        fontsize=9,
        color=color,
    )


def plot_usage(
    tsv_path: Path,
    log_path: Path,
    qleverfile_path: Path,
    out_path: Path,
    title: str,
    plot_max_points: int = 500,
) -> None:
    """
    Read the usage TSV and index log, render a dual-axis plot of
    memory and CPU over time with phase bands from the index log,
    and save it to `out_path`. Silently returns if the TSV is empty
    or unreadable. `plot_max_points` caps the number of dots drawn
    per series; raw samples are bucketed and reduced with max so
    memory peaks survive the downsampling.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    data = read_tsv(tsv_path)
    if not data or len(data.get("elapsed_s", [])) == 0:
        return

    valid = np.where(~np.isnan(data["rss"]))[0]
    if len(valid) == 0:
        return
    data = {k: v[valid[0] :] for k, v in data.items()}
    data["elapsed_s"] = data["elapsed_s"] - data["elapsed_s"][0]

    _, phases = compute_phase_boundaries(log_path)

    elapsed_s = data["elapsed_s"]
    if len(elapsed_s) == 0:
        return

    data = downsample_for_plot(data, plot_max_points)
    elapsed_s = data["elapsed_s"]

    x_label, x_factor = pick_time_unit(float(elapsed_s[-1]))
    x = elapsed_s / x_factor
    rss_gb = data["rss"] / GB
    cores = psutil.cpu_count() or 1
    cpu_cores = data["cpu_percent"] / 100.0

    fig, ax_mem = plt.subplots(figsize=(12, 6), constrained_layout=True)
    ax_cpu = ax_mem.twinx()

    band_colors = plt.colormaps["Pastel1"].colors
    total_s = float(elapsed_s[-1]) if len(elapsed_s) else 0.0
    min_label_s = total_s * 0.02
    for i, (name, (start_s, end_s)) in enumerate(phases.items()):
        band_s = end_s - start_s
        if band_s <= 0:
            continue
        ax_mem.axvspan(
            start_s / x_factor,
            end_s / x_factor,
            color=band_colors[i % len(band_colors)],
            alpha=0.4,
            zorder=0,
        )
        if band_s < min_label_s:
            continue
        mid = (start_s + end_s) / 2 / x_factor
        ax_mem.text(
            mid,
            0.98,
            name,
            transform=ax_mem.get_xaxis_transform(),
            ha="center",
            va="top",
            rotation=90,
            fontsize=8,
            alpha=0.7,
        )

    ax_mem.plot(x, rss_gb, color="#cc0000", label="RSS", linewidth=1.5)
    ax_mem.set_xlabel(x_label)
    ax_mem.set_ylabel("RSS Memory (GB)")
    ax_mem.grid(True, linestyle="--", alpha=0.3)

    if not np.all(np.isnan(cpu_cores)):
        ax_cpu.plot(
            x,
            cpu_cores,
            color="#1f77b4",
            label="CPU",
            linewidth=1.2,
            alpha=0.7,
        )
    ax_cpu.set_ylabel(f"CPU (cores, {cores} available)")
    ax_cpu.set_ylim(0, cores)

    max_rss = float(np.nanmax(rss_gb))
    x_max = float(x[-1])
    ax_mem.set_ylim(-max_rss * 0.04, max_rss * 1.4)
    ax_mem.set_xlim(-x_max * 0.02, x_max * 1.06)

    annotate_peak(ax_mem, x, rss_gb, "RSS", "#cc0000", (-25, 20))

    lines_mem, labels_mem = ax_mem.get_legend_handles_labels()
    lines_cpu, labels_cpu = ax_cpu.get_legend_handles_labels()
    ax_mem.legend(
        lines_mem + lines_cpu,
        labels_mem + labels_cpu,
        loc="center left",
        bbox_to_anchor=(1.08, 0.5),
    )

    info_line = build_info_line(log_path, qleverfile_path)
    ax_mem.set_title(f"{title}\n{info_line}" if info_line else title)
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def render_usage_plot(
    dataset: str,
    output_dir: Path | None = None,
    plot_max_points: int = 500,
) -> Path | None:
    """
    Render `<dataset>.usage-log.png` from `<dataset>.usage-log.tsv`
    in `output_dir`. Returns the plot path on success, None if the
    TSV is missing or the plot could not be rendered.
    """
    output_dir = output_dir or Path.cwd()
    tsv_path = output_dir / f"{dataset}.usage-log.tsv"
    log_path = output_dir / f"{dataset}.index-log.txt"
    qleverfile_path = Path.cwd() / "Qleverfile"
    plot_path = output_dir / f"{dataset}.usage-log.png"
    if not tsv_path.exists():
        log.warning(f"Resource-usage TSV not found: {tsv_path}")
        return None
    try:
        plot_usage(
            tsv_path=tsv_path,
            log_path=log_path,
            qleverfile_path=qleverfile_path,
            out_path=plot_path,
            title=f"{engine_name} index build: {dataset}",
            plot_max_points=plot_max_points,
        )
        return plot_path
    except Exception as e:
        log.warning(f"Could not render usage plot: {e}")
        return None


def find_process_by_binary(
    parent_pid: int | None, binary: str
) -> psutil.Process | None:
    """Find a descendant of parent_pid whose argv[0] basename matches binary."""
    try:
        root = psutil.Process(parent_pid)
        candidates = [root] + root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None
    # Both sides use Path(...).name so any path form (relative, absolute,
    # bare, symlink) matches as long as argv[0] preserves the binary's name.
    target = Path(binary).name
    for proc in candidates:
        try:
            cmdline = proc.cmdline()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if cmdline and Path(cmdline[0]).name == target:
            return proc
    return None


def sample_process(proc: psutil.Process) -> Sample:
    """One RSS+CPU read from a psutil.Process; empty Sample on access errors."""
    try:
        mem = proc.memory_info()
        cpu_pct = proc.cpu_percent(interval=None)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return Sample()
    return Sample(rss=mem.rss, cpu_percent=cpu_pct)


def sample_container(system: str, container: str) -> Sample:
    """One RSS+CPU read via `<system> stats --no-stream` on a named container."""
    try:
        output = run_command(
            f"{system} stats --no-stream"
            f" --format '{{{{.MemUsage}}}}\t{{{{.CPUPerc}}}}'"
            f" {container}",
            return_output=True,
        )
        mem_str, cpu_str = output.strip().split("\t")
        usage = mem_str.split("/")[0].strip()
        cpu_pct = float(cpu_str.strip().rstrip("%"))
        return Sample(
            rss=parse_container_mem_usage(usage),
            cpu_percent=cpu_pct,
        )
    except Exception:
        return Sample()


class ResourceMonitor:
    """
    Monitor resource usage (memory, CPU) of an index-building
    process. Works in both native mode (via psutil) and container mode
    (via docker/podman stats).

    Usage as a context manager:

        with ResourceMonitor(dataset="wikidata", binary="qlever-index"):
            run_command(cmd, show_output=True)

        # For container mode:
        with ResourceMonitor(dataset="wikidata",
                             binary="qlever-index",
                             container="qlever.index.wikidata",
                             system="docker"):
            run_command(cmd, show_output=True)
    """

    def __init__(
        self,
        dataset: str,
        binary: str,
        container: str | None = None,
        system: str | None = None,
        interval: float = 1.0,
        output_dir: Path | None = None,
        parent_pid: int | None = None,
    ):
        """
        Args:
            dataset:         Name of the dataset being indexed.
            binary:          Name of the index executable. Its basename
                             is matched against `Path(argv[0]).name` of
                             each process in the descendant tree to find
                             the worker (native mode only).
            container:       Container name to query for memory stats.
                             When set together with `system`, sampling
                             uses `docker/podman stats` instead of
                             psutil.
            system:          Container runtime ("docker" or "podman").
            interval:        Seconds between samples (default 1.0).
            output_dir:      Directory for the TSV usage log file.
            parent_pid:      PID whose descendants are searched for the
                             index process. Defaults to the current
                             Python process. Useful when the target is a
                             daemonized process that re-parents away
                             from us (e.g. virtuoso-t).
        """
        self.dataset = dataset
        self.binary = binary
        self.container = container
        self.system = system
        self.interval = interval
        self.output_dir = output_dir or Path.cwd()
        self.parent_pid = parent_pid
        self.peak_rss = 0
        self.worker_proc = None
        self.log_file = None
        self.stop_event = threading.Event()
        self.start_time = 0

    def take_sample(self) -> Sample:
        """Dispatch to container or native sampling, caching the resolved process."""
        if self.system in Containerize.supported_systems():
            return sample_container(self.system, self.container)
        if self.worker_proc is None or not self.worker_proc.is_running():
            self.worker_proc = find_process_by_binary(
                self.parent_pid, self.binary
            )
            if self.worker_proc is None:
                return Sample()
            try:
                self.worker_proc.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                self.worker_proc = None
                return Sample()
        return sample_process(self.worker_proc)

    def run_loop(self):
        """
        Polling loop on a background thread. Samples resource usage
        and appends one TSV row per iteration until stop_event is set.
        """
        while not self.stop_event.is_set():
            sample = self.take_sample()
            sample.elapsed_s = round(time.monotonic() - self.start_time, 1)
            if sample.rss is not None and self.log_file is not None:
                self.peak_rss = max(self.peak_rss, sample.rss)
                self.log_file.write(sample_to_tsv_row(sample))
                self.log_file.flush()
            self.stop_event.wait(self.interval)

    def __enter__(self):
        """Open the TSV log, write the header, start sampling thread."""
        path = self.output_dir / f"{self.dataset}.usage-log.tsv"
        self.log_file = open(path, "w")
        header = "\t".join(f.name for f in fields(Sample)) + "\n"
        self.log_file.write(header)
        self.log_file.flush()
        self.start_time = time.monotonic()
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop sampling, close the log, log peak RSS."""
        self.stop_event.set()
        self.thread.join()
        self.log_file.close()
        if self.peak_rss > 0:
            log.info(f"Peak memory: RSS {format_gb(self.peak_rss)}")
        return False
