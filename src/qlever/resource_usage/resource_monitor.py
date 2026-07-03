from __future__ import annotations

import threading
import time
from dataclasses import dataclass, fields
from pathlib import Path

import psutil

from qlever.containerize import Containerize
from qlever.log import log
from qlever.util import (
    container_memory_to_bytes,
    iter_processes_matching_regex,
    run_command,
)


@dataclass
class Sample:
    """One sample of elapsed time, memory (RSS), and CPU usage; None
    fields are written as empty TSV columns."""

    elapsed_s: float | None = None
    ts_ms: int | None = None
    rss: int | None = None
    cpu_percent: float | None = None


def sample_to_tsv_row(sample: Sample, use_epoch_ms: bool = False) -> str:
    """Format a Sample as a TSV row; None fields become empty columns."""
    names = tsv_column_names(use_epoch_ms)
    values = [getattr(sample, name) for name in names]
    return "\t".join("" if v is None else str(v) for v in values) + "\n"


def tsv_column_names(use_epoch_ms: bool) -> list[str]:
    """Ordered TSV column names; the time column is `ts_ms` when
    `use_epoch_ms`, else `elapsed_s`, and the other time field is
    dropped so there is a single time column."""
    skip = "elapsed_s" if use_epoch_ms else "ts_ms"
    return [field.name for field in fields(Sample) if field.name != skip]


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
        memory_field, cpu_field = output.strip().split("\t")
        used_memory = memory_field.split("/")[0].strip()
        cpu_percent = float(cpu_field.strip().rstrip("%"))
        return Sample(
            rss=container_memory_to_bytes(used_memory),
            cpu_percent=cpu_percent,
        )
    except Exception:
        return Sample()


# Seconds the server may be missing before the standalone monitor exits.
GRACE_PERIOD_S = 15


def next_missing_count(missing_count: int, sample: Sample) -> int:
    """Consecutive count of samples where the server was not found; resets
    to 0 on any sample that has data."""
    if sample.rss is None:
        return missing_count + 1
    return 0


class ResourceMonitor:
    """
    Monitor resource usage (memory, CPU) of a process.
    Works in both native mode (via psutil) and container mode
    (via docker/podman stats).

    Usage as a context manager:
        with ResourceMonitor(dataset="wikidata",
                             cmdline_regex="^qlever-index.* -i wikidata",
                             container="qlever.index.wikidata",
                             system="docker"):
            run_command(cmd, show_output=True)
    """

    def __init__(
        self,
        dataset: str,
        cmdline_regex: str,
        container: str | None = None,
        system: str | None = None,
        interval: float = 1.0,
        output_dir: Path | None = None,
        log_name: str | None = None,
        append_mode: bool = False,
        use_epoch_ms: bool = False,
    ):
        """
        Args:
            dataset:       Name of the dataset.
            cmdline_regex: Regex matched against each running process's
                           joined command line to find the one to sample
                           (native mode only).
            container:     Container name to sample; when set with `system`,
                           sampling uses `docker/podman stats` not psutil.
            system:        Container runtime ("docker" or "podman").
            interval:      Seconds between samples.
            output_dir:    Directory for the TSV usage log file.
            log_name:      TSV file name (default
                           `{dataset}.resource-usage-log.tsv`).
            append_mode:   Open the log file in append or write mode.
            use_epoch_ms:  Write epoch_ms instead of elapsed_s to tsv log
        """
        self.dataset = dataset
        self.cmdline_regex = cmdline_regex
        self.container = container
        self.system = system
        self.interval = interval
        self.output_dir = output_dir or Path.cwd()
        self.log_name = log_name or f"{dataset}.resource-usage-log.tsv"
        self.peak_rss = 0
        self.worker_proc = None
        self.log_file = None
        self.stop_event = threading.Event()
        self.start_time = 0
        self.mode = "a" if append_mode else "w"
        self.use_epoch_ms = use_epoch_ms

    def take_sample(self) -> Sample:
        """Dispatch to container or native sampling, caching the resolved process."""
        if self.system in Containerize.supported_systems():
            return sample_container(self.system, self.container)
        if self.worker_proc is None or not self.worker_proc.is_running():
            match = next(
                iter_processes_matching_regex(self.cmdline_regex), None
            )
            self.worker_proc = match[0] if match else None
            if self.worker_proc is None:
                return Sample()
            # cpu_percent reports usage since the previous call, so this
            # first call seeds the baseline and its 0.0 result is discarded.
            try:
                self.worker_proc.cpu_percent(interval=None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                self.worker_proc = None
                return Sample()
        return sample_process(self.worker_proc)

    def write_one_sample(self) -> Sample:
        """Take one sample, stamp it with time, and append it to the log if
        it has data; returns the sample."""
        sample = self.take_sample()
        sample.elapsed_s = round(time.monotonic() - self.start_time, 1)
        sample.ts_ms = int(time.time() * 1000)
        if sample.rss is not None and self.log_file is not None:
            self.peak_rss = max(self.peak_rss, sample.rss)
            self.log_file.write(sample_to_tsv_row(sample, self.use_epoch_ms))
            self.log_file.flush()
        return sample

    def run_loop(self):
        """
        Polling loop on a background thread. Samples resource usage
        and appends one TSV row per iteration until stop_event is set.
        """
        while not self.stop_event.is_set():
            self.write_one_sample()
            self.stop_event.wait(self.interval)

    def open_log(self):
        """Open the TSV log, writing the header if the file is new or empty."""
        self.log_path = self.output_dir / self.log_name
        is_empty = (
            not self.log_path.exists() or self.log_path.stat().st_size == 0
        )
        self.log_file = open(self.log_path, self.mode)
        if is_empty:
            header = "\t".join(tsv_column_names(self.use_epoch_ms)) + "\n"
            self.log_file.write(header)
            self.log_file.flush()

    def run_until_server_gone(self):
        """Sample in the calling thread until the server has been missing for
        `GRACE_PERIOD_S`, then close the log. Used by the standalone monitor
        process, which has no with-block body to end sampling."""
        self.open_log()
        self.start_time = time.monotonic()
        missing_limit = round(GRACE_PERIOD_S / self.interval)
        missing_count = 0
        while True:
            sample = self.write_one_sample()
            missing_count = next_missing_count(missing_count, sample)
            if missing_count >= missing_limit:
                break
            time.sleep(self.interval)
        self.log_file.close()

    def __enter__(self):
        """Open the TSV log, write the header, start sampling thread."""
        self.open_log()
        self.start_time = time.monotonic()
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop sampling, close the log, report where it was saved."""
        self.stop_event.set()
        self.thread.join()
        self.log_file.close()
        if self.peak_rss > 0:
            log.info(
                "Resource-usage log (RSS memory and CPU usage) saved to "
                f"`{self.log_path.name}`"
            )
        else:
            log.warning(
                "Resource usage was not recorded (no samples collected)."
            )
        return False
