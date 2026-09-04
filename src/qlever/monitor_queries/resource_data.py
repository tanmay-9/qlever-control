"""Data layer for the server's resource-usage log.

Live tails the log into a rolling buffer; Historic reads a past window
straight off disk. Both turn samples into the immutable render models the
sparklines and the plot draw, so no widget does unit math of its own.
Getting the rows out of the file is `resource_reader.py`'s job.
"""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from qlever.monitor_queries.models import (
    ResourceSeries,
    ResourceUsage,
    ResourceWindow,
)
from qlever.monitor_queries.resource_reader import Sample, iter_samples

LIVE_RESOURCE_WINDOW_MS = 300_000

# Intervals without a sample before the server counts as gone. Three, so
# a single missed sample still counts as live.
FRESH_INTERVALS = 3


@dataclass(frozen=True)
class Capacity:
    """What the machine has, so the bars and axes have a full scale.

    Read once at startup, since it cannot change while we run. `cores`
    is None when the count could not be read.
    """

    ram_gb: float
    cores: float | None


def buffer_size(sample_interval_s: int) -> int:
    """Samples the live window holds at this logging interval."""
    return max(1, LIVE_RESOURCE_WINDOW_MS // (sample_interval_s * 1000))


class SampleBuffer:
    """Rolling buffer of the most recent resource samples.

    `maxlen` drops the oldest sample when a new one arrives, so the
    buffer always holds the last `LIVE_RESOURCE_WINDOW_MS` of readings.
    Its size follows the log's sampling interval, so the window stays
    five minutes whatever interval the server was started with.
    """

    def __init__(self, sample_interval_s: int) -> None:
        self.size = buffer_size(sample_interval_s)
        self.samples = deque(maxlen=self.size)

    def add(self, sample: Sample) -> None:
        self.samples.append(sample)


def is_sample_fresh(
    last_ts_ms: int | None, now_ms: int, sample_interval_s: int
) -> bool:
    """Whether a sample is recent enough to prove the server is alive.

    Used only to promote to reachable; its absence is ambiguous (remote
    server, mount, wrong process) so it never forces unreachable.
    """
    if last_ts_ms is None:
        return False
    fresh_ms = FRESH_INTERVALS * sample_interval_s * 1000
    return now_ms - last_ts_ms <= fresh_ms


def zero_pad_left(values: tuple[float, ...], size: int) -> tuple[float, ...]:
    """Left-pad a partial window up to `size` with zeros.

    A not-yet-full buffer would otherwise stretch its few readings into
    fat bars. Fixing the slot count keeps the bar width constant, with
    the empty pre-monitoring past on the left and 'now' at the right.
    """
    missing = size - len(values)
    return (0.0,) * missing + values if missing > 0 else values


def get_resource_usage(
    history: SampleBuffer, capacity: Capacity
) -> ResourceUsage:
    """Snapshot the buffer as two display-ready sparkline series.

    Walks the buffer once, converting raw samples to display units: rss
    bytes to GB, cpu percent to cores.
    """
    rss_values = zero_pad_left(
        tuple(sample.rss / 1e9 for sample in history.samples), history.size
    )
    cpu_values = zero_pad_left(
        tuple(sample.cpu_percent / 100 for sample in history.samples),
        history.size,
    )
    return ResourceUsage(
        rss=ResourceSeries("RSS", rss_values, capacity.ram_gb, "GB"),
        cpu=ResourceSeries("CPU", cpu_values, capacity.cores, "cores"),
    )


class RestartTracker:
    """Finds server restarts from drops in the log's elapsed-time column.

    Fed samples in timestamp order. The elapsed-time counter resets when
    the server restarts, so a drop between two samples means a restart
    happened between them. The earlier sample is the stop, the later one
    the start, and each is kept only if it falls in the window, so a
    restart straddling a window edge still records the half that shows.
    """

    def __init__(self, start_ms: int, end_ms: int) -> None:
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.stop_times_s = []
        self.start_times_s = []
        self.last_elapsed_s = None
        self.last_ts_ms = None

    def track(self, elapsed_s: float, ts_ms: int) -> None:
        """Note one sample; record a restart if elapsed time dropped."""
        if self.last_elapsed_s is not None and elapsed_s < self.last_elapsed_s:
            if self.start_ms <= self.last_ts_ms <= self.end_ms:
                self.stop_times_s.append(self.last_ts_ms / 1000)
            if self.start_ms <= ts_ms <= self.end_ms:
                self.start_times_s.append(ts_ms / 1000)
        self.last_elapsed_s = elapsed_s
        self.last_ts_ms = ts_ms


class RebuildIndexTracker:
    """Finds index rebuilds from changes in the log's rebuild_id column.

    Fed samples in timestamp order. The server writes the id of the
    running rebuild and leaves the cell empty when none is running, so
    the id changing between two samples marks a boundary. Each marker
    is kept only if it falls in the window, so a rebuild straddling a
    window edge still records the half that shows.
    """

    def __init__(self, start_ms: int, end_ms: int) -> None:
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.start_times_s = []
        self.end_times_s = []
        self.last_rebuild_id = None
        self.last_ts_ms = None

    def track(self, rebuild_id: int | None, ts_ms: int) -> None:
        """Note one sample; record a rebuild that began or ended."""
        if self.last_ts_ms is not None and rebuild_id != self.last_rebuild_id:
            if (
                self.last_rebuild_id is not None
                and self.start_ms <= self.last_ts_ms <= self.end_ms
            ):
                self.end_times_s.append(self.last_ts_ms / 1000)
            if (
                rebuild_id is not None
                and self.start_ms <= ts_ms <= self.end_ms
            ):
                self.start_times_s.append(ts_ms / 1000)
        self.last_rebuild_id = rebuild_id
        self.last_ts_ms = ts_ms


def build_plot(
    times_s: list[float],
    rss_gb: list[float],
    cpu_cores: list[float],
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
    restarts: RestartTracker,
    rebuilds: RebuildIndexTracker,
) -> ResourceWindow:
    """Put the gathered series, edges and events into one window."""
    return ResourceWindow(
        times_s=tuple(times_s),
        rss_gb=tuple(rss_gb),
        cpu_cores=tuple(cpu_cores),
        rss_total=capacity.ram_gb,
        cpu_total=capacity.cores,
        start_s=start_ms / 1000,
        end_s=end_ms / 1000,
        stop_times_s=tuple(restarts.stop_times_s),
        start_times_s=tuple(restarts.start_times_s),
        rebuild_start_times_s=tuple(rebuilds.start_times_s),
        rebuild_end_times_s=tuple(rebuilds.end_times_s),
    )


def get_resource_plot(
    samples: list[Sample],
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
) -> ResourceWindow:
    """Turn samples in a time window into the dual-axis plot model.

    Keeps only samples inside [start_ms, end_ms] and converts each to
    display units: rss bytes to GB, cpu percent to cores. The window
    edges frame the plot's x-axis and may be wider than the samples that
    fall inside them. Restarts are detected by RestartTracker, and rebuilds
    are detected by RebuildIndexTracker.
    """
    times_s = []
    rss_gb = []
    cpu_cores_series = []
    restarts = RestartTracker(start_ms, end_ms)
    rebuilds = RebuildIndexTracker(start_ms, end_ms)
    for sample in samples:
        restarts.track(sample.elapsed_s, sample.ts_ms)
        rebuilds.track(sample.rebuild_id, sample.ts_ms)
        if start_ms <= sample.ts_ms <= end_ms:
            times_s.append(sample.ts_ms / 1000)
            rss_gb.append(sample.rss / 1e9)
            cpu_cores_series.append(sample.cpu_percent / 100)
    return build_plot(
        times_s,
        rss_gb,
        cpu_cores_series,
        capacity,
        start_ms,
        end_ms,
        restarts,
        rebuilds,
    )


def read_resource_window(
    path: Path,
    capacity: Capacity,
    start_ms: int,
    end_ms: int,
    max_points: int,
    should_cancel: Callable[[], bool] | None = None,
) -> ResourceWindow:
    """Read samples in [start_ms, end_ms] and bucket them for the plot.

    Folds each row into one of `max_points` equal time buckets as it
    arrives, keeping the bucket's peak rss and cpu so spikes survive.
    Nothing but the buckets is held, so the memory needed follows the
    plot's width and not the size of the log.
    """
    # No log yet: the server has not started, or resource logging is
    # off. Frame the window empty rather than fail the read.
    if not path.exists():
        return get_resource_plot([], capacity, start_ms, end_ms)
    max_points = max(1, max_points)
    bucket_span_ms = (end_ms - start_ms) / max_points
    if bucket_span_ms <= 0:
        bucket_span_ms = 1
    bucket_ts = [None] * max_points
    bucket_rss = [0] * max_points
    bucket_cpu = [0.0] * max_points
    restarts = RestartTracker(start_ms, end_ms)
    rebuilds = RebuildIndexTracker(start_ms, end_ms)

    with open(path, "rb") as stream:
        for sample in iter_samples(stream, start_ms, end_ms, should_cancel):
            restarts.track(sample.elapsed_s, sample.ts_ms)
            rebuilds.track(sample.rebuild_id, sample.ts_ms)
            # The rows from just outside the window are here for the
            # trackers only, so only in-window rows get a bucket.
            if start_ms <= sample.ts_ms <= end_ms:
                index = int((sample.ts_ms - start_ms) / bucket_span_ms)
                if index >= max_points:
                    index = max_points - 1
                if bucket_ts[index] is None:
                    bucket_ts[index] = sample.ts_ms
                bucket_rss[index] = max(bucket_rss[index], sample.rss)
                bucket_cpu[index] = max(bucket_cpu[index], sample.cpu_percent)

    times_s = []
    rss_gb = []
    cpu_cores_series = []
    for index in range(max_points):
        if bucket_ts[index] is not None:
            times_s.append(bucket_ts[index] / 1000)
            rss_gb.append(bucket_rss[index] / 1e9)
            cpu_cores_series.append(bucket_cpu[index] / 100)
    return build_plot(
        times_s,
        rss_gb,
        cpu_cores_series,
        capacity,
        start_ms,
        end_ms,
        restarts,
        rebuilds,
    )
