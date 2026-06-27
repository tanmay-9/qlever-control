"""Data layer for the Live header's resource sparklines.

Owns the rolling buffer of recent resource samples and turns it into the
render models the ResourceRow draws. The buffer is the living state; each
read produces a fresh, immutable snapshot of it.
"""

from collections import deque
from typing import BinaryIO

import psutil

from qlever.monitor_queries.models import (
    ResourceSample,
    ResourceSeries,
    ResourceUsage,
)

SAMPLE_INTERVAL_S = 2
LIVE_WINDOW_S = 300
BUFFER_SIZE = LIVE_WINDOW_S // SAMPLE_INTERVAL_S

# A sample this recent means the qlever-server process is alive right
# now. Three intervals, so a single missed sample still counts as live.
RESOURCE_FRESH_S = SAMPLE_INTERVAL_S * 3

# Bytes to read from the tail when seeding the buffer, a generous 64 per
# row so the last BUFFER_SIZE rows always fit however large the log grew.
SEED_TAIL_BYTES = BUFFER_SIZE * 64


def system_totals() -> tuple[float, int | None]:
    """The sparkline scale denominators: (total RAM in GB, logical cores).

    Read once at screen setup; both stay fixed for the machine's lifetime.
    """
    return (psutil.virtual_memory().total / 1e9, psutil.cpu_count())


class ResourceHistory:
    """Rolling buffer of the most recent resource samples.

    maxlen makes it a ring: appending past BUFFER_SIZE drops the oldest,
    so it always holds the last LIVE_WINDOW_S seconds of readings.
    """

    def __init__(self) -> None:
        self.samples = deque(maxlen=BUFFER_SIZE)

    def add(self, sample: ResourceSample) -> None:
        self.samples.append(sample)


def parse_tsv_row(line: str) -> ResourceSample | None:
    """Turn one TSV log row into a sample, or None if it isn't one.

    The header line and any malformed row fail the numeric parse and
    return None, so the caller never special-cases them.
    """
    fields = line.split("\t")
    if len(fields) != 3:
        return None
    ts, rss, cpu = fields
    try:
        return ResourceSample(
            ts_ms=int(ts), rss=int(rss), cpu_percent=float(cpu)
        )
    except ValueError:
        return None


def recent(samples: list[ResourceSample], now_ms: int) -> list[ResourceSample]:
    """Keep only samples from the last LIVE_WINDOW_S seconds.

    The log is opened in append mode, so it can carry rows from an
    earlier session. Dropping them when seeding the buffer makes it
    start with a true 5-minute window rather than stale history.
    """
    cutoff = now_ms - LIVE_WINDOW_S * 1000
    return [sample for sample in samples if sample.ts_ms >= cutoff]


class ResourceLogReader:
    """Tail cursor for the ResourceMonitor TSV, like LiveLogReader.

    The worker owns the open stream and passes it to each read. read_new
    returns only the rows appended since the last cursor, so the steady
    read never depends on file size. last_ts_ms is the freshest row's
    time, read by the Live screen as a fast 'server is alive' signal
    before the metrics-log and ping checks.
    """

    def __init__(self) -> None:
        self.cursor = 0
        self.last_ts_ms = None

    def seed(self, stream: BinaryIO, now_ms: int) -> list[ResourceSample]:
        """Backfill the last window from the tail of the log, once.

        Seeks near the end rather than scanning from the start, so a log
        grown large over past sessions costs a fixed read. Skips the
        partial line the seek lands in, then reads forward like a normal
        poll. recent() drops rows left from an earlier session.
        """
        stream.seek(0, 2)
        start = max(0, stream.tell() - SEED_TAIL_BYTES)
        stream.seek(start)
        if start > 0:
            stream.readline()
        self.cursor = stream.tell()
        return recent(self.read_new(stream), now_ms)

    def read_new(self, stream: BinaryIO) -> list[ResourceSample]:
        """Parse and return samples appended since the previous read.

        Stops at the first line without a trailing newline, leaving a
        half-written final row for the next read so a row is never split.
        """
        stream.seek(self.cursor)
        samples = []
        for line in stream:
            if not line.endswith(b"\n"):
                break
            self.cursor += len(line)
            sample = parse_tsv_row(line.decode())
            if sample is not None:
                samples.append(sample)
        if samples:
            self.last_ts_ms = samples[-1].ts_ms
        return samples


def is_resource_sample_fresh(last_ts_ms: int | None, now_ms: int) -> bool:
    """Whether a sample is recent enough to prove the server is alive.

    Used only to promote to reachable; its absence is ambiguous (remote
    server, mount, wrong process) so it never forces unreachable.
    """
    if last_ts_ms is None:
        return False
    return now_ms - last_ts_ms <= RESOURCE_FRESH_S * 1000


def zero_pad_left(values: tuple[float, ...]) -> tuple[float, ...]:
    """Left-pad a partial window up to BUFFER_SIZE with zeros.

    A not-yet-full buffer would otherwise stretch its few readings into
    fat bars. Fixing the slot count keeps the bar width constant, with
    the empty pre-monitoring past on the left and 'now' at the right.
    """
    missing = BUFFER_SIZE - len(values)
    return (0.0,) * missing + values if missing > 0 else values


def get_resource_usage(
    history: ResourceHistory, totals: tuple[float, float]
) -> ResourceUsage:
    """Snapshot the buffer as two display-ready sparkline series.

    Walks the buffer once, converting raw samples to display units: rss
    bytes to GB, cpu percent to cores. totals is (rss_total_gb, cpu_cores).
    """
    rss_total_gb, cpu_cores = totals
    rss_values = zero_pad_left(tuple(s.rss / 1e9 for s in history.samples))
    cpu_values = zero_pad_left(
        tuple(s.cpu_percent / 100 for s in history.samples)
    )
    return ResourceUsage(
        rss=ResourceSeries("RSS", rss_values, rss_total_gb, "GB"),
        cpu=ResourceSeries("CPU", cpu_values, cpu_cores, "cores"),
    )
