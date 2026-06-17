import pytest

from qlever.resource_usage.resource_monitor import (
    Sample,
    sample_to_tsv_row,
)
from qlever.util import container_memory_to_bytes


@pytest.mark.parametrize(
    "usage,expected",
    [
        ("2TiB", 2 * 1024**4),
        ("1.5GiB", int(1.5 * 1024**3)),
        ("512MiB", 512 * 1024**2),
        ("4KiB", 4 * 1024),
        ("2TB", 2 * 1000**4),
        ("1.5GB", int(1.5 * 1000**3)),
        ("512MB", 512 * 1000**2),
        ("4KB", 4 * 1000),
        ("100B", 100),
        ("0B", 0),
        # Longest matching suffix wins; "GiB"/"GB" must not be read as
        # bare bytes via the trailing "B".
        ("2GiB", 2 * 1024**3),
        ("2GB", 2 * 1000**3),
        # Leading/trailing whitespace and case are tolerated.
        ("  1.5gib ", int(1.5 * 1024**3)),
        # A space between number and unit is accepted by float().
        ("1.5 GiB", int(1.5 * 1024**3)),
        ("", 0),
        ("garbage", 0),
    ],
)
def test_container_memory_to_bytes(usage, expected):
    assert container_memory_to_bytes(usage) == expected


@pytest.mark.parametrize(
    "sample,expected",
    [
        (Sample(elapsed_s=1.0, rss=100, cpu_percent=5.0), "1.0\t100\t5.0\n"),
        (Sample(), "\t\t\n"),
        (Sample(elapsed_s=2.0), "2.0\t\t\n"),
        # Zero is a real reading, not a missing one: it renders as "0"
        # / "0.0", never as an empty column.
        (Sample(elapsed_s=0.0, rss=0, cpu_percent=0.0), "0.0\t0\t0.0\n"),
    ],
)
def test_sample_to_tsv_row(sample, expected):
    assert sample_to_tsv_row(sample) == expected
