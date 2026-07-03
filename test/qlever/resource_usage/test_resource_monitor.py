from unittest.mock import MagicMock

import psutil
import pytest

from qlever.resource_usage.resource_monitor import (
    Sample,
    next_missing_count,
    sample_container,
    sample_process,
    sample_to_tsv_row,
)

MODULE = "qlever.resource_usage.resource_monitor"


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


def test_sample_container_parses_stats_output(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "1.5GiB / 7.6GiB\t12.5%"
    sample = sample_container("docker", "qlever.index.test")
    assert sample.rss == int(1.5 * 1024**3)
    assert sample.cpu_percent == 12.5


def test_sample_container_returns_empty_on_malformed_output(mock_command):
    run_cmd_mock = mock_command(MODULE, "run_command")
    run_cmd_mock.return_value = "garbage"
    sample = sample_container("docker", "qlever.index.test")
    assert sample == Sample()


def test_sample_process_reads_rss_and_cpu():
    proc = MagicMock()
    proc.memory_info.return_value.rss = 2048
    proc.cpu_percent.return_value = 7.5
    sample = sample_process(proc)
    assert sample.rss == 2048
    assert sample.cpu_percent == 7.5


def test_sample_process_returns_empty_when_process_gone():
    proc = MagicMock()
    proc.memory_info.side_effect = psutil.NoSuchProcess(pid=123)
    sample = sample_process(proc)
    assert sample == Sample()


def test_next_missing_count_increments_on_missing_sample():
    assert next_missing_count(3, Sample()) == 4


def test_next_missing_count_resets_on_sample_with_data():
    assert next_missing_count(3, Sample(rss=2048)) == 0


def test_next_missing_count_accumulates_over_consecutive_misses():
    count = 0
    for _ in range(5):
        count = next_missing_count(count, Sample())
    assert count == 5


def test_next_missing_count_reset_interrupts_a_streak():
    count = next_missing_count(0, Sample())
    count = next_missing_count(count, Sample())
    count = next_missing_count(count, Sample(rss=1))
    assert count == 0
