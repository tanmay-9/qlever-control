import numpy as np
import pytest

from qlever.usage_plot import (
    compute_phase_boundaries,
    downsample_for_plot,
    parse_git_hash,
    parse_qleverfile,
    pick_time_unit,
    read_usage_tsv,
)


def write_log(tmp_path, lines):
    """Write an index-log file from full timestamped lines."""
    path = tmp_path / "index-log.txt"
    path.write_text("\n".join(lines) + "\n")
    return path


@pytest.mark.parametrize(
    "seconds,expected",
    [
        (0, ("Elapsed (s)", 1.0)),
        (199.9, ("Elapsed (s)", 1.0)),
        # Boundary: 200 leaves seconds for minutes.
        (200, ("Elapsed (min)", 60.0)),
        (3599, ("Elapsed (min)", 60.0)),
        # Boundary: 3600 leaves minutes for hours.
        (3600, ("Elapsed (h)", 3600.0)),
        (10000, ("Elapsed (h)", 3600.0)),
    ],
)
def test_pick_time_unit(seconds, expected):
    assert pick_time_unit(seconds) == expected


@pytest.mark.parametrize(
    "first_line,expected",
    [
        ("qlever-server, git hash 1a2b3c4, compiled", "1a2b3c4"),
        ("no hash on this line", None),
    ],
)
def test_parse_git_hash_reads_first_line_only(first_line, expected, tmp_path):
    path = tmp_path / "index-log.txt"
    # Second line also carries a hash; only the first line should count.
    path.write_text(first_line + "\nsomething git hash deadbeef here\n")
    assert parse_git_hash(path) == expected


def test_parse_git_hash_missing_file(tmp_path):
    assert parse_git_hash(tmp_path / "nope.txt") is None


def test_parse_git_hash_empty_file(tmp_path):
    path = tmp_path / "empty.txt"
    path.write_text("")
    assert parse_git_hash(path) is None


@pytest.mark.parametrize(
    "triples,expected",
    [
        (1_000_000, "1M"),
        (5_000_000, "5M"),
        (2_500_000, "2.5M"),
        (1_000, "1K"),
        (50_000, "50K"),
        (500, "500"),
    ],
)
def test_parse_qleverfile_batch_formatting(triples, expected, tmp_path):
    path = tmp_path / "Qleverfile"
    path.write_text(f'SETTINGS_JSON = {{ "num-triples-per-batch": {triples} }}\n')
    assert parse_qleverfile(path)["batch"] == expected


def test_parse_qleverfile_reads_stxxl(tmp_path):
    path = tmp_path / "Qleverfile"
    path.write_text("[index]\nSTXXL_MEMORY = 5G\n")
    assert parse_qleverfile(path) == {"stxxl": "5G"}


def test_parse_qleverfile_missing_file(tmp_path):
    assert parse_qleverfile(tmp_path / "nope") == {}


def test_parse_qleverfile_no_relevant_keys(tmp_path):
    path = tmp_path / "Qleverfile"
    path.write_text("[index]\nFORMAT = ttl\n")
    assert parse_qleverfile(path) == {}


def test_read_usage_tsv_parses_columns_and_blank_as_nan(tmp_path):
    path = tmp_path / "data.usage-log.tsv"
    path.write_text(
        "elapsed_s\trss\tcpu_percent\n1.0\t100\t5.0\n2.0\t\t6.0\n"
    )
    data = read_usage_tsv(path)
    assert set(data) == {"elapsed_s", "rss", "cpu_percent"}
    np.testing.assert_array_equal(data["elapsed_s"], np.array([1.0, 2.0]))
    assert data["rss"][0] == 100
    assert np.isnan(data["rss"][1])
    np.testing.assert_array_equal(data["cpu_percent"], np.array([5.0, 6.0]))


def test_read_usage_tsv_header_only(tmp_path):
    path = tmp_path / "data.tsv"
    path.write_text("elapsed_s\trss\tcpu_percent\n")
    data = read_usage_tsv(path)
    assert len(data["elapsed_s"]) == 0


def test_read_usage_tsv_empty_file(tmp_path):
    path = tmp_path / "data.tsv"
    path.write_text("")
    assert read_usage_tsv(path) == {}


def test_downsample_for_plot_returns_input_when_within_budget():
    data = {
        "elapsed_s": np.arange(5.0),
        "rss": np.arange(5.0),
        "cpu_percent": np.arange(5.0),
    }
    assert downsample_for_plot(data, 10) is data


def test_downsample_for_plot_caps_points_and_keeps_peak():
    data = {
        "elapsed_s": np.arange(10.0),
        "rss": np.array([1, 2, 3, 99, 4, 5, 6, 7, 8, 9], dtype=float),
        "cpu_percent": np.zeros(10),
    }
    out = downsample_for_plot(data, 3)
    assert len(out["elapsed_s"]) <= 3
    # nanmax bucketing must preserve the global peak exactly.
    assert np.nanmax(out["rss"]) == 99
    # elapsed_s uses nanmin, so the first bucket starts at the origin.
    assert out["elapsed_s"][0] == 0.0


def test_compute_phase_boundaries_missing_file(tmp_path):
    begin, phases = compute_phase_boundaries(tmp_path / "nope.txt")
    assert begin is None
    assert phases == {}


def test_compute_phase_boundaries_no_processing_line(tmp_path):
    path = write_log(tmp_path, ["2026-06-01 10:00:00 - INFO: Something else"])
    begin, phases = compute_phase_boundaries(path)
    assert begin is None
    assert phases == {}


def test_compute_phase_boundaries_with_permutations(tmp_path):
    path = write_log(
        tmp_path,
        [
            "2026-06-01 10:00:00 - INFO: Processing input",
            "2026-06-01 10:00:10 - INFO: Merging partial vocabularies",
            "2026-06-01 10:00:20 - INFO: Converting triples",
            "2026-06-01 10:00:30 - INFO: Creating permutations PSO and POS",
            "2026-06-01 10:00:40 - INFO: Creating permutations SPO and SOP",
            "2026-06-01 10:00:50 - INFO: Index build completed",
        ],
    )
    begin, phases = compute_phase_boundaries(path)
    assert begin is not None
    assert phases == {
        "Parse input": (0.0, 10.0),
        "Build vocabularies": (10.0, 20.0),
        "Convert to global IDs": (20.0, 30.0),
        "Permutation PSO & POS": (30.0, 40.0),
        # The last permutation ends at "Index build completed".
        "Permutation SPO & SOP": (40.0, 50.0),
    }


def test_compute_phase_boundaries_without_permutations(tmp_path):
    path = write_log(
        tmp_path,
        [
            "2026-06-01 10:00:00 - INFO: Processing input",
            "2026-06-01 10:00:10 - INFO: Merging partial vocabularies",
            "2026-06-01 10:00:20 - INFO: Converting triples",
            "2026-06-01 10:00:50 - INFO: Index build completed",
        ],
    )
    begin, phases = compute_phase_boundaries(path)
    assert phases == {
        "Parse input": (0.0, 10.0),
        "Build vocabularies": (10.0, 20.0),
        # No permutations: convert runs through to build completion.
        "Convert to global IDs": (20.0, 50.0),
    }


def test_compute_phase_boundaries_dedups_repeated_permutation_names(tmp_path):
    path = write_log(
        tmp_path,
        [
            "2026-06-01 10:00:00 - INFO: Processing input",
            "2026-06-01 10:00:10 - INFO: Merging partial vocabularies",
            "2026-06-01 10:00:20 - INFO: Converting triples",
            "2026-06-01 10:00:30 - INFO: Creating permutations PSO and POS",
            "2026-06-01 10:00:40 - INFO: Creating permutations PSO and POS",
            "2026-06-01 10:00:50 - INFO: Index build completed",
        ],
    )
    begin, phases = compute_phase_boundaries(path)
    assert "Permutation PSO & POS" in phases
    assert "Permutation PSO & POS (2)" in phases
    assert phases["Permutation PSO & POS"] == (30.0, 40.0)
    assert phases["Permutation PSO & POS (2)"] == (40.0, 50.0)


def test_compute_phase_boundaries_skips_incomplete_phase(tmp_path):
    # Only the "Processing" line: a begin is found, but no later
    # timestamps, so every phase has a None endpoint and is skipped.
    path = write_log(tmp_path, ["2026-06-01 10:00:00 - INFO: Processing input"])
    begin, phases = compute_phase_boundaries(path)
    assert begin is not None
    assert phases == {}
