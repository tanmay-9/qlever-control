import re

import pytest

from qlever.util import (
    container_memory_to_bytes,
    get_random_string,
    parse_git_hash,
    process_cmdline_regex,
)


def test_get_random_string():
    random_string_1 = get_random_string(20)
    random_string_2 = get_random_string(20)
    assert len(random_string_1) == 20
    assert len(random_string_2) == 20
    assert random_string_1 != random_string_2


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
    "binary,name,cmdline,expected",
    [
        # Exact match for the dataset the server was started for.
        (
            "qlever-server",
            "wikidata",
            "qlever-server -i wikidata -j 8 -p 7001",
            True,
        ),
        # `-i name` is followed by end of command line.
        ("qlever-server", "wikidata", "qlever-server -i wikidata", True),
        # A different binary order of flags still matches.
        (
            "qlever-server",
            "wikidata",
            "qlever-server -p 7001 -j 8 -i wikidata",
            True,
        ),
        # A longer dataset name must not match a prefix of it.
        (
            "qlever-server",
            "wikidata",
            "qlever-server -i wikidata2 -j 8",
            False,
        ),
        # A longer binary name must not match a prefix of it.
        ("qlever-server", "wikidata", "qlever-serverX -i wikidata", False),
        # The binary must be at the start of the command line.
        (
            "qlever-server",
            "wikidata",
            "nohup qlever-server -i wikidata",
            False,
        ),
        # A full path binary matches when invoked with that path.
        (
            "/opt/qlever-server",
            "wikidata",
            "/opt/qlever-server -i wikidata -p 1",
            True,
        ),
        # Regex characters in the name are matched literally.
        ("qlever-server", "my.data", "qlever-server -i my.data -p 1", True),
        ("qlever-server", "my.data", "qlever-server -i myXdata -p 1", False),
    ],
)
def test_process_cmdline_regex(binary, name, cmdline, expected):
    regex = process_cmdline_regex(binary, name)
    assert bool(re.search(regex, cmdline)) is expected
