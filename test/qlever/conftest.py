from unittest.mock import MagicMock

import pytest


@pytest.fixture
def write_log(tmp_path):
    """Write bytes to a fresh log file and return its Path."""

    def make(data):
        path = tmp_path / "q.log"
        path.write_bytes(data)
        return path

    return make


@pytest.fixture
def open_log(write_log):
    """Write bytes to a log file and hand back an open (handle, size).

    Closes every handle it produced when the test finishes.
    """
    handles = []

    def make(data):
        path = write_log(data)
        handle = path.open("rb")
        handles.append(handle)
        return handle, path.stat().st_size

    yield make
    for handle in handles:
        handle.close()


@pytest.fixture
def mock_command(monkeypatch):
    def _mock(module_name: str, function_name: str, override=None):
        if override:
            monkeypatch.setattr(f"{module_name}.{function_name}", override)
            return override
        mock = MagicMock(name=f"{function_name}_mock")
        monkeypatch.setattr(f"{module_name}.{function_name}", mock)
        return mock

    return _mock
