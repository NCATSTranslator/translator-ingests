"""Tests for source version discovery and its fallback to the last successful build's version.

Each test writes a real throwaway ingest module and real storage files into a temporary directory.
"""
import importlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

import translator_ingest.ingests as ingests_package
from translator_ingest.pipeline import get_latest_source_version
from translator_ingest.util.storage import local as storage_local
from translator_ingest.util.storage.local import IngestFileName

SOURCE = "fake_version_source"
FALLBACK_VERSION = "2026.1"

# get_latest_version() bodies that return something that cannot be used as a version.
# "pass" returns None implicitly, as an ingest that swallows its own request errors would.
INVALID_VERSION_BODIES = ["return None", "pass", "return ''", "return '   '", "return 2026"]
RAISING_BODY = "raise RuntimeError('source unavailable')"


@pytest.fixture
def storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Path, Path]]:
    """Make ingest discovery and local data storage use empty temporary directories.

    :return: (ingests directory, data directory)
    """
    ingests_dir = tmp_path / "ingests"
    data_dir = tmp_path / "data"
    monkeypatch.setattr(ingests_package, "__path__", [*ingests_package.__path__, str(ingests_dir)])
    monkeypatch.setattr(storage_local, "INGESTS_DATA_PATH", data_dir)
    yield ingests_dir, data_dir
    sys.modules.pop(f"translator_ingest.ingests.{SOURCE}.{SOURCE}", None)
    sys.modules.pop(f"translator_ingest.ingests.{SOURCE}", None)


def write_ingest(ingests_dir: Path, function_body: str) -> None:
    """Write an ingest module whose get_latest_version() consists of function_body."""
    source_dir = ingests_dir / SOURCE
    source_dir.mkdir(parents=True)
    (source_dir / "__init__.py").write_text("")
    (source_dir / f"{SOURCE}.py").write_text(f"def get_latest_version():\n    {function_body}\n")
    importlib.invalidate_caches()


def write_latest_build(data_dir: Path, source_version: str) -> None:
    """Write a latest build file recording a previous successful build of source_version."""
    source_dir = data_dir / SOURCE
    source_dir.mkdir(parents=True)
    (source_dir / IngestFileName.LATEST_BUILD_FILE).write_text(json.dumps({"source_version": source_version}))


def test_valid_version_is_used_over_previous_build(storage: tuple[Path, Path]) -> None:
    """A successful lookup wins even when a previous build exists."""
    ingests_dir, data_dir = storage
    write_ingest(ingests_dir, "return '2026.2'")
    write_latest_build(data_dir, FALLBACK_VERSION)
    assert get_latest_source_version(SOURCE) == "2026.2"


@pytest.mark.parametrize("function_body", [*INVALID_VERSION_BODIES, RAISING_BODY])
def test_failed_lookup_falls_back_to_previous_build(storage: tuple[Path, Path], function_body: str) -> None:
    """Raising and returning an unusable version both fall back to the previous build's version."""
    ingests_dir, data_dir = storage
    write_ingest(ingests_dir, function_body)
    write_latest_build(data_dir, FALLBACK_VERSION)
    assert get_latest_source_version(SOURCE) == FALLBACK_VERSION


@pytest.mark.parametrize("function_body", INVALID_VERSION_BODIES)
def test_invalid_version_without_previous_build_raises(storage: tuple[Path, Path], function_body: str) -> None:
    """Without a previous build, an unusable version fails with an error naming the bad value."""
    ingests_dir, _ = storage
    write_ingest(ingests_dir, function_body)
    with pytest.raises(ValueError, match=f"get_latest_version\\(\\) for {SOURCE} returned .*expected a non-empty string"):
        get_latest_source_version(SOURCE)


def test_raised_error_without_previous_build_is_reraised(storage: tuple[Path, Path]) -> None:
    """Without a previous build, the ingest's own error propagates unchanged."""
    ingests_dir, _ = storage
    write_ingest(ingests_dir, RAISING_BODY)
    with pytest.raises(RuntimeError, match="source unavailable"):
        get_latest_source_version(SOURCE)