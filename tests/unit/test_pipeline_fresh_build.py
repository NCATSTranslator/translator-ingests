"""Exercise real version lookup and Make scheduling without external services."""

import json
from collections.abc import Iterator
import os
import subprocess
import sys
from pathlib import Path

import pytest

import translator_ingest.ingests
from translator_ingest.pipeline import get_latest_source_version, run_pipeline
from translator_ingest.util.storage import local


@pytest.fixture
def version_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Install a file-backed ingest that fails version discovery, with empty storage."""
    source = "version_failure_fixture"
    package = tmp_path / source
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / f"{source}.py").write_text(
        'def get_latest_version():\n    raise ValueError("source version unavailable")\n'
    )
    monkeypatch.setattr(translator_ingest.ingests, "__path__", [str(tmp_path)])
    monkeypatch.setattr(local, "INGESTS_DATA_PATH", tmp_path / "data")
    yield source
    sys.modules.pop(f"translator_ingest.ingests.{source}.{source}", None)
    sys.modules.pop(f"translator_ingest.ingests.{source}", None)


def test_fresh_source_failure_does_not_publish_build(version_source: str, tmp_path: Path) -> None:
    """A source without a known version fails before producing data or success metadata."""
    with pytest.raises(ValueError, match="source version unavailable"):
        run_pipeline(version_source)
    assert not (tmp_path / "data").exists()


def test_previous_build_version_is_available_as_fallback(version_source: str, tmp_path: Path) -> None:
    """A real previous-build file provides the existing warm-machine fallback."""
    source_dir = tmp_path / "data" / version_source
    source_dir.mkdir(parents=True)
    (source_dir / "latest-build.json").write_text(json.dumps({"source_version": "2026.2"}))
    assert get_latest_source_version(version_source) == "2026.2"


@pytest.mark.parametrize("target", ["run", "transform"])
@pytest.mark.parametrize("keep_going", ["0", "1"])
def test_make_source_failure_policy(tmp_path: Path, target: str, keep_going: str) -> None:
    """Real recursive Make stops or continues independent jobs but always reports failure."""
    repository = Path(__file__).resolve().parents[2]
    # Serialize scheduling to prove the second job starts AFTER the first has failed.
    # Tiny process recipes isolate Make's behavior from network-dependent ingest stages.
    for name in ("rig.Makefile", "doc.Makefile"):
        (tmp_path / name).symlink_to(repository / name)
    fixture = tmp_path / "Makefile"
    fixture.write_text(
        f"include {repository / 'Makefile'}\n"
        ".NOTPARALLEL:\n"
        f"{target}-%:\n"
        '\t@echo "attempt:$*"\n'
        '\t@test "$*" != "broken"\n'
        '\t@touch "$*.completed"\n'
    )
    environment = dict(os.environ)
    environment.pop("MAKEFILES", None)
    environment.pop("MAKEFLAGS", None)
    environment.pop("MFLAGS", None)
    result = subprocess.run(
        ["make", target, "SOURCES=broken healthy", f"KEEP_GOING={keep_going}"],
        cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=30,
    )
    assert "attempt:broken" in result.stdout, result.stdout + result.stderr
    assert result.returncode != 0, result.stdout + result.stderr
    assert not (tmp_path / "broken.completed").exists()
    assert (tmp_path / "healthy.completed").exists() == (keep_going == "1"), result.stdout + result.stderr
