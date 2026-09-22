"""Tests for storage path resolution via INGESTS_DATA_PATH / INGESTS_RELEASES_PATH."""
import os
import subprocess
import sys
from pathlib import Path

import pytest

from translator_ingest import resolve_storage_path

ENV_VAR = "INGESTS_TEST_PATH"
DEFAULT = Path("default_data")


@pytest.mark.parametrize("configured", [None, ""])
def test_unset_env_var_returns_default(monkeypatch, configured):
    if configured is None:
        monkeypatch.delenv(ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(ENV_VAR, configured)
    assert resolve_storage_path(ENV_VAR, DEFAULT) == DEFAULT


def test_existing_directory_is_used(monkeypatch, tmp_path):
    monkeypatch.setenv(ENV_VAR, str(tmp_path))
    assert resolve_storage_path(ENV_VAR, DEFAULT) == tmp_path.absolute()


@pytest.mark.parametrize("kind", ["missing", "regular_file", "dangling_symlink"])
def test_non_directory_raises(monkeypatch, tmp_path, kind):
    target = tmp_path / kind
    if kind == "regular_file":
        target.write_text("")
    elif kind == "dangling_symlink":
        target.symlink_to(tmp_path / "nowhere")

    monkeypatch.setenv(ENV_VAR, str(target))
    with pytest.raises(NotADirectoryError, match=ENV_VAR):
        resolve_storage_path(ENV_VAR, DEFAULT)


@pytest.mark.parametrize(
    "env_var", ["INGESTS_DATA_PATH", "INGESTS_RELEASES_PATH", "INGESTS_LOGS_PATH", "INGESTS_REPORTS_PATH"]
)
def test_import_fails_before_anything_can_be_created(tmp_path, env_var):
    """Importing must fail on a missing configured path, since the pipeline creates
    subdirectories with parents=True and would otherwise write to the wrong filesystem."""
    unmounted = tmp_path / "not_mounted"
    result = subprocess.run(
        [sys.executable, "-c", "import translator_ingest"],
        env={**os.environ, env_var: str(unmounted)},
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert env_var in result.stderr
    assert not unmounted.exists()