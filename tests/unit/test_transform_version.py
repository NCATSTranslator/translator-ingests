import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from translator_ingest.util import transform_version
from translator_ingest.util.transform_version import (
    TransformVersionError,
    cli,
    compute_transform_version,
    get_ingest_sources,
    get_transform_version,
    read_recorded_transform_version,
    write_transform_version,
    UPDATE_COMMAND,
)

# Arbitrary contents
CHANGED_CODE = "x = 0xDEADBEEF"


@pytest.fixture
def fake_ingest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a fake ingest directory named fake_source and make transform_version treat it as the only ingest."""
    fake_ingests = tmp_path / "ingests"
    fake_ingest_dir = fake_ingests / "fake_source"
    fake_ingest_dir.mkdir(parents=True)
    (fake_ingest_dir / "fake_source.py").write_text("x = 1")
    (fake_ingest_dir / "fake_source.yaml").write_text("name: fake")
    (fake_ingest_dir / "download.yaml").write_text("download: fake")
    monkeypatch.setattr(transform_version, "INGESTS_PARSER_PATH", fake_ingests)
    return fake_ingest_dir


@pytest.mark.parametrize("source", get_ingest_sources())
def test_recorded_transform_version_is_current(source: str):
    """Every ingest's recorded transform version should match its code."""
    recorded = read_recorded_transform_version(source)
    computed = compute_transform_version(source)
    assert recorded == computed, (
        f"{source}: recorded transform version is {recorded or 'missing'}, but the ingest code hashes to "
        f"{computed}. Run `{UPDATE_COMMAND}` and commit the updated transform_version files."
    )


def test_get_ingest_sources():
    """Ingest directories should be found, and the ingest template should not be one of them."""
    sources = get_ingest_sources()
    assert {"ctd", "bindingdb", "go_cam"} <= set(sources)
    assert "_ingest_template" not in sources
    assert sources == sorted(sources)


@pytest.mark.parametrize("source", ["ctd", "bindingdb", "diseases", "go_cam"])
def test_compute_transform_version_returns_hex_hash(source: str):
    """compute_transform_version should return an 8-character hex string."""
    version = compute_transform_version(source)
    assert re.fullmatch(r"[0-9a-f]{8}", version), f"Expected 8-char hex hash, got: {version}"


def test_compute_transform_version_is_deterministic():
    """Calling compute_transform_version twice should return the same hash."""
    assert compute_transform_version("ctd") == compute_transform_version("ctd")


def test_compute_transform_version_differs_between_ingests():
    """Different ingests should produce different hashes."""
    assert compute_transform_version("ctd") != compute_transform_version("bindingdb")


def test_compute_transform_version_changes_with_content(fake_ingest: Path):
    """Modifying a source file should change the hash."""
    # test that changing a python file changes the transform version
    version_before = compute_transform_version("fake_source")
    (fake_ingest / "fake_source.py").write_text(CHANGED_CODE)
    version_after = compute_transform_version("fake_source")
    assert version_before != version_after

    # test that changing the ingest yaml file changes the transform version
    (fake_ingest / "fake_source.yaml").write_text("name: fake2")
    version_after_yaml = compute_transform_version("fake_source")
    assert version_after != version_after_yaml

    # test that changing a json file changes the transform version
    (fake_ingest / "mapping.json").write_text('{"key": "value"}')
    version_after_json = compute_transform_version("fake_source")
    assert version_after_json != version_after_yaml

    # test that changing the download yaml file does not change the transform version
    (fake_ingest / "download.yaml").write_text("download: fake2")
    version_after_download_yaml = compute_transform_version("fake_source")
    assert version_after_download_yaml == version_after_json


def test_write_transform_version(fake_ingest: Path):
    """Writing records the computed version, reports whether it changed, and does not change the hash itself."""
    assert read_recorded_transform_version("fake_source") is None
    computed = compute_transform_version("fake_source")

    assert write_transform_version("fake_source") is True
    assert read_recorded_transform_version("fake_source") == computed
    assert compute_transform_version("fake_source") == computed
    assert write_transform_version("fake_source") is False

    (fake_ingest / "fake_source.py").write_text(CHANGED_CODE)
    assert write_transform_version("fake_source") is True
    assert read_recorded_transform_version("fake_source") == compute_transform_version("fake_source")


def test_get_transform_version_raises_when_missing(fake_ingest: Path):
    """The pipeline should refuse to run an ingest with no recorded transform version."""
    with pytest.raises(TransformVersionError, match="recorded transform version is missing"):
        get_transform_version("fake_source")


def test_get_transform_version_raises_when_stale(fake_ingest: Path):
    """The pipeline should refuse to run an ingest whose code changed after its version was recorded."""
    write_transform_version("fake_source")
    recorded = read_recorded_transform_version("fake_source")
    (fake_ingest / "fake_source.py").write_text(CHANGED_CODE)
    with pytest.raises(TransformVersionError, match=f"recorded transform version is {recorded}"):
        get_transform_version("fake_source")


def test_get_transform_version_returns_recorded_version(fake_ingest: Path):
    """A current recorded transform version is returned."""
    write_transform_version("fake_source")
    assert get_transform_version("fake_source") == read_recorded_transform_version("fake_source")


def test_cli_check_and_update(fake_ingest: Path):
    """check fails until update records the current version, then passes."""
    runner = CliRunner()

    result = runner.invoke(cli, ["check"])
    assert result.exit_code == 1
    assert "fake_source: recorded transform version is missing" in result.output

    result = runner.invoke(cli, ["update"])
    assert result.exit_code == 0
    assert f"fake_source: transform version updated to {compute_transform_version('fake_source')}" in result.output

    result = runner.invoke(cli, ["check", "fake_source"])
    assert result.exit_code == 0


def test_cli_rejects_unknown_source(fake_ingest: Path):
    """Naming a source that is not an ingest is an error."""
    result = CliRunner().invoke(cli, ["update", "not_a_source"])
    assert result.exit_code == 2
    assert "Not ingests: not_a_source" in result.output