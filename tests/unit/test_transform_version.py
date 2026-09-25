import re

import pytest

from translator_ingest.pipeline import get_transform_version


@pytest.mark.parametrize("source", ["ctd", "bindingdb", "diseases", "go_cam"])
def test_get_transform_version_returns_hex_hash(source):
    """get_transform_version should return an 8-character hex string."""
    version = get_transform_version(source)
    assert re.fullmatch(r"[0-9a-f]{8}", version), f"Expected 8-char hex hash, got: {version}"


def test_get_transform_version_is_deterministic():
    """Calling get_transform_version twice should return the same hash."""
    assert get_transform_version("ctd") == get_transform_version("ctd")


def test_get_transform_version_differs_between_ingests():
    """Different ingests should produce different hashes."""
    assert get_transform_version("ctd") != get_transform_version("bindingdb")


def test_get_transform_version_changes_with_content(tmp_path, monkeypatch):
    """Modifying a source file should change the hash."""
    import translator_ingest
    # Create a fake ingest directory and make the pipeline think it's the real one
    fake_ingests = tmp_path / "ingests"
    fake_ingest = fake_ingests / "fake_source"
    fake_ingest.mkdir(parents=True)
    (fake_ingest / "fake_source.py").write_text("x = 1")
    (fake_ingest / "fake_source.yaml").write_text("name: fake")
    (fake_ingest / "download.yaml").write_text("download: fake")
    monkeypatch.setattr(translator_ingest.pipeline, "INGESTS_PARSER_PATH", fake_ingests)

    # test that changing a python file changes the transform version
    version_before = get_transform_version("fake_source")
    (fake_ingest / "fake_source.py").write_text("x = 2")
    version_after = get_transform_version("fake_source")
    assert version_before != version_after

    # test that changing the ingest yaml file changes the transform version
    (fake_ingest / "fake_source.yaml").write_text("name: fake2")
    version_after_yaml = get_transform_version("fake_source")
    assert version_after != version_after_yaml

    # test that changing a json file changes the transform version
    (fake_ingest / "mapping.json").write_text('{"key": "value"}')
    version_after_json = get_transform_version("fake_source")
    assert version_after_json != version_after_yaml

    # test that changing the download yaml file does not change the transform version
    (fake_ingest / "download.yaml").write_text("download: fake2")
    version_after_download_yaml = get_transform_version("fake_source")
    assert version_after_download_yaml == version_after_json
