import json

import pytest

from translator_ingest import merging
from translator_ingest.merging import _get_source_release_metadata
from translator_ingest.util.metadata import PipelineMetadata


def _point_release_lookup_at(monkeypatch, release_file):
    """Redirect the merge module's release-file lookup to a controlled path.

    _get_source_release_metadata locates a source's latest-release.json via
    get_versioned_file_paths; here we point that at a temp file so the three
    branches (missing / invalid / valid) can be exercised without a real
    releases directory.
    """
    monkeypatch.setattr(merging, "get_versioned_file_paths", lambda file_type, pipeline_metadata: release_file)


def _write_release(release_file, **fields):
    """Write a latest-release.json for a source, matching how real release files are serialized."""
    release_file.write_text(json.dumps(PipelineMetadata(**fields).get_release_metadata()))


def test_missing_release_file_returns_none(tmp_path, monkeypatch):
    """A source with no release file (e.g. a node-only source) is referenced by its build, not a release."""
    _point_release_lookup_at(monkeypatch, tmp_path / "latest-release.json")  # never created
    build_metadata = PipelineMetadata(source="node_only_source", build_version="abc123")

    assert _get_source_release_metadata("node_only_source", build_metadata) is None


def test_release_missing_version_raises(tmp_path, monkeypatch):
    """A release file that exists but has no release_version is a corrupt release and must fail loudly."""
    release_file = tmp_path / "latest-release.json"
    _write_release(release_file, source="some_source", build_version="abc123", release_version=None)
    _point_release_lookup_at(monkeypatch, release_file)
    build_metadata = PipelineMetadata(source="some_source", build_version="abc123")

    with pytest.raises(ValueError, match="missing a release_version"):
        _get_source_release_metadata("some_source", build_metadata)


def test_release_build_mismatch_raises(tmp_path, monkeypatch):
    """If the source was rebuilt since its latest release, the build files no longer match the release we'd cite."""
    release_file = tmp_path / "latest-release.json"
    _write_release(release_file, source="some_source", build_version="OLD_build", release_version="2026_01_01")
    _point_release_lookup_at(monkeypatch, release_file)
    build_metadata = PipelineMetadata(source="some_source", build_version="NEW_build")

    with pytest.raises(ValueError, match="rebuilt since its latest release"):
        _get_source_release_metadata("some_source", build_metadata)


def test_release_matching_build_is_returned(tmp_path, monkeypatch):
    """When the release corresponds to the current build, its metadata is returned for use in the merge."""
    release_file = tmp_path / "latest-release.json"
    _write_release(
        release_file,
        source="some_source",
        build_version="abc123",
        release_version="2026_01_01",
        data="https://example.org/releases/some_source/2026_01_01/",
    )
    _point_release_lookup_at(monkeypatch, release_file)
    build_metadata = PipelineMetadata(source="some_source", build_version="abc123")

    result = _get_source_release_metadata("some_source", build_metadata)

    assert result is not None
    assert result.release_version == "2026_01_01"
    assert result.build_version == "abc123"
    assert result.data == "https://example.org/releases/some_source/2026_01_01/"