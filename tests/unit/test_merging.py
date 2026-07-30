import json

import pytest

from translator_ingest import merging
from translator_ingest.merging import (
    SHARED_SOURCE_RELEASE_METADATA,
    _extract_release_kgx_files,
    _get_shared_metadata_value,
    _read_source_release_metadata,
)
from translator_ingest.release import (
    RELEASE_EDGES_FILENAME,
    RELEASE_GRAPH_METADATA_FILENAME,
    RELEASE_NODES_FILENAME,
    create_compressed_tar,
)
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import IngestFileName

# A source release with everything the merge requires of it.
COMPLETE_RELEASE_METADATA = {
    "release_version": "1.0.0",
    "build_version": "abc123",
    "biolink_version": "4.2.6",
    "babel_version": "2025jul10",
    "data": "https://example.org/releases/some_source/1.0.0/",
}


@pytest.fixture
def releases_path(tmp_path, monkeypatch):
    """Redirect both the release metadata lookup and the release archive lookup at a temp releases directory."""
    monkeypatch.setattr("translator_ingest.util.storage.local.INGESTS_RELEASES_PATH", tmp_path)
    monkeypatch.setattr(merging, "INGESTS_RELEASES_PATH", tmp_path)
    return tmp_path


def write_latest_release_metadata(releases_path, source: str, **metadata_fields) -> None:
    """Write a source's latest-release.json the way release_ingest does."""
    source_dir = releases_path / source
    source_dir.mkdir(parents=True, exist_ok=True)
    release_metadata = PipelineMetadata(source=source, **metadata_fields)
    (source_dir / IngestFileName.LATEST_RELEASE_FILE).write_text(
        json.dumps(release_metadata.get_release_metadata())
    )


def write_release_archive(releases_path, source: str, release_version: str, include_edges: bool = True) -> None:
    """Create a real release archive for a source, as release_ingest does via create_compressed_tar."""
    release_dir = releases_path / source / release_version
    release_dir.mkdir(parents=True, exist_ok=True)

    nodes_file = release_dir / "source_nodes.jsonl"
    nodes_file.write_text('{"id": "MONDO:0005148"}\n')
    edges_file = release_dir / "source_edges.jsonl"
    if include_edges:
        edges_file.write_text('{"subject": "MONDO:0005148", "object": "CHEBI:6801"}\n')
    graph_metadata_file = release_dir / RELEASE_GRAPH_METADATA_FILENAME
    graph_metadata_file.write_text(json.dumps({"name": source}))

    create_compressed_tar(nodes_file=nodes_file,
                          edges_file=edges_file,
                          graph_metadata_path=graph_metadata_file,
                          output_path=release_dir / f"{source}.tar.zst")


def test_missing_release_metadata_raises(releases_path):
    """A source with no release cannot be merged - merges are built from releases, so this must fail loudly."""
    with pytest.raises(IOError, match="Could not find latest release metadata for some_source"):
        _read_source_release_metadata("some_source")


@pytest.mark.parametrize("missing_field", sorted(COMPLETE_RELEASE_METADATA))
def test_incomplete_release_metadata_raises(releases_path, missing_field):
    """Everything the merged graph metadata cites about a source must be present in its release metadata."""
    incomplete_metadata = {**COMPLETE_RELEASE_METADATA, missing_field: None}
    write_latest_release_metadata(releases_path, "some_source", **incomplete_metadata)

    with pytest.raises(ValueError, match=f"must have a valid {missing_field}"):
        _read_source_release_metadata("some_source")


def test_complete_release_metadata_is_returned(releases_path):
    """A complete release is returned so the merge can cite its version and release URL."""
    write_latest_release_metadata(releases_path, "some_source", **COMPLETE_RELEASE_METADATA)

    release_metadata = _read_source_release_metadata("some_source")

    assert release_metadata.source == "some_source"
    assert release_metadata.release_version == "1.0.0"
    assert release_metadata.build_version == "abc123"
    assert release_metadata.data == "https://example.org/releases/some_source/1.0.0/"


def _agreeing_source_releases() -> dict[str, PipelineMetadata]:
    """Two sources built and normalized identically, which is the precondition for merging them."""
    shared = dict(biolink_version="4.2.6",
                  babel_version="2025jul10",
                  node_normalizer_version="2.4.1",
                  normalization_code_version="1.4.0",
                  normalization_conflation=True,
                  normalization_strict=True)
    return {"source_a": PipelineMetadata(source="source_a", **shared),
            "source_b": PipelineMetadata(source="source_b", **shared)}


@pytest.mark.parametrize("metadata_field", SHARED_SOURCE_RELEASE_METADATA)
def test_shared_value_returned_when_sources_agree(metadata_field):
    """Sources built and normalized the same way can merge, and the merged graph inherits those values."""
    source_releases = _agreeing_source_releases()

    shared_value = _get_shared_metadata_value(source_releases, metadata_field)

    assert shared_value == getattr(source_releases["source_a"], metadata_field)


def test_single_source_value_is_shared():
    """A single source trivially agrees with itself."""
    source_releases = {"source_a": PipelineMetadata(source="source_a", biolink_version="4.2.6")}

    assert _get_shared_metadata_value(source_releases, "biolink_version") == "4.2.6"


@pytest.mark.parametrize("metadata_field, diverging_value", [
    ("biolink_version", "4.2.7"),
    ("babel_version", "2025sep1"),
    # Same Babel data, but resolved by a different Node Normalizer API or different ORION normalization code,
    # so identifiers can not be assumed to have been resolved the same way.
    ("node_normalizer_version", "2.5.0"),
    ("normalization_code_version", "1.5.0"),
    # Mixing these produces a graph that is only conflated, or only strictly filtered, in places.
    ("normalization_conflation", False),
    ("normalization_strict", False),
])
def test_diverging_values_raise(metadata_field, diverging_value):
    """Sources that disagree on Biolink or on how they were normalized must not be merged together."""
    source_releases = _agreeing_source_releases()
    setattr(source_releases["source_b"], metadata_field, diverging_value)

    with pytest.raises(ValueError, match=f"All sources must have the same {metadata_field}"):
        _get_shared_metadata_value(source_releases, metadata_field)


def test_extract_release_kgx_files(releases_path, tmp_path):
    """The KGX files a merge reads are extracted out of each source's compressed release archive."""
    write_release_archive(releases_path, "some_source", "1.0.0")
    release_metadata = PipelineMetadata(source="some_source", release_version="1.0.0")
    staging_directory = tmp_path / "staging"

    files_to_merge = _extract_release_kgx_files("some_source", release_metadata, staging_directory)

    extraction_directory = staging_directory / "some_source"
    assert files_to_merge == [str(extraction_directory / RELEASE_NODES_FILENAME),
                              str(extraction_directory / RELEASE_EDGES_FILENAME)]
    assert (extraction_directory / RELEASE_NODES_FILENAME).read_text() == '{"id": "MONDO:0005148"}\n'


def test_extract_release_kgx_files_nodes_only(releases_path, tmp_path):
    """Nodes-only sources have no edges file in their release, so only their nodes file is merged."""
    write_release_archive(releases_path, "nodes_only_source", "1.0.0", include_edges=False)
    release_metadata = PipelineMetadata(source="nodes_only_source", release_version="1.0.0")
    staging_directory = tmp_path / "staging"

    files_to_merge = _extract_release_kgx_files("nodes_only_source", release_metadata, staging_directory)

    extraction_directory = staging_directory / "nodes_only_source"
    assert files_to_merge == [str(extraction_directory / RELEASE_NODES_FILENAME)]
    assert not (extraction_directory / RELEASE_EDGES_FILENAME).exists()


def test_extract_release_kgx_files_uses_release_version_from_metadata(releases_path, tmp_path):
    """The release named in a source's metadata is the one merged, even when newer releases exist on disk."""
    write_release_archive(releases_path, "some_source", "1.0.0")
    write_release_archive(releases_path, "some_source", "2.0.0", include_edges=False)
    release_metadata = PipelineMetadata(source="some_source", release_version="1.0.0")

    files_to_merge = _extract_release_kgx_files("some_source", release_metadata, tmp_path / "staging")

    # 1.0.0 has edges, 2.0.0 does not, so the edges file confirms which release was extracted
    assert len(files_to_merge) == 2


def test_missing_release_archive_raises(releases_path, tmp_path):
    """Release metadata pointing at a release with no archive is an error, not an empty merge."""
    write_latest_release_metadata(releases_path, "some_source", **COMPLETE_RELEASE_METADATA)
    release_metadata = PipelineMetadata(source="some_source", release_version="1.0.0")

    with pytest.raises(IOError, match="Could not find the release archive for some_source"):
        _extract_release_kgx_files("some_source", release_metadata, tmp_path / "staging")