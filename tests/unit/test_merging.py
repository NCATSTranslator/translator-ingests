import json

import pytest
import yaml

from translator_ingest import merging
from translator_ingest.merging import (
    SHARED_SOURCE_RELEASE_METADATA,
    _extract_release_kgx_files,
    _get_shared_metadata_value,
    _read_source_release_metadata,
    generate_merged_graph_release,
    merge,
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
    "biolink_version": "v4.4.4",
    "babel_version": "2025jul10",
    "node_normalizer_version": "2.4.1",
    "normalization_code_version": "1.4.0",
    "data": "https://example.org/releases/some_source/1.0.0/",
}

RELEASE_NODES = [{"id": "MONDO:0005148", "name": "type 2 diabetes mellitus", "category": ["biolink:Disease"]},
                 {"id": "CHEBI:6801", "name": "metformin", "category": ["biolink:ChemicalEntity"]}]
RELEASE_EDGE = {"subject": "MONDO:0005148",
                "predicate": "biolink:treated_by",
                "object": "CHEBI:6801",
                "primary_knowledge_source": "infores:some-source"}


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
    nodes_file.write_text("".join(f"{json.dumps(node)}\n" for node in RELEASE_NODES))
    edges_file = release_dir / "source_edges.jsonl"
    if include_edges:
        edges_file.write_text(json.dumps(RELEASE_EDGE) + "\n")
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
                  normalization_conflation=True)
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
    # Mixing conflation settings produces a graph that is only conflated in places.
    ("normalization_conflation", False),
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
    extracted_nodes = (extraction_directory / RELEASE_NODES_FILENAME).read_text().splitlines()
    assert [json.loads(node) for node in extracted_nodes] == RELEASE_NODES


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

@pytest.fixture
def merged_graph_sources(releases_path, monkeypatch):
    """Two released sources, complete with archives and rig files, ready to be merged into a graph."""
    parser_path = releases_path / "ingests"
    monkeypatch.setattr("translator_ingest.util.metadata.INGESTS_PARSER_PATH", parser_path)

    sources = ["source_a", "source_b"]
    for source in sources:
        write_latest_release_metadata(releases_path, source,
                                      **{**COMPLETE_RELEASE_METADATA, "build_version": f"{source}_build"})
        write_release_archive(releases_path, source, "1.0.0")
        rig_dir = parser_path / source
        rig_dir.mkdir(parents=True)
        (rig_dir / f"{source}_rig.yaml").write_text(
            yaml.dump({"name": source, "source_info": {"description": f"{source} description"}})
        )
    return sources


def test_merge_produces_a_release_of_its_sources(releases_path, merged_graph_sources):
    """A merge of released sources produces a versioned merged graph citing the releases it was built from."""
    merged_graph_metadata = merge("test_graph", merged_graph_sources)

    assert merged_graph_metadata.source == "test_graph"
    # First release of this graph, inheriting the values its sources agreed on.
    assert merged_graph_metadata.release_version == "1.0.0"
    assert merged_graph_metadata.biolink_version == COMPLETE_RELEASE_METADATA["biolink_version"]
    assert merged_graph_metadata.babel_version == COMPLETE_RELEASE_METADATA["babel_version"]

    output_dir = releases_path / "test_graph" / "1.0.0"
    assert (output_dir / RELEASE_NODES_FILENAME).exists()
    assert (output_dir / RELEASE_EDGES_FILENAME).exists()
    assert (output_dir / "merge-metadata.json").exists()

    # hasPart identifies the released graphs the merged graph is made of
    graph_metadata = json.loads((output_dir / RELEASE_GRAPH_METADATA_FILENAME).read_text())
    assert sorted(part["name"] for part in graph_metadata["hasPart"]) == merged_graph_sources


def test_merge_skips_when_latest_release_is_already_this_build(releases_path, merged_graph_sources):
    """Re-merging unchanged sources is a no-op, so a merged graph is not re-released for the same build."""
    first_merge = merge("test_graph", merged_graph_sources)
    generate_merged_graph_release(first_merge)

    assert merge("test_graph", merged_graph_sources) is None


def test_merge_fails_fast_on_an_unreleased_source(merged_graph_sources):
    """A source that was never released cannot be merged, and the error names the source to release."""
    with pytest.raises(IOError, match="Create a release for never_released before attempting to merge it"):
        merge("test_graph", merged_graph_sources + ["never_released"])
