"""Tests for the release write path (release_ingest) and the release archive format."""
import datetime
import json

import pytest

import translator_ingest.release
import translator_ingest.util.storage.local as local_storage
from translator_ingest.release import (
    RELEASE_EDGES_FILENAME,
    RELEASE_GRAPH_METADATA_FILENAME,
    RELEASE_NODES_FILENAME,
    create_compressed_tar,
    extract_compressed_tar,
    release_ingest,
)
from translator_ingest.util.metadata import PipelineMetadata

# Version fields used to build the on-disk directory tree. The merge directory path is derived
# from these individual fields (not from build_version), so changing only build_version between
# releases reuses the same source files while producing a new release.
SOURCE = "testsrc"
BASE_METADATA = dict(
    source=SOURCE,
    source_version="v1",
    transform_version="tv1",
    babel_version="b1",
    node_normalizer_version="nn1",
    normalization_code_version="nc1",
    normalization_conflation=True,
    normalization_strict=True,
    merging_code_version="mc1",
    biolink_version="bl1",
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(payload, f, indent=2)


@pytest.fixture
def release_env(tmp_path, monkeypatch):
    """Build a minimal data/ and releases/ tree and point the storage paths at tmp_path.

    Returns a callable ``write_latest_build(metadata_dict)`` that writes latest-build.json,
    plus the releases root path.
    """
    data_path = tmp_path / "data"
    releases_path = tmp_path / "releases"

    # Patch the path constants in the modules that read them at call time.
    monkeypatch.setattr(local_storage, "INGESTS_DATA_PATH", data_path)
    monkeypatch.setattr(local_storage, "INGESTS_RELEASES_PATH", releases_path)
    monkeypatch.setattr(translator_ingest.release, "INGESTS_RELEASES_PATH", releases_path)
    monkeypatch.setattr(translator_ingest.release, "INGESTS_RELEASES_URL", "http://example.test/releases")

    # Create the merge directory and the source artifacts release_ingest reads.
    build_metadata = PipelineMetadata(**BASE_METADATA)
    merge_dir = (
        data_path / SOURCE / build_metadata.source_version
        / f"transform_{build_metadata.transform_version}"
        / f"normalization_{build_metadata.get_composite_normalization_version()}"
        / f"merge_{build_metadata.merging_code_version}"
    )
    merge_dir.mkdir(parents=True)
    (merge_dir / "merged_nodes.jsonl").write_text('{"id": "X:1"}\n')
    (merge_dir / "merged_edges.jsonl").write_text('{"subject": "X:1", "object": "X:2"}\n')
    _write_json(merge_dir / "graph-metadata.json", {"@id": "original", "url": "original"})
    _write_json(merge_dir / "testing_data.json", {"sample": True})

    def write_latest_build(metadata_dict):
        _write_json(data_path / SOURCE / "latest-build.json", metadata_dict)

    return write_latest_build, releases_path


def _read_latest_release(releases_path):
    with (releases_path / SOURCE / "latest-release.json").open() as f:
        return json.load(f)


def test_release_ingest_first_release(release_env):
    """The first release starts at 1.0.0, stamps release_date, and carries build_date through."""
    write_latest_build, releases_path = release_env
    write_latest_build({**BASE_METADATA, "build_version": "build1", "build_date": "2026-01-01"})

    release_ingest(SOURCE)

    release_metadata = _read_latest_release(releases_path)
    assert release_metadata["release_version"] == "1.0.0"
    assert release_metadata["build_version"] == "build1"
    # build_date is preserved from the build; release_date is stamped now as a
    # timezone-aware UTC ISO 8601 timestamp (e.g. "2026-07-09T14:23:51Z").
    assert release_metadata["build_date"] == "2026-01-01"
    release_date = datetime.datetime.fromisoformat(release_metadata["release_date"])
    assert release_date.tzinfo is not None
    assert release_date.date() == datetime.datetime.now(datetime.timezone.utc).date()

    # The versioned release directory and its compressed archive exist, and are copied to latest.
    assert (releases_path / SOURCE / "1.0.0" / f"{SOURCE}.tar.zst").exists()
    assert (releases_path / SOURCE / "latest" / f"{SOURCE}.tar.zst").exists()


def test_release_ingest_bumps_patch_for_new_build(release_env):
    """A second, different build released later bumps the patch version and updates the dates."""
    write_latest_build, releases_path = release_env

    write_latest_build({**BASE_METADATA, "build_version": "build1", "build_date": "2026-01-01"})
    release_ingest(SOURCE)
    assert _read_latest_release(releases_path)["release_version"] == "1.0.0"

    # New build version -> new release. Distinct release directories avoid same-day collisions.
    write_latest_build({**BASE_METADATA, "build_version": "build2", "build_date": "2026-02-02"})
    release_ingest(SOURCE)

    release_metadata = _read_latest_release(releases_path)
    assert release_metadata["release_version"] == "1.0.1"
    assert release_metadata["build_version"] == "build2"
    assert release_metadata["build_date"] == "2026-02-02"
    assert (releases_path / SOURCE / "1.0.1" / f"{SOURCE}.tar.zst").exists()


def test_release_ingest_same_build_is_noop(release_env):
    """Re-releasing an unchanged build does not advance the release version."""
    write_latest_build, releases_path = release_env
    write_latest_build({**BASE_METADATA, "build_version": "build1", "build_date": "2026-01-01"})

    release_ingest(SOURCE)
    release_ingest(SOURCE)

    assert _read_latest_release(releases_path)["release_version"] == "1.0.0"


NODES = '{"id": "MONDO:0005148", "name": "type 2 diabetes mellitus"}\n'
EDGES = '{"subject": "MONDO:0005148", "predicate": "biolink:treated_by", "object": "CHEBI:6801"}\n'
GRAPH_METADATA = {"name": SOURCE, "version": "1.0.0"}


def _write_kgx_files(directory, include_edges: bool = True):
    """Write the build files that go into a release archive, in create_compressed_tar's argument order."""
    directory.mkdir(parents=True, exist_ok=True)
    nodes_file = directory / "merged_nodes.jsonl"
    nodes_file.write_text(NODES)
    edges_file = directory / "merged_edges.jsonl"
    if include_edges:
        edges_file.write_text(EDGES)
    graph_metadata_file = directory / RELEASE_GRAPH_METADATA_FILENAME
    graph_metadata_file.write_text(json.dumps(GRAPH_METADATA))
    return nodes_file, edges_file, graph_metadata_file


@pytest.mark.parametrize("include_edges", [True, False])
def test_compressed_tar_round_trip(tmp_path, include_edges):
    """Release archives must read back unchanged, because merged graphs are built from them.

    Build file names (merged_nodes.jsonl) are normalized to release names (nodes.jsonl) inside the archive, and a
    nodes-only source produces an archive with no edges file at all.
    """
    nodes_file, edges_file, graph_metadata_file = _write_kgx_files(tmp_path / "build", include_edges=include_edges)
    archive_path = tmp_path / f"{SOURCE}.tar.zst"

    create_compressed_tar(nodes_file=nodes_file,
                          edges_file=edges_file,
                          graph_metadata_path=graph_metadata_file,
                          output_path=archive_path)
    # the staging directory a merge extracts into does not exist beforehand
    extracted = tmp_path / "staging" / SOURCE
    extract_compressed_tar(archive_path, extracted)

    assert (extracted / RELEASE_NODES_FILENAME).read_text() == NODES
    assert json.loads((extracted / RELEASE_GRAPH_METADATA_FILENAME).read_text()) == GRAPH_METADATA
    if include_edges:
        assert (extracted / RELEASE_EDGES_FILENAME).read_text() == EDGES
    else:
        assert not (extracted / RELEASE_EDGES_FILENAME).exists()