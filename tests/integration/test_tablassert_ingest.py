"""Integration tests for the Tablassert ingest kind: the real isolated CLI and the real HF API.

These tests exercise actual `uvx` Tablassert runs and HuggingFace requests. They use a pinned CLI
version and tiny fixtures, never a full dataset download. The `integration` marker is informational;
`make test` runs these tests like every other test.
"""

import json
from pathlib import Path

import pytest
import requests
import yaml

import translator_ingest.util.tablassert as tablassert
from translator_ingest.util import hf_releases
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage import local as storage_local

CLI_VERSION = "19.7.0"
BIOLINK_VERSION = "4.4.6"

VALID_NODES = [
    {"id": "DRUGCOMPOUND:0001", "name": "Test Drug", "category": ["biolink:Drug"]},
    {"id": "MONDO:0000001", "name": "Test Disease", "category": ["biolink:Disease"]},
]
# The first edge is strictly compliant; the second carries an enum value that does not exist in the
# biolink model, so a --prune run removes exactly that edge and keeps the first one byte-identical.
VALID_EDGES = [
    {"id": "urn:id:1", "subject": "DRUGCOMPOUND:0001", "predicate": "biolink:treats", "object": "MONDO:0000001",
     "category": ["biolink:EntityToDiseaseAssociation"], "knowledge_level": "knowledge_assertion",
     "agent_type": "manual_agent", "clinical_approval_status": "approved_for_condition"},
    {"id": "urn:id:2", "subject": "DRUGCOMPOUND:0001", "predicate": "biolink:treats", "object": "MONDO:0000001",
     "category": ["biolink:EntityToDiseaseAssociation"], "knowledge_level": "knowledge_assertion",
     "agent_type": "manual_agent", "clinical_approval_status": "bogus_value"},
]
RIG_YAML = "name: Example source\nsource_info:\n  description: A tiny example source\n"


def to_ndjson(records: list[dict]) -> str:
    return "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)


@pytest.fixture
def example_source(tmp_path, monkeypatch) -> tuple[Path, PipelineMetadata]:
    ingests_dir = tmp_path / "ingests" / "example"
    ingests_dir.mkdir(parents=True)
    declaration = {
        "version": CLI_VERSION,
        "nodes": {"url": "https://example.com/{version}/nodes.ndjson", "path": "example_nodes.jsonl"},
        "edges": {"url": "https://example.com/{version}/edges.ndjson", "path": "example_edges.jsonl"},
        "rig": {"url": "https://example.com/{version}/rig.yaml", "path": "example_rig.yaml"},
    }
    (ingests_dir / tablassert.TABLASSERT_CONFIG_FILENAME).write_text(yaml.safe_dump(declaration))
    monkeypatch.setattr(tablassert, "INGESTS_PARSER_PATH", tmp_path / "ingests")

    data_dir = tmp_path / "data"
    monkeypatch.setattr(storage_local, "INGESTS_DATA_PATH", data_dir)
    metadata = PipelineMetadata(
        source="example", source_version="1.0.0", transform_version="abc",
        babel_version="3.0", node_normalizer_version="5.0", normalization_code_version="1.0",
        merging_code_version="2.0", biolink_version=BIOLINK_VERSION,
    )
    source_data_dir = storage_local.get_source_data_directory(metadata)
    source_data_dir.mkdir(parents=True)
    (source_data_dir / "example_nodes.jsonl").write_text(to_ndjson(VALID_NODES), encoding="utf-8")
    (source_data_dir / "example_edges.jsonl").write_text(to_ndjson(VALID_EDGES), encoding="utf-8")
    (source_data_dir / "example_rig.yaml").write_text(RIG_YAML, encoding="utf-8")
    return tmp_path, metadata


def test_real_cli_identity_resolution_is_pinned_and_recorded():
    identity = tablassert.resolve_cli_identity(CLI_VERSION)
    assert identity.requested == CLI_VERSION
    assert identity.tablassert_version == CLI_VERSION
    assert identity.biolink_version == BIOLINK_VERSION


def test_real_prune_keeps_valid_edges_and_prunes_the_invalid_one(example_source):
    _, metadata = example_source
    source_data_dir = storage_local.get_source_data_directory(metadata)
    original_nodes = (source_data_dir / "example_nodes.jsonl").read_bytes()
    original_edges = (source_data_dir / "example_edges.jsonl").read_bytes()

    recorded = tablassert.transform(metadata)

    transform_dir = storage_local.get_transform_directory(metadata)
    published_nodes = (transform_dir / "example_nodes.jsonl").read_bytes()
    published_edges = (transform_dir / "example_edges.jsonl").read_text().splitlines()

    # The downloaded inputs were never modified.
    assert (source_data_dir / "example_nodes.jsonl").read_bytes() == original_nodes
    assert (source_data_dir / "example_edges.jsonl").read_bytes() == original_edges
    # The published graph is the CLI's output: the invalid edge is gone, the valid one is intact.
    assert published_nodes == original_nodes
    assert len(published_edges) == 1
    assert json.loads(published_edges[0])["id"] == "urn:id:1"
    # The RIG snapshot was published alongside the graph pair.
    assert (transform_dir / "example_rig.yaml").read_text() == RIG_YAML
    # The recorded identity is complete and consistent with the pinned environment.
    assert recorded["ingest_kind"] == "tablassert"
    assert recorded["tablassert_version"] == CLI_VERSION
    assert recorded["tablassert_biolink_version"] == BIOLINK_VERSION
    assert recorded["repo_biolink_version"] == BIOLINK_VERSION
    assert recorded["biolink_mismatch"] is False
    assert recorded["node_count"] == 2
    assert recorded["edge_count"] == 1
    assert "pruned 1/2" in recorded["prune_report"]
    assert recorded["input_digest"] == tablassert.sha256_files(
        source_data_dir / "example_nodes.jsonl",
        source_data_dir / "example_edges.jsonl",
        source_data_dir / "example_rig.yaml",
    )
    # The build identity changes when the resolved CLI version changes.
    changed = PipelineMetadata(**{**metadata.get_release_metadata(), "tablassert_version": "20.0.0"})
    assert changed.generate_build_version() != metadata.generate_build_version()


def test_real_cli_failure_leaves_inputs_untouched_and_publishes_nothing(tmp_path, monkeypatch):
    ingests_dir = tmp_path / "ingests" / "broken"
    ingests_dir.mkdir(parents=True)
    declaration = {
        "version": CLI_VERSION,
        "nodes": {"url": "https://example.com/{version}/nodes.ndjson", "path": "broken_nodes.jsonl"},
        "edges": {"url": "https://example.com/{version}/edges.ndjson", "path": "broken_edges.jsonl"},
        "rig": {"url": "https://example.com/{version}/rig.yaml", "path": "broken_rig.yaml"},
    }
    (ingests_dir / tablassert.TABLASSERT_CONFIG_FILENAME).write_text(yaml.safe_dump(declaration))
    monkeypatch.setattr(tablassert, "INGESTS_PARSER_PATH", tmp_path / "ingests")

    data_dir = tmp_path / "data"
    monkeypatch.setattr(storage_local, "INGESTS_DATA_PATH", data_dir)
    metadata = PipelineMetadata(source="broken", source_version="1.0.0", transform_version="abc")
    source_data_dir = storage_local.get_source_data_directory(metadata)
    source_data_dir.mkdir(parents=True)
    invalid_nodes = 'this line is not json at all\n' + to_ndjson(VALID_NODES)
    (source_data_dir / "broken_nodes.jsonl").write_text(invalid_nodes, encoding="utf-8")
    (source_data_dir / "broken_edges.jsonl").write_text(to_ndjson(VALID_EDGES), encoding="utf-8")
    (source_data_dir / "broken_rig.yaml").write_text(RIG_YAML, encoding="utf-8")

    with pytest.raises(RuntimeError, match="Tablassert validation failed"):
        tablassert.transform(metadata)

    # Nothing was published: no transform outputs, no metadata, inputs unchanged.
    transform_dir = storage_local.get_transform_directory(metadata)
    published = sorted(path.name for path in transform_dir.iterdir()) if transform_dir.exists() else []
    assert published == []
    assert (source_data_dir / "broken_nodes.jsonl").read_text() == invalid_nodes
    from translator_ingest.util.storage.local import IngestFileType, get_versioned_file_paths

    assert not get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=metadata
    ).exists()


def test_real_empty_prune_result_is_rejected(example_source, monkeypatch):
    _, metadata = example_source
    source_data_dir = storage_local.get_source_data_directory(metadata)
    (source_data_dir / "example_edges.jsonl").write_text(to_ndjson(VALID_EDGES), encoding="utf-8")

    def prune_everything(command, working_directory):
        staged_edges = Path(command[command.index("--edges") + 1])
        staged_edges.write_text("")
        return 0, "", "edges: pruned 2/2 non-compliant records", 1.0

    monkeypatch.setattr(tablassert, "run_prune", prune_everything)
    with pytest.raises(RuntimeError, match="unusable graph"):
        tablassert.transform(metadata)


def test_real_hf_discovery_reports_the_current_latest_release():
    releases = hf_releases.list_dataset_release_directories("SkyeAv/drug-approvals-kp")
    assert releases, "The DAKP dataset must expose at least one complete release directory."
    latest = hf_releases.get_latest_dataset_version("SkyeAv/drug-approvals-kp")
    assert latest == hf_releases.latest_release(releases)

    # The discovered latest release must currently expose all three declared files.
    base_url = "https://huggingface.co/datasets/SkyeAv/drug-approvals-kp/resolve/main"
    for suffix in ("nodes.ndjson", "edges.ndjson", "RIG.yaml"):
        url = f"{base_url}/{latest}/DRUG_APPROVALS_KP_{latest}.{suffix}"
        response = requests.head(url, timeout=30, allow_redirects=True)
        assert response.status_code == 200, f"Expected {url} to be retrievable for the latest release"
