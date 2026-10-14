"""Tests for the pipeline branches that serve the Tablassert ingest kind (offline)."""

import inspect
import json
from pathlib import Path

import pytest
import yaml

import translator_ingest.util.tablassert as tablassert
from translator_ingest import pipeline
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage import local as storage_local

VALID_DECLARATION = {
    "nodes": {"url": "https://example.com/dataset/{version}/nodes.ndjson", "path": "example_nodes.jsonl"},
    "edges": {"url": "https://example.com/dataset/{version}/edges.ndjson", "path": "example_edges.jsonl"},
    "rig": {"url": "https://example.com/dataset/{version}/rig.yaml", "path": "example_rig.yaml"},
}


@pytest.fixture
def fake_ingest(tmp_path, monkeypatch) -> Path:
    ingests_dir = tmp_path / "ingests"
    ingest_dir = ingests_dir / "example"
    ingest_dir.mkdir(parents=True)
    (ingest_dir / tablassert.TABLASSERT_CONFIG_FILENAME).write_text(yaml.safe_dump(VALID_DECLARATION))
    (ingest_dir / "example.py").write_text("def get_latest_version():\n    return '1.0.0'\n")
    monkeypatch.setattr(pipeline, "INGESTS_PARSER_PATH", ingests_dir)
    monkeypatch.setattr(tablassert, "INGESTS_PARSER_PATH", ingests_dir)
    data_dir = tmp_path / "data"
    monkeypatch.setattr(storage_local, "INGESTS_DATA_PATH", data_dir)
    return ingest_dir


def build_metadata(source_version: str = "1.0.0", transform_version: str = "abc") -> PipelineMetadata:
    return PipelineMetadata(source="example", source_version=source_version, transform_version=transform_version)


def stage_declared_inputs(ingest_dir: Path, metadata: PipelineMetadata) -> Path:
    source_data_dir = storage_local.get_source_data_directory(metadata)
    source_data_dir.mkdir(parents=True, exist_ok=True)
    for declared in ("example_nodes.jsonl", "example_edges.jsonl", "example_rig.yaml"):
        (source_data_dir / declared).write_text("content\n")
    return source_data_dir


def test_get_transform_version_changes_when_the_tablassert_declaration_changes(fake_ingest):
    version_before = pipeline.get_transform_version("example")
    declaration = yaml.safe_load((fake_ingest / tablassert.TABLASSERT_CONFIG_FILENAME).read_text())
    declaration["version"] = "19.7.0"
    (fake_ingest / tablassert.TABLASSERT_CONFIG_FILENAME).write_text(yaml.safe_dump(declaration))
    assert pipeline.get_transform_version("example") != version_before


def test_load_koza_config_records_no_edge_limit_without_koza(fake_ingest):
    metadata = build_metadata()
    pipeline.load_koza_config("example", metadata)
    assert metadata.koza_config == {"max_edge_count": None}


def test_download_materializes_the_durable_manifest(fake_ingest, monkeypatch):
    metadata = build_metadata()
    source_data_dir = stage_declared_inputs(fake_ingest, metadata)

    def fake_download(yaml_file, output_dir, ignore_cache=False):
        assert Path(yaml_file) == source_data_dir / "download.yaml"
        assert str(output_dir) == str(source_data_dir)
        return None

    monkeypatch.setattr(pipeline, "kghub_download", fake_download)
    pipeline.download(metadata)
    manifest = yaml.safe_load((source_data_dir / "download.yaml").read_text())
    assert manifest[0]["url"] == "https://example.com/dataset/1.0.0/nodes.ndjson"


def test_download_requires_a_resolved_source_version(fake_ingest):
    with pytest.raises(ValueError, match="resolved source version"):
        pipeline.download(build_metadata(source_version=None))


def completed_build(fake_ingest) -> PipelineMetadata:
    metadata = build_metadata()
    source_data_dir = stage_declared_inputs(fake_ingest, metadata)
    transform_dir = storage_local.get_transform_directory(metadata)
    transform_dir.mkdir(parents=True, exist_ok=True)
    for declared in ("example_nodes.jsonl", "example_edges.jsonl", "example_rig.yaml"):
        (transform_dir / declared).write_text("content\n")
    (transform_dir / "transform-metadata.json").write_text(json.dumps({
        "tablassert_version": "19.7.0", "tablassert_biolink_version": "4.4.6", "input_digest": "ab" * 32,
    }))
    assert tablassert.sha256_files(source_data_dir / "example_nodes.jsonl") is not None
    return metadata


def test_is_transform_complete_requires_outputs_metadata_and_snapshot(fake_ingest):
    metadata = build_metadata()
    assert not pipeline.is_transform_complete(metadata)

    metadata = completed_build(fake_ingest)
    assert pipeline.is_transform_complete(metadata)

    transform_dir = storage_local.get_transform_directory(metadata)
    (transform_dir / "example_rig.yaml").unlink()
    assert not pipeline.is_transform_complete(metadata)

    metadata = completed_build(fake_ingest)
    transform_dir = storage_local.get_transform_directory(metadata)
    (transform_dir / "example_edges.jsonl").unlink()
    assert not pipeline.is_transform_complete(metadata)


def test_failed_rebuild_leaves_no_previous_generation(fake_ingest, monkeypatch):
    """A failed rebuild must not leave the old complete generation behind for later reuse."""
    metadata = completed_build(fake_ingest)
    transform_dir = storage_local.get_transform_directory(metadata)
    assert pipeline.is_transform_complete(metadata)

    monkeypatch.setattr(tablassert, "resolve_cli_identity",
                        lambda cli_version: (_ for _ in ()).throw(RuntimeError("resolver down")))
    with pytest.raises(RuntimeError, match="resolver down"):
        pipeline.transform(metadata)
    assert list(transform_dir.iterdir()) == []
    assert not pipeline.is_transform_complete(metadata)


def test_get_transform_version_covers_the_shared_adapter(fake_ingest):
    """The Tablassert transform hash must include the shared adapter files."""
    adapter_paths = {path.name for path in tablassert.adapter_files()}
    assert {"tablassert.py", "hf_releases.py"} <= adapter_paths
    source_text = inspect.getsource(pipeline.get_transform_version)
    assert "adapter_files" in source_text


def test_is_transform_complete_rejects_metadata_without_identity(fake_ingest):
    metadata = completed_build(fake_ingest)
    transform_dir = storage_local.get_transform_directory(metadata)
    (transform_dir / "transform-metadata.json").write_text(json.dumps({"source": "example"}))
    assert not pipeline.is_transform_complete(metadata)


def test_transform_runs_the_kind_and_returns_early(fake_ingest, monkeypatch):
    metadata = completed_build(fake_ingest)

    def fail_koza(*args, **kwargs):
        raise AssertionError("A Tablassert transform must never instantiate koza.")

    monkeypatch.setattr(pipeline, "KozaRunner", fail_koza)

    identity_results = iter([
        tablassert.CliIdentity(requested="latest", tablassert_version="19.7.0", biolink_version="4.4.6")
    ])
    monkeypatch.setattr(tablassert, "resolve_cli_identity", lambda cli_version: next(identity_results))
    monkeypatch.setattr(
        tablassert, "installed_package_version", lambda package: "4.4.6" if package == "biolink-model" else "?"
    )

    def fake_prune(command, working_directory):
        return 0, "", "edges: pruned 0/1 non-compliant records", 1.0

    monkeypatch.setattr(tablassert, "run_prune", fake_prune)

    pipeline.transform(metadata)
    recorded = json.loads((storage_local.get_transform_directory(metadata) / "transform-metadata.json").read_text())
    assert recorded["ingest_kind"] == "tablassert"
    assert recorded["tablassert_version"] == "19.7.0"
    assert recorded["input_digest"]
    assert metadata.tablassert_version == "19.7.0"
