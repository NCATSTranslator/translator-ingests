"""Tests for the Tablassert ingest kind's declaration model and pure helpers.

These tests are offline: they exercise validation, manifests, digests, mismatch detection, and the
build-version fingerprint without running the isolated CLI (the real CLI runs are covered by
tests/integration).
"""

import hashlib
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import translator_ingest.util.tablassert as tablassert
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import IngestFileType, get_versioned_file_paths

VALID_DECLARATION = {
    "nodes": {"url": "https://example.com/dataset/{version}/nodes.ndjson", "path": "example_nodes.jsonl"},
    "edges": {"url": "https://example.com/dataset/{version}/edges.ndjson", "path": "example_edges.jsonl"},
    "rig": {"url": "https://example.com/dataset/{version}/rig.yaml", "path": "example_rig.yaml"},
}


def write_declaration(ingests_dir: Path, source: str, declaration: dict) -> Path:
    ingest_dir = ingests_dir / source
    ingest_dir.mkdir(parents=True, exist_ok=True)
    config_path = ingest_dir / tablassert.TABLASSERT_CONFIG_FILENAME
    config_path.write_text(yaml.safe_dump(declaration), encoding="utf-8")
    return config_path


@pytest.fixture
def fake_source(tmp_path, monkeypatch) -> Path:
    ingests_dir = tmp_path / "ingests"
    ingests_dir.mkdir()
    monkeypatch.setattr(tablassert, "INGESTS_PARSER_PATH", ingests_dir)
    return ingests_dir


def test_declares_requires_the_marker_file(fake_source):
    assert not tablassert.declares("example")
    write_declaration(fake_source, "example", VALID_DECLARATION)
    assert tablassert.declares("example")


def test_load_rejects_a_missing_declaration(fake_source):
    with pytest.raises(tablassert.TablassertConfigError, match="must declare"):
        tablassert.load("example")


def test_load_rejects_an_ambiguous_koza_declaration(fake_source):
    write_declaration(fake_source, "example", VALID_DECLARATION)
    (fake_source / "example" / "example.yaml").write_text("name: example")
    with pytest.raises(tablassert.TablassertConfigError, match="exactly one kind"):
        tablassert.load("example")


def test_load_rejects_a_legacy_download_manifest(fake_source):
    write_declaration(fake_source, "example", VALID_DECLARATION)
    (fake_source / "example" / "download.yaml").write_text("- url: https://a/b")
    with pytest.raises(tablassert.TablassertConfigError, match="legacy"):
        tablassert.load("example")


def test_load_rejects_unknown_keys(fake_source):
    declaration = {**VALID_DECLARATION, "ingest_kind": "tablassert"}
    write_declaration(fake_source, "example", declaration)
    with pytest.raises(tablassert.TablassertConfigError, match="Invalid Tablassert declaration"):
        tablassert.load("example")


def test_load_rejects_a_bad_cli_version(fake_source):
    declaration = {**VALID_DECLARATION, "version": "19.x"}
    write_declaration(fake_source, "example", declaration)
    with pytest.raises(tablassert.TablassertConfigError, match="Invalid Tablassert declaration"):
        tablassert.load("example")


def test_load_accepts_an_exact_cli_version_pin(fake_source):
    write_declaration(fake_source, "example", {**VALID_DECLARATION, "version": "19.7.0"})
    config = tablassert.load("example")
    assert config.version == "19.7.0"
    assert config.nodes.path == "example_nodes.jsonl"


@pytest.mark.parametrize("path", ["../escape.jsonl", "sub/dir.jsonl", "", ".", "..", "download.yaml", "tablassert.yaml"])
def test_remote_file_rejects_unsafe_paths(path):
    with pytest.raises(ValidationError):
        tablassert.RemoteFile(url="https://example.com/file", path=path)


@pytest.mark.parametrize("url", ["ftp://example.com/file", "file:///etc/passwd"])
def test_remote_file_rejects_non_http_urls(url):
    with pytest.raises(ValidationError):
        tablassert.RemoteFile(url=url, path="file.jsonl")


def test_declaration_rejects_duplicate_paths():
    declaration = {
        "nodes": {"url": "https://example.com/n", "path": "same.jsonl"},
        "edges": {"url": "https://example.com/e", "path": "same.jsonl"},
        "rig": {"url": "https://example.com/r", "path": "rig.yaml"},
    }
    with pytest.raises(ValidationError, match="distinct"):
        tablassert.TablassertIngest(**declaration)


def test_declaration_rejects_misfiled_nodes_and_edges_paths():
    declaration = {
        "nodes": {"url": "https://example.com/n", "path": "nodes.jsonl"},
        "edges": {"url": "https://example.com/e", "path": "edges_nodes.jsonl"},
        "rig": {"url": "https://example.com/r", "path": "rig.yaml"},
    }
    with pytest.raises(ValidationError, match="edges path"):
        tablassert.TablassertIngest(**declaration)


def test_declared_paths_must_exist_before_a_build(fake_source, tmp_path):
    write_declaration(fake_source, "example", VALID_DECLARATION)
    config = tablassert.load("example")
    with pytest.raises(FileNotFoundError, match="missing"):
        tablassert._declared_paths(tmp_path, config)


def test_write_download_yaml_substitutes_dataset_versions(fake_source, tmp_path):
    write_declaration(fake_source, "example", VALID_DECLARATION)
    config = tablassert.load("example")
    manifest_path = tablassert.write_download_yaml("example", config, "1.23.5", tmp_path)
    entries = yaml.safe_load(manifest_path.read_text())
    assert [entry["local_name"] for entry in entries] == ["example_nodes.jsonl", "example_edges.jsonl", "example_rig.yaml"]
    assert entries[0]["url"] == "https://example.com/dataset/1.23.5/nodes.ndjson"
    assert all("1.0.0" not in entry["url"] and "{" not in entry["url"] for entry in entries)


def test_resolve_dataset_version_placeholder_rejects_stray_placeholders():
    assert tablassert.resolve_dataset_version_placeholder("https://a/{version}/f", "1.2.3") == "https://a/1.2.3/f"
    with pytest.raises(tablassert.TablassertConfigError, match="placeholder"):
        tablassert.resolve_dataset_version_placeholder("https://a/{version}/{other}", "1.2.3")


def test_biolink_mismatch_message_only_fires_on_a_difference():
    assert tablassert.biolink_mismatch_message("4.4.6", "4.4.6") is None
    message = tablassert.biolink_mismatch_message("4.4.6", "4.5.0")
    assert message and "4.5.0" in message and "4.4.6" in message and "Continuing" in message


def test_sha256_files_binds_order_and_content(tmp_path):
    first = tmp_path / "a"
    second = tmp_path / "b"
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    expected = hashlib.sha256(b"onetwo").hexdigest()
    assert tablassert.sha256_files(first, second) == expected
    assert tablassert.sha256_files(second, first) != expected


def test_count_records_ignores_blank_lines(tmp_path):
    records = tmp_path / "edges.jsonl"
    records.write_text('{"id": "1"}\n\n{"id": "2"}\n   \n')
    assert tablassert.count_records(records) == 2


def test_build_version_includes_the_tablassert_identity_only_when_recorded():
    shared = dict(
        source_version="1.23.5", transform_version="abc",
        babel_version="3.0", node_normalizer_version="5.0", normalization_code_version="1.0",
        merging_code_version="2.0", biolink_version="4.4.6",
    )
    koza_metadata = PipelineMetadata(source="ctd", **shared)
    baseline = koza_metadata.generate_build_version()
    assert "None" not in baseline

    digest = hashlib.sha256(b"x").hexdigest()
    tablassert_metadata = PipelineMetadata(
        source="dakp", tablassert_version="19.7.0", tablassert_biolink_version="4.4.6", input_digest=digest,
        **shared,
    )
    fingerprinted = tablassert_metadata.generate_build_version()
    assert fingerprinted == f"{baseline.replace('ctd', 'dakp')}_19.7.0_4.4.6_{digest}"

    # A changed CLI version or changed input bytes must change the build identity.
    newer_cli = PipelineMetadata(**{**tablassert_metadata.get_release_metadata(), "tablassert_version": "20.0.0"})
    assert newer_cli.generate_build_version() != fingerprinted
    changed_bytes = PipelineMetadata(**{**tablassert_metadata.get_release_metadata(), "input_digest": "0" * 64})
    assert changed_bytes.generate_build_version() != fingerprinted


def test_release_metadata_round_trips_the_identity_and_drops_koza_config():
    metadata = PipelineMetadata(
        source="dakp", source_version="1.23.5", koza_config={"max_edge_count": None},
        tablassert_version="19.7.0", tablassert_biolink_version="4.4.6", input_digest="ab" * 32,
    )
    restored = PipelineMetadata.from_dict(metadata.get_release_metadata())
    assert restored.koza_config == {}
    assert restored.tablassert_version == "19.7.0"
    assert restored.input_digest == "ab" * 32


def test_load_execution_identity_reads_back_the_recorded_build(tmp_path, monkeypatch, fake_source):
    write_declaration(fake_source, "example", VALID_DECLARATION)
    (fake_source / "example" / "example_nodes.jsonl").write_text("{}\n")
    (fake_source / "example" / "example_edges.jsonl").write_text("{}\n")
    (fake_source / "example" / "example_rig.yaml").write_text("name: example\nsource_info: {}\n")

    from translator_ingest.util.storage import local as storage_local

    data_dir = tmp_path / "data"
    monkeypatch.setattr(storage_local, "INGESTS_DATA_PATH", data_dir)

    metadata = PipelineMetadata(source="example", source_version="1.0.0", transform_version="abc")
    transform_metadata = {
        "tablassert_version": "19.7.0", "tablassert_biolink_version": "4.4.6", "input_digest": "cd" * 32,
    }
    from translator_ingest.util.storage.local import write_ingest_file

    metadata_output_dir = data_dir / "example" / "1.0.0" / "transform_abc"
    metadata_output_dir.mkdir(parents=True)
    write_ingest_file(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=metadata, data=transform_metadata
    )

    cached = PipelineMetadata(source="example", source_version="1.0.0", transform_version="abc")
    tablassert.load_execution_identity(cached)
    assert cached.tablassert_version == "19.7.0"
    assert cached.tablassert_biolink_version == "4.4.6"
    assert cached.input_digest == "cd" * 32
    assert get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=metadata
    ).exists()
