"""Tests for the single-source merge stage of the ingest pipeline.

ORION's KGXFileMerger refuses to write over existing merged files and reports that as a merge
error instead of raising. The pipeline stage must therefore clear stale output before merging,
otherwise an OVERWRITE run keeps old merged files while normalization already changed.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from translator_ingest import pipeline
from translator_ingest.util import storage
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import IngestFileType, get_versioned_file_paths

BASE_METADATA: dict[str, Any] = dict(
    source="testsrc",
    source_version="v1",
    transform_version="tv1",
    babel_version="b1",
    node_normalizer_version="nn1",
    normalization_code_version="nc1",
    normalization_conflation=True,
    normalization_strict=True,
    merging_code_version="mc1",
)

NODES: list[dict[str, Any]] = [
    {"id": "MONDO:0005148", "name": "type 2 diabetes mellitus", "category": ["biolink:Disease"]},
    {"id": "CHEBI:6801", "name": "metformin", "category": ["biolink:ChemicalEntity"]},
    {"id": "CHEBI:4027", "name": "cyclosporin", "category": ["biolink:ChemicalEntity"]},
]

FIRST_EDGE: dict[str, Any] = {
    "subject": "MONDO:0005148",
    "predicate": "biolink:treated_by",
    "object": "CHEBI:6801",
    "primary_knowledge_source": "infores:some-source",
}
SECOND_EDGE: dict[str, Any] = {**FIRST_EDGE, "object": "CHEBI:4027"}


@pytest.fixture
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the storage module's data path at tmp_path so directory helpers resolve there."""
    data_path = tmp_path / "data"
    monkeypatch.setattr(storage.local, "INGESTS_DATA_PATH", data_path)
    return data_path


def write_normalized_kgx(pipeline_metadata: PipelineMetadata, edges: list[dict[str, Any]]) -> None:
    """Write the normalized nodes and edges the merge stage reads."""
    nodes_file, edges_file = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    nodes_file.parent.mkdir(parents=True, exist_ok=True)
    nodes_file.write_text("".join(f"{json.dumps(node)}\n" for node in NODES))
    edges_file.write_text("".join(f"{json.dumps(edge)}\n" for edge in edges))


def read_merged_edges(pipeline_metadata: PipelineMetadata) -> list[dict[str, Any]]:
    """Return the merged edges the stage wrote."""
    _, merged_edges_file = get_versioned_file_paths(
        file_type=IngestFileType.MERGED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    return [json.loads(line) for line in merged_edges_file.read_text().splitlines() if line]


def test_merge_writes_merged_files_and_metadata(data_root: Path) -> None:
    pipeline_metadata = PipelineMetadata(**BASE_METADATA)
    write_normalized_kgx(pipeline_metadata, [FIRST_EDGE])

    pipeline.merge(pipeline_metadata)

    merged_edges = read_merged_edges(pipeline_metadata)
    assert [edge["object"] for edge in merged_edges] == ["CHEBI:6801"]
    metadata_file = get_versioned_file_paths(
        file_type=IngestFileType.MERGE_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    assert "merge_error" not in json.loads(metadata_file.read_text())


def test_merge_replaces_stale_output_when_run_again(data_root: Path) -> None:
    """A second merge (an OVERWRITE run) must reflect the new normalized input, not the old output."""
    pipeline_metadata = PipelineMetadata(**BASE_METADATA)
    write_normalized_kgx(pipeline_metadata, [FIRST_EDGE])
    pipeline.merge(pipeline_metadata)

    write_normalized_kgx(pipeline_metadata, [SECOND_EDGE])
    pipeline.merge(pipeline_metadata)

    merged_edges = read_merged_edges(pipeline_metadata)
    assert [edge["object"] for edge in merged_edges] == ["CHEBI:4027"]
    metadata_file = get_versioned_file_paths(
        file_type=IngestFileType.MERGE_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    assert "merge_error" not in json.loads(metadata_file.read_text())
