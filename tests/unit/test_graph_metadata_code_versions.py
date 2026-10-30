"""Tests that a build records the code version each pipeline stage ran with.

Stage outputs are reused across pipeline runs, so a build's code versions must come from the metadata each stage
recorded when it ran, not from the code running the build.
"""

import json
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest
import yaml

from translator_ingest import pipeline
from translator_ingest.util import metadata, storage
from translator_ingest.util.code_version import CODE_VERSION, CODE_VERSION_KEY, CodeVersion
from translator_ingest.util.metadata import TRANSLATOR_CODE_VERSIONS, PipelineMetadata
from translator_ingest.util.storage.local import IngestFileType, get_versioned_file_paths, write_ingest_file

SOURCE = "testsrc"

BUILD_METADATA: dict[str, Any] = dict(
    source=SOURCE,
    source_version="v1",
    transform_version="tv1",
    babel_version="b1",
    node_normalizer_version="nn1",
    normalization_code_version="nc1",
    merging_code_version="mc1",
    biolink_version=version("biolink-model"),
)

NODES: list[dict[str, Any]] = [
    {"id": "MONDO:0005148", "name": "type 2 diabetes mellitus", "category": ["biolink:Disease"]},
    {"id": "CHEBI:6801", "name": "metformin", "category": ["biolink:ChemicalEntity"]},
]

EDGES: list[dict[str, Any]] = [{
    "subject": "MONDO:0005148",
    "predicate": "biolink:treated_by",
    "object": "CHEBI:6801",
    "primary_knowledge_source": "infores:some-source",
}]

# The code version of an earlier run, whose transform output the build reuses
EARLIER_CODE_VERSION = CodeVersion(commit="a" * 40, version="v0.9.0")


@pytest.fixture
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the storage module's data path at tmp_path so directory helpers resolve there."""
    data_path = tmp_path / "data"
    monkeypatch.setattr(storage.local, "INGESTS_DATA_PATH", data_path)
    return data_path


@pytest.fixture
def rig_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point rig lookups at tmp_path, holding a minimal rig for the test source."""
    parser_path = tmp_path / "ingests"
    rig_file = parser_path / SOURCE / f"{SOURCE}_rig.yaml"
    rig_file.parent.mkdir(parents=True)
    rig_file.write_text(yaml.safe_dump({"name": "Test Source", "source_info": {"description": "A test source."}}))
    monkeypatch.setattr(metadata, "INGESTS_PARSER_PATH", parser_path)
    return parser_path


@pytest.fixture
def merged_build(data_root: Path, rig_root: Path) -> PipelineMetadata:
    """A build whose merge just ran, reusing a transform from an earlier run."""
    pipeline_metadata = PipelineMetadata(**BUILD_METADATA)
    nodes_file, edges_file = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    nodes_file.parent.mkdir(parents=True)
    nodes_file.write_text("".join(f"{json.dumps(node)}\n" for node in NODES))
    edges_file.write_text("".join(f"{json.dumps(edge)}\n" for edge in EDGES))
    pipeline.merge(pipeline_metadata)
    write_ingest_file(file_type=IngestFileType.TRANSFORM_METADATA_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data={CODE_VERSION_KEY: EARLIER_CODE_VERSION})
    return pipeline_metadata


def read_graph_metadata(pipeline_metadata: PipelineMetadata) -> dict[str, Any]:
    """Return the graph metadata written for a build."""
    graph_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.GRAPH_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    return json.loads(graph_metadata_path.read_text())


@pytest.mark.parametrize("normalization_metadata", [
    None,  # the stage metadata file is missing
    {"normalization": "recorded before code versions"},  # the stage metadata predates code version recording
])
def test_graph_metadata_records_the_code_version_each_stage_ran_with(
        merged_build: PipelineMetadata, normalization_metadata: dict[str, Any] | None):
    if normalization_metadata is not None:
        write_ingest_file(file_type=IngestFileType.NORMALIZATION_METADATA_FILE,
                          pipeline_metadata=merged_build,
                          data=normalization_metadata)

    pipeline.generate_graph_metadata(merged_build)

    assert read_graph_metadata(merged_build)[TRANSLATOR_CODE_VERSIONS] == {
        "transform": EARLIER_CODE_VERSION,
        "normalization": None,
        "merge": CODE_VERSION,
        "build": CODE_VERSION,
    }


def test_recorded_code_versions_are_read_from_the_graph_metadata(merged_build: PipelineMetadata):
    pipeline.generate_graph_metadata(merged_build)

    recorded_code_versions = pipeline.read_recorded_code_versions(merged_build)

    assert recorded_code_versions == read_graph_metadata(merged_build)[TRANSLATOR_CODE_VERSIONS]
    assert recorded_code_versions["transform"] == EARLIER_CODE_VERSION


def test_recorded_code_versions_of_graph_metadata_that_predates_them_are_none(data_root: Path):
    pipeline_metadata = PipelineMetadata(**BUILD_METADATA)
    graph_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.GRAPH_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    graph_metadata_path.parent.mkdir(parents=True)
    write_ingest_file(file_type=IngestFileType.GRAPH_METADATA_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data={"name": SOURCE})

    assert pipeline.read_recorded_code_versions(pipeline_metadata) is None