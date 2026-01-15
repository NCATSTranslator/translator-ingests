import json
from enum import Enum, StrEnum
from pathlib import Path

from translator_ingest import INGESTS_DATA_PATH, INGESTS_RELEASES_PATH
from translator_ingest.util.metadata import PipelineMetadata


class IngestFileType(Enum):
    SOURCE_DATA_FILE = 1
    SOURCE_METADATA_FILE = 2
    TRANSFORM_KGX_FILES = 3
    TRANSFORM_METADATA_FILE = 4
    NORMALIZED_KGX_FILES = 5
    NORMALIZATION_METADATA_FILE = 6
    NORMALIZATION_MAP_FILE = 7
    NORMALIZATION_FAILURES_FILE = 8
    PREDICATE_NORMALIZATION_MAP_FILE = 9
    MERGED_KGX_FILES = 10
    MERGE_METADATA_FILE = 11
    TEST_DATA_FILE = 12
    EXAMPLE_EDGES_FILE = 13
    INGEST_METADATA_FILE = 14
    GRAPH_METADATA_FILE = 15
    VALIDATION_REPORT_FILE = 16
    LATEST_BUILD_FILE = 17
    LATEST_RELEASE_FILE = 18



class IngestFileName(StrEnum):
    TRANSFORM_METADATA = "transform-metadata.json"
    NORMALIZED_NODES = "normalized_nodes.jsonl"
    NORMALIZED_EDGES = "normalized_edges.jsonl"
    NORMALIZATION_METADATA = "normalization-metadata.json"
    NORMALIZATION_MAP = "normalization_map.json"
    NORMALIZATION_FAILURES = "normalization_failures.txt"
    PREDICATE_NORMALIZATION_MAP = "predicate_map.json"
    MERGED_NODES = "merged_nodes.jsonl"
    MERGED_EDGES = "merged_edges.jsonl"
    MERGE_METADATA_FILE = "merge_metadata.json"
    TEST_DATA_FILENAME = "testing_data.json"
    EXAMPLE_EDGES_FILENAME = "example_edges.jsonl"
    INGEST_METADATA_FILE = "ingest-metadata.json"
    GRAPH_METADATA_FILE = "graph-metadata.json"
    VALIDATION_REPORT_FILE = "validation-report.json"
    LATEST_BUILD_FILE = "latest-build.json"
    LATEST_RELEASE_FILE = "latest-release.json"


FILE_PATH_LOOKUP = {
    IngestFileType.TRANSFORM_KGX_FILES: lambda pipeline_metadata: __find_transform_kgx_files(
        get_transform_directory(pipeline_metadata)
    ),
    IngestFileType.TRANSFORM_METADATA_FILE: lambda pipeline_metadata: get_transform_directory(pipeline_metadata)
    / IngestFileName.TRANSFORM_METADATA,
    IngestFileType.NORMALIZED_KGX_FILES: lambda pipeline_metadata: (
        get_normalization_directory(pipeline_metadata) / IngestFileName.NORMALIZED_NODES,
        get_normalization_directory(pipeline_metadata) / IngestFileName.NORMALIZED_EDGES,
    ),
    IngestFileType.NORMALIZATION_METADATA_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.NORMALIZATION_METADATA,
    IngestFileType.NORMALIZATION_MAP_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.NORMALIZATION_MAP,
    IngestFileType.NORMALIZATION_FAILURES_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.NORMALIZATION_FAILURES,
    IngestFileType.PREDICATE_NORMALIZATION_MAP_FILE: lambda pipeline_metadata: get_normalization_directory(
        pipeline_metadata
    )
    / IngestFileName.PREDICATE_NORMALIZATION_MAP,
    IngestFileType.MERGED_KGX_FILES: lambda pipeline_metadata: (
        get_normalization_directory(pipeline_metadata) / IngestFileName.MERGED_NODES,
        get_normalization_directory(pipeline_metadata) / IngestFileName.MERGED_EDGES,
    ),
    IngestFileType.MERGE_METADATA_FILE: lambda pipeline_metadata:
        get_normalization_directory(pipeline_metadata) / IngestFileName.MERGE_METADATA_FILE,
    IngestFileType.TEST_DATA_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.TEST_DATA_FILENAME,
    IngestFileType.EXAMPLE_EDGES_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.EXAMPLE_EDGES_FILENAME,
    IngestFileType.INGEST_METADATA_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.INGEST_METADATA_FILE,
    IngestFileType.GRAPH_METADATA_FILE: lambda pipeline_metadata: get_normalization_directory(pipeline_metadata)
    / IngestFileName.GRAPH_METADATA_FILE,
    IngestFileType.VALIDATION_REPORT_FILE: lambda pipeline_metadata: get_validation_directory(pipeline_metadata)
    / IngestFileName.VALIDATION_REPORT_FILE,
    IngestFileType.LATEST_BUILD_FILE: lambda pipeline_metadata: Path(INGESTS_DATA_PATH) / pipeline_metadata.source
    / IngestFileName.LATEST_BUILD_FILE,
    IngestFileType.LATEST_RELEASE_FILE: lambda pipeline_metadata: Path(INGESTS_RELEASES_PATH) / pipeline_metadata.source
                                                                  / IngestFileName.LATEST_RELEASE_FILE,
}

def get_versioned_file_paths(
    file_type: IngestFileType, pipeline_metadata: PipelineMetadata
) -> tuple[Path, Path] | Path:
    return FILE_PATH_LOOKUP[file_type](pipeline_metadata)

def get_output_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return Path(INGESTS_DATA_PATH) / pipeline_metadata.source / pipeline_metadata.source_version

def get_source_data_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_output_directory(pipeline_metadata) / "source_data"

def get_transform_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_output_directory(pipeline_metadata) / pipeline_metadata.transform_version

def get_normalization_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_transform_directory(pipeline_metadata) / f"normalization_{pipeline_metadata.node_norm_version}"

def get_validation_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_normalization_directory(pipeline_metadata) / f"validation_{pipeline_metadata.biolink_version}"

# Find the KGX files in a given directory
def __find_transform_kgx_files(directory: Path) -> (str, str):
    if not (directory and directory.exists()):
        return None, None
    nodes_file_path = None
    edges_file_path = None
    for child_path in directory.iterdir():
        if "nodes.jsonl" in child_path.name:
            if nodes_file_path is None:
                nodes_file_path = child_path
            else:
                raise IOError(
                    f"Multiple nodes files were found in {directory}. "
                    "This should not happen with normal ingest pipeline usage and is likely to cause bugs."
                )
        elif "edges.jsonl" in child_path.name:
            if edges_file_path is None:
                edges_file_path = child_path
            else:
                raise IOError(
                    f"Multiple edges files were found in {directory}. "
                    "This should not happen with normal ingest pipeline usage and is likely to cause bugs."
                )
    return nodes_file_path, edges_file_path

def write_ingest_file(file_type: IngestFileType,
                      pipeline_metadata: PipelineMetadata,
                      data) -> None:
    output_file_path = get_versioned_file_paths(
        file_type=file_type, pipeline_metadata=pipeline_metadata
    )
    with output_file_path.open("w") as output_file:
        output_file.write(json.dumps(data, indent=2))
