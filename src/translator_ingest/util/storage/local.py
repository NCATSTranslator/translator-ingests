from enum import Enum, StrEnum
from pathlib import Path

from translator_ingest import PRIMARY_DATA_PATH
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
    META_KG_FILE = 10
    TEST_DATA_FILE = 11
    EXAMPLE_DATA_FILE = 12
    FINAL_METADATA_FILE = 13
    VALIDATION_REPORT_FILE = 14


class IngestFileName(StrEnum):
    TRANSFORM_NODES = "nodes.jsonl"
    TRANSFORM_EDGES = "edges.jsonl"
    TRANSFORM_METADATA = "transform_metadata.json"
    NORMALIZED_NODES = "normalized_nodes.jsonl"
    NORMALIZED_EDGES = "normalized_edges.jsonl"
    NORMALIZATION_METADATA = "normalization_metadata.json"
    NORMALIZATION_MAP = "normalization_map.json"
    NORMALIZATION_FAILURES = "normalization_failures.txt"
    PREDICATE_NORMALIZATION_MAP = "predicate_map.json"
    META_KG_FILENAME = "meta_knowledge_graph.json"
    TEST_DATA_FILENAME = "testing_data.json"
    EXAMPLE_DATA_FILENAME = "example_edges.jsonl"
    FINAL_METADATA_FILE = "final_metadata.json"
    VALIDATION_REPORT_FILE = "validation_report.json"


FILE_PATH_LOOKUP = {
    IngestFileType.TRANSFORM_KGX_FILES:
        lambda pipeline_metadata: __find_kgx_files(get_transform_directory(pipeline_metadata)),
    IngestFileType.TRANSFORM_METADATA_FILE:
        lambda pipeline_metadata: get_transform_directory(pipeline_metadata) / IngestFileName.TRANSFORM_METADATA,
    IngestFileType.NORMALIZED_KGX_FILES:
        lambda pipeline_metadata: (get_normalization_directory(pipeline_metadata) / IngestFileName.NORMALIZED_NODES,
                                   get_normalization_directory(pipeline_metadata) / IngestFileName.NORMALIZED_EDGES
    ),
    IngestFileType.NORMALIZATION_METADATA_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) /
                                  IngestFileName.NORMALIZATION_METADATA,
    IngestFileType.NORMALIZATION_MAP_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) /
                                  IngestFileName.NORMALIZATION_MAP,
    IngestFileType.NORMALIZATION_FAILURES_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) /
                                  IngestFileName.NORMALIZATION_FAILURES,
    IngestFileType.PREDICATE_NORMALIZATION_MAP_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) /
                                  IngestFileName.PREDICATE_NORMALIZATION_MAP,
    IngestFileType.META_KG_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) / IngestFileName.META_KG_FILENAME,
    IngestFileType.TEST_DATA_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) / IngestFileName.TEST_DATA_FILENAME,
    IngestFileType.EXAMPLE_DATA_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) / IngestFileName.EXAMPLE_DATA_FILENAME,
    IngestFileType.FINAL_METADATA_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) / IngestFileName.FINAL_METADATA_FILE,
    IngestFileType.VALIDATION_REPORT_FILE:
        lambda pipeline_metadata: get_normalization_directory(pipeline_metadata) / IngestFileName.VALIDATION_REPORT_FILE
}


def get_versioned_file_paths(file_type: IngestFileType,
                             pipeline_metadata: PipelineMetadata) -> tuple[Path, Path] | Path:
    return FILE_PATH_LOOKUP[file_type](pipeline_metadata)

def get_output_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return Path(PRIMARY_DATA_PATH) / pipeline_metadata.source / pipeline_metadata.source_version

def get_source_data_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_output_directory(pipeline_metadata) / "source_data"

def get_transform_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_output_directory(pipeline_metadata) / pipeline_metadata.transform_version

def get_normalization_directory(pipeline_metadata: PipelineMetadata) -> Path:
    return get_transform_directory(pipeline_metadata) / pipeline_metadata.normalization_version

# Find the KGX files in a given directory
def __find_kgx_files(directory: Path) -> (str, str):
    if not (directory and directory.exists()):
        return None, None
    nodes_file_path = None
    edges_file_path = None
    for child_path in directory.iterdir():
        if "nodes.jsonl" in child_path.name:
            if nodes_file_path is None:
                nodes_file_path = child_path
            else:
                raise IOError(f"Multiple nodes files were found in {directory}. "
                              f"This should not happen with normal ingest pipeline usage and is likely to cause bugs.")
        elif "edges.jsonl" in child_path.name:
            if edges_file_path is None:
                edges_file_path = child_path
            else:
                raise IOError(f"Multiple edges files were found in {directory}. "
                              f"This should not happen with normal ingest pipeline usage and is likely to cause bugs.")
    if not (nodes_file_path and edges_file_path):
        raise IOError(f"KGX files could not be found in {directory}")
    return nodes_file_path, edges_file_path
