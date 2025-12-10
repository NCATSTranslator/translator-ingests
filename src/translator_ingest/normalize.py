import json
import tempfile

from pathlib import Path

from orion.kgx_file_normalizer import KGXFileNormalizer
from orion.normalization import NodeNormalizer, NormalizationScheme

from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

CURRENT_NODE_NORM_VERSION = None


def get_current_node_norm_version():
    global CURRENT_NODE_NORM_VERSION
    if CURRENT_NODE_NORM_VERSION is None:
        CURRENT_NODE_NORM_VERSION = NodeNormalizer().get_current_node_norm_version()
    return CURRENT_NODE_NORM_VERSION


def normalize_kgx_files(
    input_nodes_file_path: str,
    input_edges_file_path: str,
    nodes_output_file_path: str,
    node_norm_map_file_path: str,
    node_norm_failures_file_path: str,
    edges_output_file_path: str,
    predicate_map_file_path: str,
    normalization_metadata_file_path: str,
    pipeline_metadata: PipelineMetadata = None,
):
    normalization_scheme = NormalizationScheme(conflation=True)

    # Get max_edge_count from pipeline metadata if available
    max_edge_count = None
    if pipeline_metadata:
        max_edge_count = pipeline_metadata.koza_config.get('max_edge_count')

    # Build kwargs for KGXFileNormalizer based on whether this is nodes-only
    normalizer_kwargs = {
        "source_nodes_file_path": str(input_nodes_file_path),
        "nodes_output_file_path": str(nodes_output_file_path),
        "node_norm_map_file_path": str(node_norm_map_file_path),
        "node_norm_failures_file_path": str(node_norm_failures_file_path),
        "normalization_scheme": normalization_scheme,
        "has_sequence_variants": False,
        "process_in_memory": True,
    }

    # For nodes-only ingests, create temporary empty edges file
    if max_edge_count == 0:
        logger.info("Running nodes-only normalization (max_edge_count = 0)")
        temp_dir = tempfile.mkdtemp()
        temp_edges_file = Path(temp_dir) / "empty_edges.jsonl"
        temp_edges_file.touch()
        input_edges_file_path = str(temp_edges_file)

    # Add edge-related parameters
    normalizer_kwargs.update({
        "source_edges_file_path": str(input_edges_file_path),
        "edges_output_file_path": str(edges_output_file_path),
        "edge_norm_predicate_map_file_path": str(predicate_map_file_path),
        "preserve_unconnected_nodes": max_edge_count == 0,  # True for nodes-only
    })

    file_normalizer = KGXFileNormalizer(**normalizer_kwargs)
    normalization_metadata = file_normalizer.normalize_kgx_files()

    # Clean up temp file if created
    if max_edge_count == 0:
        Path(input_edges_file_path).unlink(missing_ok=True)
        # Create empty output edges file for consistency
        Path(edges_output_file_path).touch()

    with Path(normalization_metadata_file_path).open("w") as normalization_metadata_file:
        normalization_metadata_file.write(json.dumps(normalization_metadata, indent=4))


# TODO redo cli for this module?
