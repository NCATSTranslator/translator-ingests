import json
import logging

from pathlib import Path

from orion.kgx_file_normalizer import KGXFileNormalizer
from orion.normalization import NodeNormalizer, NormalizationScheme

from translator_ingest.util.metadata import PipelineMetadata

logger = logging.getLogger(__name__)

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
    
    # Only add edge-related parameters if not nodes-only
    if max_edge_count != 0:
        normalizer_kwargs.update({
            "source_edges_file_path": str(input_edges_file_path),
            "edges_output_file_path": str(edges_output_file_path),
            "edge_norm_predicate_map_file_path": str(predicate_map_file_path),
            "preserve_unconnected_nodes": False,
        })
    else:
        # For nodes-only, preserve all nodes
        normalizer_kwargs["preserve_unconnected_nodes"] = True
        logger.info("Running nodes-only normalization (max_edge_count = 0)")
    
    file_normalizer = KGXFileNormalizer(**normalizer_kwargs)
    normalization_metadata = file_normalizer.normalize_kgx_files()
    with Path(normalization_metadata_file_path).open("w") as normalization_metadata_file:
        normalization_metadata_file.write(json.dumps(normalization_metadata, indent=4))


# TODO redo cli for this module?
