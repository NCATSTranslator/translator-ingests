import json
import logging

from pathlib import Path

from orion import KGXFileNormalizer
from orion.normalization import NodeNormalizer, NormalizationScheme

logger = logging.getLogger(__name__)

CURRENT_NODE_NORM_VERSION = None

def get_current_node_norm_version():
    global CURRENT_NODE_NORM_VERSION
    if CURRENT_NODE_NORM_VERSION is None:
        CURRENT_NODE_NORM_VERSION = NodeNormalizer().get_current_node_norm_version()
    return CURRENT_NODE_NORM_VERSION

def normalize_kgx_files(input_nodes_file_path: str,
                        input_edges_file_path: str,
                        nodes_output_file_path: str,
                        node_norm_map_file_path: str,
                        node_norm_failures_file_path: str,
                        edges_output_file_path: str,
                        predicate_map_file_path: str,
                        normalization_metadata_file_path: str):

    normalization_scheme = NormalizationScheme(conflation=True)
    file_normalizer = KGXFileNormalizer(source_nodes_file_path=str(input_nodes_file_path),
                                        nodes_output_file_path=str(nodes_output_file_path),
                                        node_norm_map_file_path=str(node_norm_map_file_path),
                                        node_norm_failures_file_path=str(node_norm_failures_file_path),
                                        source_edges_file_path=str(input_edges_file_path),
                                        edges_output_file_path=str(edges_output_file_path),
                                        edge_norm_predicate_map_file_path=str(predicate_map_file_path),
                                        normalization_scheme=normalization_scheme,
                                        has_sequence_variants=False,
                                        process_in_memory=True,
                                        preserve_unconnected_nodes=False)
    normalization_metadata = file_normalizer.normalize_kgx_files()
    with Path(normalization_metadata_file_path).open("w") as normalization_metadata_file:
        normalization_metadata_file.write(json.dumps(normalization_metadata, indent=4))


# TODO redo cli for this module?