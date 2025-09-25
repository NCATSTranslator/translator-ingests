import json
import logging
import sys
import click
from pathlib import Path

from orion import KGXFileNormalizer
from orion.normalization import NodeNormalizer, NormalizationScheme

logger = logging.getLogger(__name__)

def normalize_kgx_files(output_dir: str,
                        input_nodes_file_path: str = None,
                        input_edges_file_path: str = None):

    # if no file paths provided, assume the file names
    if not input_nodes_file_path:
        input_nodes_file_path = Path(output_dir) / "nodes.jsonl"
    if not input_edges_file_path:
        input_edges_file_path = Path(output_dir) / "edges.jsonl"

    # get the current version of the Node Normalizer and make a versioned directory
    current_node_norm_version = NodeNormalizer().get_current_node_norm_version()
    versioned_output_dir = Path(output_dir) / current_node_norm_version
    versioned_output_dir.mkdir(exist_ok=True)

    nodes_output_file_path = versioned_output_dir / "normalized_nodes.jsonl"
    node_norm_map_file_path = versioned_output_dir / "node_normalization_map.json"
    node_norm_failures_file_path = versioned_output_dir / "node_normalization_failures.txt"
    edges_output_file_path = versioned_output_dir / "normalized_edges.jsonl"
    edge_norm_predicate_map_file_path = versioned_output_dir / "edge_predicate_map.json"

    if edges_output_file_path.exists():
        logger.info("Normalization output files already exist. Skipping normalization.")
        return

    normalization_scheme = NormalizationScheme(conflation=True)
    file_normalizer = KGXFileNormalizer(source_nodes_file_path=str(input_nodes_file_path),
                                        nodes_output_file_path=str(nodes_output_file_path),
                                        node_norm_map_file_path=str(node_norm_map_file_path),
                                        node_norm_failures_file_path=str(node_norm_failures_file_path),
                                        source_edges_file_path=str(input_edges_file_path),
                                        edges_output_file_path=str(edges_output_file_path),
                                        edge_norm_predicate_map_file_path=str(edge_norm_predicate_map_file_path),
                                        normalization_scheme=normalization_scheme,
                                        has_sequence_variants=False,
                                        process_in_memory=True,
                                        preserve_unconnected_nodes=False)
    normalization_metadata = file_normalizer.normalize_kgx_files()

    normalization_metadata_file_path = versioned_output_dir / "normalization_metadata.json"
    with normalization_metadata_file_path.open("w") as normalization_metadata_file:
        normalization_metadata_file.write(json.dumps(normalization_metadata, indent=4))


@click.command(help="Given a directory with KGX files, normalize them and produce normalization maps and metadata.")
@click.argument(
    "output-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path)
)
@click.option(
    "--files",
    nargs=2,
    metavar="NODES_FILE EDGES_FILE",
    help="Specific nodes and edges files to normalize. "
         "Provide these if nodes and edges files are not inside of the output-dir, "
         "or not named nodes.jsonl and edges.jsonl"
)
def main(output_dir, files):
    """Normalize KGX files and produce normalization maps and metadata."""

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # If files were provided, make sure they exist
    if files:
        nodes_file, edges_file = Path(files[0]), Path(files[1])
        if not nodes_file.exists():
            logger.error(f"Nodes file not found: {nodes_file}")
            sys.exit(1)
        if not edges_file.exists():
            logger.error(f"Edges file not found: {edges_file}")
            sys.exit(1)
    else:
        # otherwise, look in the output dir and try to find a nodes file and an edges file
        # TODO - this is NOT how we should do this, the pipeline should provide specific file paths, or they should
        #  always have consistent names, but with source kgx files named arbitrary things (designated in source yaml)
        #  this hacky approach works as long as only one node and one edges file exist in the output dir
        nodes_file = None
        edges_file = None
        for child_path in Path(output_dir).iterdir():
            if "nodes" in child_path.name:
                nodes_file = child_path
            if "edges" in child_path.name:
                edges_file = child_path
        if not nodes_file and edges_file:
            logger.error(f"Nodes and edges files not found in output_dir: {output_dir}")
            sys.exit(1)

    try:
        normalize_kgx_files(output_dir=output_dir,
                            input_nodes_file_path=nodes_file,
                            input_edges_file_path=edges_file)
        sys.exit(0)
    except Exception as e:
        logger.error(str(e))
        # raise e
        sys.exit(1)


if __name__ == "__main__":
    main()