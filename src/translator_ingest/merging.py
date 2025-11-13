import click
import json
import hashlib
import logging
from pathlib import Path

from orion.kgx_file_merger import KGXFileMerger
from orion.kgxmodel import GraphSpec, GraphSource
from orion.kgx_validation import validate_graph as generate_graph_summary
from orion.merging import DiskGraphMerger


from translator_ingest import INGESTS_DATA_PATH
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import get_versioned_file_paths, IngestFileType, IngestFileName

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def merge(graph_id: str, sources: list[str], overwrite: bool = False) -> str:
    """Use ORION to merge multiple sources together into a single KGX output."""
    logger.info(f"Merging {graph_id} with {sources}.")
    graph_spec_sources = []
    graph_source_versions = []
    for source in sources:
        latest_path = Path(INGESTS_DATA_PATH) / source / IngestFileName.LATEST_RELEASE_FILE
        if not latest_path.exists():
            raise IOError(f"Could not find latest release metadata for {source}")

        with latest_path.open() as latest_pipeline_metadata_file:
            latest_pipeline_metadata = json.load(latest_pipeline_metadata_file)
            pipeline_metadata = PipelineMetadata(**latest_pipeline_metadata)

        norm_node_path, norm_edge_path = get_versioned_file_paths(
            file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
        )
        graph_spec_sources.append(GraphSource(id=source, file_paths=[str(norm_node_path), str(norm_edge_path)]))
        graph_source_versions.append(pipeline_metadata.build_version)

    graph_version = hashlib.md5("".join(graph_source_versions).encode()).hexdigest()[:12]
    logger.info(f"Graph version determined: {graph_version}")
    graph_spec = GraphSpec(
        graph_id=graph_id,
        graph_name=graph_id,
        graph_description="",
        graph_url="",
        graph_version=graph_version,
        graph_output_format="jsonl",
        sources=graph_spec_sources,
        subgraphs=[],
    )
    output_dir = Path(INGESTS_DATA_PATH) / graph_id / graph_version
    nodes_output_file = output_dir / "nodes.jsonl"
    edges_output_file = output_dir / "edges.jsonl"
    if nodes_output_file.exists() and edges_output_file.exists():
        if not overwrite:
            logger.info(f"Graph {graph_id} ({graph_version}) already exists! Exiting...")
            return graph_version
        else:
            logger.info(f"Graph {graph_id} ({graph_version}) already exists! OVERWRITE mode enabled, overwriting...")

    output_dir.mkdir(parents=True, exist_ok=True)
    file_merger = KGXFileMerger(
        graph_spec=graph_spec,
        output_directory=str(output_dir),
        nodes_output_filename="nodes.jsonl",
        edges_output_filename="edges.jsonl",
        save_memory=True
    )
    file_merger.merge()

    merge_metadata = file_merger.get_merge_metadata()
    if "merge_error" in merge_metadata:
        logger.error(f"Merging error occurred: {merge_metadata['merge_error']}")
    else:
        metadata_output = output_dir / "merge-metadata.json"
        with open(metadata_output, "w") as metadata_file:
            metadata_file.write(json.dumps(merge_metadata, indent=4))

    return graph_version

def merge_graph_metadata(graph_id: str, graph_version: str, overwrite: bool = False):
    merged_graph_dir = Path(INGESTS_DATA_PATH) / graph_id / graph_version
    merged_graph_nodes = merged_graph_dir / "nodes.jsonl"
    merged_graph_edges = merged_graph_dir / "edges.jsonl"
    graph_metadata_file_path = merged_graph_dir / "graph-metadata.json"
    if graph_metadata_file_path.exists():
        if not overwrite:
            logger.error(f"Graph metadata file already exists: {graph_metadata_file_path}. Exiting...")
            return
        else:
            merged_graph_nodes.unlink()
            merged_graph_edges.unlink()
            logger.info(f"Graph metadata file already exists: {graph_metadata_file_path}. "
                        f"OVERWRITE mode enabled, overwriting...")

    # TODO need to get the equivalent of the RIG source_info for this graph
    graph_metadata = generate_graph_summary(
        nodes_file_path=str(merged_graph_nodes),
        edges_file_path=str(merged_graph_edges),
        graph_id=graph_id,
        graph_version=graph_version,
        logger=logger,
    )
    with graph_metadata_file_path.open("w") as output_file:
        output_file.write(json.dumps(graph_metadata, indent=2))



@click.command()
@click.argument("graph_id", required=True)
@click.argument("sources", nargs=-1, required=True)
@click.option("--overwrite", is_flag=True, help="Start fresh and overwrite previously generated files.")
def main(graph_id, sources, overwrite):
    graph_version = merge(graph_id, sources=list(sources), overwrite=overwrite)
    merge_graph_metadata(graph_id, graph_version, overwrite=overwrite)


if __name__ == "__main__":
    main()
