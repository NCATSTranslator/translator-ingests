import click
import json
import hashlib
import logging
import datetime
from pathlib import Path

from orion.kgx_file_merger import KGXFileMerger
from orion.kgxmodel import GraphSpec, GraphSource
from orion.kgx_metadata import KGXGraphMetadata, KGXSource, analyze_graph

from translator_ingest import INGESTS_DATA_PATH, INGESTS_STORAGE_URL
from translator_ingest.util.metadata import PipelineMetadata, get_kgx_source_from_rig
from translator_ingest.util.storage.local import get_versioned_file_paths, IngestFileType, IngestFileName

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def merge(graph_id: str, sources: list[str], overwrite: bool = False) -> tuple[str, list[KGXSource], str, str]:
    """Use ORION to merge multiple sources together into a single KGX output.

    Returns:
        Tuple of (graph_version, kgx_sources, biolink_version, babel_version)
    """
    logger.info(f"Merging {graph_id} with {sources}.")
    graph_spec_sources = []
    graph_source_versions = []
    kgx_sources = []
    biolink_versions = set()
    babel_versions = set()

    # Collect metadata from all sources and validate version consistency
    for source in sources:
        latest_path = Path(INGESTS_DATA_PATH) / source / IngestFileName.LATEST_RELEASE_FILE
        if not latest_path.exists():
            raise IOError(f"Could not find latest release metadata for {source}")

        with latest_path.open() as latest_pipeline_metadata_file:
            latest_pipeline_metadata = json.load(latest_pipeline_metadata_file)
            pipeline_metadata = PipelineMetadata(**latest_pipeline_metadata)

        # Validate that this source has all required version information
        if pipeline_metadata.biolink_version is None:
            logger.error(f"Source {source} has None for biolink_version")
            raise ValueError(f"Source {source} must have a valid biolink_version in release metadata.")

        if pipeline_metadata.node_norm_version is None:
            logger.error(f"Source {source} has None for node_norm_version")
            raise ValueError(f"Source {source} must have a valid node_norm_version in release metadata.")

        if pipeline_metadata.transform_version is None:
            logger.error(f"Source {source} has None for transform_version")
            raise ValueError(f"Source {source} must have a valid transform_version in release metadata.")

        # Collect versions for validation
        biolink_versions.add(pipeline_metadata.biolink_version)
        babel_versions.add(pipeline_metadata.node_norm_version)

        # Get KGXSource metadata from the rig file
        data_source_info = get_kgx_source_from_rig(source)
        kgx_sources.append(data_source_info)

        norm_node_path, norm_edge_path = get_versioned_file_paths(
            file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
        )
        graph_spec_sources.append(GraphSource(id=source, file_paths=[str(norm_node_path), str(norm_edge_path)]))
        graph_source_versions.append(pipeline_metadata.build_version)

    # Validate that all sources have the same biolink and babel versions
    if len(biolink_versions) > 1:
        logger.error(f"Biolink versions are not consistent across sources: {biolink_versions}")
        raise ValueError(f"All sources must have the same biolink version. Found: {biolink_versions}")

    if len(babel_versions) > 1:
        logger.error(f"Node normalization versions are not consistent across sources: {babel_versions}")
        raise ValueError(f"All sources must have the same node normalization version. Found: {babel_versions}")

    biolink_version = list(biolink_versions)[0]
    babel_version = list(babel_versions)[0]

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
            return graph_version, kgx_sources, biolink_version, babel_version
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

    return graph_version, kgx_sources, biolink_version, babel_version

def merge_graph_metadata(graph_id: str, graph_version: str, kgx_sources: list[KGXSource],
                         biolink_version: str, babel_version: str, overwrite: bool = False):
    """Generate graph metadata for a merged graph.

    Args:
        graph_id: The ID of the merged graph
        graph_version: The version of the merged graph
        kgx_sources: List of KGXSource metadata instances for each source in the merge
        biolink_version: The biolink model version used by all sources
        babel_version: The node normalization version used by all sources
        overwrite: Whether to overwrite existing metadata
    """
    logger.info(f"Generating graph metadata for {graph_id} ({graph_version})...")
    merged_graph_dir = Path(INGESTS_DATA_PATH) / graph_id / graph_version
    merged_graph_nodes = merged_graph_dir / "nodes.jsonl"
    merged_graph_edges = merged_graph_dir / "edges.jsonl"
    graph_metadata_file_path = merged_graph_dir / "graph-metadata.json"
    if graph_metadata_file_path.exists():
        if not overwrite:
            logger.info(f"Graph metadata file already exists: {graph_metadata_file_path}. Exiting...")
            return
        else:
            logger.info(f"Graph metadata file already exists: {graph_metadata_file_path}. "
                        f"OVERWRITE mode enabled, overwriting...")

    release_url = f"{INGESTS_STORAGE_URL}/{graph_id}/{graph_version}"
    source_metadata = KGXGraphMetadata(
        id=release_url,
        name=graph_id,
        description=f"A merged knowledge graph built for the NCATS Biomedical Data Translator project using "
                    f"Translator-Ingests, Biolink Model, and Node Normalizer.",
        license="MIT",
        url=release_url,
        version=graph_version,
        date_created=datetime.datetime.now().strftime("%Y_%m_%d"),
        biolink_version=biolink_version,
        babel_version=babel_version,
        kgx_sources=kgx_sources
    )

    graph_metadata = analyze_graph(
        nodes_file_path=str(merged_graph_nodes),
        edges_file_path=str(merged_graph_edges),
        graph_metadata=source_metadata
    )
    with graph_metadata_file_path.open("w") as output_file:
        output_file.write(json.dumps(graph_metadata, indent=2))
    logger.info(f"Graph metadata complete for {graph_id} ({graph_version}).")



@click.command()
@click.argument("graph_id", required=True)
@click.argument("sources", nargs=-1, required=True)
@click.option("--overwrite", is_flag=True, help="Start fresh and overwrite previously generated files.")
def main(graph_id, sources, overwrite):
    graph_version, kgx_sources, biolink_version, babel_version = merge(
        graph_id, sources=list(sources), overwrite=overwrite
    )
    merge_graph_metadata(
        graph_id, graph_version, kgx_sources, biolink_version, babel_version, overwrite=overwrite
    )


if __name__ == "__main__":
    main()
