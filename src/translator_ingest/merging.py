import click
import json
import hashlib
import datetime
from pathlib import Path

from orion.kgx_file_merger import KGXFileMerger, DONT_MERGE
from orion.kgxmodel import GraphSpec, SubGraphSource
from orion.kgx_metadata import KGXGraphMetadata, KGXSource, analyze_graph

from translator_ingest import INGESTS_DATA_PATH, INGESTS_RELEASES_PATH, INGESTS_STORAGE_URL
from translator_ingest.release import create_compressed_tar
from translator_ingest.util.metadata import PipelineMetadata, get_kgx_source_from_rig
from translator_ingest.util.storage.local import get_versioned_file_paths, IngestFileType, IngestFileName, \
    write_ingest_file
from translator_ingest.util.logging_utils import get_logger, setup_logging

logger = get_logger(__name__)


def merge_single(
    source_id: str,
    input_nodes_file: Path,
    input_edges_file: Path,
    output_nodes_file: Path,
    output_edges_file: Path,
    output_metadata_file: Path,
    source_version: str = None
) -> dict:
    """Merge KGX files using ORION's KGXFileMerger.

    This is the low-level merge function that handles a single set of KGX files.
    It deduplicates nodes and edges, outputting merged files and merge metadata.

    Args:
        source_id: Identifier for the source being merged
        input_nodes_file: Path to input nodes JSONL file
        input_edges_file: Path to input edges JSONL file
        output_nodes_file: Path for output merged nodes file
        output_edges_file: Path for output merged edges file
        output_metadata_file: Path for output merge metadata JSON file
        source_version: Optional version string for the source
        overwrite: Whether to overwrite existing output files

    Returns:
        dict: Merge metadata from KGXFileMerger
    """
    output_dir = output_nodes_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    graph_spec = GraphSpec(
        graph_id=source_id,
        graph_name=source_id,
        graph_description="",
        graph_url="",
        graph_version=source_version or "",
        graph_output_format="jsonl",
        sources=[
            SubGraphSource(
                id=source_id,
                file_paths=[str(input_nodes_file), str(input_edges_file)],
                graph_version=source_version
            )
        ],
        subgraphs=[],
    )

    logger.info(f"Running KGXFileMerger for {source_id}...")
    file_merger = KGXFileMerger(
        graph_spec=graph_spec,
        output_directory=str(output_dir),
        nodes_output_filename=output_nodes_file.name,
        edges_output_filename=output_edges_file.name,
        save_memory=True
    )
    file_merger.merge()

    merge_metadata = file_merger.get_merge_metadata()
    if "merge_error" in merge_metadata:
        logger.error(f"Merging error occurred for {source_id}: {merge_metadata['merge_error']}")
    else:
        with open(output_metadata_file, "w") as metadata_file:
            json.dump(merge_metadata, metadata_file, indent=4)
        logger.info(f"Merge metadata written to {output_metadata_file}")

    return merge_metadata


def is_merged_graph_release_current(merged_graph_metadata: PipelineMetadata) -> bool:
    """Check if a merged graph release is already current by comparing build versions.

    Unlike individual source releases which use get_versioned_file_paths and write to INGESTS_DATA_PATH,
    merged graphs write directly to INGESTS_RELEASES_PATH.
    """
    release_metadata_path = get_versioned_file_paths(IngestFileType.LATEST_RELEASE_FILE, merged_graph_metadata)
    if not release_metadata_path.exists():
        return False
    with release_metadata_path.open("r") as latest_release_file:
        latest_release_metadata = PipelineMetadata(**json.load(latest_release_file))
    return merged_graph_metadata.build_version == latest_release_metadata.build_version


def create_merged_graph_compressed_tar(merged_graph_metadata: PipelineMetadata):
    """Create a tar.xz compressed archive of the merged graph KGX files and metadata.

    Unlike individual sources which use get_versioned_file_paths, merged graphs
    are already in INGESTS_RELEASES_PATH, so we compress from there directly.
    After compression, the original nodes.jsonl and edges.jsonl files are removed.
    """
    graph_id = merged_graph_metadata.source
    release_version = merged_graph_metadata.release_version
    release_version_dir = Path(INGESTS_RELEASES_PATH) / graph_id / release_version

    tar_filename = f"{graph_id}.tar.zst"
    tar_path = release_version_dir / tar_filename

    if tar_path.exists():
        logger.info(f"Compressed archive already exists: {tar_path}")
        return

    logger.info(f"Creating compressed archive {tar_filename}...")
    nodes_file = release_version_dir / "nodes.jsonl"
    edges_file = release_version_dir / "edges.jsonl"
    metadata_file = release_version_dir / "graph-metadata.json"

    create_compressed_tar(nodes_file=nodes_file,
                          edges_file=edges_file,
                          graph_metadata_path=metadata_file,
                          output_path=tar_path)

    # Clean up the original files
    if nodes_file.exists():
        nodes_file.unlink()
    if edges_file.exists():
        edges_file.unlink()
    logger.info(f"Compressed archive created: {tar_path}")


def generate_merged_graph_release(merged_graph_metadata: PipelineMetadata):
    """Generate release metadata and compressed archive for a merged graph."""
    logger.info(f"Generating release for merged graph {merged_graph_metadata.source}... "
                f"release: {merged_graph_metadata.release_version}")

    # Create compressed tar.xz archive
    create_merged_graph_compressed_tar(merged_graph_metadata)

    # Write latest release metadata
    release_dir = Path(INGESTS_RELEASES_PATH) / merged_graph_metadata.source
    release_dir.mkdir(parents=True, exist_ok=True)
    write_ingest_file(IngestFileType.LATEST_RELEASE_FILE,
                      pipeline_metadata=merged_graph_metadata,
                      data=merged_graph_metadata.get_release_metadata())
    logger.info(f"Release generated for merged graph {merged_graph_metadata.source}... ")


def merge(graph_id: str, sources: list[str], overwrite: bool = False) -> tuple[PipelineMetadata, list[KGXSource]]:
    """Use ORION to merge multiple sources together into a single KGX output.

    Returns:
        Tuple of (merged_graph_metadata, kgx_sources)
    """
    logger.info(f"Merging {graph_id}. Sources: {sources}.")
    graph_spec_sources = []
    graph_source_versions = []
    kgx_sources = []
    biolink_versions = set()
    babel_versions = set()

    # Collect metadata from all sources and validate version consistency
    for source in sources:
        latest_path = Path(INGESTS_DATA_PATH) / source / IngestFileName.LATEST_BUILD_FILE
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
        data_source_info.version = pipeline_metadata.source_version
        kgx_sources.append(data_source_info)

        node_path, edge_path = get_versioned_file_paths(
            file_type=IngestFileType.MERGED_KGX_FILES, pipeline_metadata=pipeline_metadata
        )
        # ORION expects str not Path
        files_to_merge = [str(node_path)]
        # handle node-only ingests
        if edge_path.exists():
            files_to_merge.append(str(edge_path))
        # NOTE: merge_strategy=DONT_MERGE really means don't merge edges, nodes are always merged.
        # We already merged edges for every ingest and don't have overlapping
        # primary-knowledge-sources, so we don't need to merge edges here.
        graph_spec_sources.append(SubGraphSource(id=source,
                                                 file_paths=files_to_merge,
                                                 graph_version=pipeline_metadata.source_version,
                                                 merge_strategy=DONT_MERGE))
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

    # Generate a build version based on the build versions of all source graphs
    build_version = hashlib.md5("".join(sorted(graph_source_versions)).encode()).hexdigest()[:12]
    release_version = datetime.datetime.now().strftime("%Y_%m_%d")
    data_path = f"{INGESTS_STORAGE_URL}/{graph_id}/{release_version}/"

    # Create PipelineMetadata for the merged graph
    merged_graph_metadata = PipelineMetadata(
        source=graph_id,
        source_version=None,  # Merged graphs don't have a single source version
        transform_version=None,  # or transform version
        node_norm_version=babel_version,
        biolink_version=biolink_version,
        build_version=build_version,
        release_version=release_version,
        data=data_path
    )

    # Check if the latest release already has this build version
    if is_merged_graph_release_current(merged_graph_metadata) and not overwrite:
        logger.info(f"Graph {graph_id} latest release is already current (build: {build_version}). Skipping merge.")
        return merged_graph_metadata, kgx_sources

    logger.info(f"Graph {graph_id} versioned, release version: {release_version}, build version: {build_version}")
    graph_spec = GraphSpec(
        graph_id=graph_id,
        graph_name=graph_id,
        graph_description="",
        graph_url="",
        graph_version=release_version,
        graph_output_format="jsonl",
        sources=graph_spec_sources,
        subgraphs=[],
    )
    output_dir = Path(INGESTS_RELEASES_PATH) / graph_id / release_version
    nodes_output_file = output_dir / "nodes.jsonl"
    edges_output_file = output_dir / "edges.jsonl"
    if not overwrite and (nodes_output_file.exists() and edges_output_file.exists()):
        logger.info(f"Graph {graph_id} ({build_version}) already exists..")
    else:
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

    # Generate graph metadata after successful merge
    merge_graph_metadata(pipeline_metadata=merged_graph_metadata, kgx_sources=kgx_sources, overwrite=overwrite)

    return merged_graph_metadata, kgx_sources

def merge_graph_metadata(pipeline_metadata: PipelineMetadata, kgx_sources: list[KGXSource], overwrite: bool = False):
    """Generate graph metadata for a merged graph.

    Args:
        pipeline_metadata: PipelineMetadata instance for the merged graph
        kgx_sources: List of KGXSource metadata instances for each source in the merge
        overwrite: Whether to overwrite existing metadata
    """
    graph_id = pipeline_metadata.source
    release_version = pipeline_metadata.release_version
    biolink_version = pipeline_metadata.biolink_version
    babel_version = pipeline_metadata.node_norm_version

    logger.info(f"Generating graph metadata for {graph_id} ({release_version})...")
    merged_graph_dir = Path(INGESTS_RELEASES_PATH) / graph_id / release_version
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

    release_url = f"{INGESTS_STORAGE_URL}/{graph_id}/{release_version}"
    source_metadata = KGXGraphMetadata(
        id=release_url,
        name=graph_id,
        description="A merged knowledge graph built for the NCATS Biomedical Data Translator project using "
                    "Translator-Ingests, Biolink Model, and Node Normalizer.",
        license="MIT",
        url=release_url,
        version=release_version,
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
    logger.info(f"Graph metadata complete for {graph_id} ({release_version}).")



@click.command()
@click.argument("graph_id", required=True)
@click.argument("sources", nargs=-1, required=True)
@click.option("--overwrite", is_flag=True, help="Start fresh and overwrite previously generated files.")
def main(graph_id, sources, overwrite):
    setup_logging()

    # Merge the sources into one KGX and generate metadata
    merged_graph_metadata, kgx_sources = merge(
        graph_id, sources=list(sources), overwrite=overwrite
    )

    # Generate latest release metadata for the merged graph
    if is_merged_graph_release_current(merged_graph_metadata) and not overwrite:
        logger.info(f"Latest release already up to date for {graph_id}, build: {merged_graph_metadata.build_version}")
    else:
        generate_merged_graph_release(merged_graph_metadata)


if __name__ == "__main__":
    main()
