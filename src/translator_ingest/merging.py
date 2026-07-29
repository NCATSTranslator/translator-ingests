import click
import json
import hashlib
import tempfile
from pathlib import Path

from orion import KGXFileMerger, KGXGraphMetadata, KGXKnowledgeSource, generate_schema, GraphSpec, SubGraphSource

from translator_ingest import INGESTS_RELEASES_PATH, INGESTS_RELEASES_URL
from translator_ingest.release import create_compressed_tar, extract_compressed_tar, atomic_copy_directory, \
    RELEASE_NODES_FILENAME, RELEASE_EDGES_FILENAME, RELEASE_GRAPH_METADATA_FILENAME
from translator_ingest.util.metadata import PipelineMetadata, get_kgx_source_from_rig, next_release_version, \
    current_iso_date
from translator_ingest.util.storage.local import get_versioned_file_paths, IngestFileType, write_ingest_file
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
    """Merge KGX files using ORION's KGXFileMerger. Note that merge_single is used in a different way than most of the
    rest of the functionality in this file.

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
        add_edge_id=True,
        edge_id_type="uuid",
        overwrite_edge_ids=False,
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


"""
!!! Note: The rest of this module contains functions for building a KG from multiple individual ingests. It does not
!!! follow all of the same patterns as the individual ingest pipeline, for example it generates its own releases and 
!!! has its own asset dependency management functionality.
"""

def is_merged_graph_release_current(merged_graph_metadata: PipelineMetadata) -> bool:
    """Check if a merged graph release is already current by comparing build versions.

    Unlike individual source releases which use get_versioned_file_paths and write to INGESTS_DATA_PATH,
    merged graphs write directly to INGESTS_RELEASES_PATH.
    """
    release_metadata_path = get_versioned_file_paths(IngestFileType.LATEST_RELEASE_FILE, merged_graph_metadata)
    if not release_metadata_path.exists():
        return False
    with release_metadata_path.open("r") as latest_release_file:
        latest_release_metadata = PipelineMetadata.from_dict(json.load(latest_release_file))
    return merged_graph_metadata.build_version == latest_release_metadata.build_version


def create_merged_graph_compressed_tar(merged_graph_metadata: PipelineMetadata):
    """Create a tar.zst compressed archive of the merged graph KGX files and metadata.

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
    nodes_file = release_version_dir / RELEASE_NODES_FILENAME
    edges_file = release_version_dir / RELEASE_EDGES_FILENAME
    metadata_file = release_version_dir / RELEASE_GRAPH_METADATA_FILENAME

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

    # Create compressed tar.zst archive
    create_merged_graph_compressed_tar(merged_graph_metadata)

    # Copy release to "latest" directory
    release_dir = Path(INGESTS_RELEASES_PATH) / merged_graph_metadata.source / merged_graph_metadata.release_version
    latest_dir = Path(INGESTS_RELEASES_PATH) / merged_graph_metadata.source / "latest"
    atomic_copy_directory(release_dir, latest_dir)

    # Write latest release metadata, stamping the date the release was made
    merged_graph_metadata.release_date = current_iso_date()
    release_dir = Path(INGESTS_RELEASES_PATH) / merged_graph_metadata.source
    release_dir.mkdir(parents=True, exist_ok=True)
    write_ingest_file(IngestFileType.LATEST_RELEASE_FILE,
                      pipeline_metadata=merged_graph_metadata,
                      data=merged_graph_metadata.get_release_metadata())
    logger.info(f"Release generated for merged graph {merged_graph_metadata.source}... ")


# Versions a source release must have for it to be merged into a multi-source graph.
REQUIRED_SOURCE_RELEASE_METADATA = ("release_version",
                                    "build_version",
                                    "biolink_version",
                                    "babel_version",
                                    "data")


def _read_source_release_metadata(source: str) -> PipelineMetadata:
    """Read the latest release metadata for one of the sources of a merged graph.

    Merged graphs are built from released sources, so a source that has no release, or one with incomplete release
    metadata, is an error - release the source before merging it.

    Args:
        source: id of the source to read release metadata for

    Returns:
        The source's latest release PipelineMetadata.
    """
    latest_release_path = get_versioned_file_paths(IngestFileType.LATEST_RELEASE_FILE, PipelineMetadata(source=source))
    if not latest_release_path.exists():
        raise IOError(f"Could not find latest release metadata for {source} ({latest_release_path}). "
                      f"Create a release for {source} before attempting to merge it.")

    with latest_release_path.open() as latest_release_file:
        release_metadata = PipelineMetadata.from_dict(json.load(latest_release_file))

    missing_versions = [version for version in REQUIRED_SOURCE_RELEASE_METADATA
                        if getattr(release_metadata, version) is None]
    if missing_versions:
        logger.error(f"Source {source} release metadata is missing {missing_versions}")
        raise ValueError(f"Source {source} release metadata must have a valid "
                         f"{', '.join(missing_versions)}.")
    return release_metadata


def _get_shared_version(source_releases: dict[str, PipelineMetadata], version_field: str) -> str:
    """Return the value of version_field shared by every source, raising if the sources disagree.

    A merged graph is only coherent if all of its sources were built with the same Biolink Model and Babel versions.

    Args:
        source_releases: release metadata for each source of a merged graph, keyed by source id
        version_field: name of the PipelineMetadata version attribute that must be shared
    """
    versions = {getattr(release_metadata, version_field) for release_metadata in source_releases.values()}
    if len(versions) > 1:
        logger.error(f"Sources do not have consistent {version_field}s: {versions}")
        raise ValueError(f"All sources must have the same {version_field}. Found: {versions}")
    return versions.pop()


def _extract_release_kgx_files(source: str, release_metadata: PipelineMetadata, staging_directory: Path) -> list[str]:
    """Extract the KGX files from a source's release archive, returning the paths for merging.

    Args:
        source: id of the source whose release archive should be extracted
        release_metadata: the source's latest release metadata, identifying which release to extract
        staging_directory: directory to extract the source's release into

    Returns:
        Paths of the extracted KGX files, nodes first. Nodes-only sources have no edges file in their release.
    """
    release_archive = Path(INGESTS_RELEASES_PATH) / source / release_metadata.release_version / f"{source}.tar.zst"
    if not release_archive.exists():
        raise IOError(f"Could not find the release archive for {source} ({release_archive})")

    logger.info(f"Extracting the {release_metadata.release_version} release of {source}...")
    extraction_directory = staging_directory / source
    extract_compressed_tar(release_archive, extraction_directory)

    nodes_file = extraction_directory / RELEASE_NODES_FILENAME
    if not nodes_file.exists():
        raise IOError(f"The release archive for {source} ({release_archive}) did not contain "
                      f"{RELEASE_NODES_FILENAME}")

    edges_file = extraction_directory / RELEASE_EDGES_FILENAME

    return [str(nodes_file)] + ([str(edges_file)] if edges_file.exists() else [])


def merge(graph_id: str, sources: list[str], overwrite: bool = False) -> PipelineMetadata:
    """Use ORION to merge the latest releases of multiple sources together into a single KGX output.
    Note that this process skips writing files to the data/storage directory and immediately generates a release,
    unlike single_merge and single-ingest merges done by the pipeline.

    Returns:
        The merged graph's PipelineMetadata.
    """
    logger.info(f"Merging {graph_id}. Sources: {sources}.")

    # Read the latest release metadata for every source, which determines which release of each source is merged.
    source_releases = {source: _read_source_release_metadata(source) for source in sources}

    biolink_version = _get_shared_version(source_releases, "biolink_version")
    babel_version = _get_shared_version(source_releases, "babel_version")

    # Identify the released graphs the merged graph is built from (hasPart in the graph metadata).
    kgx_sources = [{"@id": release_metadata.data,
                    "name": source,
                    "release_version": release_metadata.release_version,
                    "build_version": release_metadata.build_version}
                   for source, release_metadata in source_releases.items()]

    # Get KGXKnowledgeSource metadata from the rig files (isBasedOn in the graph metadata).
    knowledge_sources = []
    for source, release_metadata in source_releases.items():
        data_source_info = get_kgx_source_from_rig(source)
        data_source_info.version = release_metadata.source_version
        knowledge_sources.append(data_source_info)

    # Read the previous release version (if any) to determine the next semantic version
    previous_release_metadata_path = get_versioned_file_paths(
        IngestFileType.LATEST_RELEASE_FILE, PipelineMetadata(source=graph_id)
    )
    previous_release_version = None
    if previous_release_metadata_path.exists():
        with previous_release_metadata_path.open("r") as previous_release_file:
            previous_release_version = PipelineMetadata.from_dict(json.load(previous_release_file)).release_version

    # Generate a build version based on the build versions of all source graphs
    source_build_versions = sorted(release_metadata.build_version for release_metadata in source_releases.values())
    build_version = hashlib.md5("".join(source_build_versions).encode()).hexdigest()[:12]
    release_version = next_release_version(previous_release_version)
    data_path = f"{INGESTS_RELEASES_URL}/{graph_id}/{release_version}/"

    # TODO - this should probably use a different kind of Metadata object, PipelineMetadata is designed for one ingest
    # Create PipelineMetadata for the merged graph.
    # other PipelineMetadata attributes like conflation / strict are per-source and don't apply to a merge.
    merged_graph_metadata = PipelineMetadata(
        source=graph_id,
        babel_version=babel_version,
        biolink_version=biolink_version,
        build_version=build_version,
        build_date=current_iso_date(),
        release_version=release_version,
        data=data_path,
    )

    # Check if the latest release already has this build version
    if is_merged_graph_release_current(merged_graph_metadata) and not overwrite:
        logger.info(f"Graph {graph_id} latest release is already current (build: {build_version}). Skipping merge.")
        return merged_graph_metadata

    logger.info(f"Graph {graph_id} versioned, release version: {release_version}, build version: {build_version}")
    output_dir = Path(INGESTS_RELEASES_PATH) / graph_id / release_version
    nodes_output_file = output_dir / RELEASE_NODES_FILENAME
    edges_output_file = output_dir / RELEASE_EDGES_FILENAME
    if not overwrite and (nodes_output_file.exists() and edges_output_file.exists()):
        logger.info(f"Graph {graph_id} ({build_version}) already exists..")
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
        # Releases are compressed, so the KGX files of each source are extracted into a staging directory for the
        # merge, then discarded. Staging goes alongside the releases so it lands on the same volume.
        with tempfile.TemporaryDirectory(dir=Path(INGESTS_RELEASES_PATH),
                                         prefix=f".merge_staging_{graph_id}_") as staging_directory:
            graph_spec = GraphSpec(
                graph_id=graph_id,
                graph_name=graph_id,
                graph_description="",
                graph_url=data_path,
                graph_version=release_version,
                graph_output_format="jsonl",
                # NOTE: merge_strategy=DONT_MERGE really means don't merge edges, nodes are always merged.
                # We already merged edges for every ingest and don't have overlapping
                # primary-knowledge-sources, so we don't need to merge edges here.
                sources=[SubGraphSource(id=source,
                                        file_paths=_extract_release_kgx_files(source,
                                                                              release_metadata,
                                                                              Path(staging_directory)),
                                        graph_version=release_metadata.release_version,
                                        merge_strategy=KGXFileMerger.DONT_MERGE)
                         for source, release_metadata in source_releases.items()],
                subgraphs=[],
            )
            file_merger = KGXFileMerger(
                graph_spec=graph_spec,
                output_directory=str(output_dir),
                nodes_output_filename=RELEASE_NODES_FILENAME,
                edges_output_filename=RELEASE_EDGES_FILENAME,
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
    merge_graph_metadata(pipeline_metadata=merged_graph_metadata, knowledge_sources=knowledge_sources,
                         kgx_sources=kgx_sources, overwrite=overwrite)

    return merged_graph_metadata

def merge_graph_metadata(pipeline_metadata: PipelineMetadata,
                         knowledge_sources: list[KGXKnowledgeSource],
                         kgx_sources: list[dict],
                         overwrite: bool = False):
    """Generate graph metadata for a merged graph.

    Args:
        pipeline_metadata: PipelineMetadata instance for the merged graph
        knowledge_sources: KGXKnowledgeSource metadata for the upstream knowledge sources of the merge (isBasedOn)
        kgx_sources: metadata identifying the released graphs the merged graph is built from (hasPart)
        overwrite: Whether to overwrite existing metadata
    """
    graph_id = pipeline_metadata.source
    release_version = pipeline_metadata.release_version
    biolink_version = pipeline_metadata.biolink_version
    babel_version = pipeline_metadata.babel_version

    logger.info(f"Generating graph metadata for {graph_id} ({release_version})...")
    merged_graph_dir = Path(INGESTS_RELEASES_PATH) / graph_id / release_version
    merged_graph_nodes = merged_graph_dir / RELEASE_NODES_FILENAME
    merged_graph_edges = merged_graph_dir / RELEASE_EDGES_FILENAME
    graph_metadata_file_path = merged_graph_dir / RELEASE_GRAPH_METADATA_FILENAME
    if graph_metadata_file_path.exists():
        if not overwrite:
            logger.info(f"Graph metadata file already exists: {graph_metadata_file_path}. Exiting...")
            return
        else:
            logger.info(f"Graph metadata file already exists: {graph_metadata_file_path}. "
                        f"OVERWRITE mode enabled, overwriting...")

    release_url = f"{INGESTS_RELEASES_URL}/{graph_id}/{release_version}"
    source_metadata = KGXGraphMetadata(
        id=release_url,
        name=graph_id,
        description="A merged knowledge graph built for the NCATS Biomedical Data Translator project using "
                    "Translator-Ingests, Biolink Model, and Node Normalizer.",
        license="MIT",
        url=release_url,
        version=release_version,
        date_created=current_iso_date(),
        biolink_version=biolink_version,
        babel_version=babel_version,
        knowledge_sources=knowledge_sources,
        kg_sources=kgx_sources,
    )
    source_metadata.schema = generate_schema(nodes_file_path=str(merged_graph_nodes),
                                             edges_file_path=str(merged_graph_edges),
                                             biolink_version=biolink_version)

    with graph_metadata_file_path.open("w") as output_file:
        output_file.write(source_metadata.to_json())
    logger.info(f"Graph metadata complete for {graph_id} ({release_version}).")



def _warn_if_sources_diverge_from_declaration(graph_id: str, sources: list[str]) -> None:
    """Warn when ``graph_id`` is declared in graphs.yaml but sources don't match.

    Protects against ``make merge GRAPH_ID=translator_kg_open SOURCES="ctd"``
    silently producing a build labeled ``translator_kg_open`` whose contents
    contradict the yaml declaration. Missing or unreadable graphs.yaml is
    non-fatal — ad-hoc graph_ids remain allowed.
    """
    try:
        # Imported lazily so merging.py has no hard dependency on graphs.yaml
        # for ad-hoc graph_ids that aren't declared there.
        from translator_ingest.graphs import GraphConfigError, resolve_sources
    except ImportError:
        return

    try:
        declared = resolve_sources(graph_id)
    except GraphConfigError:
        return  # graph_id is not declared; ad-hoc builds are fine

    if set(declared) != set(sources):
        extra = sorted(set(sources) - set(declared))
        missing = sorted(set(declared) - set(sources))
        logger.warning(
            "!!! Sources passed on the command line do not match the graphs.yaml "
            "declaration for %r. Proceeding anyway, but the resulting build "
            "will be labeled %r despite not matching its declared source set. "
            "Extra: %s. Missing: %s. !!!",
            graph_id, graph_id, extra or "(none)", missing or "(none)",
        )


@click.command()
@click.argument("graph_id", required=True)
@click.argument("sources", nargs=-1, required=True)
@click.option("--overwrite", is_flag=True, help="Start fresh and overwrite previously generated files.")
def main(graph_id, sources, overwrite):
    setup_logging()

    _warn_if_sources_diverge_from_declaration(graph_id, list(sources))

    # Merge the sources into one KGX and generate metadata
    merged_graph_metadata = merge(
        graph_id, sources=list(sources), overwrite=overwrite
    )

    # Generate latest release metadata for the merged graph
    if is_merged_graph_release_current(merged_graph_metadata) and not overwrite:
        logger.info(f"Latest release already up to date for {graph_id}, build: {merged_graph_metadata.build_version}")
    else:
        generate_merged_graph_release(merged_graph_metadata)


if __name__ == "__main__":
    main()
