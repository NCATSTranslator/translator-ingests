import json
import shutil
import tarfile
import click
import zstandard as zstd
from pathlib import Path

from translator_ingest import INGESTS_RELEASES_PATH, INGESTS_RELEASES_URL
from translator_ingest.util.metadata import PipelineMetadata, next_release_version, current_iso_date
from translator_ingest.util.storage.local import (get_versioned_file_paths, IngestFileName, IngestFileType,
                                                  write_ingest_file)
from translator_ingest.util.logging_utils import get_logger, setup_logging

logger = get_logger(__name__)

FILE_NAME_CHANGES = {
    "testing_data.json": "test-data.json"
}

RELEASE_NODES_FILENAME = "nodes.jsonl"
RELEASE_EDGES_FILENAME = "edges.jsonl"
RELEASE_GRAPH_METADATA_FILENAME = "graph-metadata.json"


def atomic_copy_directory(src: Path, dest: Path):
    """Copy a directory to a destination using atomic rename to minimize downtime.

    Uses a temp directory and atomic renames to ensure the destination is always
    valid (either old or new version), with only microseconds of transition time.

    Args:
        src: Source directory to copy
        dest: Destination path (will be overwritten if exists)
    """
    dest_tmp = dest.with_name(f"{dest.name}_new")
    dest_old = dest.with_name(f"{dest.name}_old")

    # Clean up any leftover temp directories from previous failed runs
    if dest_tmp.exists():
        shutil.rmtree(dest_tmp)
    if dest_old.exists():
        shutil.rmtree(dest_old)

    # Copy to temp location first
    shutil.copytree(src, dest_tmp)

    # Atomic swap: rename old -> old_backup, then new -> dest
    if dest.exists():
        dest.rename(dest_old)
    dest_tmp.rename(dest)

    # Clean up old version
    if dest_old.exists():
        shutil.rmtree(dest_old)

def create_compressed_tar(nodes_file: Path,
                          edges_file: Path,
                          graph_metadata_path: Path,
                          output_path: Path):
    # Create a zstd compressed tar archive of KGX files. Compressing a graph can take a while, so the archive is
    # written to a partial file and renamed into place once it is complete. This prevents unfinished files left
    # behind by crashes from looking like completed tars.
    partial_output_path = output_path.with_name(f"{output_path.name}.partial")
    cctx = zstd.ZstdCompressor(level=12)
    with open(partial_output_path, 'wb') as fh:
        with cctx.stream_writer(fh) as compressor:
            with tarfile.open(fileobj=compressor, mode='w|') as tar:
                tar.add(nodes_file, arcname=RELEASE_NODES_FILENAME)
                if edges_file.exists():
                    tar.add(edges_file, arcname=RELEASE_EDGES_FILENAME)
                tar.add(graph_metadata_path, arcname=RELEASE_GRAPH_METADATA_FILENAME)
    partial_output_path.rename(output_path)


def extract_compressed_tar(tar_path: Path, output_directory: Path):
    """Extract a zstd compressed tar archive created by create_compressed_tar.

    Args:
        tar_path: Path of the compressed archive to extract
        output_directory: Directory to extract into, created if it does not already exist
    """
    output_directory.mkdir(parents=True, exist_ok=True)
    dctx = zstd.ZstdDecompressor()
    with open(tar_path, 'rb') as fh:
        with dctx.stream_reader(fh) as decompressor:
            with tarfile.open(fileobj=decompressor, mode='r|') as tar:
                tar.extractall(path=output_directory, filter='data')


def update_graph_metadata_for_release(source_graph_metadata_path: Path,
                                      release_dir: Path,
                                      release_url: str) -> Path:
    """Update graph-metadata.json with release URL for id and url fields.

    Reads the existing graph metadata, updates the id and url fields to use
    the release versioning, and writes the updated version to the release directory.

    Args:
        source_graph_metadata_path: Path to the original graph-metadata.json
        release_dir: Directory where the release files are being created
        release_url: The release URL to use for id and url fields

    Returns:
        Path to the updated graph-metadata.json in the release directory
    """
    with open(source_graph_metadata_path, 'r') as f:
        graph_metadata = json.load(f)

    graph_metadata['@id'] = release_url
    graph_metadata['url'] = release_url

    output_path = release_dir / "graph-metadata.json"
    with open(output_path, 'w') as f:
        json.dump(graph_metadata, f, indent=2)

    logger.info(f"Updated graph-metadata.json with release URL: {release_url}")
    return output_path


def get_existing_release_build_version(release_dir: Path) -> str | None:
    """Return the build version an existing release was made from, or None if there is no release in release_dir.

    Releases made before release-metadata.json was written to every release directory recorded their build version 
    as the "version" of their graph-metadata.json.

    Args:
        release_dir: Directory of a single release, which may or may not exist yet
    """
    release_metadata_path = release_dir / IngestFileName.RELEASE_METADATA_FILE
    if release_metadata_path.exists():
        with release_metadata_path.open() as release_metadata_file:
            return PipelineMetadata.from_dict(json.load(release_metadata_file)).build_version
    graph_metadata_path = release_dir / RELEASE_GRAPH_METADATA_FILENAME
    if graph_metadata_path.exists():
        with graph_metadata_path.open() as graph_metadata_file:
            return json.load(graph_metadata_file).get("version")
    return None


def release_ingest(source: str):
    # Locate and read the latest build metadata for the source
    latest_build_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.LATEST_BUILD_FILE,
        pipeline_metadata=PipelineMetadata(source=source)
    )
    if not latest_build_metadata_file_path.exists():
        logger.info(f"No latest build metadata found for {source}, can not make a release.")
        return
    with open(latest_build_metadata_file_path, 'r') as f:
        latest_build_metadata = PipelineMetadata.from_dict(json.load(f))
        latest_build = latest_build_metadata.build_version

    # Locate and read the latest release metadata for the source
    latest_release_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.LATEST_RELEASE_FILE,
        pipeline_metadata=PipelineMetadata(source=source)
    )
    previous_release_version = None
    if latest_release_metadata_file_path.exists():
        with open(latest_release_metadata_file_path, 'r') as f:
            latest_release_metadata = PipelineMetadata.from_dict(json.load(f))
            latest_released_build = latest_release_metadata.build_version
            previous_release_version = latest_release_metadata.release_version
            # if the latest release is already of the latest build, no need to do anything
            if latest_released_build == latest_build:
                logger.info(f"Release already current for {source}.")
                return

    # Get all the file paths for the relevant files
    nodes_file_path, edges_file_path = get_versioned_file_paths(IngestFileType.MERGED_KGX_FILES, latest_build_metadata)
    graph_metadata_path = get_versioned_file_paths(IngestFileType.GRAPH_METADATA_FILE, latest_build_metadata)
    test_data_path = get_versioned_file_paths(IngestFileType.TEST_DATA_FILE, latest_build_metadata)

    # Create the release, bumping the version from the previous release
    release_version = next_release_version(previous_release_version)
    release_url = f"{INGESTS_RELEASES_URL}/{source}/{release_version}/"
    
    # Populate the new release metadata, stamping the date the release was made
    release_metadata = latest_build_metadata
    release_metadata.release_version = release_version
    release_metadata.release_date = current_iso_date()
    release_metadata.data = release_url

    # Create the release
    release_dir = Path(INGESTS_RELEASES_PATH) / source / release_version

    # This prevents issues that could be caused by previous crashes or file manipulation, such as missing or
    # misleading latest-release metadata, or half completed releases. Here we check that if a release directory 
    # already exists where we are about to write to that it contains or was supposed to contain the same build.
    existing_build_version = get_existing_release_build_version(release_dir)
    if existing_build_version is not None and existing_build_version != release_metadata.build_version:
        raise ValueError(
            f"Release {release_version} of {source} already exists and was made from build "
            f"{existing_build_version}, but build {release_metadata.build_version} is being released. The latest "
            f"release metadata ({latest_release_metadata_file_path}) is probably missing or out of date."
        )

    create_release(release_metadata,
                   release_dir,
                   release_url=release_url,
                   nodes_file=nodes_file_path,
                   edges_file=edges_file_path,
                   graph_metadata_file=graph_metadata_path,
                   files_to_copy=[test_data_path])

    # Copy release to "latest" directory
    latest_dir = Path(INGESTS_RELEASES_PATH) / source / "latest"
    atomic_copy_directory(release_dir, latest_dir)
    logger.info("Copied release to latest directory")

    # Write the new latest-release-metadata, the same metadata recorded inside the release directory
    write_ingest_file(IngestFileType.LATEST_RELEASE_FILE,
                      pipeline_metadata=release_metadata,
                      data=release_metadata.get_release_metadata())
    logger.info(f"Release files processed for {source}, release version: {release_metadata.release_version}")


def create_release(release_metadata: PipelineMetadata,
                   release_dir: Path,
                   release_url: str,
                   nodes_file: Path,
                   edges_file: Path,
                   graph_metadata_file: Path,
                   files_to_copy: list[Path]):
    source = release_metadata.source

    # Create or locate release directory
    release_dir.mkdir(parents=True, exist_ok=True)

    # Record the metadata of this release alongside its artifacts. latest-release.json only ever describes the
    # most recent release, so without this the provenance of a release is lost as soon as the next one is made.
    write_ingest_file(IngestFileType.RELEASE_METADATA_FILE,
                      pipeline_metadata=release_metadata,
                      data=release_metadata.get_release_metadata())

    # Update graph-metadata.json with release URL (must be done before creating tar)
    release_graph_metadata_path = release_dir / "graph-metadata.json"
    if not release_graph_metadata_path.exists():
        release_graph_metadata_path = update_graph_metadata_for_release(
            source_graph_metadata_path=graph_metadata_file,
            release_dir=release_dir,
            release_url=release_url
        )

    # Check if release files already exist
    tar_path = release_dir / f"{source}.tar.zst"
    if not tar_path.exists():
        # Create compressed tar archive and save it to the release dir
        logger.info(f"Creating compressed tar for release of {source}...")
        create_compressed_tar(nodes_file=nodes_file,
                              edges_file=edges_file,
                              graph_metadata_path=release_graph_metadata_path,
                              output_path=tar_path)
    else:
        logger.info(f"Release already exists for {source} at {release_dir}, skipping...")

    # Copy other release files over
    logger.info(f"Copying other release files over for {source} if needed...")
    for path in files_to_copy:
        # Some files we might want to change the name of for releases
        output_name = FILE_NAME_CHANGES.get(path.name, path.name)
        output_path = release_dir / output_name
        if not output_path.exists():
            shutil.copy2(path, output_path)


def generate_release_summary():
    """Generate a summary of all latest releases in the releases directory.

    Scans INGESTS_RELEASES_PATH for source directories and reads the
    LATEST_RELEASE_FILE for each, writing a combined release_summary.json.
    """
    releases_path = Path(INGESTS_RELEASES_PATH)
    summary = {}

    for source_dir in sorted(releases_path.iterdir()):
        if not source_dir.is_dir():
            continue

        source = source_dir.name
        latest_release_path = get_versioned_file_paths(
            file_type=IngestFileType.LATEST_RELEASE_FILE,
            pipeline_metadata=PipelineMetadata(source=source)
        )

        if latest_release_path.exists():
            with open(latest_release_path, 'r') as f:
                summary[source] = json.load(f)
        else:
            summary[source] = None
            logger.info(f"No latest release metadata found for {source}")

    summary_path = releases_path / "latest-release-summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info("Release summary updated.")


@click.command()
@click.argument("source", type=str, required=False)
@click.option("--summary", is_flag=True, help="Generate release summary for all sources in releases directory")
def main(source, summary):
    setup_logging()
    if summary:
        generate_release_summary()
    elif source:
        release_ingest(source)
    else:
        raise click.UsageError("Provide a source name or use --summary")


if __name__ == "__main__":
    main()