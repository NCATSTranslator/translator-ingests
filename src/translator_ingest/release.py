import json
import shutil
import tarfile
import click
import datetime
import zstandard as zstd
from pathlib import Path

from translator_ingest import INGESTS_RELEASES_PATH, INGESTS_STORAGE_URL
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import get_versioned_file_paths, IngestFileType, write_ingest_file
from translator_ingest.util.logging_utils import get_logger, setup_logging

logger = get_logger(__name__)

FILE_NAME_CHANGES = {
    "testing_data.json": "test-data.json"
}


def atomic_copy_directory(src: Path, dest: Path):
    """Copy a directory to a destination using atomic rename to minimize downtime.

    Uses a temp directory and atomic renames to ensure the destination is always
    valid (either old or new version), with only microseconds of transition time.

    :param src: Source directory to copy
    :param dest: Destination path (will be overwritten if exists)
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

    logger.info(f"Copied {src} to {dest}")


def create_compressed_tar(nodes_file: Path,
                          edges_file: Path,
                          graph_metadata_path: Path,
                          output_path: Path):
    # Create a zstd compressed tar archive of KGX files
    cctx = zstd.ZstdCompressor(level=12)
    with open(output_path, 'wb') as fh:
        with cctx.stream_writer(fh) as compressor:
            with tarfile.open(fileobj=compressor, mode='w|') as tar:
                tar.add(nodes_file, arcname="nodes.jsonl")
                if edges_file.exists():
                    tar.add(edges_file, arcname="edges.jsonl")
                tar.add(graph_metadata_path, arcname="graph-metadata.json")


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
        latest_build_metadata = PipelineMetadata(**json.load(f))
        latest_build = latest_build_metadata.build_version

    # Locate and read the latest release metadata for the source
    latest_release_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.LATEST_RELEASE_FILE,
        pipeline_metadata=PipelineMetadata(source=source)
    )
    if latest_release_metadata_file_path.exists():
        with open(latest_release_metadata_file_path, 'r') as f:
            latest_release_metadata = PipelineMetadata(**json.load(f))
            latest_released_build = latest_release_metadata.build_version
            # if the latest release is already of the latest build, no need to do anything
            if latest_released_build == latest_build:
                logger.info(f"Release already current for {source}.")
                return

    # Get all the file paths for the relevant files
    nodes_file_path, edges_file_path = get_versioned_file_paths(IngestFileType.MERGED_KGX_FILES, latest_build_metadata)
    graph_metadata_path = get_versioned_file_paths(IngestFileType.GRAPH_METADATA_FILE, latest_build_metadata)
    test_data_path = get_versioned_file_paths(IngestFileType.TEST_DATA_FILE, latest_build_metadata)

    # Create the release
    release_version = datetime.datetime.now().strftime("%Y_%m_%d")
    release_dir = Path(INGESTS_RELEASES_PATH) / source / release_version
    create_release(source,
                   release_dir,
                   nodes_file=nodes_file_path,
                   edges_file=edges_file_path,
                   graph_metadata_file=graph_metadata_path,
                   files_to_copy=[graph_metadata_path, test_data_path])

    # Copy release to "latest" directory
    latest_dir = Path(INGESTS_RELEASES_PATH) / source / "latest"
    atomic_copy_directory(release_dir, latest_dir)

    # Write the new latest-release-metadata
    latest_release_metadata = latest_build_metadata
    latest_release_metadata.release_version = release_version
    latest_release_metadata.data = f"{INGESTS_STORAGE_URL}/{source}/{release_version}/"

    write_ingest_file(IngestFileType.LATEST_RELEASE_FILE,
                      pipeline_metadata=latest_release_metadata,
                      data=latest_release_metadata.get_release_metadata())
    logger.info(f"Release files processed for {source}, release version: {latest_release_metadata.release_version}")


def create_release(source: str,
                   release_dir: Path,
                   nodes_file: Path,
                   edges_file: Path,
                   graph_metadata_file: Path,
                   files_to_copy: list[Path]):

    # Create or locate release directory
    release_dir.mkdir(parents=True, exist_ok=True)

    # Check if release files already exist
    tar_path = release_dir / f"{source}.tar.zst"
    if not tar_path.exists():
        # Create compressed tar archive and save it to the release dir
        logger.info(f"Creating compressed tar for release of {source}...")
        create_compressed_tar(nodes_file=nodes_file,
                              edges_file=edges_file,
                              graph_metadata_path=graph_metadata_file,
                              output_path=tar_path)
    else:
        logger.info(f"Release already exists for {source} at {release_dir}, skipping...")

    # Copy other release files over (intentionally include the GRAPH_METADATA_FILE in the tar and outside)
    logger.info(f"Copying other release files over for {source} if needed...")
    for path in files_to_copy:
        # Some files we might want to change the name of for releases
        output_name = FILE_NAME_CHANGES.get(path.name, path.name)
        output_path = release_dir / output_name
        if not output_path.exists():
            shutil.copy2(path, output_path)


@click.command()
@click.argument("source", type=str)
def main(source):
    setup_logging()
    release_ingest(source)


if __name__ == "__main__":
    main()