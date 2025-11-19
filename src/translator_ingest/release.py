import json
import shutil
import tarfile
import logging
import click
from pathlib import Path

from translator_ingest import INGESTS_RELEASES_PATH
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import get_versioned_file_paths, IngestFileType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def copy_file_to_release(file_type: IngestFileType,
                         pipeline_metadata: PipelineMetadata,
                         release_dir: Path,
                         output_name: str = None):
    source_path = get_versioned_file_paths(file_type=file_type, pipeline_metadata=pipeline_metadata)

    # Handle tuple return (for KGX files which return multiple paths)
    if isinstance(source_path, tuple):
        if output_name:
            raise NotImplementedError()
        for path in source_path:
            if path and path.exists():
                shutil.copy2(path, release_dir / path.name)
    else:
        if source_path and source_path.exists():
            if not output_name:
                output_name = source_path.name
            shutil.copy2(source_path, release_dir / output_name)


def create_compressed_tar(pipeline_metadata: PipelineMetadata, release_dir: Path, tar_filename: str):
    """Create a tar.xz compressed archive of NORMALIZED_KGX_FILES and GRAPH_METADATA_FILE."""
    tar_path = release_dir / tar_filename
    with tarfile.open(tar_path, 'w:xz') as tar:
        kgx_files = get_versioned_file_paths(IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata)
        for file_path in kgx_files:
            # Rename normalized_nodes/edges to nodes/edges in the archive
            if "nodes" in file_path.name:
                arcname = "nodes.jsonl"
            elif "edges" in file_path.name:
                arcname = "edges.jsonl"
            else:
                arcname = file_path.name
            tar.add(file_path, arcname=arcname)
        graph_metadata_path = get_versioned_file_paths(IngestFileType.GRAPH_METADATA_FILE, pipeline_metadata)
        tar.add(graph_metadata_path, arcname=graph_metadata_path.name)


def create_release(source: str):
    release_file_path = get_versioned_file_paths(
        file_type=IngestFileType.LATEST_RELEASE_FILE,
        pipeline_metadata=PipelineMetadata(source=source)
    )
    with open(release_file_path, 'r') as f:
        latest_release_data = PipelineMetadata(**json.load(f))

    # Create release directory
    release_src_dir = Path(INGESTS_RELEASES_PATH) / source
    release_version_dir = release_src_dir / latest_release_data.release_version
    release_version_dir.mkdir(parents=True, exist_ok=True)

    # Check if release files already exist
    tar_path = release_version_dir / f"{source}.tar.xz"
    if tar_path.exists():
        logger.info(f"Release files already exist for {source} version {latest_release_data.release_version}, "
                    f"skipping processing...")
    else:
        # Create compressed tar archive and save it to the release dir
        create_compressed_tar(latest_release_data, release_version_dir, f"{source}.tar.xz")

        # Copy other release files over (intentionally include the GRAPH_METADATA_FILE in the tar and outside)
        copy_file_to_release(IngestFileType.GRAPH_METADATA_FILE, latest_release_data, release_version_dir)
        copy_file_to_release(IngestFileType.TEST_DATA_FILE, latest_release_data, release_version_dir, "test-data.json")

    # copy the latest release metadata to the source level of the releases dir
    copy_file_to_release(IngestFileType.LATEST_RELEASE_FILE, latest_release_data, release_src_dir)

    logger.info(f"Release files processed for {source}, release version: {latest_release_data.release_version}")

@click.command()
@click.argument("source", type=str)
def main(source):
    create_release(source)


if __name__ == "__main__":
    main()