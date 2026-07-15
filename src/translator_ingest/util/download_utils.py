"""Utilities for handling download.yaml files and version substitution."""

import tempfile
import yaml
from pathlib import Path
from typing import Any, Optional, Union

from kghub_downloader.model import DownloadReport

from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.metadata import PipelineMetadata, current_iso_date
from translator_ingest.util.storage.local import (
    IngestFileType,
    get_versioned_file_paths,
    write_ingest_file,
)

logger = get_logger(__name__)


def record_download_metadata(
    pipeline_metadata: PipelineMetadata,
    report: Optional[DownloadReport],
) -> None:
    """
    Persist source-data download metadata, stamping downloaded_at only when a real fetch occurred.

    kghub-downloader serves already-present files from its cache, so most pipeline reruns download
    nothing. We record the downloaded_at timestamp only when at least one file was actually fetched
    this run; on cache-hit reruns the existing source-metadata.json (and its timestamp) is left
    untouched, so downloaded_at reflects when the data was genuinely last retrieved.

    Args:
        pipeline_metadata: Metadata identifying the source and version being downloaded.
        report: The DownloadReport returned by kghub-downloader, or None if unavailable.
    """
    if report is None or not report.any_downloaded:
        logger.info(
            f"No new source data downloaded for {pipeline_metadata.source}; "
            f"leaving existing download metadata unchanged."
        )
        return

    metadata: dict[str, Any] = {
        "source": pipeline_metadata.source,
        "source_version": pipeline_metadata.source_version,
        "downloaded_at": current_iso_date(),
        "downloaded": [path.name for path in report.downloaded],
        "skipped": [path.name for path in report.skipped],
    }
    write_ingest_file(
        file_type=IngestFileType.SOURCE_METADATA_FILE,
        pipeline_metadata=pipeline_metadata,
        data=metadata,
    )
    source_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.SOURCE_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    logger.info(
        f"Recorded download of {len(report.downloaded)} file(s) for {pipeline_metadata.source} "
        f"at {source_metadata_path}."
    )


def substitute_version_in_download_yaml(
    download_yaml_path: Union[str, Path],
    version: str,
    placeholder: str = "{version}"
) -> Path:
    """
    Read a download.yaml file, substitute version placeholders in URLs, and write to a temporary file.

    This allows download.yaml files to use version placeholders like:
        url: https://example.com/data_{version}/file.tsv

    Which will be substituted with the actual version before downloading:
        url: https://example.com/data_2024-01-15/file.tsv

    Args:
        download_yaml_path: Path to the original download.yaml file
        version: The version string to substitute (from get_latest_version())
        placeholder: The placeholder string to replace (default: "{version}")

    Returns:
        Path to a temporary YAML file with substituted URLs

    Example:
        >>> temp_yaml = substitute_version_in_download_yaml(
        ...     "ingests/example/download.yaml",
        ...     "2024-01-15"
        ... )
        >>> # Now use temp_yaml with kghub_downloader
    """
    download_yaml_path = Path(download_yaml_path)

    if not download_yaml_path.exists():
        raise FileNotFoundError(f"Download YAML file not found: {download_yaml_path}")

    # Read the original YAML file
    with open(download_yaml_path, 'r') as f:
        download_config = yaml.safe_load(f)

    # Check if any URLs contain the placeholder
    has_placeholder = False
    if download_config:
        for entry in download_config:
            if 'url' in entry and placeholder in entry['url']:
                has_placeholder = True
                break

    # If no placeholders found, return the original path (no substitution needed)
    if not has_placeholder:
        logger.debug(f"No version placeholders found in {download_yaml_path}")
        return download_yaml_path

    # Substitute version in all URLs
    logger.info(f"Substituting '{placeholder}' with '{version}' in download URLs")
    for entry in download_config:
        if 'url' in entry:
            original_url = entry['url']
            entry['url'] = entry['url'].replace(placeholder, version)
            if original_url != entry['url']:
                logger.info(f"  {original_url} -> {entry['url']}")

    # Write to a temporary file
    # Use delete=False so the file persists after the context manager closes
    temp_file = tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.yaml',
        prefix='download_',
        delete=False
    )

    try:
        yaml.safe_dump(download_config, temp_file, default_flow_style=False)
        temp_file.close()
        temp_path = Path(temp_file.name)
        logger.debug(f"Created temporary download YAML: {temp_path}")
        return temp_path
    except Exception as e:
        # Clean up the temp file if something goes wrong
        temp_file.close()
        Path(temp_file.name).unlink(missing_ok=True)
        raise e
