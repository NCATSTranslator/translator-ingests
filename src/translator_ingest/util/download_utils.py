"""Utilities for handling download.yaml files and version substitution."""

import tempfile
import yaml
from pathlib import Path
from typing import Union, List

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)


class EmptyDownloadedFileError(Exception):
    """Raised when one or more downloaded files are empty (file size 0)."""
    pass


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


def validate_downloaded_files(download_directory: Union[str, Path]) -> None:
    """
    Validate that downloaded files are not empty (file size > 0).

    Checks all files in the download directory to ensure none have a size of 0 bytes.
    This helps catch scenarios where a download succeeded but the source provided an empty file,
    which would otherwise cause confusing errors during data processing.

    Args:
        download_directory: Path to the directory containing downloaded files

    Raises:
        EmptyDownloadedFileError: If one or more downloaded files have size 0

    Example:
        >>> validate_downloaded_files("/path/to/downloads")
        # Raises EmptyDownloadedFileError if any files are empty
    """
    download_directory = Path(download_directory)

    if not download_directory.exists():
        logger.warning(f"Download directory does not exist: {download_directory}")
        return

    # Get all files in the directory (not subdirectories)
    downloaded_files = [f for f in download_directory.iterdir() if f.is_file()]

    if not downloaded_files:
        logger.warning(f"No files found in download directory: {download_directory}")
        return

    # Check each file for empty size
    empty_files: List[Path] = []
    for file_path in downloaded_files:
        file_size = file_path.stat().st_size
        if file_size == 0:
            empty_files.append(file_path)
            logger.error(f"Downloaded file is empty (0 bytes): {file_path.name}")

    # If any files are empty, raise an informative error
    if empty_files:
        file_names = ", ".join([f.name for f in empty_files])
        error_message = (
            f"Downloaded file(s) are empty (file size 0): {file_names}. "
            f"This likely indicates an issue with the data source's pipeline. "
            f"Please check the source's status and contact the data provider if necessary."
        )
        raise EmptyDownloadedFileError(error_message)

    logger.info(f"Validated {len(downloaded_files)} downloaded file(s) - all non-empty")
