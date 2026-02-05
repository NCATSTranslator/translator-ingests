"""Tests for download_utils module."""

import pytest
import yaml
from pathlib import Path
from tempfile import TemporaryDirectory

from translator_ingest.util.download_utils import (
    substitute_version_in_download_yaml,
    validate_downloaded_files,
    EmptyDownloadedFileError,
)


def test_substitute_version_in_urls():
    """Test that version placeholders are correctly substituted in download URLs."""
    # Create a temporary download.yaml with version placeholders
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        download_yaml = tmpdir / "download.yaml"

        # Write a test download.yaml with version placeholders
        test_config = [
            {
                "url": "https://example.com/data_{version}/file1.tsv.gz",
                "local_name": "file1.tsv.gz"
            },
            {
                "url": "https://example.com/data_{version}/file2.tsv.gz",
                "local_name": "file2.tsv.gz"
            }
        ]

        with open(download_yaml, 'w') as f:
            yaml.safe_dump(test_config, f)

        # Substitute version
        version = "2024-01-15"
        result_yaml = substitute_version_in_download_yaml(download_yaml, version)

        # Read the result
        with open(result_yaml, 'r') as f:
            result_config = yaml.safe_load(f)

        # Verify substitution
        assert result_config[0]["url"] == "https://example.com/data_2024-01-15/file1.tsv.gz"
        assert result_config[1]["url"] == "https://example.com/data_2024-01-15/file2.tsv.gz"
        assert result_config[0]["local_name"] == "file1.tsv.gz"
        assert result_config[1]["local_name"] == "file2.tsv.gz"

        # Clean up temp file
        result_yaml.unlink(missing_ok=True)


def test_no_placeholder_returns_original():
    """Test that when no placeholders exist, the original path is returned."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        download_yaml = tmpdir / "download.yaml"

        # Write a test download.yaml WITHOUT version placeholders
        test_config = [
            {
                "url": "https://example.com/data/file1.tsv.gz",
                "local_name": "file1.tsv.gz"
            }
        ]

        with open(download_yaml, 'w') as f:
            yaml.safe_dump(test_config, f)

        # Substitute version (should return the original path)
        version = "2024-01-15"
        result_yaml = substitute_version_in_download_yaml(download_yaml, version)

        # Should return the same path since no substitution needed
        assert result_yaml == download_yaml


def test_custom_placeholder():
    """Test using a custom placeholder string."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        download_yaml = tmpdir / "download.yaml"

        # Write a test download.yaml with a custom placeholder
        test_config = [
            {
                "url": "https://example.com/data_VERSION/file.tsv.gz",
                "local_name": "file.tsv.gz"
            }
        ]

        with open(download_yaml, 'w') as f:
            yaml.safe_dump(test_config, f)

        # Substitute version with custom placeholder
        version = "v2.0"
        result_yaml = substitute_version_in_download_yaml(
            download_yaml,
            version,
            placeholder="VERSION"
        )

        # Read the result
        with open(result_yaml, 'r') as f:
            result_config = yaml.safe_load(f)

        # Verify substitution
        assert result_config[0]["url"] == "https://example.com/data_v2.0/file.tsv.gz"

        # Clean up temp file
        result_yaml.unlink(missing_ok=True)


def test_file_not_found():
    """Test that FileNotFoundError is raised for non-existent files."""
    with pytest.raises(FileNotFoundError):
        substitute_version_in_download_yaml(
            "nonexistent/download.yaml",
            "2024-01-15"
        )


def test_validate_downloaded_files_with_valid_files():
    """Test that validation passes when all files have content."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create non-empty files
        (tmpdir / "file1.tsv").write_text("data content here")
        (tmpdir / "file2.json").write_text('{"key": "value"}')
        
        # Should not raise any exception
        validate_downloaded_files(tmpdir)


def test_validate_downloaded_files_with_empty_file():
    """Test that validation raises error when a file is empty."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create one empty file and one non-empty file
        (tmpdir / "file1.tsv").write_text("data content here")
        (tmpdir / "empty_file.tsv").write_text("")  # Empty file
        
        # Should raise EmptyDownloadedFileError
        with pytest.raises(EmptyDownloadedFileError) as exc_info:
            validate_downloaded_files(tmpdir)
        
        # Verify error message contains the filename
        assert "empty_file.tsv" in str(exc_info.value)
        assert "file size 0" in str(exc_info.value)


def test_validate_downloaded_files_with_multiple_empty_files():
    """Test that validation raises error listing all empty files."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create multiple empty files
        (tmpdir / "file1.tsv").write_text("data content here")
        (tmpdir / "empty1.tsv").write_text("")
        (tmpdir / "empty2.json").write_text("")
        
        # Should raise EmptyDownloadedFileError
        with pytest.raises(EmptyDownloadedFileError) as exc_info:
            validate_downloaded_files(tmpdir)
        
        # Verify error message contains both filenames
        error_msg = str(exc_info.value)
        assert "empty1.tsv" in error_msg
        assert "empty2.json" in error_msg
        assert "file size 0" in error_msg


def test_validate_downloaded_files_with_no_files():
    """Test that validation handles directory with no files gracefully."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Directory exists but is empty - should not raise an error
        # (warning is logged but no exception)
        validate_downloaded_files(tmpdir)


def test_validate_downloaded_files_with_nonexistent_directory():
    """Test that validation handles non-existent directory gracefully."""
    # Should not raise an error, just log a warning
    validate_downloaded_files("/nonexistent/directory/path")


def test_validate_downloaded_files_ignores_subdirectories():
    """Test that validation only checks files, not subdirectories."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create a valid file
        (tmpdir / "file1.tsv").write_text("data content here")
        
        # Create a subdirectory (should be ignored)
        subdir = tmpdir / "subdir"
        subdir.mkdir()
        (subdir / "file_in_subdir.txt").write_text("")
        
        # Should not raise an error since subdirectories are ignored
        validate_downloaded_files(tmpdir)


def _fake_download(download_yaml_file: Path):
    # This method replicates the essential parts of 'url' version management within the
    # code design pattern flanking the 'kghub_download' within the pipeline.download() method

    # Substitute version placeholders in download.yaml if they exist
    download_yaml_with_version = substitute_version_in_download_yaml(
        download_yaml_file,
        version="testing-1-2-3"
    )

    ########################################################################################################
    # Ignored code context of the 'kghub_download' within the pipeline.download() method
    #
    # # Get a path for the subdirectory for the source data
    # source_data_output_dir = get_source_data_directory(pipeline_metadata)
    # Path.mkdir(source_data_output_dir, exist_ok=True)
    try:
    #     # Download the data
    #     # Don't need to check if file(s) already downloaded, kg downloader handles that
    #     kghub_download(yaml_file=str(download_yaml_with_version), output_dir=str(source_data_output_dir))
        pass
    finally:
    #     ... indentation
    ########################################################################################################

        # Clean up the specified 'download_yaml' file if it exists and
        # is a temporary file with versioning resolved but is
        # **NOT** rather the original unmodified download.yaml!
        if download_yaml_with_version and \
                download_yaml_with_version != download_yaml_file:
            download_yaml_with_version.unlink(missing_ok=True)

    # returning the 'target' path, which will either be a temporary file
    # with versioning resolved or the original unmodified download.yaml
    return download_yaml_with_version


@pytest.mark.parametrize(
    "download_url",
    [
        # URL *withOUT* the default 'version' placeholder
        "https://example.com/current/file.tsv.gz",

        # URL *with* the default 'version' placeholder
        "https://example.com/version/file.tsv.gz"
    ]
)
def test_fake_download(download_url):
    """Test that the code design pattern which wraps 'kghub_download' in the
       pipeline.download() method, properly handles temporary file management."""
    with TemporaryDirectory() as tmpdir:

        tmpdir = Path(tmpdir)
        download_yaml = tmpdir / "download.yaml"

        test_config = [
            {
                "url": download_url,
                "local_name": "file.tsv.gz"
            }
        ]
        with open(download_yaml, 'w') as f:
            yaml.safe_dump(test_config, f)

        target_download_yaml = _fake_download(download_yaml)

        # Check that for either case of a
        # 'download_url' *with* or *withOUT* a version placeholder
        # that the original "download_yaml" file is **NOT** deleted
        assert download_yaml.exists()

        # Check that when a versioned "url" is provided, thus the target download.yaml file path
        # points to a different (temporary) file which is not the original download.yaml path,
        # then the (temporary) target file is deleted within once the (simulated) download is done.
        assert not (target_download_yaml != download_yaml and target_download_yaml.exists())
