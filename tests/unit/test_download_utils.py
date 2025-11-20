"""Tests for download_utils module."""

import pytest
import yaml
from pathlib import Path
from tempfile import TemporaryDirectory

from translator_ingest.util.download_utils import substitute_version_in_download_yaml


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

        # Substitute version (should return original path)
        version = "2024-01-15"
        result_yaml = substitute_version_in_download_yaml(download_yaml, version)

        # Should return the same path since no substitution needed
        assert result_yaml == download_yaml


def test_custom_placeholder():
    """Test using a custom placeholder string."""
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        download_yaml = tmpdir / "download.yaml"

        # Write a test download.yaml with custom placeholder
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
