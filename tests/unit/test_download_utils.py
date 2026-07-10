"""Tests for download_utils module."""

import json

import pytest
import yaml
from pathlib import Path
from tempfile import TemporaryDirectory

from kghub_downloader.model import DownloadReport

from translator_ingest.util.download_utils import (
    record_download_metadata,
    substitute_version_in_download_yaml,
)
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage import local as storage_local
from translator_ingest.util.storage.local import (
    IngestFileType,
    get_source_data_directory,
    get_versioned_file_paths,
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


@pytest.fixture
def source_data_dir(tmp_path, monkeypatch):
    """Redirect the ingest data directory to a temp dir and return the source_data dir + metadata."""
    monkeypatch.setattr(storage_local, "INGESTS_DATA_PATH", tmp_path)
    pipeline_metadata = PipelineMetadata(source="test_source", source_version="v1")
    data_dir = get_source_data_directory(pipeline_metadata)
    data_dir.mkdir(parents=True)
    metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.SOURCE_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    return pipeline_metadata, data_dir, metadata_path


def test_record_download_metadata_writes_on_real_download(source_data_dir):
    """A real download stamps downloaded_at and records the downloaded/skipped file names."""
    pipeline_metadata, data_dir, metadata_path = source_data_dir
    report = DownloadReport(
        downloaded=[data_dir / "fetched.tsv"],
        skipped=[data_dir / "cached.tsv"],
    )

    record_download_metadata(pipeline_metadata, report)

    assert metadata_path.exists()
    metadata = json.loads(metadata_path.read_text())
    assert metadata["source"] == "test_source"
    assert metadata["source_version"] == "v1"
    assert metadata["downloaded"] == ["fetched.tsv"]
    assert metadata["skipped"] == ["cached.tsv"]
    # downloaded_at must be a parseable ISO timestamp
    from datetime import datetime
    datetime.fromisoformat(metadata["downloaded_at"])


def test_record_download_metadata_preserves_on_cache_hit(source_data_dir):
    """When everything is served from cache, the existing metadata (and timestamp) is left intact."""
    pipeline_metadata, data_dir, metadata_path = source_data_dir
    existing = {
        "source": "test_source",
        "source_version": "v1",
        "downloaded_at": "2024-01-15T00:00:00",
        "downloaded": ["fetched.tsv"],
        "skipped": [],
    }
    metadata_path.write_text(json.dumps(existing))

    cache_hit_report = DownloadReport(skipped=[data_dir / "fetched.tsv"])
    record_download_metadata(pipeline_metadata, cache_hit_report)

    assert json.loads(metadata_path.read_text()) == existing


def test_record_download_metadata_none_report_writes_nothing(source_data_dir):
    """A missing report (e.g. no download.yaml) should not create a metadata file or raise."""
    pipeline_metadata, _data_dir, metadata_path = source_data_dir

    record_download_metadata(pipeline_metadata, None)

    assert not metadata_path.exists()


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
