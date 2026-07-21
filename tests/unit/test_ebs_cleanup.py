"""Tests for EBS storage cleanup functions.

Tests cover:
- cleanup_old_source_versions: keeps latest data version, deletes old ones
- cleanup_old_releases: keeps latest release + 'latest' dir, deletes old ones
- Edge cases: missing dirs, missing metadata files, empty directories
- make cleanup-ebs equivalent behavior
"""

import json
from pathlib import Path

import pytest

from translator_ingest.util.storage.s3 import (
    cleanup_old_source_versions,
    cleanup_old_releases,
)


@pytest.fixture()
def fake_data_dir(tmp_path, monkeypatch):
    """Create a fake /data/{source}/ directory structure with multiple versions."""
    data_path = tmp_path / "data"
    data_path.mkdir()
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_DATA_PATH", str(data_path))
    return data_path


@pytest.fixture()
def fake_releases_dir(tmp_path, monkeypatch):
    """Create a fake /releases/{source}/ directory structure."""
    releases_path = tmp_path / "releases"
    releases_path.mkdir()
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_RELEASES_PATH", str(releases_path))
    return releases_path


def _create_version_dir(source_dir: Path, version: str, size_bytes: int = 1024) -> Path:
    """Helper: create a version directory with a dummy file of given size."""
    version_dir = source_dir / version
    version_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = version_dir / "data.jsonl"
    dummy_file.write_bytes(b"x" * size_bytes)
    return version_dir


def _write_latest_build(source_dir: Path, version: str):
    """Helper: write latest-build.json pointing to given version."""
    metadata = {"source_version": version, "source": source_dir.name}
    (source_dir / "latest-build.json").write_text(json.dumps(metadata))


def _write_latest_release(source_dir: Path, version: str):
    """Helper: write latest-release.json pointing to given version."""
    metadata = {"release_version": version, "source": source_dir.name}
    (source_dir / "latest-release.json").write_text(json.dumps(metadata))


# ── cleanup_old_source_versions ──────────────────────────────────────────────


def test_cleanup_keeps_latest_deletes_old(fake_data_dir):
    """Only the version in latest-build.json is kept; older versions are deleted."""
    source_dir = fake_data_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "v1_old", size_bytes=2048)
    _create_version_dir(source_dir, "v2_old", size_bytes=1024)
    _create_version_dir(source_dir, "v3_current", size_bytes=512)
    _write_latest_build(source_dir, "v3_current")

    result = cleanup_old_source_versions("ctd", keep_latest=True)

    assert result["deleted"] == 2
    assert result["kept"] == 1
    assert result["bytes_freed"] > 0
    assert not (source_dir / "v1_old").exists()
    assert not (source_dir / "v2_old").exists()
    assert (source_dir / "v3_current").exists()


def test_cleanup_keeps_nothing_when_keep_latest_false(fake_data_dir):
    """When keep_latest=False, all version directories are deleted."""
    source_dir = fake_data_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "v1", size_bytes=1024)
    _create_version_dir(source_dir, "v2", size_bytes=1024)
    _write_latest_build(source_dir, "v2")

    result = cleanup_old_source_versions("ctd", keep_latest=False)

    assert result["deleted"] == 2
    assert result["kept"] == 0


def test_cleanup_no_op_when_only_latest_exists(fake_data_dir):
    """No deletion when only the current version directory exists."""
    source_dir = fake_data_dir / "go_cam"
    source_dir.mkdir()

    _create_version_dir(source_dir, "v1_current")
    _write_latest_build(source_dir, "v1_current")

    result = cleanup_old_source_versions("go_cam")

    assert result["deleted"] == 0
    assert result["kept"] == 1
    assert (source_dir / "v1_current").exists()


def test_cleanup_missing_source_dir(fake_data_dir):
    """Returns empty result for non-existent source directory."""
    result = cleanup_old_source_versions("nonexistent")

    assert result["deleted"] == 0
    assert result["kept"] == 0
    assert result["bytes_freed"] == 0


def test_cleanup_missing_latest_build_json(fake_data_dir):
    """Without latest-build.json, all dirs are deleted when keep_latest=True."""
    source_dir = fake_data_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "v1")
    _create_version_dir(source_dir, "v2")
    # No latest-build.json written

    result = cleanup_old_source_versions("ctd", keep_latest=True)

    # Without metadata, current_version is None, so nothing matches "keep"
    assert result["deleted"] == 2
    assert result["kept"] == 0


def test_cleanup_skips_files_only_deletes_dirs(fake_data_dir):
    """Files at the source root (like latest-build.json) are not deleted."""
    source_dir = fake_data_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "v1_old")
    _create_version_dir(source_dir, "v2_current")
    _write_latest_build(source_dir, "v2_current")

    # Add a stray file at source root
    (source_dir / "some_metadata.json").write_text("{}")

    result = cleanup_old_source_versions("ctd")

    assert result["deleted"] == 1
    assert (source_dir / "some_metadata.json").exists()
    assert (source_dir / "latest-build.json").exists()


def test_cleanup_bytes_freed_is_accurate(fake_data_dir):
    """bytes_freed should match the actual size of deleted files."""
    source_dir = fake_data_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "v1_old", size_bytes=4096)
    _create_version_dir(source_dir, "v2_current", size_bytes=1024)
    _write_latest_build(source_dir, "v2_current")

    result = cleanup_old_source_versions("ctd")

    assert result["bytes_freed"] == 4096


def test_cleanup_multiple_sources_independent(fake_data_dir):
    """Cleanup for one source doesn't affect another."""
    for source in ("ctd", "go_cam"):
        source_dir = fake_data_dir / source
        source_dir.mkdir()
        _create_version_dir(source_dir, "old_version")
        _create_version_dir(source_dir, "current_version")
        _write_latest_build(source_dir, "current_version")

    cleanup_old_source_versions("ctd")

    # ctd old is gone
    assert not (fake_data_dir / "ctd" / "old_version").exists()
    # go_cam old is untouched
    assert (fake_data_dir / "go_cam" / "old_version").exists()


# ── cleanup_old_releases ─────────────────────────────────────────────────────


def test_releases_keeps_latest_and_latest_dir(fake_releases_dir):
    """Keeps the 'latest' symlink/dir and current release, deletes old releases."""
    source_dir = fake_releases_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "2026_01_01", size_bytes=2048)
    _create_version_dir(source_dir, "2026_02_01", size_bytes=1024)
    _create_version_dir(source_dir, "2026_03_01", size_bytes=512)
    _create_version_dir(source_dir, "latest", size_bytes=512)
    _write_latest_release(source_dir, "2026_03_01")

    result = cleanup_old_releases("ctd")

    assert result["deleted"] == 2
    assert result["kept"] == 2  # latest dir + current release
    assert not (source_dir / "2026_01_01").exists()
    assert not (source_dir / "2026_02_01").exists()
    assert (source_dir / "2026_03_01").exists()
    assert (source_dir / "latest").exists()


def test_releases_no_op_when_only_latest_exists(fake_releases_dir):
    """No deletion when only current release and 'latest' dir exist."""
    source_dir = fake_releases_dir / "go_cam"
    source_dir.mkdir()

    _create_version_dir(source_dir, "2026_03_01")
    _create_version_dir(source_dir, "latest")
    _write_latest_release(source_dir, "2026_03_01")

    result = cleanup_old_releases("go_cam")

    assert result["deleted"] == 0
    assert result["kept"] == 2


def test_releases_missing_releases_dir(fake_releases_dir):
    """Returns empty result for non-existent source releases directory."""
    result = cleanup_old_releases("nonexistent")

    assert result["deleted"] == 0
    assert result["kept"] == 0


def test_releases_missing_latest_release_json(fake_releases_dir):
    """Without latest-release.json, all dirs except 'latest' are deleted."""
    source_dir = fake_releases_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "2026_01_01")
    _create_version_dir(source_dir, "2026_02_01")
    _create_version_dir(source_dir, "latest")

    result = cleanup_old_releases("ctd")

    assert result["deleted"] == 2
    assert result["kept"] == 1  # only 'latest' kept
    assert (source_dir / "latest").exists()


def test_releases_skips_files_only_deletes_dirs(fake_releases_dir):
    """Files like latest-release.json are not deleted."""
    source_dir = fake_releases_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "2026_01_01")
    _create_version_dir(source_dir, "2026_03_01")
    _write_latest_release(source_dir, "2026_03_01")

    result = cleanup_old_releases("ctd")

    assert result["deleted"] == 1
    assert (source_dir / "latest-release.json").exists()


def test_releases_bytes_freed_is_accurate(fake_releases_dir):
    """bytes_freed should match the actual size of deleted files."""
    source_dir = fake_releases_dir / "ctd"
    source_dir.mkdir()

    _create_version_dir(source_dir, "2026_01_01", size_bytes=8192)
    _create_version_dir(source_dir, "2026_03_01", size_bytes=1024)
    _write_latest_release(source_dir, "2026_03_01")

    result = cleanup_old_releases("ctd")

    assert result["bytes_freed"] == 8192


def test_releases_nested_files_in_old_release(fake_releases_dir):
    """Old releases with nested subdirectories are fully removed."""
    source_dir = fake_releases_dir / "ctd"
    source_dir.mkdir()

    old_dir = source_dir / "2026_01_01"
    old_dir.mkdir()
    (old_dir / "subdir").mkdir()
    (old_dir / "subdir" / "nodes.jsonl").write_bytes(b"x" * 1000)
    (old_dir / "subdir" / "edges.jsonl").write_bytes(b"y" * 2000)
    (old_dir / "archive.tar.zst").write_bytes(b"z" * 5000)

    _create_version_dir(source_dir, "2026_03_01")
    _write_latest_release(source_dir, "2026_03_01")

    result = cleanup_old_releases("ctd")

    assert result["deleted"] == 1
    assert result["bytes_freed"] == 8000
    assert not old_dir.exists()


# ── Combined cleanup flow (simulates make cleanup-ebs) ──────────────────────


def test_full_cleanup_flow(fake_data_dir, fake_releases_dir):
    """Both data and release cleanup work together for a source."""
    source = "ctd"

    # Set up data with old versions
    data_dir = fake_data_dir / source
    data_dir.mkdir()
    _create_version_dir(data_dir, "v1_old", size_bytes=4096)
    _create_version_dir(data_dir, "v2_current", size_bytes=1024)
    _write_latest_build(data_dir, "v2_current")

    # Set up releases with old versions
    rel_dir = fake_releases_dir / source
    rel_dir.mkdir()
    _create_version_dir(rel_dir, "2026_01_01", size_bytes=8192)
    _create_version_dir(rel_dir, "2026_03_01", size_bytes=2048)
    _create_version_dir(rel_dir, "latest", size_bytes=2048)
    _write_latest_release(rel_dir, "2026_03_01")

    # Run both cleanups (simulates make cleanup-ebs loop body)
    data_result = cleanup_old_source_versions(source)
    release_result = cleanup_old_releases(source)

    total_freed = data_result["bytes_freed"] + release_result["bytes_freed"]

    assert data_result["deleted"] == 1
    assert release_result["deleted"] == 1
    assert total_freed == 4096 + 8192

    # Verify only latest kept
    assert (data_dir / "v2_current").exists()
    assert not (data_dir / "v1_old").exists()
    assert (rel_dir / "2026_03_01").exists()
    assert (rel_dir / "latest").exists()
    assert not (rel_dir / "2026_01_01").exists()


@pytest.mark.parametrize("source", ["ctd", "go_cam", "semmeddb", "chembl"])
def test_cleanup_is_source_independent(fake_data_dir, source):
    """Each source cleanup only touches its own directory."""
    # Create dirs for target source and a neighbor
    for s in [source, "neighbor"]:
        d = fake_data_dir / s
        d.mkdir()
        _create_version_dir(d, "old_v")
        _create_version_dir(d, "current_v")
        _write_latest_build(d, "current_v")

    cleanup_old_source_versions(source)

    assert not (fake_data_dir / source / "old_v").exists()
    assert (fake_data_dir / "neighbor" / "old_v").exists()
