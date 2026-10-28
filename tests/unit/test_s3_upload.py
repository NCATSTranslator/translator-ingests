"""Tests for the S3 upload logic that does not need S3: ETags, release upload order and release overwrite checks."""
import hashlib
import json
from pathlib import Path

import pytest

import translator_ingest.util.storage.s3 as s3_storage
from translator_ingest.util.storage.local import IngestFileName
from translator_ingest.util.storage.s3 import (
    LATEST_RELEASE_DIR_NAME,
    check_release_not_overwritten,
    compute_s3_etag,
    order_release_files,
    read_release_build_version,
    release_version_dirs,
)

SOURCE = "ctd"
RELEASE_FILE_NAMES = ("ctd.tar.zst", "graph-metadata.json", IngestFileName.RELEASE_METADATA_FILE, "test-data.json")

# MD5 of empty content, which is also the ETag S3 gives an empty object
EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e"


def _expected_multipart_etag(content: bytes, part_size: int) -> str:
    """The multipart ETag as AWS documents it, computed from the parts of content in memory."""
    parts = [content[start:start + part_size] for start in range(0, len(content), part_size)]
    digests = b"".join(hashlib.md5(part).digest() for part in parts)
    return f"{hashlib.md5(digests).hexdigest()}-{len(parts)}"


def _make_source_releases(releases_dir: Path, release_versions: list[str]) -> Path:
    """Write a releases directory for SOURCE with the given release versions, a latest/ copy and latest-release.json."""
    source_releases_dir = releases_dir / SOURCE
    for release_dir_name in [*release_versions, LATEST_RELEASE_DIR_NAME]:
        release_dir = source_releases_dir / release_dir_name
        release_dir.mkdir(parents=True)
        for file_name in RELEASE_FILE_NAMES:
            (release_dir / file_name).write_text(file_name)
    (source_releases_dir / IngestFileName.LATEST_RELEASE_FILE).write_text("{}")
    return source_releases_dir


def test_compute_s3_etag_of_empty_file(tmp_path):
    empty_file = tmp_path / "empty"
    empty_file.write_bytes(b"")
    assert compute_s3_etag(empty_file) == EMPTY_MD5


def test_compute_s3_etag_single_request_is_md5(tmp_path):
    local_file = tmp_path / "hello"
    local_file.write_bytes(b"hello")
    assert compute_s3_etag(local_file) == "5d41402abc4b2a76b9719d911017c592"


@pytest.mark.parametrize(
    ("content_length", "part_size"),
    [
        (10, 4),  # last part shorter than the others
        (12, 4),  # parts all the same size
        (4, 4),  # one part: a multipart ETag still ends with "-1", unlike a single request ETag
        (3, 4),  # one part shorter than the part size
    ],
)
@pytest.mark.parametrize("hash_read_size", [1, 3, 1024])
def test_compute_s3_etag_multipart(tmp_path, monkeypatch, content_length, part_size, hash_read_size):
    # Reading less than a part at a time must not change the ETag
    monkeypatch.setattr(s3_storage, "HASH_READ_SIZE", hash_read_size)
    content = bytes(range(content_length))
    local_file = tmp_path / "parts"
    local_file.write_bytes(content)
    assert compute_s3_etag(local_file, part_size) == _expected_multipart_etag(content, part_size)


def test_release_version_dirs_leaves_out_latest_and_files(tmp_path):
    source_releases_dir = _make_source_releases(tmp_path, ["1.0.4", "1.0.3"])
    assert [path.name for path in release_version_dirs(source_releases_dir)] == ["1.0.3", "1.0.4"]


def test_order_release_files_uploads_pointers_after_releases(tmp_path):
    source_releases_dir = _make_source_releases(tmp_path, ["1.0.3", "1.0.4"])

    ordered = [path.relative_to(source_releases_dir).as_posix() for path in order_release_files(source_releases_dir)]

    expected = []
    for release_dir_name in ["1.0.3", "1.0.4", LATEST_RELEASE_DIR_NAME]:
        expected += [f"{release_dir_name}/ctd.tar.zst",
                     f"{release_dir_name}/graph-metadata.json",
                     f"{release_dir_name}/test-data.json",
                     f"{release_dir_name}/{IngestFileName.RELEASE_METADATA_FILE}"]
    expected.append(IngestFileName.LATEST_RELEASE_FILE)
    assert ordered == expected


def test_read_release_build_version():
    release_metadata = {"source": SOURCE, "build_version": "ctd_build_b", "release_version": "1.0.4"}
    assert read_release_build_version(json.dumps(release_metadata).encode()) == "ctd_build_b"


@pytest.mark.parametrize(
    ("local_build_version", "published_build_version"),
    [
        ("ctd_build_b", None),  # never published
        ("ctd_build_b", "ctd_build_b"),  # published from the same build, uploading it again changes nothing
        (None, None),  # incomplete local release that was never published
    ],
)
def test_check_release_not_overwritten_allows(local_build_version, published_build_version):
    check_release_not_overwritten(SOURCE, "1.0.4", local_build_version, published_build_version)


@pytest.mark.parametrize(
    ("local_build_version", "published_build_version"),
    [
        ("ctd_build_b", "ctd_build_a"),  # versioning restarted, like after latest-release.json was lost
        (None, "ctd_build_a"),  # incomplete local release would overwrite a published one
    ],
)
def test_check_release_not_overwritten_refuses(local_build_version, published_build_version):
    with pytest.raises(ValueError, match="Release 1.0.4 of ctd is already on S3 from build ctd_build_a"):
        check_release_not_overwritten(SOURCE, "1.0.4", local_build_version, published_build_version)
