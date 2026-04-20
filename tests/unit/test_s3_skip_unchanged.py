"""Tests for the skip-if-unchanged logic in S3Uploader.

Verifies the fix for the LastModified overwrite bug:
  - Files already on S3 with identical size and content hash must be skipped
  - Files with different size or content must be uploaded
  - Missing local files return "missing" status
  - Multipart ETag computation matches boto3's default (8 MiB chunks)

A FakeS3Client simulates the boto3 S3 client with correct ETag semantics so
we can run the full upload flow without network or credentials.
"""

import hashlib
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from translator_ingest.util.storage.s3 import (
    S3_MULTIPART_CHUNK_SIZE,
    S3Uploader,
    upload_and_cleanup,
)


# ── FakeS3Client ─────────────────────────────────────────────────────────────


def _boto3_style_etag(body: bytes, chunk_size: int = S3_MULTIPART_CHUNK_SIZE) -> str:
    """Reproduce how boto3 would assign an ETag when uploading ``body``.

    Files at or below ``chunk_size`` are uploaded as a single part and the
    ETag is the hex MD5. Larger files are uploaded in parts; the ETag is
    md5(concat(md5(part1), md5(part2), ...)) + "-N".
    """
    if len(body) <= chunk_size:
        return hashlib.md5(body).hexdigest()

    part_digests: list[bytes] = []
    for i in range(0, len(body), chunk_size):
        part_digests.append(hashlib.md5(body[i:i + chunk_size]).digest())
    concat_hex = hashlib.md5(b"".join(part_digests)).hexdigest()
    return f"{concat_hex}-{len(part_digests)}"


class FakeS3Client:
    """In-memory S3 client mock with correct ETag semantics.

    Records every head_object and upload_file call so tests can assert on
    which files were skipped vs uploaded.
    """

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.head_calls: list[str] = []
        self.upload_calls: list[str] = []  # list of S3 keys uploaded

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803 (boto3 uses CapitalCase)
        self.head_calls.append(Key)
        if Key not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadObject",
            )
        obj = self.objects[Key]
        return {"ContentLength": obj["size"], 'ETag': f'"{obj["etag"]}"'}

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:  # noqa: N803
        self.upload_calls.append(Key)
        body = Path(Filename).read_bytes()
        self.objects[Key] = {
            "size": len(body),
            "etag": _boto3_style_etag(body),
        }

    def seed(self, key: str, body: bytes) -> None:
        """Pre-populate a file on the fake S3 (simulates prior upload)."""
        self.objects[key] = {"size": len(body), "etag": _boto3_style_etag(body)}


@pytest.fixture()
def uploader() -> S3Uploader:
    """Create an S3Uploader with a FakeS3Client, bypassing boto3 client init."""
    instance = S3Uploader.__new__(S3Uploader)
    instance.bucket_name = "test-bucket"
    instance.s3_client = FakeS3Client()
    import logging
    instance.logger = logging.getLogger("test")
    return instance


def _write(path: Path, body: bytes) -> Path:
    """Write bytes to ``path`` creating parents, return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path


# ── MD5 / ETag helpers ───────────────────────────────────────────────────────


def test_compute_md5_small_file(tmp_path):
    """_compute_md5 returns the correct hex MD5 of file content."""
    body = b"hello world"
    path = _write(tmp_path / "file.txt", body)

    assert S3Uploader._compute_md5(path) == hashlib.md5(body).hexdigest()


def test_compute_md5_streaming_large_file(tmp_path):
    """_compute_md5 handles files larger than the read chunk size correctly."""
    # 20 MiB, larger than HASH_READ_CHUNK_SIZE (8 MiB)
    body = b"a" * (20 * 1024 * 1024)
    path = _write(tmp_path / "big.bin", body)

    assert S3Uploader._compute_md5(path) == hashlib.md5(body).hexdigest()


def test_compute_multipart_etag_single_part_file(tmp_path):
    """Multipart ETag for a single-chunk file is just the hex MD5 (no -N suffix)."""
    body = b"x" * 1024
    path = _write(tmp_path / "small.bin", body)

    etag = S3Uploader._compute_multipart_etag(path, S3_MULTIPART_CHUNK_SIZE)
    assert etag == hashlib.md5(body).hexdigest()
    assert "-" not in etag


def test_compute_multipart_etag_multi_part_file(tmp_path):
    """Multipart ETag format matches boto3's: md5(concat(part_md5s))-N."""
    chunk_size = S3_MULTIPART_CHUNK_SIZE
    # 3 parts: chunk_size + chunk_size + 1 byte
    body = b"a" * chunk_size + b"b" * chunk_size + b"c"
    path = _write(tmp_path / "multi.bin", body)

    etag = S3Uploader._compute_multipart_etag(path, chunk_size)

    expected_parts = [
        hashlib.md5(b"a" * chunk_size).digest(),
        hashlib.md5(b"b" * chunk_size).digest(),
        hashlib.md5(b"c").digest(),
    ]
    expected_hex = hashlib.md5(b"".join(expected_parts)).hexdigest()
    assert etag == f"{expected_hex}-3"


def test_compute_multipart_etag_matches_fake_s3_etag(tmp_path):
    """Our local multipart ETag must equal what the fake S3 records after upload."""
    # 3 parts of 8 MiB chunks
    body = b"x" * (S3_MULTIPART_CHUNK_SIZE * 2 + 42)
    path = _write(tmp_path / "bigfile.bin", body)

    local_etag = S3Uploader._compute_multipart_etag(path, S3_MULTIPART_CHUNK_SIZE)
    fake_etag = _boto3_style_etag(body)

    assert local_etag == fake_etag


# ── _s3_object_matches ───────────────────────────────────────────────────────


def test_object_matches_returns_false_when_missing_on_s3(uploader, tmp_path):
    """head_object 404 -> object does not match (needs upload)."""
    path = _write(tmp_path / "new.txt", b"hello")

    assert uploader._s3_object_matches("data/new.txt", path) is False


def test_object_matches_true_when_identical(uploader, tmp_path):
    """Local file byte-identical to S3 object -> matches -> skip."""
    body = b"identical content"
    path = _write(tmp_path / "file.txt", body)
    uploader.s3_client.seed("data/file.txt", body)

    assert uploader._s3_object_matches("data/file.txt", path) is True


def test_object_matches_false_on_size_difference(uploader, tmp_path):
    """Different size short-circuits to False without any hash work."""
    path = _write(tmp_path / "file.txt", b"short")
    uploader.s3_client.seed("data/file.txt", b"much longer content")

    assert uploader._s3_object_matches("data/file.txt", path) is False


def test_object_matches_false_on_content_difference_same_size(uploader, tmp_path):
    """Same size but different content -> hash mismatch -> False."""
    path = _write(tmp_path / "file.txt", b"abcdef")
    uploader.s3_client.seed("data/file.txt", b"xyz123")  # same length

    assert uploader._s3_object_matches("data/file.txt", path) is False


def test_object_matches_true_for_multipart_file(uploader, tmp_path):
    """Files larger than the multipart threshold match via multipart ETag."""
    body = b"p" * (S3_MULTIPART_CHUNK_SIZE * 2 + 1024)
    path = _write(tmp_path / "big.bin", body)
    uploader.s3_client.seed("releases/source/big.bin", body)

    assert uploader._s3_object_matches("releases/source/big.bin", path) is True


def test_object_matches_false_when_multipart_etag_uses_different_chunk_size(
    uploader, tmp_path,
):
    """If an existing S3 object was uploaded with a non-default chunk size, the
    ETag won't match our 8 MiB computation and we safely fall back to re-upload."""
    body = b"q" * (S3_MULTIPART_CHUNK_SIZE + 1024)
    path = _write(tmp_path / "file.bin", body)
    # Seed with an ETag computed using a 4 MiB chunk size (not our default 8 MiB)
    other_etag = _boto3_style_etag(body, chunk_size=4 * 1024 * 1024)
    uploader.s3_client.objects["data/file.bin"] = {"size": len(body), "etag": other_etag}

    # Our match check uses 8 MiB chunks; ETag won't match -> False -> re-upload
    assert uploader._s3_object_matches("data/file.bin", path) is False


def test_object_matches_raises_on_non_404_client_error(uploader, tmp_path):
    """A real error (not 404) from head_object must propagate, not silently skip."""
    path = _write(tmp_path / "file.txt", b"data")

    def _raise_access_denied(Bucket, Key):  # noqa: N803
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
            "HeadObject",
        )

    uploader.s3_client.head_object = _raise_access_denied

    with pytest.raises(ClientError):
        uploader._s3_object_matches("data/file.txt", path)


# ── upload_file ──────────────────────────────────────────────────────────────


def test_upload_file_missing_local(uploader, tmp_path):
    """upload_file returns 'missing' when local file does not exist."""
    status = uploader.upload_file(tmp_path / "does-not-exist.txt", "data/foo.txt")

    assert status == "missing"
    assert uploader.s3_client.upload_calls == []


def test_upload_file_new_file_uploads(uploader, tmp_path):
    """upload_file returns 'uploaded' when S3 has no matching object."""
    path = _write(tmp_path / "new.txt", b"hello")

    status = uploader.upload_file(path, "data/new.txt")

    assert status == "uploaded"
    assert uploader.s3_client.upload_calls == ["data/new.txt"]


def test_upload_file_identical_file_skips(uploader, tmp_path):
    """upload_file returns 'skipped' when S3 already has an identical copy.

    This is the core fix for the LastModified overwrite bug.
    """
    body = b"already there"
    path = _write(tmp_path / "file.txt", body)
    uploader.s3_client.seed("data/file.txt", body)

    status = uploader.upload_file(path, "data/file.txt")

    assert status == "skipped"
    assert uploader.s3_client.upload_calls == []  # no re-upload


def test_upload_file_changed_content_reuploads(uploader, tmp_path):
    """upload_file returns 'uploaded' when local content differs from S3."""
    path = _write(tmp_path / "file.txt", b"new content")
    uploader.s3_client.seed("data/file.txt", b"old content")

    status = uploader.upload_file(path, "data/file.txt")

    assert status == "uploaded"
    assert uploader.s3_client.upload_calls == ["data/file.txt"]


# ── upload_directory ─────────────────────────────────────────────────────────


def test_upload_directory_aggregates_skipped_and_uploaded(uploader, tmp_path):
    """upload_directory counts skipped and uploaded files separately."""
    src = tmp_path / "release_dir"
    _write(src / "old_file.txt", b"kept on s3")
    _write(src / "new_file.txt", b"fresh content")
    # Pre-seed one file so it gets skipped; the other is new.
    uploader.s3_client.seed("releases/source/old_file.txt", b"kept on s3")

    stats = uploader.upload_directory(src, "releases/source")

    assert stats["uploaded"] == 1
    assert stats["skipped"] == 1
    assert stats["failed"] == 0
    assert stats["uploaded_files"] == ["releases/source/new_file.txt"]
    assert stats["skipped_files"] == ["releases/source/old_file.txt"]
    assert stats["bytes_transferred"] == len(b"fresh content")
    assert uploader.s3_client.upload_calls == ["releases/source/new_file.txt"]


def test_upload_directory_returns_empty_stats_for_missing_dir(uploader, tmp_path):
    """Missing local directory returns zero counts with all expected keys."""
    stats = uploader.upload_directory(tmp_path / "does-not-exist", "data/x")

    assert stats["uploaded"] == 0
    assert stats["skipped"] == 0
    assert stats["failed"] == 0
    assert stats["uploaded_files"] == []
    assert stats["skipped_files"] == []
    assert stats["failed_files"] == []


def test_upload_directory_handles_client_errors(uploader, tmp_path):
    """upload_directory catches ClientError from upload_file and records a failure."""
    src = tmp_path / "dir"
    _write(src / "a.txt", b"a")

    def _raise(Bucket, Key):  # noqa: N803
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Forbidden"}},
            "HeadObject",
        )

    uploader.s3_client.head_object = _raise

    stats = uploader.upload_directory(src, "data/dir")

    assert stats["failed"] == 1
    assert stats["uploaded"] == 0
    assert stats["skipped"] == 0


# ── Integration: simulated two-build flow ────────────────────────────────────


def test_second_build_preserves_lastmodified_on_unchanged_files(uploader, tmp_path):
    """Simulates two back-to-back builds. On the second run, unchanged files
    are skipped so their LastModified on S3 is preserved.

    This is the exact scenario that was causing release dates to get stomped.
    """
    release_dir = tmp_path / "release_2026_04_08"
    _write(release_dir / "data.tar.zst", b"compressed bytes")
    _write(release_dir / "metadata.json", b'{"release": "2026_04_08"}')

    # First "build": upload all files fresh
    first_stats = uploader.upload_directory(release_dir, "releases/source/2026_04_08")
    assert first_stats["uploaded"] == 2
    assert first_stats["skipped"] == 0

    # Reset call history so we only see the second build's behavior
    uploader.s3_client.upload_calls.clear()
    uploader.s3_client.head_calls.clear()

    # Second "build": same files on disk (cleanup hasn't removed them yet)
    second_stats = uploader.upload_directory(release_dir, "releases/source/2026_04_08")

    assert second_stats["uploaded"] == 0
    assert second_stats["skipped"] == 2
    assert second_stats["bytes_transferred"] == 0
    # No re-upload calls on second run -> LastModified on S3 preserved
    assert uploader.s3_client.upload_calls == []
    # head_object was still called for each file (that's how we check)
    assert len(uploader.s3_client.head_calls) == 2


def test_mixed_new_and_existing_files_handled_correctly(uploader, tmp_path):
    """A release dir with a mix of new and already-uploaded files handles each
    correctly: new files upload, existing unchanged files skip."""
    release_dir = tmp_path / "release"
    _write(release_dir / "old.tar.zst", b"unchanged")
    _write(release_dir / "new.tar.zst", b"brand new")
    uploader.s3_client.seed("releases/src/old.tar.zst", b"unchanged")

    stats = uploader.upload_directory(release_dir, "releases/src")

    assert stats["uploaded"] == 1
    assert stats["skipped"] == 1
    assert uploader.s3_client.upload_calls == ["releases/src/new.tar.zst"]


# ── upload_and_cleanup total_skipped aggregation ─────────────────────────────


def test_upload_and_cleanup_aggregates_total_skipped(tmp_path, monkeypatch):
    """upload_and_cleanup populates total_skipped from per-source stats.

    The print_upload_summary() consumer in upload_s3.py reads this key, so the
    producer must always include it.
    """
    data_dir = tmp_path / "data"
    releases_dir = tmp_path / "releases"
    data_dir.mkdir()
    releases_dir.mkdir()

    source_data = data_dir / "go_cam"
    _write(source_data / "nodes.jsonl", b"node1\nnode2\n")
    _write(source_data / "edges.jsonl", b"edge1\nedge2\n")

    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_DATA_PATH", str(data_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_RELEASES_PATH", str(releases_dir))

    fake_client = FakeS3Client()
    # Pre-seed one file so it gets skipped on the first call
    fake_client.seed("data/go_cam/nodes.jsonl", b"node1\nnode2\n")

    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.boto3.client",
        lambda *_, **__: fake_client,
    )

    results = upload_and_cleanup(
        data_sources=["go_cam"],
        release_sources=[],
        cleanup=False,
        upload_reports=False,  # tested separately, see test_upload_reports_* below
        upload_logs=False,     # tested separately, see test_upload_logs_* below
    )

    assert results["total_uploaded"] == 1  # edges.jsonl
    assert results["total_skipped"] == 1  # nodes.jsonl
    assert results["total_failed"] == 0
    per_source = results["per_source_stats"]["go_cam"]["data_upload"]
    assert per_source["uploaded"] == 1
    assert per_source["skipped"] == 1


# ── Reports upload ───────────────────────────────────────────────────────────


def test_upload_reports_missing_dir_returns_zero_stats(uploader, tmp_path, monkeypatch):
    """upload_reports handles a missing reports directory gracefully."""
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.REPORTS_BASE", tmp_path / "does-not-exist",
    )

    stats = uploader.upload_reports()

    assert stats["uploaded"] == 0
    assert stats["skipped"] == 0
    assert stats["failed"] == 0
    assert uploader.s3_client.upload_calls == []


def test_upload_reports_uploads_all_files_under_reports_prefix(uploader, tmp_path, monkeypatch):
    """upload_reports uploads every file in /reports/ to s3://bucket/reports/."""
    reports_dir = tmp_path / "reports"
    _write(reports_dir / "2026_04_15" / "build-report.json", b'{"status": "ok"}')
    _write(reports_dir / "2026_04_15" / "stages" / "run" / "_summary.json", b'{"stage": "RUN"}')
    _write(reports_dir / "upload-results-latest.json", b'{"latest": true}')
    monkeypatch.setattr("translator_ingest.util.storage.s3.REPORTS_BASE", reports_dir)

    stats = uploader.upload_reports()

    assert stats["uploaded"] == 3
    assert stats["skipped"] == 0
    assert stats["failed"] == 0
    # All S3 keys are prefixed with "reports/"
    assert all(k.startswith("reports/") for k in uploader.s3_client.upload_calls)
    assert "reports/2026_04_15/build-report.json" in uploader.s3_client.upload_calls
    assert "reports/2026_04_15/stages/run/_summary.json" in uploader.s3_client.upload_calls
    assert "reports/upload-results-latest.json" in uploader.s3_client.upload_calls


def test_upload_reports_skips_unchanged_files(uploader, tmp_path, monkeypatch):
    """On second call, identical report files are skipped (no LastModified reset)."""
    reports_dir = tmp_path / "reports"
    _write(reports_dir / "2026_04_15" / "report.json", b'{"unchanged": true}')
    monkeypatch.setattr("translator_ingest.util.storage.s3.REPORTS_BASE", reports_dir)

    # First upload
    first = uploader.upload_reports()
    assert first["uploaded"] == 1
    assert first["skipped"] == 0

    uploader.s3_client.upload_calls.clear()

    # Second upload: same content -> skip
    second = uploader.upload_reports()
    assert second["uploaded"] == 0
    assert second["skipped"] == 1
    assert uploader.s3_client.upload_calls == []


def test_upload_and_cleanup_includes_reports_upload_by_default(tmp_path, monkeypatch):
    """upload_and_cleanup uploads /reports/ when upload_reports=True (the default)."""
    data_dir = tmp_path / "data"
    releases_dir = tmp_path / "releases"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir()
    releases_dir.mkdir()
    _write(reports_dir / "2026_04_15" / "report.json", b'{"build": 1}')

    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_DATA_PATH", str(data_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_RELEASES_PATH", str(releases_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.REPORTS_BASE", reports_dir)

    fake_client = FakeS3Client()
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.boto3.client",
        lambda *_, **__: fake_client,
    )

    results = upload_and_cleanup(
        data_sources=[],
        release_sources=[],
        cleanup=False,
        upload_logs=False,  # logs upload tested separately
    )

    assert results["reports_upload"] is not None
    assert results["reports_upload"]["uploaded"] == 1
    assert results["total_uploaded"] == 1
    # The report file ends up under the reports/ prefix on S3
    assert "reports/2026_04_15/report.json" in fake_client.upload_calls


def test_upload_and_cleanup_skips_reports_when_disabled(tmp_path, monkeypatch):
    """upload_reports=False leaves /reports/ alone (None in results, no upload)."""
    data_dir = tmp_path / "data"
    releases_dir = tmp_path / "releases"
    reports_dir = tmp_path / "reports"
    data_dir.mkdir()
    releases_dir.mkdir()
    _write(reports_dir / "2026_04_15" / "report.json", b'{"build": 1}')

    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_DATA_PATH", str(data_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_RELEASES_PATH", str(releases_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.REPORTS_BASE", reports_dir)

    fake_client = FakeS3Client()
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.boto3.client",
        lambda *_, **__: fake_client,
    )

    results = upload_and_cleanup(
        data_sources=[],
        release_sources=[],
        cleanup=False,
        upload_reports=False,
        upload_logs=False,
    )

    assert results["reports_upload"] is None
    assert fake_client.upload_calls == []


# ── Logs upload ──────────────────────────────────────────────────────────────


def test_upload_logs_missing_dir_returns_zero_stats(uploader, tmp_path, monkeypatch):
    """upload_logs handles a missing logs directory gracefully."""
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.INGESTS_LOGS_PATH", tmp_path / "does-not-exist",
    )

    stats = uploader.upload_logs()

    assert stats["uploaded"] == 0
    assert stats["skipped"] == 0
    assert stats["failed"] == 0
    assert uploader.s3_client.upload_calls == []


def test_upload_logs_uploads_all_files_under_logs_prefix(uploader, tmp_path, monkeypatch):
    """upload_logs uploads every file in /logs/ to s3://bucket/logs/ preserving the tree."""
    logs_dir = tmp_path / "logs"
    _write(logs_dir / "run" / "2026_04_15_070744" / "run.log", b"run log content")
    _write(logs_dir / "merge" / "2026_04_15_070744" / "merge.log", b"merge log content")
    _write(logs_dir / "errors" / "2026_04_15_070744" / "errors.log", b"")
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_LOGS_PATH", logs_dir)

    stats = uploader.upload_logs()

    assert stats["uploaded"] == 3
    assert stats["skipped"] == 0
    assert stats["failed"] == 0
    # All S3 keys are prefixed with "logs/" and preserve the stage/timestamp tree
    assert all(k.startswith("logs/") for k in uploader.s3_client.upload_calls)
    assert "logs/run/2026_04_15_070744/run.log" in uploader.s3_client.upload_calls
    assert "logs/merge/2026_04_15_070744/merge.log" in uploader.s3_client.upload_calls
    assert "logs/errors/2026_04_15_070744/errors.log" in uploader.s3_client.upload_calls


def test_upload_logs_skips_unchanged_timestamped_dirs(uploader, tmp_path, monkeypatch):
    """Timestamped log dirs from prior builds skip on subsequent uploads.

    This is what preserves LastModified on old log directories so they keep
    their historical timestamps instead of getting overwritten each build.
    """
    logs_dir = tmp_path / "logs"
    _write(logs_dir / "run" / "2026_04_15_070744" / "run.log", b"older build log")
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_LOGS_PATH", logs_dir)

    # First build uploads the log
    first = uploader.upload_logs()
    assert first["uploaded"] == 1
    assert first["skipped"] == 0

    uploader.s3_client.upload_calls.clear()

    # Second build: same old log dir still present, plus a new timestamped dir
    _write(logs_dir / "run" / "2026_04_16_020700" / "run.log", b"new build log")
    second = uploader.upload_logs()

    # Only the new log is uploaded; old one is skipped -> LastModified preserved
    assert second["uploaded"] == 1
    assert second["skipped"] == 1
    assert uploader.s3_client.upload_calls == ["logs/run/2026_04_16_020700/run.log"]


def test_upload_and_cleanup_includes_logs_upload_by_default(tmp_path, monkeypatch):
    """upload_and_cleanup uploads /logs/ when upload_logs=True (the default)."""
    data_dir = tmp_path / "data"
    releases_dir = tmp_path / "releases"
    reports_dir = tmp_path / "reports"
    logs_dir = tmp_path / "logs"
    data_dir.mkdir()
    releases_dir.mkdir()
    reports_dir.mkdir()
    _write(logs_dir / "run" / "2026_04_15_070744" / "run.log", b"log content")

    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_DATA_PATH", str(data_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_RELEASES_PATH", str(releases_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.REPORTS_BASE", reports_dir)
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_LOGS_PATH", logs_dir)

    fake_client = FakeS3Client()
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.boto3.client",
        lambda *_, **__: fake_client,
    )

    results = upload_and_cleanup(
        data_sources=[],
        release_sources=[],
        cleanup=False,
        upload_reports=False,  # focus on logs only
    )

    assert results["logs_upload"] is not None
    assert results["logs_upload"]["uploaded"] == 1
    assert "logs/run/2026_04_15_070744/run.log" in fake_client.upload_calls


def test_upload_and_cleanup_skips_logs_when_disabled(tmp_path, monkeypatch):
    """upload_logs=False leaves /logs/ alone (None in results, no upload)."""
    data_dir = tmp_path / "data"
    releases_dir = tmp_path / "releases"
    reports_dir = tmp_path / "reports"
    logs_dir = tmp_path / "logs"
    data_dir.mkdir()
    releases_dir.mkdir()
    reports_dir.mkdir()
    _write(logs_dir / "run" / "2026_04_15_070744" / "run.log", b"log content")

    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_DATA_PATH", str(data_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_RELEASES_PATH", str(releases_dir))
    monkeypatch.setattr("translator_ingest.util.storage.s3.REPORTS_BASE", reports_dir)
    monkeypatch.setattr("translator_ingest.util.storage.s3.INGESTS_LOGS_PATH", logs_dir)

    fake_client = FakeS3Client()
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.boto3.client",
        lambda *_, **__: fake_client,
    )

    results = upload_and_cleanup(
        data_sources=[],
        release_sources=[],
        cleanup=False,
        upload_reports=False,
        upload_logs=False,
    )

    assert results["logs_upload"] is None
    assert fake_client.upload_calls == []
