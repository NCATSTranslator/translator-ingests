"""
Tests for the per-stage incremental log upload helper.

Pins the contract that the orchestrator relies on:

  * upload_stage_logs(stage, timestamp) maps to a specific local directory
    (logs/{stage}/{timestamp}/) and a specific S3 prefix
    (logs/{stage}/{timestamp}/) so per-stage uploads land alongside the
    existing build-wide upload_logs() output without overwriting each other.

  * Missing local directories return a zero-counts result rather than
    raising — a stage that was skipped (e.g. because memory was critical)
    will not have produced a log directory, and the orchestrator should
    keep moving instead of crashing on the post-stage upload.

S3 itself is not exercised here. A recording fake stands in for boto3 so
the test runs offline and pins the path layout / dispatch contract. End-to-
end S3 behaviour is covered by the integration suite.
"""

import logging
from pathlib import Path
from typing import Any

from translator_ingest.util.storage.s3 import S3Uploader


class _RecordingUploader(S3Uploader):
    """S3Uploader subclass that records upload_directory calls instead of
    talking to S3. Used to pin the per-stage helper's local-path / s3-prefix
    contract without touching the network."""

    def __init__(self) -> None:
        # skip the real __init__ — we don't want a boto3 client in tests.
        # populate the attributes upload_stage_logs reads (bucket_name + logger)
        # so the method runs without crashing.
        self.calls: list[tuple[Path, str]] = []
        self.bucket_name = "test-bucket"
        self.logger = logging.getLogger("test-recording-uploader")

    def upload_directory(self, local_dir: Path, s3_prefix: str) -> dict[str, Any]:
        self.calls.append((local_dir, s3_prefix))
        return {
            "uploaded": 1,
            "skipped": 0,
            "failed": 0,
            "bytes_transferred": 100,
            "uploaded_files": ["fake.log"],
            "skipped_files": [],
            "failed_files": [],
        }


def test_upload_stage_logs_targets_specific_stage_timestamp(tmp_path, monkeypatch):
    # per-stage upload narrows the existing logs/ tree to one stage so each
    # stage transition can flush its own logs without re-walking the whole
    # log tree for every stage
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.INGESTS_LOGS_PATH",
        str(tmp_path),
    )

    stage = "run"
    timestamp = "2026_05_24_120000"
    log_dir = tmp_path / stage / timestamp
    log_dir.mkdir(parents=True)
    (log_dir / "run.log").write_text("some log content")

    uploader = _RecordingUploader()
    stats = uploader.upload_stage_logs(stage=stage, timestamp=timestamp)

    assert len(uploader.calls) == 1
    local, s3_prefix = uploader.calls[0]
    assert local == log_dir
    # s3 layout mirrors local — logs/{stage}/{timestamp}/ lands at
    # s3://bucket/logs/{stage}/{timestamp}/ so the build-wide upload_logs()
    # and the per-stage incremental upload produce identical paths
    assert s3_prefix == f"logs/{stage}/{timestamp}"
    assert stats["uploaded"] == 1


def test_upload_stage_logs_missing_directory_returns_zero_counts(tmp_path, monkeypatch):
    # a skipped stage (memory-critical, hard-stop on missing-fallback, etc.)
    # never creates its log directory; the upload helper must not crash in
    # that case — the orchestrator calls it unconditionally in a finally block
    monkeypatch.setattr(
        "translator_ingest.util.storage.s3.INGESTS_LOGS_PATH",
        str(tmp_path),
    )

    uploader = _RecordingUploader()
    stats = uploader.upload_stage_logs(stage="merge", timestamp="never_ran")

    assert uploader.calls == []  # nothing to upload, no S3 call
    assert stats == {
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
        "bytes_transferred": 0,
        "uploaded_files": [],
        "skipped_files": [],
        "failed_files": [],
    }
