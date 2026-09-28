# end-to-end check that S3Uploader.upload_file preserves the S3 LastModified
# timestamp for files whose content has not changed. the unit suite
# (test_s3_skip_unchanged.py) pins the logic against a fake boto3 client;
# this test goes one step further and verifies the behavior against real S3.
#
# pins the contract called out in the kgx_storage refactor #351:
#   - re-uploading an unchanged file is a no-op (LastModified stays put)
#   - re-uploading after one byte changes does push a new object (LastModified
#     advances)
#
# gated behind @pytest.mark.integration so default `make test` runs skip it.
# enable with: uv run pytest -m integration tests/integration/
#
# requires:
#   - AWS credentials in the environment (IRSA in K8s, IAM role on EC2, or
#     ~/.aws/credentials locally)
#   - env var TRANSLATOR_INGESTS_S3_TEST_BUCKET set to a bucket the credentials
#     can write to. tests use a unique key prefix per-run so multiple
#     concurrent runs don't stomp each other and so leftover artifacts are
#     easy to clean up.
import os
import time
import uuid
from pathlib import Path

import boto3
import pytest

from translator_ingest.util.storage.s3 import S3Uploader


# read the test bucket from env so the test doesn't hard-code a bucket name
# that doesn't exist in everyone's account. if the env var is missing we
# skip rather than fail — the integration test is opt-in
_TEST_BUCKET = os.environ.get("TRANSLATOR_INGESTS_S3_TEST_BUCKET")
_NEEDS_BUCKET = pytest.mark.skipif(
    not _TEST_BUCKET,
    reason="set TRANSLATOR_INGESTS_S3_TEST_BUCKET to run this integration test",
)


@pytest.fixture
def s3_uploader() -> S3Uploader:
    # constructs a real boto3 client; the test will fail if no creds are
    # available, which is the right failure mode for an integration test
    return S3Uploader(bucket_name=_TEST_BUCKET)


@pytest.fixture
def test_key_prefix() -> str:
    # unique prefix per test run so parallel runs don't collide and leftover
    # artifacts from this run are easy to identify in the bucket
    return f"_integration_tests/{uuid.uuid4().hex[:8]}"


def _head_last_modified(bucket: str, key: str) -> float:
    client = boto3.client("s3")
    response = client.head_object(Bucket=bucket, Key=key)
    return response["LastModified"].timestamp()


def _delete_key(bucket: str, key: str) -> None:
    boto3.client("s3").delete_object(Bucket=bucket, Key=key)


@pytest.mark.integration
@_NEEDS_BUCKET
def test_lastmodified_preserved_on_unchanged_reupload(
    tmp_path: Path,
    s3_uploader: S3Uploader,
    test_key_prefix: str,
) -> None:
    # write a file, upload it, capture LastModified.
    sample = tmp_path / "sample.txt"
    sample.write_bytes(b"unchanged content")
    s3_key = f"{test_key_prefix}/sample.txt"

    try:
        first_status = s3_uploader.upload_file(sample, s3_key)
        assert first_status == "uploaded"

        first_modified = _head_last_modified(_TEST_BUCKET, s3_key)

        # sleep a couple seconds so any new PutObject would show a clearly
        # different timestamp — without this an aggressive optimization on
        # S3's side could in principle return the same second
        time.sleep(2)

        # re-upload identical content; the skip-if-unchanged path should fire
        second_status = s3_uploader.upload_file(sample, s3_key)
        assert second_status == "skipped"

        second_modified = _head_last_modified(_TEST_BUCKET, s3_key)

        # the core contract: an unchanged file's LastModified does not advance
        assert second_modified == first_modified
    finally:
        _delete_key(_TEST_BUCKET, s3_key)


@pytest.mark.integration
@_NEEDS_BUCKET
def test_lastmodified_advances_when_content_changes(
    tmp_path: Path,
    s3_uploader: S3Uploader,
    test_key_prefix: str,
) -> None:
    # upload original content, then modify by one byte and re-upload.
    # LastModified MUST change on the second upload — that's how downstream
    # consumers detect new data.
    sample = tmp_path / "sample.txt"
    sample.write_bytes(b"original")
    s3_key = f"{test_key_prefix}/sample.txt"

    try:
        s3_uploader.upload_file(sample, s3_key)
        first_modified = _head_last_modified(_TEST_BUCKET, s3_key)

        time.sleep(2)

        # one-byte change should bust the size+ETag match and trigger a real
        # upload, advancing LastModified
        sample.write_bytes(b"original2")
        second_status = s3_uploader.upload_file(sample, s3_key)
        assert second_status == "uploaded"

        second_modified = _head_last_modified(_TEST_BUCKET, s3_key)
        assert second_modified > first_modified
    finally:
        _delete_key(_TEST_BUCKET, s3_key)
