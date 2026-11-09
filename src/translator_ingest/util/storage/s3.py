"""
S3 storage component for translator-ingests.

This module provides simple, elegant S3 upload functionality for syncing
local /data and /releases directories to S3 bucket with EBS cleanup.
Data sources and release sources are handled as separate, independent lists.

Files S3 already has with the same content are not uploaded again, so their
LastModified on S3 stays at the time they were really uploaded. A release
version that is already on S3 is never overwritten by a release made from a
different build, and latest/ and latest-release.json are uploaded only after
the releases they point at.

The S3 bucket name is sourced from the ``S3_BUCKET_NAME`` environment variable,
falling back to ``translator-ingests``. Override per call by passing
``bucket_name=...``.

Requirements:
    - Must run on EC2 instance with IAM role granting S3 permissions
    - Required permissions: s3:PutObject, s3:GetObject, s3:ListBucket, s3:DeleteObject

Usage:
    from translator_ingest.util.storage.s3 import S3Uploader, upload_and_cleanup

    # Upload single source (low-level)
    uploader = S3Uploader()
    uploader.upload_source_data("go_cam")
    uploader.upload_source_releases("go_cam")

    # Upload separate data and release source lists (high-level)
    upload_and_cleanup(
        data_sources=["ctd", "go_cam", "ncbigene"],
        release_sources=["translator_kg", "ctd", "go_cam"],
        cleanup=True
    )
"""

import hashlib
import json
import os
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Literal, TypedDict

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

from translator_ingest import INGESTS_DATA_PATH, INGESTS_RELEASES_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import get_existing_release_build_version, IngestFileName

logger = get_logger(__name__)

DEFAULT_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "translator-ingests")

# Directory next to the release versions of a source that holds a copy of its latest release
LATEST_RELEASE_DIR_NAME = "latest"

# How much of a file is read at once when hashing it, so hashing uses the same memory for any file size
HASH_READ_SIZE = 8 * 1024 * 1024

# Errors of one S3 request that count as a failed file instead of stopping the whole upload: an error response from
# S3, a failed upload (boto3 raises it as S3UploadFailedError, which is not a ClientError) and network errors left
# after botocore's retries. Missing credentials or invalid parameters are not caught, they are not about one file.
S3_REQUEST_ERRORS = (ClientError, S3UploadFailedError, BotoConnectionError, HTTPClientError)

# Error codes S3 returns for an object that does not exist
S3_NOT_FOUND_ERROR_CODES = frozenset({"404", "NoSuchKey", "NotFound"})

# What S3Uploader.upload_file did with a file
UploadStatus = Literal["uploaded", "skipped", "missing"]


class UploadStats(TypedDict):
    """Result of uploading a set of files. Files are listed by S3 key, failed files by local path."""

    uploaded: int
    skipped: int
    failed: int
    bytes_transferred: int
    uploaded_files: list[str]
    skipped_files: list[str]
    failed_files: list[str]


def empty_upload_stats() -> UploadStats:
    """Return the statistics of an upload that had nothing to upload."""
    return UploadStats(uploaded=0, skipped=0, failed=0, bytes_transferred=0,
                       uploaded_files=[], skipped_files=[], failed_files=[])


def _is_not_found(error: ClientError) -> bool:
    """Check whether an S3 error means the requested object does not exist."""
    return error.response.get("Error", {}).get("Code") in S3_NOT_FOUND_ERROR_CODES


def list_files(directory: Path) -> list[Path]:
    """List every file under a directory, sorted, without following symlinked directories."""
    return [root / name for root, _, names in sorted(directory.walk()) for name in sorted(names)]


def _part_md5_digests(local_path: Path, part_size: int) -> list[bytes]:
    """Return the MD5 digest of each consecutive part_size byte part of a file. The last part may be shorter."""
    digests: list[bytes] = []
    with local_path.open("rb") as local_file:
        while True:
            part_md5 = hashlib.md5(usedforsecurity=False)
            part_length = 0
            while part_length < part_size:
                chunk = local_file.read(min(HASH_READ_SIZE, part_size - part_length))
                if not chunk:
                    break
                part_md5.update(chunk)
                part_length += len(chunk)
            if part_length == 0:
                return digests
            digests.append(part_md5.digest())


def compute_s3_etag(local_path: Path, part_size: int | None = None) -> str:
    """Compute the ETag S3 gives a file uploaded in one request, or uploaded in parts of part_size bytes.

    The ETag of an object uploaded in one request is the MD5 of its content. The ETag of an object uploaded in
    parts is the MD5 of the concatenated MD5 digests of its parts, followed by "-" and the number of parts.

    Args:
        local_path: File to compute the ETag of
        part_size: Size of each part of a multipart upload, or None for an upload in one request

    Returns:
        The ETag, without the quotes S3 puts around it
    """
    if part_size is None:
        file_md5 = hashlib.md5(usedforsecurity=False)
        with local_path.open("rb") as local_file:
            while chunk := local_file.read(HASH_READ_SIZE):
                file_md5.update(chunk)
        return file_md5.hexdigest()
    part_digests = _part_md5_digests(local_path, part_size)
    return f"{hashlib.md5(b''.join(part_digests), usedforsecurity=False).hexdigest()}-{len(part_digests)}"


def release_version_dirs(source_releases_dir: Path) -> list[Path]:
    """Return the release version directories of a source, sorted, without its latest/ copy."""
    return sorted(path for path in source_releases_dir.iterdir()
                  if path.is_dir() and path.name != LATEST_RELEASE_DIR_NAME)


def order_release_files(source_releases_dir: Path) -> list[Path]:
    """List the release files of a source in the order they have to be uploaded to S3.

    Nothing may reach S3 before what it points at. Release version directories go first, then the latest/ copy,
    then latest-release.json and any other file next to them. Inside each release directory release-metadata.json
    goes last, because S3 having it is what marks the release as published (see published_release_build_version).

    Args:
        source_releases_dir: Local releases directory of one source, like releases/ctd

    Returns:
        Every file under source_releases_dir, in upload order
    """
    release_dirs = release_version_dirs(source_releases_dir)
    latest_dir = source_releases_dir / LATEST_RELEASE_DIR_NAME
    if latest_dir.is_dir():
        release_dirs.append(latest_dir)

    ordered_files: list[Path] = []
    for release_dir in release_dirs:
        # sorted() is stable, so this only moves release-metadata.json to the end
        ordered_files += sorted(list_files(release_dir),
                                key=lambda path: path.name == IngestFileName.RELEASE_METADATA_FILE)
    ordered_files += [path for path in list_files(source_releases_dir) if path.parent == source_releases_dir]
    return ordered_files


def read_release_build_version(release_metadata_json: str | bytes) -> str | None:
    """Return the build version recorded in the content of a release-metadata.json file."""
    return PipelineMetadata.from_dict(json.loads(release_metadata_json)).build_version


def check_release_not_overwritten(source: str,
                                  release_version: str,
                                  local_build_version: str | None,
                                  published_build_version: str | None) -> None:
    """Fail if uploading a local release would overwrite a release on S3 that was made from a different build.

    A release version has to mean the same data forever. Release versions are numbered from the local
    latest-release.json, so if that file is lost, like on a new disk, numbering starts again at 1.0.0 and the
    upload would overwrite releases that were already published under those versions.

    Args:
        source: Source the release belongs to
        release_version: Version of the release, like "1.0.4"
        local_build_version: Build the local release was made from, None if its metadata names none
        published_build_version: Build of the release with this version on S3, None if it was never published

    >>> check_release_not_overwritten("ctd", "1.0.4", local_build_version="ctd_b", published_build_version=None)
    >>> check_release_not_overwritten("ctd", "1.0.4", local_build_version="ctd_b", published_build_version="ctd_b")
    >>> check_release_not_overwritten("ctd", "1.0.4", local_build_version="ctd_b",
    ...                               published_build_version="ctd_a")  # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ValueError: Release 1.0.4 of ctd is already on S3 from build ctd_a, but the local release 1.0.4 is from build ...
    """
    if published_build_version is not None and local_build_version != published_build_version:
        raise ValueError(
            f"Release {release_version} of {source} is already on S3 from build {published_build_version}, but the "
            f"local release {release_version} is from build {local_build_version}. Published releases are never "
            f"overwritten. The local latest-release.json of {source} is probably missing or out of date: restore it "
            f"from S3, delete the local release {release_version} and release {source} again."
        )


class S3Uploader:
    """S3 uploader for translator-ingests data and releases.

    Provides rsync-like upload functionality: a file is uploaded unless S3 already has the same content at its key.
    Designed to run on EC2 instance with IAM role permissions.
    """

    def __init__(self, bucket_name: str = DEFAULT_BUCKET_NAME):
        """Initialize S3 uploader with EC2 IAM role credentials.

        Args:
            bucket_name: S3 bucket name (default: ``DEFAULT_BUCKET_NAME``)
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.logger = logger

    def _s3_object_matches(self, local_path: Path, s3_key: str) -> bool:
        """Check whether S3 already has the same content as local_path at s3_key.

        Compares the sizes, then the ETag of the S3 object with the ETag computed for the local file. For an object
        that was uploaded in parts, the local ETag is computed with the part size S3 reports for its first part.
        """
        try:
            head = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as error:
            if _is_not_found(error):
                return False
            raise

        if head["ContentLength"] != local_path.stat().st_size:
            return False

        s3_etag = head["ETag"].strip('"')
        part_size = None
        if "-" in s3_etag:
            first_part = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key, PartNumber=1)
            part_size = first_part["ContentLength"]
        return s3_etag == compute_s3_etag(local_path, part_size)

    def upload_file(self, local_path: Path, s3_key: str) -> UploadStatus:
        """Upload single file to S3, unless S3 already has the same content at s3_key.

        Not uploading unchanged files keeps their LastModified on S3 at the time they were really uploaded, so a
        release on S3 shows when it was released, not when a later build last uploaded it again.

        Args:
            local_path: Local file path to upload
            s3_key: S3 object key (path in bucket)

        Returns:
            "uploaded", "skipped" if S3 already had the same content, or "missing" if local_path does not exist
        """
        if not local_path.exists():
            self.logger.warning(f"File not found, skipping: {local_path}")
            return "missing"

        if self._s3_object_matches(local_path, s3_key):
            self.logger.info(f"Unchanged on S3, not uploading: s3://{self.bucket_name}/{s3_key}")
            return "skipped"

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Uploading {local_path.name} ({file_size_mb:.2f} MB) to s3://{self.bucket_name}/{s3_key}")

        self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
        self.logger.info(f"Uploaded: {s3_key}")
        return "uploaded"

    def upload_files(self, local_paths: list[Path], local_dir: Path, s3_prefix: str) -> UploadStats:
        """Upload files in the given order, each to s3_prefix followed by its path relative to local_dir.

        Stops at the first file that fails, so a file later in the order, like latest-release.json after the
        release it points at, never reaches S3 without everything before it.

        Args:
            local_paths: Files to upload, in upload order, all under local_dir
            local_dir: Local directory that corresponds to s3_prefix
            s3_prefix: S3 prefix (directory path in bucket)

        Returns:
            Upload statistics
        """
        stats = empty_upload_stats()
        for local_path in local_paths:
            s3_key = f"{s3_prefix}/{local_path.relative_to(local_dir).as_posix()}"
            status: UploadStatus | None
            try:
                status = self.upload_file(local_path, s3_key)
            except S3_REQUEST_ERRORS as error:
                self.logger.error(f"Failed to upload {local_path}: {error}")
                status = None

            if status == "uploaded":
                stats['uploaded'] += 1
                stats['bytes_transferred'] += local_path.stat().st_size
                stats['uploaded_files'].append(s3_key)
            elif status == "skipped":
                stats['skipped'] += 1
                stats['skipped_files'].append(s3_key)
            else:
                stats['failed'] += 1
                stats['failed_files'].append(str(local_path))
                self.logger.error(f"Not uploading the rest of {local_dir} because {local_path} failed")
                break

        self.logger.info(f"Upload of {local_dir} complete: {stats['uploaded']} uploaded, "
                         f"{stats['skipped']} unchanged, {stats['failed']} failed, "
                         f"{stats['bytes_transferred'] / (1024 * 1024):.2f} MB transferred")
        return stats

    def upload_directory(self, local_dir: Path, s3_prefix: str) -> UploadStats:
        """Recursively upload directory to S3, skipping files S3 already has.

        Args:
            local_dir: Local directory to upload
            s3_prefix: S3 prefix (directory path in bucket)

        Returns:
            Upload statistics
        """
        if not local_dir.exists():
            self.logger.warning(f"Directory not found, skipping: {local_dir}")
            return empty_upload_stats()

        self.logger.info(f"Uploading directory: {local_dir} -> s3://{self.bucket_name}/{s3_prefix}")
        return self.upload_files(list_files(local_dir), local_dir, s3_prefix)

    def upload_source_data(self, source: str) -> UploadStats:
        """Upload /data/{source}/ directory to S3.

        Uploads entire source data directory including:
        - source_data/ (raw input files)
        - {source_version}/ directories
        - transform and normalization outputs
        - all metadata files

        Args:
            source: Source name (e.g., 'go_cam', 'ctd')

        Returns:
            Upload statistics
        """
        local_dir = Path(INGESTS_DATA_PATH) / source
        s3_prefix = f"data/{source}"

        if not local_dir.exists():
            self.logger.warning(f"Source data directory not found: {local_dir}")
            return empty_upload_stats()

        self.logger.info(f"Uploading source data for {source}...")
        return self.upload_directory(local_dir, s3_prefix)

    def upload_source_releases(self, source: str) -> UploadStats:
        """Upload /releases/{source}/ directory to S3.

        Uploads entire releases directory including:
        - {release_version}/ directories with tar.zst archives
        - latest/ directory
        - all metadata files

        Files are uploaded in the order of order_release_files, so latest/ and latest-release.json never point at a
        release that is not completely on S3.

        Args:
            source: Source name (e.g., 'go_cam', 'ctd')

        Returns:
            Upload statistics
        """
        local_dir = Path(INGESTS_RELEASES_PATH) / source
        s3_prefix = f"releases/{source}"

        if not local_dir.exists():
            self.logger.warning(f"Source releases directory not found: {local_dir}")
            return empty_upload_stats()

        self.logger.info(f"Uploading releases for {source}...")
        return self.upload_files(order_release_files(local_dir), local_dir, s3_prefix)

    def upload_release_summary(self) -> UploadStats:
        """Upload /releases/latest-release-summary.json to S3.

        Returns:
            Upload statistics, where a missing local summary counts as failed
        """
        releases_dir = Path(INGESTS_RELEASES_PATH)
        self.logger.info("Uploading release summary...")
        return self.upload_files([releases_dir / "latest-release-summary.json"], releases_dir, "releases")

    def published_release_build_version(self, source: str, release_version: str) -> str | None:
        """Return the build a release on S3 was made from, or None if that release version was never published.

        A release counts as published once its release-metadata.json is on S3, which is the last file of a release
        to be uploaded.

        Args:
            source: Source name (e.g., 'go_cam', 'ctd')
            release_version: Version of the release, like "1.0.4"
        """
        s3_key = f"releases/{source}/{release_version}/{IngestFileName.RELEASE_METADATA_FILE}"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
        except ClientError as error:
            if _is_not_found(error):
                return None
            raise
        return read_release_build_version(response["Body"].read())

    def check_releases_not_overwritten(self, source: str) -> None:
        """Fail if any local release of a source would overwrite a release on S3 made from a different build.

        Args:
            source: Source name (e.g., 'go_cam', 'ctd')
        """
        source_releases_dir = Path(INGESTS_RELEASES_PATH) / source
        if not source_releases_dir.exists():
            return

        for release_dir in release_version_dirs(source_releases_dir):
            check_release_not_overwritten(
                source,
                release_dir.name,
                local_build_version=get_existing_release_build_version(release_dir),
                published_build_version=self.published_release_build_version(source, release_dir.name),
            )


def cleanup_old_source_versions(source: str, keep_latest: bool = True) -> dict:
    """Delete old /data/{source}/{old_versions}/ directories from EBS.

    Keeps only the version specified in latest-build.json.

    Args:
        source: Source name (e.g., 'go_cam', 'ctd')
        keep_latest: If True, keep the latest version (default: True)

    Returns:
        Dictionary with cleanup statistics:
            {
                'deleted': int,
                'kept': int,
                'bytes_freed': int,
                'deleted_dirs': list[str],
                'kept_dirs': list[str]
            }
    """
    source_dir = Path(INGESTS_DATA_PATH) / source

    if not source_dir.exists():
        logger.warning(f"Source directory not found: {source_dir}")
        return {'deleted': 0, 'kept': 0, 'bytes_freed': 0, 'deleted_dirs': [], 'kept_dirs': []}

    # Read latest-build.json to find current version
    latest_build_file = source_dir / IngestFileName.LATEST_BUILD_FILE
    current_version = None

    if keep_latest and latest_build_file.exists():
        with open(latest_build_file, 'r') as f:
            build_metadata = json.load(f)
            current_version = build_metadata.get('source_version')

    deleted = 0
    kept = 0
    bytes_freed = 0
    deleted_dirs = []
    kept_dirs = []

    logger.info(f"Cleaning up old versions for {source}, keeping version: {current_version}")

    # Iterate through version directories
    for item in source_dir.iterdir():
        if not item.is_dir():
            continue  # Skip files like release-metadata.json

        # Skip the current version
        if keep_latest and current_version and item.name == current_version:
            kept += 1
            kept_dirs.append(str(item))
            logger.info(f"Keeping current version: {item.name}")
            continue

        # Delete old version directory
        logger.info(f"Deleting old version: {item.name}")
        dir_size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
        shutil.rmtree(item)
        deleted += 1
        bytes_freed += dir_size
        deleted_dirs.append(str(item))

    logger.info(f"Cleanup complete for {source}: {deleted} versions deleted, {kept} kept, "
               f"{bytes_freed / (1024 * 1024 * 1024):.2f} GB freed")

    return {
        'deleted': deleted,
        'kept': kept,
        'bytes_freed': bytes_freed,
        'deleted_dirs': deleted_dirs,
        'kept_dirs': kept_dirs
    }


def cleanup_old_releases(source: str, keep_latest: bool = True) -> dict:
    """Delete old /releases/{source}/{old_dates}/ directories from EBS.

    Keeps latest/ directory and the version specified in latest-release.json.

    Args:
        source: Source name (e.g., 'go_cam', 'ctd')
        keep_latest: If True, keep the latest release (default: True)

    Returns:
        Dictionary with cleanup statistics
    """
    releases_dir = Path(INGESTS_RELEASES_PATH) / source

    if not releases_dir.exists():
        logger.warning(f"Releases directory not found: {releases_dir}")
        return {'deleted': 0, 'kept': 0, 'bytes_freed': 0, 'deleted_dirs': [], 'kept_dirs': []}

    # Read latest-release.json to find current release version
    latest_release_file = releases_dir / IngestFileName.LATEST_RELEASE_FILE
    current_release = None

    if keep_latest and latest_release_file.exists():
        with open(latest_release_file, 'r') as f:
            release_metadata = json.load(f)
            current_release = release_metadata.get('release_version')

    deleted = 0
    kept = 0
    bytes_freed = 0
    deleted_dirs = []
    kept_dirs = []

    logger.info(f"Cleaning up old releases for {source}, keeping release: {current_release}")

    # Iterate through release directories
    for item in releases_dir.iterdir():
        if not item.is_dir():
            continue  # Skip files like latest-release.json

        # Always keep 'latest' directory
        if item.name == LATEST_RELEASE_DIR_NAME:
            kept += 1
            kept_dirs.append(str(item))
            logger.info("Keeping latest directory")
            continue

        # Skip the current release version
        if keep_latest and current_release and item.name == current_release:
            kept += 1
            kept_dirs.append(str(item))
            logger.info(f"Keeping current release: {item.name}")
            continue

        # Delete old release directory
        logger.info(f"Deleting old release: {item.name}")
        dir_size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
        shutil.rmtree(item)
        deleted += 1
        bytes_freed += dir_size
        deleted_dirs.append(str(item))

    logger.info(f"Cleanup complete for {source} releases: {deleted} releases deleted, {kept} kept, "
               f"{bytes_freed / (1024 * 1024 * 1024):.2f} GB freed")

    return {
        'deleted': deleted,
        'kept': kept,
        'bytes_freed': bytes_freed,
        'deleted_dirs': deleted_dirs,
        'kept_dirs': kept_dirs
    }


def upload_and_cleanup(
    data_sources: list[str] | None = None,
    release_sources: list[str] | None = None,
    cleanup: bool = True,
) -> dict:
    """Upload sources to S3 and cleanup EBS, handling data and releases separately.

    Before anything is uploaded, every local release is checked against S3, so a release version that S3 already
    has from a different build stops the upload with an error instead of being overwritten.

    Args:
        data_sources: List of source names to upload from /data (None = skip data uploads)
        release_sources: List of source names to upload from /releases (None = skip release uploads)
        cleanup: If True, cleanup old versions from EBS after successful upload (default: True)

    Returns:
        Aggregate statistics dictionary:
            {
                'sources_processed': int,
                'total_uploaded': int,
                'total_skipped': int,
                'total_failed': int,
                'total_bytes_transferred': int,
                'total_bytes_freed': int,
                'per_source_stats': dict
            }
    """
    uploader = S3Uploader()

    # Normalize inputs
    data_sources = data_sources or []
    release_sources = release_sources or []

    for source in release_sources:
        uploader.check_releases_not_overwritten(source)

    # Collect all unique sources for tracking
    all_sources = set(data_sources) | set(release_sources)

    sources_processed = 0
    total_uploaded = 0
    total_skipped = 0
    total_failed = 0
    release_files_failed = 0
    total_bytes_transferred = 0
    total_bytes_freed = 0
    per_source_stats: dict[str, dict[str, Mapping[str, object]]] = {}

    # Process each source
    for source in sorted(all_sources):
        logger.info(f"Processing source: {source}")
        source_stats: dict[str, Mapping[str, object]] = {
            'data_upload': {},
            'releases_upload': {},
            'data_cleanup': {},
            'releases_cleanup': {}
        }

        # Upload data if source is in data_sources list
        if source in data_sources:
            try:
                data_stats = uploader.upload_source_data(source)
                source_stats['data_upload'] = data_stats
                total_uploaded += data_stats['uploaded']
                total_skipped += data_stats['skipped']
                total_failed += data_stats['failed']
                total_bytes_transferred += data_stats['bytes_transferred']
            except ClientError as e:
                logger.error(f"Failed to upload data for {source}: {e}")
                source_stats['data_upload'] = {'error': str(e)}

        # Upload releases if source is in release_sources list
        if source in release_sources:
            try:
                releases_stats = uploader.upload_source_releases(source)
                source_stats['releases_upload'] = releases_stats
                total_uploaded += releases_stats['uploaded']
                total_skipped += releases_stats['skipped']
                total_failed += releases_stats['failed']
                release_files_failed += releases_stats['failed']
                total_bytes_transferred += releases_stats['bytes_transferred']
            except ClientError as e:
                logger.error(f"Failed to upload releases for {source}: {e}")
                source_stats['releases_upload'] = {'error': str(e)}

        # Cleanup EBS if requested and uploads succeeded
        if cleanup:
            # Only cleanup if all attempted uploads succeeded
            data_failed = source_stats.get('data_upload', {}).get('failed', 0)
            releases_failed = source_stats.get('releases_upload', {}).get('failed', 0)

            if data_failed == 0 and releases_failed == 0:
                logger.info(f"Upload successful for {source}, proceeding with EBS cleanup...")

                # Cleanup old data versions if we uploaded data
                if source in data_sources:
                    cleanup_stats = cleanup_old_source_versions(source, keep_latest=True)
                    source_stats['data_cleanup'] = cleanup_stats
                    total_bytes_freed += cleanup_stats['bytes_freed']

                # Cleanup old releases if we uploaded releases
                if source in release_sources:
                    cleanup_stats = cleanup_old_releases(source, keep_latest=True)
                    source_stats['releases_cleanup'] = cleanup_stats
                    total_bytes_freed += cleanup_stats['bytes_freed']
            else:
                logger.warning(f"Upload had failures for {source}, skipping EBS cleanup for safety")

        per_source_stats[source] = source_stats
        sources_processed += 1

    # The release summary points at the latest release of every source, so it is only uploaded if all of them were
    if release_sources:
        if release_files_failed:
            logger.error("Not uploading the release summary, because some releases failed to upload")
        else:
            summary_stats = uploader.upload_release_summary()
            total_uploaded += summary_stats['uploaded']
            total_skipped += summary_stats['skipped']
            total_failed += summary_stats['failed']
            total_bytes_transferred += summary_stats['bytes_transferred']

    logger.info(f"Upload and cleanup complete: {sources_processed} sources processed, "
               f"{total_uploaded} files uploaded, {total_skipped} unchanged, {total_failed} failed, "
               f"{total_bytes_transferred / (1024 * 1024 * 1024):.2f} GB transferred, "
               f"{total_bytes_freed / (1024 * 1024 * 1024):.2f} GB freed from EBS")

    return {
        'sources_processed': sources_processed,
        'total_uploaded': total_uploaded,
        'total_skipped': total_skipped,
        'total_failed': total_failed,
        'total_bytes_transferred': total_bytes_transferred,
        'total_bytes_freed': total_bytes_freed,
        'per_source_stats': per_source_stats
    }


# =============================================================================
# S3 BUCKET CLEANUP (DANGEROUS - USE WITH CAUTION)
# =============================================================================

def get_s3_bucket_stats(bucket_name: str = DEFAULT_BUCKET_NAME) -> dict:
    """Get statistics about the S3 bucket contents.

    Args:
        bucket_name: S3 bucket name

    Returns:
        Dictionary with bucket statistics:
            {
                'total_objects': int,
                'total_size_bytes': int,
                'total_size_gb': float,
                'prefixes': dict  # breakdown by top-level prefix
            }
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')

    total_objects = 0
    total_size = 0
    prefixes = {}

    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            total_objects += 1
            size = obj.get('Size', 0)
            total_size += size

            # Track by top-level prefix
            key = obj['Key']
            top_prefix = key.split('/')[0] if '/' in key else key
            if top_prefix not in prefixes:
                prefixes[top_prefix] = {'count': 0, 'size': 0}
            prefixes[top_prefix]['count'] += 1
            prefixes[top_prefix]['size'] += size

    return {
        'total_objects': total_objects,
        'total_size_bytes': total_size,
        'total_size_gb': total_size / (1024 * 1024 * 1024),
        'prefixes': prefixes
    }


def list_s3_objects_for_deletion(
    bucket_name: str = DEFAULT_BUCKET_NAME,
    prefix: str = ""
) -> list[dict]:
    """List all objects in S3 bucket/prefix that would be deleted.

    Args:
        bucket_name: S3 bucket name
        prefix: Optional prefix to filter (e.g., 'data/', 'releases/')

    Returns:
        List of object dictionaries with Key and Size
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')

    objects = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            objects.append({
                'Key': obj['Key'],
                'Size': obj.get('Size', 0),
                'LastModified': obj.get('LastModified')
            })

    return objects


def cleanup_s3_bucket(
    bucket_name: str = DEFAULT_BUCKET_NAME,
    prefix: str = "",
    require_confirmation: bool = True
) -> dict:
    """Delete all objects from S3 bucket or prefix.

    WARNING: This is a DANGEROUS operation that permanently deletes data.
    By default, requires interactive confirmation.

    Args:
        bucket_name: S3 bucket name
        prefix: Optional prefix to limit deletion (e.g., 'data/go_cam/')
        require_confirmation: If True, prompts for confirmation (default: True)

    Returns:
        Dictionary with deletion statistics:
            {
                'deleted': int,
                'failed': int,
                'bytes_deleted': int,
                'cancelled': bool
            }
    """
    s3_client = boto3.client('s3')

    # Get list of objects to delete
    objects = list_s3_objects_for_deletion(bucket_name, prefix)

    if not objects:
        logger.info(f"No objects found in s3://{bucket_name}/{prefix}")
        return {'deleted': 0, 'failed': 0, 'bytes_deleted': 0, 'cancelled': False}

    total_size = sum(obj['Size'] for obj in objects)
    total_size_gb = total_size / (1024 * 1024 * 1024)

    # Display what will be deleted
    print("\n" + "=" * 80)
    print("WARNING: S3 BUCKET CLEANUP - DANGEROUS OPERATION")
    print("=" * 80)
    print(f"\nBucket:  s3://{bucket_name}")
    print(f"Prefix:  {prefix if prefix else '(entire bucket)'}")
    print(f"\nObjects to delete: {len(objects):,}")
    print(f"Total size:        {total_size_gb:.2f} GB ({total_size:,} bytes)")
    print("\nThis action CANNOT be undone!")
    print("=" * 80)

    # Show sample of objects
    print("\nSample objects to be deleted:")
    for obj in objects[:10]:
        size_mb = obj['Size'] / (1024 * 1024)
        print(f"  - {obj['Key']} ({size_mb:.2f} MB)")
    if len(objects) > 10:
        print(f"  ... and {len(objects) - 10} more objects")

    print()

    if require_confirmation:
        # First confirmation
        response = input("Are you sure you want to delete these objects? (yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            logger.info("S3 cleanup cancelled by user")
            print("Cleanup cancelled.")
            return {'deleted': 0, 'failed': 0, 'bytes_deleted': 0, 'cancelled': True}

        # Second confirmation for safety
        print(f"\nType 'DELETE {len(objects)} OBJECTS' to confirm:")
        confirm_text = input().strip()
        expected = f"DELETE {len(objects)} OBJECTS"
        if confirm_text != expected:
            logger.info("S3 cleanup cancelled - confirmation text did not match")
            print("Confirmation text did not match. Cleanup cancelled.")
            return {'deleted': 0, 'failed': 0, 'bytes_deleted': 0, 'cancelled': True}

    # Perform deletion
    logger.info(f"Starting S3 cleanup: deleting {len(objects)} objects from s3://{bucket_name}/{prefix}")
    print(f"\nDeleting {len(objects)} objects...")

    deleted = 0
    failed = 0
    bytes_deleted = 0

    # Delete in batches of 1000 (S3 limit)
    batch_size = 1000
    for i in range(0, len(objects), batch_size):
        batch = objects[i:i + batch_size]
        delete_objects = [{'Key': obj['Key']} for obj in batch]

        try:
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': delete_objects}
            )

            # Count successful deletions
            deleted_in_batch = len(response.get('Deleted', []))
            deleted += deleted_in_batch
            bytes_deleted += sum(obj['Size'] for obj in batch[:deleted_in_batch])

            # Count errors
            errors = response.get('Errors', [])
            failed += len(errors)
            for error in errors:
                logger.error(f"Failed to delete {error['Key']}: {error['Message']}")

            # Progress update
            print(f"  Deleted {deleted}/{len(objects)} objects...")

        except ClientError as e:
            logger.error(f"Batch deletion failed: {e}")
            failed += len(batch)

    bytes_deleted_gb = bytes_deleted / (1024 * 1024 * 1024)

    print("\nS3 cleanup complete:")
    print(f"  Deleted: {deleted:,} objects ({bytes_deleted_gb:.2f} GB)")
    print(f"  Failed:  {failed:,} objects")

    logger.info(f"S3 cleanup complete: {deleted} deleted, {failed} failed, {bytes_deleted_gb:.2f} GB freed")

    return {
        'deleted': deleted,
        'failed': failed,
        'bytes_deleted': bytes_deleted,
        'cancelled': False
    }


def cleanup_s3_source(
    source: str,
    cleanup_data: bool = True,
    cleanup_releases: bool = True,
    bucket_name: str = DEFAULT_BUCKET_NAME,
    require_confirmation: bool = True
) -> dict:
    """Delete a specific source from S3 bucket.

    This deletes both data/{source}/ and releases/{source}/ prefixes.

    Args:
        source: Source name to delete (e.g., 'go_cam')
        cleanup_data: If True, delete data/{source}/ prefix
        cleanup_releases: If True, delete releases/{source}/ prefix
        bucket_name: S3 bucket name
        require_confirmation: If True, prompts for confirmation

    Returns:
        Combined deletion statistics
    """
    results = {
        'data': {'deleted': 0, 'failed': 0, 'bytes_deleted': 0, 'cancelled': False},
        'releases': {'deleted': 0, 'failed': 0, 'bytes_deleted': 0, 'cancelled': False}
    }

    if cleanup_data:
        print(f"\n--- Cleaning up data/{source}/ ---")
        results['data'] = cleanup_s3_bucket(
            bucket_name=bucket_name,
            prefix=f"data/{source}/",
            require_confirmation=require_confirmation
        )
        if results['data']['cancelled']:
            return results

    if cleanup_releases:
        print(f"\n--- Cleaning up releases/{source}/ ---")
        results['releases'] = cleanup_s3_bucket(
            bucket_name=bucket_name,
            prefix=f"releases/{source}/",
            require_confirmation=require_confirmation
        )

    return results
