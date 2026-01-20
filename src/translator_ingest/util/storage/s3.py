"""
S3 storage component for translator-ingests.

This module provides simple, elegant S3 upload functionality for syncing
local /data and /releases directories to S3 bucket with EBS cleanup.
Data sources and release sources are handled as separate, independent lists.

Requirements:
    - Must run on EC2 instance with IAM role granting S3 permissions
    - S3 bucket: translator-ingests
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

import json
import shutil
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from translator_ingest import INGESTS_DATA_PATH, INGESTS_RELEASES_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.storage.local import IngestFileName

logger = get_logger(__name__)


class S3Uploader:
    """S3 uploader for translator-ingests data and releases.

    Provides rsync-like upload functionality that always overwrites existing files.
    Designed to run on EC2 instance with IAM role permissions.
    """

    def __init__(self, bucket_name: str = "translator-ingests"):
        """Initialize S3 uploader with EC2 IAM role credentials.

        Args:
            bucket_name: S3 bucket name (default: translator-ingests)
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.logger = logger

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """Upload single file to S3, always overwriting.

        Args:
            local_path: Local file path to upload
            s3_key: S3 object key (path in bucket)

        Returns:
            True if upload succeeded, False otherwise
        """
        if not local_path.exists():
            self.logger.warning(f"File not found, skipping: {local_path}")
            return False

        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Uploading {local_path.name} ({file_size_mb:.2f} MB) to s3://{self.bucket_name}/{s3_key}")

        self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
        self.logger.info(f"Uploaded: {s3_key}")
        return True

    def upload_directory(self, local_dir: Path, s3_prefix: str) -> dict:
        """Recursively upload directory to S3 with rsync behavior (always overwrite).

        Args:
            local_dir: Local directory to upload
            s3_prefix: S3 prefix (directory path in bucket)

        Returns:
            Dictionary with upload statistics:
                {
                    'uploaded': int,
                    'failed': int,
                    'bytes_transferred': int,
                    'uploaded_files': list[str],
                    'failed_files': list[str]
                }
        """
        if not local_dir.exists():
            self.logger.warning(f"Directory not found, skipping: {local_dir}")
            return {
                'uploaded': 0,
                'failed': 0,
                'bytes_transferred': 0,
                'uploaded_files': [],
                'failed_files': []
            }

        uploaded = 0
        failed = 0
        bytes_transferred = 0
        uploaded_files = []
        failed_files = []

        self.logger.info(f"Uploading directory: {local_dir} -> s3://{self.bucket_name}/{s3_prefix}")

        # Walk through all files in directory
        for root, _, files in sorted(local_dir.walk()):
            for file in sorted(files):
                local_path = root / file
                # Calculate relative path from local_dir
                relative_path = local_path.relative_to(local_dir)
                s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")  # Handle Windows paths

                try:
                    if self.upload_file(local_path, s3_key):
                        uploaded += 1
                        bytes_transferred += local_path.stat().st_size
                        uploaded_files.append(s3_key)
                    else:
                        failed += 1
                        failed_files.append(str(local_path))
                except ClientError as e:
                    self.logger.error(f"Failed to upload {local_path}: {e}")
                    failed += 1
                    failed_files.append(str(local_path))

        self.logger.info(f"Directory upload complete: {uploaded} files uploaded, {failed} failed, "
                        f"{bytes_transferred / (1024 * 1024):.2f} MB transferred")

        return {
            'uploaded': uploaded,
            'failed': failed,
            'bytes_transferred': bytes_transferred,
            'uploaded_files': uploaded_files,
            'failed_files': failed_files
        }

    def upload_source_data(self, source: str) -> dict:
        """Upload /data/{source}/ directory to S3.

        Uploads entire source data directory including:
        - source_data/ (raw input files)
        - {source_version}/ directories
        - transform and normalization outputs
        - all metadata files

        Args:
            source: Source name (e.g., 'go_cam', 'ctd')

        Returns:
            Upload statistics dictionary
        """
        local_dir = Path(INGESTS_DATA_PATH) / source
        s3_prefix = f"data/{source}"

        if not local_dir.exists():
            self.logger.warning(f"Source data directory not found: {local_dir}")
            return {'uploaded': 0, 'failed': 0, 'bytes_transferred': 0, 'uploaded_files': [], 'failed_files': []}

        self.logger.info(f"Uploading source data for {source}...")
        return self.upload_directory(local_dir, s3_prefix)

    def upload_source_releases(self, source: str) -> dict:
        """Upload /releases/{source}/ directory to S3.

        Uploads entire releases directory including:
        - {release_version}/ directories with tar.zst archives
        - latest/ directory
        - all metadata files

        Args:
            source: Source name (e.g., 'go_cam', 'ctd')

        Returns:
            Upload statistics dictionary
        """
        local_dir = Path(INGESTS_RELEASES_PATH) / source
        s3_prefix = f"releases/{source}"

        if not local_dir.exists():
            self.logger.warning(f"Source releases directory not found: {local_dir}")
            return {'uploaded': 0, 'failed': 0, 'bytes_transferred': 0, 'uploaded_files': [], 'failed_files': []}

        self.logger.info(f"Uploading releases for {source}...")
        return self.upload_directory(local_dir, s3_prefix)

    def upload_release_summary(self) -> bool:
        """Upload /releases/latest-release-summary.json to S3.

        Returns:
            True if upload succeeded, False otherwise
        """
        local_path = Path(INGESTS_RELEASES_PATH) / "latest-release-summary.json"
        s3_key = "releases/latest-release-summary.json"

        if not local_path.exists():
            self.logger.warning(f"Release summary not found: {local_path}")
            return False

        self.logger.info("Uploading release summary...")
        return self.upload_file(local_path, s3_key)


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
        if item.name == 'latest':
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

    Args:
        data_sources: List of source names to upload from /data (None = skip data uploads)
        release_sources: List of source names to upload from /releases (None = skip release uploads)
        cleanup: If True, cleanup old versions from EBS after successful upload (default: True)

    Returns:
        Aggregate statistics dictionary:
            {
                'sources_processed': int,
                'total_uploaded': int,
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
    
    # Collect all unique sources for tracking
    all_sources = set(data_sources) | set(release_sources)
    
    sources_processed = 0
    total_uploaded = 0
    total_failed = 0
    total_bytes_transferred = 0
    total_bytes_freed = 0
    per_source_stats = {}

    # Process each source
    for source in sorted(all_sources):
        logger.info(f"Processing source: {source}")
        source_stats = {
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
                total_failed += releases_stats['failed']
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

    # Upload release summary if any releases were uploaded
    if release_sources:
        logger.info("Uploading release summary...")
        uploader.upload_release_summary()

    logger.info(f"Upload and cleanup complete: {sources_processed} sources processed, "
               f"{total_uploaded} files uploaded, {total_failed} failed, "
               f"{total_bytes_transferred / (1024 * 1024 * 1024):.2f} GB transferred, "
               f"{total_bytes_freed / (1024 * 1024 * 1024):.2f} GB freed from EBS")

    return {
        'sources_processed': sources_processed,
        'total_uploaded': total_uploaded,
        'total_failed': total_failed,
        'total_bytes_transferred': total_bytes_transferred,
        'total_bytes_freed': total_bytes_freed,
        'per_source_stats': per_source_stats
    }


# =============================================================================
# S3 BUCKET CLEANUP (DANGEROUS - USE WITH CAUTION)
# =============================================================================

def get_s3_bucket_stats(bucket_name: str = "translator-ingests") -> dict:
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
    bucket_name: str = "translator-ingests",
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
    bucket_name: str = "translator-ingests",
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
    bucket_name: str = "translator-ingests",
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
