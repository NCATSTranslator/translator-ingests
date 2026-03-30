# Storage — S3 Upload and EBS Management

## Overview

The storage component handles uploading translator-ingests data and releases to S3, managing local EBS versions, and providing path utilities for the versioned directory layout. Data is served publicly via `https://kgx-storage.rtx.ai`.

Key modules:

- `s3.py` — S3Uploader class, upload and cleanup functions
- `local.py` — Local path management, file type enums, versioned directory layout
- `upload_s3.py` — CLI entry point for standalone upload

## EC2 Instance Requirements

**Recommended instance:** t3.2xlarge with 400 GiB EBS storage

This code must run on an EC2 instance with an IAM role that grants S3 permissions. It will not work from local development machines or EC2 instances without proper IAM roles.

The IAM role needs the following permissions on the translator-ingests bucket:
- s3:PutObject
- s3:GetObject
- s3:ListBucket
- s3:DeleteObject

## Public Access

Uploaded data is served publicly via `https://kgx-storage.rtx.ai`. The base URLs are configurable via environment variables:

- `INGESTS_STORAGE_URL` — defaults to `https://kgx-storage.rtx.ai/data`
- `INGESTS_RELEASES_URL` — defaults to `https://kgx-storage.rtx.ai/releases`

The kgx-webserver that serves these files runs on a separate t3.medium EC2 instance.

## S3 Directory Structure

The S3 bucket mirrors the local directory structure with two main directories:

**data/** contains all build artifacts for each source, including source versions, transform outputs (nodes and edges files), normalization outputs, validation reports, and metadata files.

**releases/** contains compressed release archives (tar.zst files), the latest directory for each source, and release metadata files.

## Upload Commands

**make upload-all** — Auto-discover and upload all sources (data and releases separately)

**make upload SOURCES="go_cam ctd"** — Upload specified sources to S3 with EBS cleanup

**make upload-go_cam** — Upload a single source (pattern: upload-{source})

**make cleanup-ebs** — Clean up old versions from EBS without uploading

**make cleanup-s3** — Delete all objects from S3 bucket (dangerous, requires confirmation)

**make cleanup-s3-source SOURCES="go_cam"** — Delete specific source from S3 (dangerous, requires confirmation)

## Direct Python Upload (Advanced)

For explicit control over separate data vs release source lists:

```bash
# auto-discover data and release sources separately (same as make upload-all)
uv run python -m translator_ingest.util.storage.upload_s3

# specify different lists for data and releases
uv run python -m translator_ingest.util.storage.upload_s3 \
    --data-sources "ctd go_cam ncbigene" \
    --release-sources "translator_kg ctd go_cam"

# upload only data for specific sources
uv run python -m translator_ingest.util.storage.upload_s3 --data-sources "ncbigene"

# upload only releases for specific sources
uv run python -m translator_ingest.util.storage.upload_s3 --release-sources "translator_kg"
```

## Upload Workflow

When you run upload, the entire data and releases directories for each source are uploaded to S3. The upload always overwrites existing files (rsync-like behavior), so it is safe to re-run multiple times.

After a successful upload, old versions are automatically removed from EBS to free up disk space. Only the latest version is kept locally. If any upload fails, cleanup is skipped for safety.

## EBS Cleanup Strategy

The cleanup process preserves only the latest version of data and releases on the local EBS storage. It reads the latest-build.json and latest-release.json files to determine which versions to keep. All older versions are deleted from EBS but remain archived in S3.

Cleanup only happens after successful uploads to prevent data loss. The latest directory in releases is always preserved.

## S3 Bucket Cleanup (Dangerous)

The S3 cleanup functions permanently delete data from the S3 bucket. These operations require two-step confirmation for safety:

1. First, you must type "yes" to confirm you want to proceed
2. Then, you must type "DELETE N OBJECTS" exactly (where N is the actual count)

Before deletion, the system displays the bucket name, prefix being deleted, number of objects, total size in GB, and a sample of files that will be deleted.

## Error Handling

- Upload failures are logged but don't stop other sources from being processed
- EBS cleanup is automatically skipped if any upload errors occurred
- Failed uploads can be retried safely since the upload always overwrites

## Troubleshooting

If you get "Access Denied" errors, verify you're running on an EC2 instance with the proper IAM role attached.

If no sources are found, make sure the pipeline and release commands have been run first.

If cleanup deletes something you needed, all versions remain archived in S3 and can be downloaded using the AWS CLI.
