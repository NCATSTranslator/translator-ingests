# Storage Component - S3 Upload and EBS Management

## Overview

The storage component handles uploading translator-ingests data and releases to an S3 bucket, with automatic cleanup of old versions from EBS to save local storage space.

## EC2 Instance Requirements

**Recommended instance:** t3.2xlarge with 200 GiB EBS storage

Smaller instances have resulted in crashes when running all ingests due to memory and storage constraints. The current number of ingests requires significant resources for parallel processing.

This code must run on an EC2 instance with an IAM role that grants S3 permissions. It will not work from local development machines or EC2 instances without proper IAM roles.

The IAM role needs the following permissions on the translator-ingests bucket:
- s3:PutObject
- s3:GetObject
- s3:ListBucket
- s3:DeleteObject

## S3 Directory Structure

The S3 bucket mirrors the local directory structure with two main directories:

**data/** contains all build artifacts for each source, including source versions, transform outputs (nodes and edges files), normalization outputs, validation reports, and metadata files.

**releases/** contains compressed release archives (tar.zst files), the latest directory for each source, and release metadata files.

## Upload Workflow

The full upload pipeline consists of three steps:

```bash
make run        # Run the full ingest pipeline (download, transform, normalize, validate)
make release    # Generate release archives for each source
make upload     # Upload to S3 and cleanup old EBS versions
```

When you run upload, the entire data and releases directories for each source are uploaded to S3. The upload always overwrites existing files (rsync-like behavior), so it's safe to re-run multiple times.

After a successful upload, old versions are automatically removed from EBS to free up disk space. Only the latest version is kept locally. If any upload fails, cleanup is skipped for safety.

## Makefile Commands

**make upload SOURCES="go_cam ctd"** - Upload specified sources to S3 with EBS cleanup

**make upload-all** - Upload all sources discovered in the data directory

**make upload-go_cam** - Upload a single source (pattern: upload-{source})

**make cleanup-ebs** - Clean up old versions from EBS without uploading

**make cleanup-s3** - Delete all objects from S3 bucket (dangerous, requires confirmation)

**make cleanup-s3-source SOURCES="go_cam"** - Delete specific source from S3 (dangerous, requires confirmation)

## Logging

Each pipeline run creates logs in `/logs/{source}/{timestamp}/run.log`. This allows tracking what happened for each source over time. The logs directory is gitignored.

## EBS Cleanup Strategy

The cleanup process preserves only the latest version of data and releases on the local EBS storage. It reads the latest-build.json and latest-release.json files to determine which versions to keep. All older versions are deleted from EBS but remain archived in S3.

Cleanup only happens after successful uploads to prevent data loss. The latest directory in releases is always preserved.

## S3 Bucket Cleanup (Dangerous)

The S3 cleanup functions permanently delete data from the S3 bucket. These operations require two-step confirmation for safety:

1. First, you must type "yes" to confirm you want to proceed
2. Then, you must type "DELETE N OBJECTS" exactly (where N is the actual count)

Before deletion, the system displays the bucket name, prefix being deleted, number of objects, total size in GB, and a sample of files that will be deleted.

## Reproducibility

This setup is portable and not tied to any specific AWS account. To run on a new EC2 instance:

1. Launch a t3.2xlarge instance with 200 GiB EBS storage
2. Create an S3 bucket for storing outputs
3. Create an IAM role with the required S3 permissions
4. Attach the IAM role to the EC2 instance
5. Clone the repository and run `make install`

## Error Handling

Upload failures are logged but don't stop other sources from being processed. EBS cleanup is automatically skipped if any upload errors occurred. Failed uploads can be retried safely since the upload always overwrites.

## Troubleshooting

If you get "Access Denied" errors, verify you're running on an EC2 instance with the proper IAM role attached.

If no sources are found, make sure the pipeline and release commands have been run first.

If cleanup deletes something you needed, all versions remain archived in S3 and can be downloaded using the AWS CLI.
