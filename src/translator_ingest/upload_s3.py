"""
CLI entry point for uploading translator-ingests data and releases to S3.

This module provides the command-line interface for the `make upload` target.

Requirements:
    - Must run on EC2 instance with IAM role permissions
    - Run after `make run` and `make release` have been executed
    - Uploads both /data and /releases directories to S3 by default

Usage:
    # Upload specific sources
    uv run python src/translator_ingest/upload_s3.py go_cam ctd

    # Upload all sources (auto-discover)
    uv run python src/translator_ingest/upload_s3.py

    # Upload without cleanup
    uv run python src/translator_ingest/upload_s3.py go_cam --no-cleanup

    # Upload only data directory
    uv run python src/translator_ingest/upload_s3.py --data-only

    # Upload only releases directory
    uv run python src/translator_ingest/upload_s3.py --releases-only

Via Makefile:
    make upload SOURCES="go_cam ctd"
    make upload-all
    make upload-go_cam
"""

from pathlib import Path

import click

from translator_ingest import INGESTS_DATA_PATH, INGESTS_RELEASES_PATH
from translator_ingest.util.logging_utils import get_logger, setup_logging
from translator_ingest.util.storage.s3 import upload_and_cleanup

logger = get_logger(__name__)


def discover_sources() -> list[str]:
    """Auto-discover sources from /data and /releases directories.

    Returns:
        List of unique source names found in both directories
    """
    sources = set()

    # Discover from /data
    data_path = Path(INGESTS_DATA_PATH)
    if data_path.exists():
        for item in data_path.iterdir():
            if item.is_dir():
                sources.add(item.name)

    # Discover from /releases
    releases_path = Path(INGESTS_RELEASES_PATH)
    if releases_path.exists():
        for item in releases_path.iterdir():
            if item.is_dir():
                sources.add(item.name)

    return sorted(list(sources))


def print_upload_summary(results: dict):
    """Print formatted upload summary to console.

    Args:
        results: Results dictionary from upload_and_cleanup()
    """
    print("\n" + "=" * 80)
    print("S3 UPLOAD SUMMARY")
    print("=" * 80)
    print(f"Sources processed:    {results['sources_processed']}")
    print(f"Files uploaded:       {results['total_uploaded']}")
    print(f"Files failed:         {results['total_failed']}")
    print(f"Data transferred:     {results['total_bytes_transferred'] / (1024 * 1024 * 1024):.2f} GB")
    print(f"EBS space freed:      {results['total_bytes_freed'] / (1024 * 1024 * 1024):.2f} GB")
    print("=" * 80)

    # Per-source details
    print("\nPer-Source Details:")
    print("-" * 80)
    for source, stats in results['per_source_stats'].items():
        print(f"\n{source}:")

        # Data upload
        data_upload = stats.get('data_upload', {})
        if 'error' in data_upload:
            print(f"  Data upload:     ERROR - {data_upload['error']}")
        else:
            print(f"  Data upload:     {data_upload.get('uploaded', 0)} files, "
                  f"{data_upload.get('bytes_transferred', 0) / (1024 * 1024):.2f} MB")

        # Releases upload
        releases_upload = stats.get('releases_upload', {})
        if 'error' in releases_upload:
            print(f"  Releases upload: ERROR - {releases_upload['error']}")
        else:
            print(f"  Releases upload: {releases_upload.get('uploaded', 0)} files, "
                  f"{releases_upload.get('bytes_transferred', 0) / (1024 * 1024):.2f} MB")

        # Cleanup stats
        data_cleanup = stats.get('data_cleanup', {})
        if data_cleanup:
            print(f"  Data cleanup:    {data_cleanup.get('deleted', 0)} versions deleted, "
                  f"{data_cleanup.get('bytes_freed', 0) / (1024 * 1024 * 1024):.2f} GB freed")

        releases_cleanup = stats.get('releases_cleanup', {})
        if releases_cleanup:
            print(f"  Releases cleanup: {releases_cleanup.get('deleted', 0)} releases deleted, "
                  f"{releases_cleanup.get('bytes_freed', 0) / (1024 * 1024 * 1024):.2f} GB freed")

    print("\n" + "=" * 80 + "\n")


@click.command()
@click.argument("sources", nargs=-1, required=False)
@click.option("--no-cleanup", is_flag=True, help="Skip EBS cleanup after upload")
@click.option("--data-only", is_flag=True, help="Upload only /data, skip /releases")
@click.option("--releases-only", is_flag=True, help="Upload only /releases, skip /data")
def main(sources, no_cleanup, data_only, releases_only):
    """Upload translator-ingests data and releases to S3.

    If no SOURCES are specified, automatically discovers and uploads all sources
    found in /data and /releases directories.

    Examples:
        \b
        # Upload specific sources
        uv run python src/translator_ingest/upload_s3.py go_cam ctd

        \b
        # Upload all sources (auto-discover)
        uv run python src/translator_ingest/upload_s3.py

        \b
        # Upload without cleanup
        uv run python src/translator_ingest/upload_s3.py go_cam --no-cleanup

        \b
        # Upload only data directory
        uv run python src/translator_ingest/upload_s3.py --data-only

        \b
        # Upload only releases directory
        uv run python src/translator_ingest/upload_s3.py --releases-only
    """
    # Use first source for logging, or "upload" if multiple/none specified
    log_source = sources[0] if len(sources) == 1 else "upload"
    setup_logging(source=log_source)

    # Validate conflicting options
    if data_only and releases_only:
        logger.error("Cannot specify both --data-only and --releases-only")
        raise click.UsageError("Cannot specify both --data-only and --releases-only")

    # Auto-discover sources if not specified
    if not sources:
        logger.info("No sources specified, auto-discovering sources...")
        sources = discover_sources()
        if not sources:
            logger.warning("No sources found in /data or /releases directories")
            print("No sources found to upload.")
            return
        logger.info(f"Discovered {len(sources)} sources: {', '.join(sources)}")
    else:
        sources = list(sources)

    # Determine what to upload
    upload_data = not releases_only
    upload_releases = not data_only

    logger.info(f"Starting S3 upload for {len(sources)} sources...")
    logger.info(f"Upload data: {upload_data}, Upload releases: {upload_releases}, Cleanup: {not no_cleanup}")

    # Execute upload and cleanup
    results = upload_and_cleanup(
        sources=sources,
        cleanup=not no_cleanup,
        upload_data=upload_data,
        upload_releases=upload_releases
    )

    # Print summary
    print_upload_summary(results)

    # Exit with error if there were failures
    if results['total_failed'] > 0:
        logger.error(f"Upload completed with {results['total_failed']} failures")
        exit(1)
    else:
        logger.info("Upload completed successfully")


if __name__ == "__main__":
    main()
