"""
CLI entry point for uploading translator-ingests data and releases to S3.

This module provides the command-line interface for the `make upload` target.
Data sources and release sources are handled as separate, independent lists.

Requirements:
    - Must run on EC2 instance with IAM role permissions
    - Run after `make run` and `make release` have been executed

Usage:
    # auto-discover data and release sources separately (no combining)
    uv run python src/translator_ingest/upload_s3.py

    # explicit control over different source lists
    uv run python src/translator_ingest/upload_s3.py \
        --data-sources "ctd go_cam ncbigene" \
        --release-sources "translator_kg ctd go_cam"

    # upload data only for specific sources
    uv run python src/translator_ingest/upload_s3.py --data-sources "ncbigene"

    # upload releases only for specific sources
    uv run python src/translator_ingest/upload_s3.py --release-sources "translator_kg"

    # skip cleanup after upload
    uv run python src/translator_ingest/upload_s3.py --no-cleanup

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


def discover_data_sources() -> list[str]:
    """Discover sources from /data directory only.

    Returns:
        Sorted list of source names found in /data directory
    """
    sources = []
    data_path = Path(INGESTS_DATA_PATH)
    
    if data_path.exists():
        for item in data_path.iterdir():
            if item.is_dir():
                sources.append(item.name)
    
    return sorted(sources)


def discover_release_sources() -> list[str]:
    """Discover sources from /releases directory only.

    Returns:
        Sorted list of source names found in /releases directory
    """
    sources = []
    releases_path = Path(INGESTS_RELEASES_PATH)
    
    if releases_path.exists():
        for item in releases_path.iterdir():
            if item.is_dir():
                sources.append(item.name)
    
    return sorted(sources)


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
@click.option(
    "--data-sources",
    help="Space-separated list of sources to upload from /data (e.g., 'ctd go_cam ncbigene')"
)
@click.option(
    "--release-sources", 
    help="Space-separated list of sources to upload from /releases (e.g., 'translator_kg ctd go_cam')"
)
@click.option("--no-cleanup", is_flag=True, help="Skip EBS cleanup after upload")
def main(data_sources, release_sources, no_cleanup):
    """Upload translator-ingests data and releases to S3.

    If no sources are specified, automatically discovers sources from /data 
    and /releases directories separately (no combining).

    Examples:
        \b
        # auto-discover both data and release sources separately
        uv run python src/translator_ingest/upload_s3.py

        \b
        # explicit control over different source lists
        uv run python src/translator_ingest/upload_s3.py \\
            --data-sources "ctd go_cam ncbigene" \\
            --release-sources "translator_kg ctd go_cam"

        \b
        # upload data only for specific sources
        uv run python src/translator_ingest/upload_s3.py --data-sources "ncbigene"

        \b
        # upload releases only for specific sources
        uv run python src/translator_ingest/upload_s3.py --release-sources "translator_kg"

        \b
        # skip cleanup after upload
        uv run python src/translator_ingest/upload_s3.py --no-cleanup
    """
    setup_logging(source="upload")

    # Parse source lists from space-separated strings
    data_source_list = None
    release_source_list = None
    
    if data_sources:
        data_source_list = data_sources.split()
        logger.info(f"Data sources specified: {', '.join(data_source_list)}")
    
    if release_sources:
        release_source_list = release_sources.split()
        logger.info(f"Release sources specified: {', '.join(release_source_list)}")

    # Auto-discover if neither specified
    if data_sources is None and release_sources is None:
        logger.info("No sources specified, auto-discovering sources separately...")
        
        data_source_list = discover_data_sources()
        release_source_list = discover_release_sources()
        
        if not data_source_list and not release_source_list:
            logger.warning("No sources found in /data or /releases directories")
            print("No sources found to upload.")
            return
            
        logger.info(f"Discovered {len(data_source_list)} data sources: {', '.join(data_source_list) if data_source_list else 'none'}")
        logger.info(f"Discovered {len(release_source_list)} release sources: {', '.join(release_source_list) if release_source_list else 'none'}")

    logger.info(f"Starting S3 upload...")
    logger.info(f"Data sources: {data_source_list if data_source_list else 'none'}")
    logger.info(f"Release sources: {release_source_list if release_source_list else 'none'}")
    logger.info(f"Cleanup: {not no_cleanup}")

    # Execute upload and cleanup
    results = upload_and_cleanup(
        data_sources=data_source_list,
        release_sources=release_source_list,
        cleanup=not no_cleanup
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
