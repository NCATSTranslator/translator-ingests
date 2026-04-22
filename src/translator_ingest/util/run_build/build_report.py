"""
Automated build report generator for the translator-ingests pipeline.

Reads pipeline artifacts (latest-build.json, latest-release.json, validation reports,
upload logs, merge metadata) to produce a comprehensive build report with zero
manual data entry.

Usage:
    # Generate report after a full pipeline run
    uv run python -m translator_ingest.util.run_build.build_report

    # Generate report for specific sources
    uv run python -m translator_ingest.util.run_build.build_report --sources "ctd go_cam"

    # Include upload summary from a previous upload run
    uv run python -m translator_ingest.util.run_build.build_report --upload-results upload-results.json

    # Output as JSON instead of text
    uv run python -m translator_ingest.util.run_build.build_report --format json

Via Makefile:
    make report
    make report SOURCES="ctd go_cam"
"""

import datetime
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import click

from translator_ingest import (
    INGESTS_DATA_PATH,
    INGESTS_RELEASES_PATH,
)
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import (
    IngestFileName,
    get_validation_directory,
)
from translator_ingest.util.logging_utils import get_logger, setup_logging
from translator_ingest.util.run_build import REPORTS_BASE
from translator_ingest.util.run_build.utils import BYTES_PER_GB, format_duration, update_latest_copy

logger = get_logger(__name__)


@dataclass
class SourceReport:
    """Report data for a single source ingest."""

    source: str
    status: str = "unknown"
    source_version: str | None = None
    transform_version: str | None = None
    node_norm_version: str | None = None
    biolink_version: str | None = None
    build_version: str | None = None
    release_version: str | None = None
    validation_status: str | None = None
    validation_errors: int = 0
    validation_warnings: int = 0
    total_nodes: int = 0
    total_edges: int = 0
    duration_seconds: float | None = None
    peak_memory_mb: float = 0.0
    avg_memory_mb: float = 0.0
    min_memory_mb: float = 0.0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class MergedGraphReport:
    """Report data for a merged graph."""

    graph_id: str
    status: str = "unknown"
    build_version: str | None = None
    release_version: str | None = None
    biolink_version: str | None = None
    node_norm_version: str | None = None
    sources_included: list[str] = field(default_factory=list)
    duration_seconds: float | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class UploadReport:
    """Report data from S3 upload."""

    sources_processed: int = 0
    files_uploaded: int = 0
    files_failed: int = 0
    data_transferred_gb: float = 0.0
    ebs_space_freed_gb: float = 0.0
    duration_seconds: float | None = None
    per_source_errors: dict[str, str] = field(default_factory=dict)


@dataclass
class StageTimingReport:
    """Timing and resource data for a pipeline stage."""

    stage: str
    duration_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    avg_memory_mb: float = 0.0
    min_memory_mb: float = 0.0
    avg_cpu_percent: float = 0.0
    status: str = "pending"
    error: str | None = None


@dataclass
class BuildReport:
    """Complete pipeline build report."""

    timestamp: str = ""
    total_duration_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    avg_memory_mb: float = 0.0
    min_memory_mb: float = 0.0
    disk_free_gb: float = 0.0
    disk_total_gb: float = 0.0
    pipeline_stages_completed: list[str] = field(default_factory=list)
    pipeline_stages_failed: list[str] = field(default_factory=list)
    stage_timings: list[StageTimingReport] = field(default_factory=list)
    biolink_version: str | None = None
    node_norm_version: str | None = None
    release_version: str | None = None
    storage_url: str = "https://kgx-storage.rtx.ai"
    docs_url: str = "https://kgx-storage.rtx.ai/docs"
    source_reports: list[SourceReport] = field(default_factory=list)
    merged_graph: MergedGraphReport | None = None
    upload: UploadReport | None = None
    build_notes: list[str] = field(default_factory=list)
    errors_log: list[str] = field(default_factory=list)


def collect_source_report(source: str, node_properties: list[str]) -> SourceReport:
    """Collect build report data for a single source from pipeline artifacts.

    Args:
        source: Source name (e.g. "ctd")
        node_properties: List of sources that are node-properties only

    Returns:
        SourceReport with data from latest-build.json and validation-report.json
    """
    report = SourceReport(source=source)
    data_path = Path(INGESTS_DATA_PATH)

    # Read latest-build.json
    latest_build_path = data_path / source / IngestFileName.LATEST_BUILD_FILE
    if not latest_build_path.exists():
        report.status = "no_build"
        report.errors.append("No latest-build.json found - pipeline may not have completed")
        return report

    with latest_build_path.open() as f:
        build_data = json.load(f)

    metadata = PipelineMetadata(**build_data)
    report.source_version = metadata.source_version
    report.transform_version = metadata.transform_version
    report.node_norm_version = metadata.node_norm_version
    report.biolink_version = metadata.biolink_version
    report.build_version = metadata.build_version

    # Read validation report
    validation_dir = get_validation_directory(metadata)
    validation_report_path = validation_dir / IngestFileName.VALIDATION_REPORT_FILE
    if validation_report_path.exists():
        with validation_report_path.open() as f:
            val_data = json.load(f)

        # Handle both single-file and directory validation report formats
        if "sources" in val_data:
            source_val = next(iter(val_data["sources"].values()), {})
        else:
            source_val = val_data

        stats = source_val.get("statistics", {})
        report.validation_status = source_val.get("validation_status", "unknown")
        report.validation_errors = stats.get("validation_errors", 0)
        report.validation_warnings = stats.get("validation_warnings", 0)
        report.total_nodes = stats.get("total_nodes", 0)
        report.total_edges = stats.get("total_edges", 0)

        if report.validation_status == "FAILED":
            report.status = "validation_failed"
            errors = source_val.get("issues", {}).get("errors", [])
            for err in errors[:3]:
                report.errors.append(err.get("message", str(err)))
        else:
            report.status = "built"
    else:
        report.status = "built"
        report.notes.append("No validation report found")

    # Check release status
    release_path = Path(INGESTS_RELEASES_PATH) / source / IngestFileName.LATEST_RELEASE_FILE
    if release_path.exists():
        with release_path.open() as f:
            release_data = json.load(f)
        report.release_version = release_data.get("release_version")
        if report.status == "built":
            report.status = "released"
    elif source in node_properties:
        report.notes.append("Node properties only - not released as standalone source")
    else:
        report.notes.append("Not yet released")

    return report


def collect_merged_graph_report(graph_id: str, expected_sources: list[str]) -> MergedGraphReport:
    """Collect report data for a merged graph.

    Args:
        graph_id: The graph identifier (e.g. "translator_kg")
        expected_sources: List of source names expected to be in the merge

    Returns:
        MergedGraphReport with data from merge artifacts
    """
    report = MergedGraphReport(graph_id=graph_id)
    releases_path = Path(INGESTS_RELEASES_PATH) / graph_id

    # Read latest-release.json for the merged graph
    release_metadata_path = releases_path / IngestFileName.LATEST_RELEASE_FILE
    if not release_metadata_path.exists():
        report.status = "no_merge"
        report.errors.append("No merged graph release found")
        return report

    with release_metadata_path.open() as f:
        release_data = json.load(f)

    report.build_version = release_data.get("build_version")
    report.release_version = release_data.get("release_version")
    report.biolink_version = release_data.get("biolink_version")
    report.node_norm_version = release_data.get("node_norm_version")

    # Read graph-metadata.json to get included sources
    release_version = report.release_version
    if release_version:
        graph_metadata_path = releases_path / release_version / "graph-metadata.json"
        if not graph_metadata_path.exists():
            graph_metadata_path = releases_path / "latest" / "graph-metadata.json"

        if graph_metadata_path.exists():
            with graph_metadata_path.open() as f:
                graph_metadata = json.load(f)

            sources_in_graph = []
            for source_info in graph_metadata.get("isBasedOn", []):
                source_id = source_info.get("id", "")
                if source_id:
                    sources_in_graph.append(source_id)
            report.sources_included = sorted(sources_in_graph)
            report.status = "merged"

            # Check for missing expected sources
            missing = set(expected_sources) - set(sources_in_graph)
            if missing:
                report.errors.append(f"Expected sources missing from merge: {sorted(missing)}")
        else:
            report.status = "merged"
            report.errors.append("graph-metadata.json not found, cannot verify included sources")
    else:
        report.status = "merged"

    return report


def load_upload_results(upload_results_path: Path | None) -> UploadReport | None:
    """Load upload results from a JSON file saved by upload_s3.py.

    Args:
        upload_results_path: Path to upload-results.json, or None

    Returns:
        UploadReport if file exists and is valid, None otherwise
    """
    if upload_results_path is None or not upload_results_path.exists():
        return None

    with upload_results_path.open() as f:
        data = json.load(f)

    report = UploadReport(
        sources_processed=data.get("sources_processed", 0),
        files_uploaded=data.get("total_uploaded", 0),
        files_failed=data.get("total_failed", 0),
        data_transferred_gb=data.get("total_bytes_transferred", 0) / BYTES_PER_GB,
        ebs_space_freed_gb=data.get("total_bytes_freed", 0) / BYTES_PER_GB,
    )

    for source, stats in data.get("per_source_stats", {}).items():
        for upload_type in ("data_upload", "releases_upload"):
            upload_stats = stats.get(upload_type, {})
            if "error" in upload_stats:
                report.per_source_errors[f"{source}/{upload_type}"] = upload_stats["error"]

    return report


def generate_build_report(
    sources: list[str],
    graph_id: str,
    node_properties: list[str],
    upload_results_path: Path | None = None,
    stage_timings: list[StageTimingReport] | None = None,
    source_durations: dict[str, float] | None = None,
    source_memory: dict[str, dict[str, Any]] | None = None,
    total_duration: float = 0.0,
    peak_memory_mb: float = 0.0,
    avg_memory_mb: float = 0.0,
    min_memory_mb: float = 0.0,
    disk_usage: dict[str, float] | None = None,
    failed_sources: list[str] | None = None,
    build_notes: list[str] | None = None,
) -> BuildReport:
    """Generate a complete build report from pipeline artifacts.

    Args:
        sources: List of source names processed
        graph_id: Merged graph identifier
        node_properties: Sources that are node-properties only
        upload_results_path: Optional path to upload results JSON
        stage_timings: Optional list of stage timing reports from run_build
        source_durations: Optional dict mapping source -> duration_seconds
        source_memory: Optional dict mapping source -> {peak_mb, avg_mb, min_mb}
        total_duration: Total build duration in seconds
        peak_memory_mb: Peak memory usage in MB
        avg_memory_mb: Average memory usage in MB
        min_memory_mb: Minimum memory usage in MB
        disk_usage: Optional dict with disk stats {free_gb, total_gb, ...}
        failed_sources: Sources that failed RUN and used previous build data
        build_notes: Optional extra notes to include in the report
            (e.g. memory abort messages)

    Returns:
        BuildReport populated from on-disk artifacts
    """
    report = BuildReport(
        timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
        total_duration_seconds=total_duration,
        peak_memory_mb=peak_memory_mb,
        avg_memory_mb=avg_memory_mb,
        min_memory_mb=min_memory_mb,
        disk_free_gb=disk_usage.get("free_gb", 0) if disk_usage else 0,
        disk_total_gb=disk_usage.get("total_gb", 0) if disk_usage else 0,
    )

    if build_notes:
        report.build_notes.extend(build_notes)

    if stage_timings:
        report.stage_timings = stage_timings

    # Collect individual source reports
    for source in sorted(sources):
        source_report = collect_source_report(source, node_properties)
        if source_durations and source in source_durations:
            source_report.duration_seconds = source_durations[source]
        if source_memory and source in source_memory:
            sm = source_memory[source]
            source_report.peak_memory_mb = sm.get("peak_mb", 0)
            source_report.avg_memory_mb = sm.get("avg_mb", 0)
            source_report.min_memory_mb = sm.get("min_mb", 0)
        report.source_reports.append(source_report)

    # Determine common versions from built sources
    built_sources = [s for s in report.source_reports if s.status not in ("no_build", "unknown")]
    if built_sources:
        biolink_versions = {s.biolink_version for s in built_sources if s.biolink_version}
        nodenorm_versions = {s.node_norm_version for s in built_sources if s.node_norm_version}
        release_versions = {s.release_version for s in built_sources if s.release_version}

        if len(biolink_versions) == 1:
            report.biolink_version = biolink_versions.pop()
        elif biolink_versions:
            report.biolink_version = ", ".join(sorted(biolink_versions))
            report.build_notes.append(f"WARNING: Multiple biolink versions found: {report.biolink_version}")

        if len(nodenorm_versions) == 1:
            report.node_norm_version = nodenorm_versions.pop()
        elif nodenorm_versions:
            report.node_norm_version = ", ".join(sorted(nodenorm_versions))
            report.build_notes.append(
                f"WARNING: Multiple node normalization versions found: {report.node_norm_version}"
            )

        if release_versions:
            report.release_version = sorted(release_versions)[-1]

    # Determine which pipeline stages completed
    all_built = all(s.status != "no_build" for s in report.source_reports)
    any_validation_failed = any(s.status == "validation_failed" for s in report.source_reports)
    all_released = all(
        s.status == "released" or s.source in node_properties
        for s in report.source_reports
        if s.status != "no_build"
    )

    if all_built:
        report.pipeline_stages_completed.append("run (download, transform, normalize, merge, validate)")
    else:
        no_build_sources = [s.source for s in report.source_reports if s.status == "no_build"]
        report.pipeline_stages_failed.append(f"run: {len(no_build_sources)} sources failed: {', '.join(no_build_sources)}")

    if any_validation_failed:
        failed_val = [s.source for s in report.source_reports if s.status == "validation_failed"]
        report.pipeline_stages_failed.append(f"validate: {len(failed_val)} sources failed: {', '.join(failed_val)}")

    # Collect merged graph report
    releasable_sources = [s for s in sources if s not in node_properties]
    report.merged_graph = collect_merged_graph_report(graph_id, releasable_sources)
    if report.merged_graph.status == "merged":
        report.pipeline_stages_completed.append("merge")
    else:
        report.pipeline_stages_failed.append(f"merge: {', '.join(report.merged_graph.errors)}")

    if all_released:
        report.pipeline_stages_completed.append("release")
    else:
        unreleased = [
            s.source
            for s in report.source_reports
            if s.status not in ("released", "no_build") and s.source not in node_properties
        ]
        if unreleased:
            report.pipeline_stages_failed.append(f"release: {len(unreleased)} sources not released")

    # Load upload results
    report.upload = load_upload_results(upload_results_path)
    if report.upload:
        if report.upload.files_failed == 0 and report.upload.files_uploaded > 0:
            report.pipeline_stages_completed.append("upload")
        elif report.upload.files_failed > 0:
            report.pipeline_stages_failed.append(f"upload: {report.upload.files_failed} files failed")

    # Detect noteworthy per-source conditions
    failed_set = set(failed_sources) if failed_sources else set()
    for sr in report.source_reports:
        if sr.source in node_properties:
            report.build_notes.append(
                f"{sr.source}: Not released as standalone source by design (NODE_PROPERTIES). "
                "Included in merged graph as node properties only."
            )
        if sr.source in failed_set:
            report.build_notes.append(
                f"{sr.source}: RUN failed -- merged/released/uploaded using previous successful build data."
            )

    return report


def _report_counts(report: BuildReport) -> tuple[int, list[str], int]:
    """Compute released count, node-property source names, and merged count."""
    released_count = sum(1 for s in report.source_reports if s.status == "released")
    node_prop_sources = [
        s.source for s in report.source_reports
        if s.status == "built" and any("node properties" in n.lower() for n in s.notes)
    ]
    merged_count = 1 if report.merged_graph and report.merged_graph.status == "merged" else 0
    return released_count, node_prop_sources, merged_count


def _format_report_header(report: BuildReport) -> list[str]:
    """Format report header: greeting, URLs, and source summary counts."""
    lines: list[str] = []
    lines.append("The latest ingest pipeline build has finished.")
    if report.upload and report.upload.files_uploaded > 0:
        lines.append("The S3 upload is done.")
    lines.append("")

    lines.append(f"The latest data is available at: {report.storage_url}")
    lines.append(f"Endpoints documentation: {report.docs_url}")
    lines.append("")

    released_count, node_prop_sources, merged_count = _report_counts(report)
    total_processed = released_count + len(node_prop_sources) + merged_count
    breakdown_parts = [f"{released_count} released sources"]
    if node_prop_sources:
        breakdown_parts.append(f"{len(node_prop_sources)} node-properties-only ({', '.join(node_prop_sources)})")
    if merged_count:
        breakdown_parts.append(f"{merged_count} merged graph ({report.merged_graph.graph_id})")

    lines.append(
        f"{total_processed} sources were processed ({' + '.join(breakdown_parts)})"
        f"{f' and {report.upload.files_uploaded:,} files were uploaded successfully' if report.upload and report.upload.files_uploaded > 0 else ''}."
    )
    lines.append("")
    return lines


def _format_upload_section(report: BuildReport) -> list[str]:
    """Format the S3 upload summary block."""
    if not report.upload:
        return []

    released_count, node_prop_sources, merged_count = _report_counts(report)
    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("S3 UPLOAD SUMMARY")
    lines.append("=" * 80)
    upload_breakdown = f"{released_count} individual"
    if node_prop_sources:
        upload_breakdown += f" + {len(node_prop_sources)} node-properties"
    if merged_count:
        upload_breakdown += f" + {merged_count} merged graph"
    lines.append(f"Sources uploaded:     {report.upload.sources_processed} ({upload_breakdown})")
    lines.append(f"Files uploaded:       {report.upload.files_uploaded:,}")
    lines.append(f"Files failed:         {report.upload.files_failed:,}")
    lines.append(f"Data transferred:     {report.upload.data_transferred_gb:.2f} GB")
    lines.append(f"EBS space freed:      {report.upload.ebs_space_freed_gb:.2f} GB")
    lines.append("=" * 80)
    lines.append("")
    return lines


def _format_source_list(report: BuildReport) -> list[str]:
    """Format source list and merged graph details."""
    _, _, merged_count = _report_counts(report)
    lines: list[str] = []
    individual_sources = sorted([s.source for s in report.source_reports if s.status == "released"])
    lines.append(f"Individual sources ({len(individual_sources)}): {', '.join(individual_sources)}")
    if report.merged_graph and report.merged_graph.status == "merged":
        lines.append(f"Merged graph ({merged_count}): {report.merged_graph.graph_id}")
    lines.append("")

    if report.merged_graph and report.merged_graph.status == "merged":
        mg = report.merged_graph
        included = mg.sources_included
        expected = [s.source for s in report.source_reports if s.status == "released"]
        all_included = set(expected).issubset(set(included))
        if all_included:
            lines.append(f"All {len(expected)} sources are included in {mg.graph_id}.")
        else:
            missing = sorted(set(expected) - set(included))
            lines.append(
                f"{len(included)} of {len(expected)} sources are included in {mg.graph_id}. "
                f"Missing: {', '.join(missing)}"
            )
    return lines


def _format_versions_section(report: BuildReport) -> list[str]:
    """Format version information."""
    version_parts = []
    if report.biolink_version:
        version_parts.append(f"Biolink version: {report.biolink_version}")
    if report.node_norm_version:
        version_parts.append(f"Node normalization version: {report.node_norm_version}")
    if report.release_version:
        version_parts.append(f"Release version: {report.release_version}")
    lines: list[str] = []
    if version_parts:
        lines.append(" ".join(version_parts))
    lines.append("")
    return lines


def _format_validation_section(report: BuildReport) -> list[str]:
    """Format validation failures and warnings."""
    lines: list[str] = []
    val_failed = [s for s in report.source_reports if s.validation_status == "FAILED"]
    val_warnings = [s for s in report.source_reports if s.validation_warnings > 0]
    if val_failed:
        lines.append("VALIDATION FAILURES:")
        for s in val_failed:
            lines.append(f"  {s.source}: {s.validation_errors} errors, {s.validation_warnings} warnings")
        lines.append("")

    if val_warnings and not val_failed:
        total_warnings = sum(s.validation_warnings for s in val_warnings)
        lines.append(
            f"Validation: All sources PASSED ({total_warnings} total warnings across {len(val_warnings)} sources)"
        )
        lines.append("")
    return lines


def _format_notes_and_stages(report: BuildReport) -> list[str]:
    """Format build notes, errors log, and pipeline stage issues."""
    lines: list[str] = []

    # Errors section
    all_errors: list[str] = []
    for sr in report.source_reports:
        for err in sr.errors:
            all_errors.append(f"  {sr.source}: {err}")
    if report.merged_graph:
        for err in report.merged_graph.errors:
            all_errors.append(f"  {report.merged_graph.graph_id}: {err}")
    if report.upload and report.upload.per_source_errors:
        for key, err in report.upload.per_source_errors.items():
            all_errors.append(f"  upload/{key}: {err}")
    if all_errors:
        lines.append("ERRORS:")
        lines.extend(all_errors)
        lines.append("")

    notes_and_errors = report.build_notes + report.errors_log
    if notes_and_errors:
        lines.append("Build notes:")
        lines.append("")
        for note in notes_and_errors:
            lines.append(f"  {note}")
        lines.append("")

    if report.pipeline_stages_failed:
        lines.append("PIPELINE STAGE ISSUES:")
        for stage in report.pipeline_stages_failed:
            lines.append(f"  FAILED - {stage}")
        lines.append("")
    return lines


def format_text_report(report: BuildReport) -> str:
    """Format a BuildReport as a detailed text report with performance data.

    This is the full report with build performance, per-source timing,
    memory usage, and all details. Saved as build-report.txt.

    Args:
        report: The BuildReport to format

    Returns:
        Formatted text string
    """
    lines: list[str] = []
    lines.extend(_format_report_header(report))

    # Timing and resource summary (detailed report only)
    if report.total_duration_seconds > 0 or report.peak_memory_mb > 0:
        lines.append("=" * 80)
        lines.append("BUILD PERFORMANCE")
        lines.append("=" * 80)
        if report.total_duration_seconds > 0:
            lines.append(f"Total build time:     {format_duration(report.total_duration_seconds, precise=True)}")
        if report.peak_memory_mb > 0:
            lines.append(
                f"Memory (RAM):         peak {report.peak_memory_mb:.0f} MB ({report.peak_memory_mb / 1024:.2f} GB)"
                f"  |  avg {report.avg_memory_mb:.0f} MB  |  min {report.min_memory_mb:.0f} MB"
            )
        if report.disk_total_gb > 0:
            lines.append(
                f"Disk:                 {report.disk_free_gb:.1f} GB free / {report.disk_total_gb:.1f} GB total"
            )
        if report.stage_timings:
            lines.append("")
            hdr = f"  {'STAGE':<12} {'STATUS':<6} {'TIME':>10}   {'PEAK':>8}   {'AVG':>8}   {'MIN':>8}   {'CPU':>5}"
            lines.append(hdr)
            lines.append(
                f"  {'-' * 12} {'-' * 6} {'-' * 10}   {'-' * 8}   {'-' * 8}   {'-' * 8}   {'-' * 5}"
            )
            for st in report.stage_timings:
                icon = "OK" if st.status == "completed" else "FAIL" if st.status == "failed" else "--"
                dur_s = format_duration(st.duration_seconds, precise=True) if st.duration_seconds > 0 else "--"
                peak_s = f"{st.peak_memory_mb:.0f} MB" if st.peak_memory_mb > 0 else "--"
                avg_s = f"{st.avg_memory_mb:.0f} MB" if st.avg_memory_mb > 0 else "--"
                min_s = f"{st.min_memory_mb:.0f} MB" if st.min_memory_mb > 0 else "--"
                cpu_s = f"{st.avg_cpu_percent:.0f}%" if st.avg_cpu_percent > 0 else "--"
                lines.append(f"  {st.stage:<12} {icon:<6} {dur_s:>10}   {peak_s:>8}   {avg_s:>8}   {min_s:>8}   {cpu_s:>5}")
                if st.error:
                    lines.append(f"               ERROR: {st.error}")
        lines.append("=" * 80)
        lines.append("")

    # Per-source timing table (detailed report only)
    sources_with_time = [s for s in report.source_reports if s.duration_seconds and s.duration_seconds > 0]
    if sources_with_time:
        sources_with_time.sort(key=lambda s: s.duration_seconds or 0, reverse=True)
        lines.append(f"SOURCE TIMING ({len(sources_with_time)} sources including node-properties, slowest first):")
        hdr = f"  {'SOURCE':<20} {'TIME':>10}   {'PEAK MEM':>10}   {'NODES':>12}   {'EDGES':>12}   {'STATUS':<8}"
        lines.append(hdr)
        lines.append(f"  {'-' * 20} {'-' * 10}   {'-' * 10}   {'-' * 12}   {'-' * 12}   {'-' * 8}")
        for s in sources_with_time:
            dur_s = format_duration(s.duration_seconds, precise=True) if s.duration_seconds else "--"
            mem_s = f"{s.peak_memory_mb:.0f} MB" if s.peak_memory_mb > 0 else "--"
            nodes_s = f"{s.total_nodes:,}" if s.total_nodes else "--"
            edges_s = f"{s.total_edges:,}" if s.total_edges else "--"
            lines.append(f"  {s.source:<20} {dur_s:>10}   {mem_s:>10}   {nodes_s:>12}   {edges_s:>12}   {s.status:<8}")
        lines.append("")

    lines.extend(_format_upload_section(report))
    lines.extend(_format_source_list(report))
    lines.extend(_format_versions_section(report))
    lines.extend(_format_notes_and_stages(report))
    lines.extend(_format_validation_section(report))

    return "\n".join(lines)


def format_summary_report(report: BuildReport) -> str:
    """Format a BuildReport as a short summary for sharing (Slack, email).

    Omits performance details, per-source timing, and memory data.
    Includes upload summary, source list, validation, notes, and errors.

    Args:
        report: The BuildReport to format

    Returns:
        Formatted text string
    """
    lines: list[str] = []
    lines.extend(_format_report_header(report))
    lines.extend(_format_upload_section(report))
    lines.extend(_format_source_list(report))
    lines.extend(_format_versions_section(report))
    lines.extend(_format_validation_section(report))
    lines.extend(_format_notes_and_stages(report))

    return "\n".join(lines)


def format_json_report(report: BuildReport) -> str:
    """Format a BuildReport as JSON.

    Args:
        report: The BuildReport to format

    Returns:
        JSON string
    """
    data = asdict(report)
    return json.dumps(data, indent=2)


def load_errors_log(errors_path: Path) -> list[str]:
    """Load error lines from an errors.log file.

    Args:
        errors_path: Direct path to the errors.log file (typically
            logs/errors/{timestamp}/errors.log). The prior signature
            accepted a directory and looked for ``errors.log`` inside
            it, which never matched the orchestrator's actual layout.

    Returns:
        List of non-header error lines
    """
    if not errors_path.exists():
        return []

    lines = []
    for line in errors_path.read_text().splitlines():
        line = line.strip()
        # Skip header lines
        if not line or line.startswith("ERRORS AND WARNINGS LOG") or line.startswith("Build started:"):
            continue
        lines.append(line)
    return lines


def save_report(
    report: BuildReport,
    report_dir: Path,
    errors_log_path: Path | None = None,
) -> tuple[Path, Path, Path]:
    """Save report to a directory as detailed text, summary text, and JSON.

    If ``errors_log_path`` is provided and ``report.errors_log`` is empty,
    loads error lines from that file into the report before generating the
    text reports.

    Args:
        report: The BuildReport to save
        report_dir: Directory to save reports into
        errors_log_path: Optional explicit path to the build's errors.log
            (typically ``logs/errors/{timestamp}/errors.log``). Passing this
            populates ``report.errors_log`` so errors appear in the text
            and JSON outputs.

    Returns:
        Tuple of (detailed_path, summary_path, json_path)
    """
    report_dir.mkdir(parents=True, exist_ok=True)

    # Load errors from the real errors.log path into the report
    if not report.errors_log and errors_log_path is not None:
        report.errors_log = load_errors_log(errors_log_path)

    json_path = report_dir / "build-report.json"
    with json_path.open("w") as f:
        f.write(format_json_report(report))

    text_path = report_dir / "build-report.txt"
    with text_path.open("w") as f:
        f.write(format_text_report(report))

    summary_path = report_dir / "build-summary.txt"
    with summary_path.open("w") as f:
        f.write(format_summary_report(report))

    return text_path, summary_path, json_path


@click.command()
@click.option("--sources", type=str, default=None, help="Space-separated list of sources (default: auto-discover)")
@click.option("--graph-id", type=str, default="translator_kg", help="Merged graph ID (default: translator_kg)")
@click.option(
    "--node-properties",
    type=str,
    default="ncbi_gene",
    help="Space-separated node-property-only sources (default: ncbi_gene)",
)
@click.option(
    "--upload-results",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to upload results JSON file",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format (default: text)",
)
@click.option("--output", type=click.Path(path_type=Path), default=None, help="Write report to file")
def main(sources, graph_id, node_properties, upload_results, output_format, output) -> None:
    """Generate automated build report from pipeline artifacts."""
    setup_logging(source="report")

    # Parse sources
    if sources:
        source_list = sources.split()
    else:
        # Auto-discover from /data directory
        data_path = Path(INGESTS_DATA_PATH)
        source_list = sorted([d.name for d in data_path.iterdir() if d.is_dir()]) if data_path.exists() else []

    node_props = node_properties.split() if node_properties else []

    logger.info("Generating build report for %d sources...", len(source_list))

    report = generate_build_report(
        sources=source_list,
        graph_id=graph_id,
        node_properties=node_props,
        upload_results_path=upload_results,
    )

    if output_format == "json":
        formatted = format_json_report(report)
    else:
        formatted = format_text_report(report)

    # Save to timestamped report directory
    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y_%m_%d_%H%M%S")
    report_dir = REPORTS_BASE / timestamp
    text_path, summary_path, json_path = save_report(report, report_dir)
    logger.info("Report saved to: %s", report_dir)

    # Also save as "latest" symlink
    update_latest_copy(REPORTS_BASE, report_dir.name)

    # Write to user-specified output file
    if output:
        with output.open("w") as f:
            f.write(formatted)
        logger.info("Report written to: %s", output)

    # Print to stdout
    print(formatted)


if __name__ == "__main__":
    main()
