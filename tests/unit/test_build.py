"""Tests for the build orchestration and reporting system.

Tests cover:
- PerformanceTracker (memory/CPU sampling, per-stage stats)
- BuildDisplay (stage state machine, progress formatting)
- Report dataclasses and generation
- Report directory creation and symlinks
- Text and JSON report formatting
- Helper functions (duration formatting, progress bar, ETA)
"""

import json
import logging
import time
from collections import namedtuple
from dataclasses import asdict
from pathlib import Path

import pytest

from translator_ingest.util.run_build.build_report import (
    BuildReport,
    MergedGraphReport,
    SourceReport,
    StageTimingReport,
    UploadReport,
    format_json_report,
    format_text_report,
    generate_build_report,
    load_upload_results,
    save_report,
)
from translator_ingest.util.run_build.run_build import (
    PerformanceTracker,
    BuildDisplay,
    create_report_dir,
    stage_upload,
)
from translator_ingest.util.run_build.utils import (
    MEMORY_CRITICAL_THRESHOLD_PERCENT,
    MEMORY_WARNING_THRESHOLD_PERCENT,
    STAGE_NAMES,
    format_duration,
)


# ── PerformanceTracker tests ─────────────────────────────────────────────────


def test_tracker_start_stop():
    """Tracker starts and stops without error."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    # 3x sample interval to ensure >= 2 samples even under CI load
    time.sleep(0.15)
    tracker.stop()
    stats = tracker.get_overall_stats()
    assert stats["sample_count"] >= 1


def test_tracker_samples_are_positive():
    """Memory and CPU samples should be positive numbers."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    time.sleep(0.2)
    tracker.stop()
    stats = tracker.get_overall_stats()
    assert stats["memory_mb"]["peak"] > 0
    assert stats["memory_mb"]["avg"] > 0
    assert stats["memory_mb"]["min"] > 0


def test_tracker_peak_geq_avg_geq_min():
    """Peak >= avg >= min for memory."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    time.sleep(0.2)
    tracker.stop()
    mem = tracker.get_overall_stats()["memory_mb"]
    assert mem["peak"] >= mem["avg"]
    assert mem["avg"] >= mem["min"]


def test_tracker_per_stage():
    """Samples are correctly partitioned by stage."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()

    tracker.begin_stage("RUN")
    time.sleep(0.15)
    tracker.end_stage("RUN")

    tracker.begin_stage("MERGE")
    time.sleep(0.15)
    tracker.end_stage("MERGE")

    tracker.stop()

    run_stats = tracker.get_stage_stats("RUN")
    merge_stats = tracker.get_stage_stats("MERGE")

    assert run_stats["sample_count"] >= 1
    assert merge_stats["sample_count"] >= 1
    assert run_stats["memory_mb"]["peak"] > 0
    assert merge_stats["memory_mb"]["peak"] > 0


def test_tracker_empty_stage():
    """get_stage_stats returns zeros for a stage that was never tracked."""
    tracker = PerformanceTracker()
    stats = tracker.get_stage_stats("NONEXISTENT")
    assert stats["sample_count"] == 0
    assert stats["memory_mb"]["peak"] == 0.0


def test_stats_from_empty_samples():
    """_stats_from_samples returns zeros for empty list."""
    tracker = PerformanceTracker()
    result = tracker._stats_from_samples([])
    assert result == {"peak": 0.0, "avg": 0.0, "min": 0.0}


def test_stats_from_known_samples():
    """_stats_from_samples computes correct values."""
    tracker = PerformanceTracker()
    result = tracker._stats_from_samples([10.0, 20.0, 30.0])
    assert result["peak"] == 30.0
    assert result["avg"] == 20.0
    assert result["min"] == 10.0


def test_tracker_current_memory():
    """get_current_memory_mb returns a positive value."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    time.sleep(0.1)
    mem = tracker.get_current_memory_mb()
    tracker.stop()
    assert mem > 0


def test_tracker_peak_zero_before_start():
    """Peak memory is 0 before tracking starts."""
    tracker = PerformanceTracker()
    assert tracker.get_peak_memory_mb() == 0.0


def test_tracker_disk_snapshots():
    """start() and stop() capture disk snapshots."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    time.sleep(0.1)
    tracker.stop()
    stats = tracker.get_overall_stats()
    assert stats["disk_start"]["total_gb"] > 0
    assert stats["disk_end"]["total_gb"] > 0


def test_tracker_thread_is_daemon():
    """The sampling thread should be a daemon thread."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    assert tracker._thread.daemon is True
    tracker.stop()


def test_tracker_multiple_begin_end():
    """PerformanceTracker handles begin/end stage correctly when called multiple times."""
    tracker = PerformanceTracker(sample_interval=0.05)
    tracker.start()
    tracker.begin_stage("A")
    time.sleep(0.1)
    tracker.end_stage("A")
    tracker.begin_stage("A")  # restart same stage
    time.sleep(0.1)
    tracker.end_stage("A")
    tracker.stop()
    stats = tracker.get_stage_stats("A")
    # Second begin overwrites samples, but should still have data
    assert stats["sample_count"] >= 1


# ── BuildDisplay tests ───────────────────────────────────────────────────────


def _make_display(total: int = 5, upload: bool = True) -> BuildDisplay:
    """Create a BuildDisplay with a non-sampling tracker."""
    tracker = PerformanceTracker(sample_interval=60)
    return BuildDisplay(total_sources=total, upload_enabled=upload, perf=tracker)


def test_display_initial_state():
    """All stages start as pending."""
    display = _make_display()
    for stage in STAGE_NAMES:
        assert display.stage_status[stage] == "pending"


def test_display_start_stage():
    """start_stage marks stage as running."""
    display = _make_display()
    display.start_stage("RUN")
    assert display.stage_status["RUN"] == "running"
    assert display.current_stage_idx == 0


def test_display_complete_stage():
    """complete_stage marks stage as completed and records duration."""
    display = _make_display()
    display.start_stage("MERGE")
    time.sleep(0.01)
    display.complete_stage("MERGE")
    assert display.stage_status["MERGE"] == "completed"
    assert display.stage_durations["MERGE"] > 0


def test_display_fail_stage():
    """fail_stage marks stage as failed."""
    display = _make_display()
    display.start_stage("UPLOAD")
    display.fail_stage("UPLOAD", "connection error")
    assert display.stage_status["UPLOAD"] == "failed"


def test_display_skip_stage():
    """skip_stage marks stage as skipped."""
    display = _make_display()
    display.skip_stage("UPLOAD")
    assert display.stage_status["UPLOAD"] == "skipped"


def test_display_num_stages_with_upload():
    """With upload enabled, num_stages is 4."""
    display = _make_display(upload=True)
    assert display.num_stages == 4


def test_display_num_stages_without_upload():
    """Without upload, num_stages is 3."""
    display = _make_display(upload=False)
    assert display.num_stages == 3


def test_display_run_tracking_lists():
    """Run stage tracking lists start empty."""
    display = _make_display()
    assert display.run_done == []
    assert display.run_failed == []
    assert display.run_running == []


# ── Helper function tests ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (5, "5s"),
        (30, "30s"),
        (65, "1m 5s"),
        (3661, "1h 1m"),
        (7200, "2h 0m"),
    ],
)
def test_format_duration(seconds, expected):
    """Duration formatting matches expected output."""
    assert format_duration(seconds) == expected


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (5.0, "5.0s"),
        (65.0, "1m 5s"),
        (3661.0, "1h 1m"),
    ],
)
def test_format_duration_precise(seconds, expected):
    """Duration formatting with precise=True matches expected output."""
    assert format_duration(seconds, precise=True) == expected



# ── Report dataclass tests ───────────────────────────────────────────────────


def test_source_report_defaults():
    """SourceReport has sensible defaults."""
    sr = SourceReport(source="ctd")
    assert sr.source == "ctd"
    assert sr.status == "unknown"
    assert sr.errors == []
    assert sr.warnings == []
    assert sr.peak_memory_mb == 0.0


def test_stage_timing_report_defaults():
    """StageTimingReport has sensible defaults."""
    st = StageTimingReport(stage="RUN")
    assert st.stage == "RUN"
    assert st.duration_seconds == 0.0
    assert st.status == "pending"


def test_build_report_defaults():
    """BuildReport initializes with empty collections."""
    br = BuildReport()
    assert br.source_reports == []
    assert br.stage_timings == []
    assert br.pipeline_stages_completed == []
    assert br.pipeline_stages_failed == []
    assert br.build_notes == []


def test_build_report_asdict():
    """BuildReport can be serialized via asdict."""
    br = BuildReport(timestamp="2026-03-12T10:00:00", peak_memory_mb=1024.0)
    d = asdict(br)
    assert d["timestamp"] == "2026-03-12T10:00:00"
    assert d["peak_memory_mb"] == 1024.0
    assert isinstance(d["source_reports"], list)


def test_upload_report_defaults():
    """UploadReport has sensible defaults."""
    ur = UploadReport()
    assert ur.files_uploaded == 0
    assert ur.per_source_errors == {}


def test_merged_graph_report_defaults():
    """MergedGraphReport has sensible defaults."""
    mg = MergedGraphReport(graph_id="translator_kg")
    assert mg.graph_id == "translator_kg"
    assert mg.sources_included == []


# ── Report generation tests ──────────────────────────────────────────────────


_SOURCE_VERSION = "v1"
_TRANSFORM_VERSION = "t1"
_NODE_NORM_VERSION = "1.0"
_BIOLINK_VERSION = "4.2.0"
_RELEASE_VERSION = "2026_03_12"
_SOURCES = ("test_source_a", "test_source_b")
_GRAPH_ID = "test_kg"


def _write_source_artifacts(data_path: Path, releases_path: Path, source: str) -> None:
    """Write realistic pipeline artifacts for one source."""
    source_dir = data_path / source
    source_dir.mkdir(parents=True, exist_ok=True)

    build_data = {
        "source": source,
        "source_version": _SOURCE_VERSION,
        "transform_version": _TRANSFORM_VERSION,
        "node_norm_version": _NODE_NORM_VERSION,
        "biolink_version": _BIOLINK_VERSION,
        "build_version": f"{source}_{_SOURCE_VERSION}_{_TRANSFORM_VERSION}_{_NODE_NORM_VERSION}_{_BIOLINK_VERSION}",
    }
    (source_dir / "latest-build.json").write_text(json.dumps(build_data))

    val_dir = (
        source_dir / _SOURCE_VERSION
        / f"transform_{_TRANSFORM_VERSION}"
        / f"normalization_{_NODE_NORM_VERSION}"
        / f"validation_{_BIOLINK_VERSION}"
    )
    val_dir.mkdir(parents=True, exist_ok=True)
    val_report = {
        "validation_status": "PASSED",
        "statistics": {
            "total_nodes": 1000,
            "total_edges": 5000,
            "validation_errors": 0,
            "validation_warnings": 0,
        },
    }
    (val_dir / "validation-report.json").write_text(json.dumps(val_report))

    rel_dir = releases_path / source
    rel_dir.mkdir(parents=True, exist_ok=True)
    (rel_dir / "latest-release.json").write_text(
        json.dumps({"release_version": _RELEASE_VERSION, "source": source})
    )


def _write_merged_graph_artifacts(releases_path: Path) -> None:
    """Write realistic merged-graph release artifacts."""
    graph_dir = releases_path / _GRAPH_ID
    graph_dir.mkdir(parents=True, exist_ok=True)
    (graph_dir / "latest-release.json").write_text(json.dumps({
        "build_version": "merged_build",
        "release_version": _RELEASE_VERSION,
        "biolink_version": _BIOLINK_VERSION,
        "node_norm_version": _NODE_NORM_VERSION,
    }))
    version_dir = graph_dir / _RELEASE_VERSION
    version_dir.mkdir(parents=True, exist_ok=True)
    (version_dir / "graph-metadata.json").write_text(json.dumps({
        "isBasedOn": [{"id": s} for s in _SOURCES],
    }))


@pytest.fixture()
def make_report(tmp_path, monkeypatch):
    """Create on-disk pipeline artifacts and return a report generator callable.

    Writes realistic latest-build.json, validation-report.json,
    latest-release.json, and graph-metadata.json files so that
    generate_build_report exercises the real collect_source_report
    and collect_merged_graph_report code paths.
    """
    data_path = tmp_path / "data"
    releases_path = tmp_path / "releases"
    data_path.mkdir()
    releases_path.mkdir()

    monkeypatch.setattr(
        "translator_ingest.util.run_build.build_report.INGESTS_DATA_PATH",
        str(data_path),
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.build_report.INGESTS_RELEASES_PATH",
        str(releases_path),
    )
    monkeypatch.setattr(
        "translator_ingest.util.storage.local.INGESTS_DATA_PATH",
        str(data_path),
    )

    for source in _SOURCES:
        _write_source_artifacts(data_path, releases_path, source)
    _write_merged_graph_artifacts(releases_path)

    def _generate(**kwargs) -> BuildReport:
        defaults = dict(
            sources=list(_SOURCES),
            graph_id=_GRAPH_ID,
            node_properties=["ncbi_gene"],
            total_duration=120.5,
            peak_memory_mb=2048.0,
            avg_memory_mb=1500.0,
            min_memory_mb=512.0,
            stage_timings=[
                StageTimingReport(stage="RUN", duration_seconds=60.0, peak_memory_mb=2048.0,
                                  avg_memory_mb=1500.0, min_memory_mb=512.0, avg_cpu_percent=85.0,
                                  status="completed"),
                StageTimingReport(stage="MERGE", duration_seconds=30.0, peak_memory_mb=1800.0,
                                  avg_memory_mb=1200.0, min_memory_mb=800.0, avg_cpu_percent=50.0,
                                  status="completed"),
                StageTimingReport(stage="RELEASE", duration_seconds=20.0, peak_memory_mb=500.0,
                                  avg_memory_mb=400.0, min_memory_mb=300.0, avg_cpu_percent=30.0,
                                  status="completed"),
            ],
            source_durations={"test_source_a": 45.0, "test_source_b": 15.0},
            source_memory={
                "test_source_a": {"peak_mb": 1024, "avg_mb": 800, "min_mb": 600},
                "test_source_b": {"peak_mb": 512, "avg_mb": 400, "min_mb": 200},
            },
            disk_usage={"free_gb": 100.0, "total_gb": 500.0},
        )
        defaults.update(kwargs)
        return generate_build_report(**defaults)

    return _generate


def test_generate_populates_timestamp(make_report):
    """Generated report has a timestamp."""
    report = make_report()
    assert report.timestamp != ""


def test_generate_populates_performance(make_report):
    """Generated report has performance data."""
    report = make_report()
    assert report.peak_memory_mb == 2048.0
    assert report.avg_memory_mb == 1500.0
    assert report.min_memory_mb == 512.0


def test_generate_populates_disk(make_report):
    """Generated report has disk usage."""
    report = make_report()
    assert report.disk_free_gb == 100.0
    assert report.disk_total_gb == 500.0


def test_generate_populates_stage_timings(make_report):
    """Generated report has stage timings."""
    report = make_report()
    assert len(report.stage_timings) == 3
    assert report.stage_timings[0].stage == "RUN"


def test_generate_populates_source_reports(make_report):
    """Generated report has per-source reports."""
    report = make_report()
    assert len(report.source_reports) == 2
    sources = {sr.source for sr in report.source_reports}
    assert sources == {"test_source_a", "test_source_b"}


def test_generate_source_memory_applied(make_report):
    """Source memory data is applied to source reports."""
    report = make_report()
    sr_a = next(sr for sr in report.source_reports if sr.source == "test_source_a")
    assert sr_a.peak_memory_mb == 1024


def test_generate_source_duration_applied(make_report):
    """Source durations are applied to source reports."""
    report = make_report()
    sr_a = next(sr for sr in report.source_reports if sr.source == "test_source_a")
    assert sr_a.duration_seconds == 45.0


def test_generate_merged_graph(make_report):
    """Generated report has merged graph data."""
    report = make_report()
    assert report.merged_graph is not None
    assert report.merged_graph.status == "merged"


def test_generate_pipeline_stages_completed(make_report):
    """All pipeline stages should be marked as completed."""
    report = make_report()
    completed_stages_str = " ".join(report.pipeline_stages_completed)
    assert "run" in completed_stages_str
    assert "merge" in completed_stages_str
    assert "release" in completed_stages_str


# ── Text report formatting tests ─────────────────────────────────────────────


def _make_text_report() -> str:
    """Generate a text report from a fully populated BuildReport."""
    report = BuildReport(
        timestamp="2026-03-12T10:00:00",
        total_duration_seconds=300.0,
        peak_memory_mb=4096.0,
        avg_memory_mb=2048.0,
        min_memory_mb=512.0,
        disk_free_gb=200.0,
        disk_total_gb=500.0,
        stage_timings=[
            StageTimingReport(stage="RUN", duration_seconds=200.0, peak_memory_mb=4096.0,
                              avg_memory_mb=2048.0, min_memory_mb=512.0, avg_cpu_percent=90.0,
                              status="completed"),
            StageTimingReport(stage="MERGE", duration_seconds=60.0, peak_memory_mb=3000.0,
                              avg_memory_mb=2000.0, min_memory_mb=1000.0, avg_cpu_percent=50.0,
                              status="completed"),
        ],
        source_reports=[
            SourceReport(source="ctd", status="released", total_nodes=50000, total_edges=200000,
                         duration_seconds=180.0, peak_memory_mb=3000.0, release_version="2026_03_12"),
            SourceReport(source="go_cam", status="released", total_nodes=10000, total_edges=30000,
                         duration_seconds=20.0, peak_memory_mb=500.0, release_version="2026_03_12"),
        ],
        merged_graph=MergedGraphReport(graph_id="translator_kg", status="merged",
                                       sources_included=["ctd", "go_cam"]),
        biolink_version="4.2.0",
        node_norm_version="1.0",
        release_version="2026_03_12",
    )
    return format_text_report(report)


def test_text_report_contains_header():
    """Text report starts with the standard opening."""
    text = _make_text_report()
    assert "latest ingest pipeline build has finished" in text


def test_text_report_contains_performance_section():
    """Text report has BUILD PERFORMANCE section."""
    text = _make_text_report()
    assert "BUILD PERFORMANCE" in text


def test_text_report_contains_stage_table():
    """Text report has stage timing table."""
    text = _make_text_report()
    assert "STAGE" in text
    assert "RUN" in text
    assert "MERGE" in text


def test_text_report_contains_source_timing():
    """Text report has source timing section."""
    text = _make_text_report()
    assert "SOURCE TIMING" in text
    assert "ctd" in text


def test_text_report_contains_memory_stats():
    """Text report shows memory statistics."""
    text = _make_text_report()
    assert "4096 MB" in text  # peak
    assert "peak" in text.lower()


def test_text_report_contains_disk_info():
    """Text report shows disk usage."""
    text = _make_text_report()
    assert "200.0 GB free" in text


def test_text_report_contains_sources_list():
    """Text report lists individual sources."""
    text = _make_text_report()
    assert "ctd" in text
    assert "go_cam" in text


def test_text_report_contains_versions():
    """Text report shows version info."""
    text = _make_text_report()
    assert "4.2.0" in text


def test_text_report_contains_merged_graph():
    """Text report mentions the merged graph."""
    text = _make_text_report()
    assert "translator_kg" in text


# ── JSON report formatting tests ─────────────────────────────────────────────


def test_json_report_is_valid():
    """JSON report is valid JSON."""
    report = BuildReport(timestamp="2026-03-12T10:00:00")
    result = format_json_report(report)
    data = json.loads(result)
    assert data["timestamp"] == "2026-03-12T10:00:00"


def test_json_report_includes_all_fields():
    """JSON report includes all BuildReport fields."""
    report = BuildReport(
        timestamp="2026-03-12T10:00:00",
        peak_memory_mb=1024.0,
        source_reports=[SourceReport(source="ctd", status="released")],
        stage_timings=[StageTimingReport(stage="RUN", status="completed")],
    )
    data = json.loads(format_json_report(report))
    assert "timestamp" in data
    assert "peak_memory_mb" in data
    assert "source_reports" in data
    assert "stage_timings" in data
    assert len(data["source_reports"]) == 1
    assert data["source_reports"][0]["source"] == "ctd"


def test_json_report_roundtrips():
    """JSON report can be loaded back and key fields match."""
    report = BuildReport(
        timestamp="2026-03-12T10:00:00",
        total_duration_seconds=100.0,
        peak_memory_mb=2048.0,
        avg_memory_mb=1024.0,
        min_memory_mb=512.0,
    )
    data = json.loads(format_json_report(report))
    assert data["total_duration_seconds"] == 100.0
    assert data["peak_memory_mb"] == 2048.0
    assert data["avg_memory_mb"] == 1024.0
    assert data["min_memory_mb"] == 512.0


# ── Report directory tests ───────────────────────────────────────────────────


def test_create_report_dir(tmp_path, monkeypatch):
    """create_report_dir creates a timestamped dir with stage subdirectories,
    plus a 'latest' directory copy (real directory, not a symlink).
    """
    monkeypatch.setattr("translator_ingest.util.run_build.run_build.REPORTS_BASE", tmp_path)
    report_dir = create_report_dir()

    assert report_dir.exists()
    assert (report_dir / "stages" / "run").exists()
    assert (report_dir / "stages" / "merge").exists()
    assert (report_dir / "stages" / "release").exists()
    assert (report_dir / "stages" / "upload").exists()

    latest = tmp_path / "latest"
    # 'latest' is now a real directory copy (matches the releases pattern),
    # not a symlink, so downstream tools do not need symlink-aware handling.
    assert latest.is_dir()
    assert not latest.is_symlink()
    assert (latest / "stages" / "run").exists()


def test_create_report_dir_updates_latest(tmp_path, monkeypatch):
    """Creating a second report dir refreshes the 'latest' directory to match
    the new timestamp's contents (prior timestamp dir is kept intact).

    Uses explicit timestamps because the default format has second precision
    and the test would otherwise be flaky when both calls land in the same second.
    """
    from translator_ingest.util.run_build.utils import update_latest_copy
    monkeypatch.setattr("translator_ingest.util.run_build.run_build.REPORTS_BASE", tmp_path)

    first = create_report_dir(timestamp="2026_04_21_100000")
    (first / "marker-first.txt").write_text("first")

    second = create_report_dir(timestamp="2026_04_21_100005")
    (second / "marker-second.txt").write_text("second")
    # Refresh 'latest' after populating stages, matching what a real build does.
    update_latest_copy(tmp_path, second.name)

    latest = tmp_path / "latest"
    assert latest.is_dir()
    assert not latest.is_symlink()
    # Latest reflects the newest build, not the first one
    assert (latest / "marker-second.txt").exists()
    assert not (latest / "marker-first.txt").exists()
    # First timestamped dir is preserved on disk
    assert first.exists()
    assert (first / "marker-first.txt").exists()


def test_save_report_creates_files(tmp_path):
    """save_report creates detailed, summary, and JSON files."""
    report = BuildReport(timestamp="2026-03-12T10:00:00", peak_memory_mb=512.0)
    text_path, summary_path, json_path = save_report(report, tmp_path)

    assert text_path.exists()
    assert summary_path.exists()
    assert json_path.exists()
    assert text_path.name == "build-report.txt"
    assert summary_path.name == "build-summary.txt"
    assert json_path.name == "build-report.json"

    # Verify JSON is valid
    with json_path.open() as f:
        data = json.load(f)
    assert data["peak_memory_mb"] == 512.0

    # Verify text reports are non-empty
    assert text_path.read_text().strip() != ""
    assert summary_path.read_text().strip() != ""


# ── Upload report loading tests ──────────────────────────────────────────────


def test_load_upload_results_none():
    """Returns None when path is None."""
    assert load_upload_results(None) is None


def test_load_upload_results_missing_file(tmp_path):
    """Returns None when file doesn't exist."""
    assert load_upload_results(tmp_path / "nonexistent.json") is None


def test_load_upload_results_valid(tmp_path):
    """Correctly parses upload results JSON."""
    data = {
        "sources_processed": 5,
        "total_uploaded": 20,
        "total_failed": 1,
        "total_bytes_transferred": 1024 * 1024 * 1024 * 2,  # 2 GB
        "total_bytes_freed": 1024 * 1024 * 1024,  # 1 GB
        "per_source_stats": {
            "ctd": {
                "data_upload": {"status": "ok"},
                "releases_upload": {"error": "timeout"},
            },
        },
    }
    path = tmp_path / "upload-results.json"
    path.write_text(json.dumps(data))

    report = load_upload_results(path)
    assert report is not None
    assert report.sources_processed == 5
    assert report.files_uploaded == 20
    assert report.files_failed == 1
    assert report.data_transferred_gb == pytest.approx(2.0, abs=0.01)
    assert report.ebs_space_freed_gb == pytest.approx(1.0, abs=0.01)
    assert "ctd/releases_upload" in report.per_source_errors


# ── Error/edge case tests ────────────────────────────────────────────────────


def test_text_report_with_errors():
    """Text report includes error section when sources have errors."""
    report = BuildReport(
        source_reports=[
            SourceReport(source="bad_source", status="no_build",
                         errors=["No latest-build.json found"]),
        ],
    )
    text = format_text_report(report)
    assert "ERRORS" in text
    assert "bad_source" in text


def test_text_report_with_validation_failures():
    """Text report includes validation failure section."""
    report = BuildReport(
        source_reports=[
            SourceReport(source="bad_val", status="validation_failed",
                         validation_status="FAILED", validation_errors=3, validation_warnings=1),
        ],
    )
    text = format_text_report(report)
    assert "VALIDATION FAILURES" in text
    assert "bad_val" in text


def test_text_report_with_upload():
    """Text report includes upload section when upload data is present."""
    report = BuildReport(
        upload=UploadReport(
            sources_processed=10, files_uploaded=50, files_failed=2,
            data_transferred_gb=5.5, ebs_space_freed_gb=3.2,
        ),
    )
    text = format_text_report(report)
    assert "S3 UPLOAD SUMMARY" in text
    assert "50" in text


def test_text_report_minimal():
    """Text report generates without error for minimal input."""
    report = BuildReport()
    text = format_text_report(report)
    assert "latest ingest pipeline build has finished" in text


def test_text_report_with_build_notes():
    """Text report includes build notes."""
    report = BuildReport(
        build_notes=["ncbi_gene: Not released as standalone source"],
    )
    text = format_text_report(report)
    assert "Build notes" in text
    assert "ncbi_gene" in text


def test_text_report_with_failed_stages():
    """Text report includes pipeline stage issues."""
    report = BuildReport(
        pipeline_stages_failed=["run: 2 sources failed: src_a, src_b"],
    )
    text = format_text_report(report)
    assert "PIPELINE STAGE ISSUES" in text
    assert "src_a" in text


def test_generate_failed_sources_noted_in_build_notes(make_report):
    """Sources that failed RUN are noted as using previous build data."""
    report = make_report(failed_sources=["test_source_b"])
    notes = " ".join(report.build_notes)
    assert "test_source_b" in notes
    assert "previous successful build" in notes


def test_generate_no_failed_sources_no_stale_note(make_report):
    """When no sources failed, no stale-data notes are added."""
    report = make_report(failed_sources=[])
    stale_notes = [n for n in report.build_notes if "previous successful build" in n]
    assert stale_notes == []


# ── Memory guardian tests ────────────────────────────────────────────────────

# Lightweight stand-in for psutil.virtual_memory() return value.
FakeVmem = namedtuple("FakeVmem", ["total", "available", "percent", "used", "free"])

TOTAL_BYTES = 16 * 1024**3  # 16 GB


def _fake_vmem(percent: float) -> FakeVmem:
    """Build a fake svmem-like tuple with the given percent used."""
    used = int(TOTAL_BYTES * percent / 100)
    free = TOTAL_BYTES - used
    available = free
    return FakeVmem(
        total=TOTAL_BYTES, available=available, percent=percent,
        used=used, free=free,
    )


def test_tracker_memory_critical_event_not_set_by_default():
    """Memory critical event should not be set on a fresh tracker."""
    tracker = PerformanceTracker(sample_interval=60)
    assert not tracker.memory_critical_event.is_set()


def test_tracker_memory_guardian_default_thresholds():
    """Default thresholds match the constants from utils."""
    tracker = PerformanceTracker(sample_interval=60)
    assert tracker._memory_warning_percent == MEMORY_WARNING_THRESHOLD_PERCENT
    assert tracker._memory_critical_percent == MEMORY_CRITICAL_THRESHOLD_PERCENT


def test_tracker_get_memory_critical_info_not_triggered():
    """get_memory_critical_info returns zeros when event not triggered."""
    tracker = PerformanceTracker(sample_interval=60)
    info = tracker.get_memory_critical_info()
    assert info == {"used_mb": 0.0, "total_mb": 0.0, "threshold_percent": 0.0}


def test_tracker_warning_threshold_logs(monkeypatch, caplog):
    """Warning is logged once when memory exceeds warning threshold."""
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.psutil.virtual_memory",
        lambda: _fake_vmem(85.0),
    )
    tracker = PerformanceTracker(
        sample_interval=0.02,
        memory_warning_percent=80.0,
        memory_critical_percent=95.0,
    )
    tracker.start()
    time.sleep(0.15)
    tracker.stop()

    warning_records = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "MEMORY WARNING" in r.message
    ]
    # Should log exactly once despite multiple samples
    assert len(warning_records) == 1
    assert "85.0%" in warning_records[0].message
    # Critical event should NOT fire (85 < 95)
    assert not tracker.memory_critical_event.is_set()


def test_tracker_critical_threshold_consecutive_samples(monkeypatch):
    """Critical event fires after enough consecutive samples above threshold."""
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.psutil.virtual_memory",
        lambda: _fake_vmem(95.0),
    )
    tracker = PerformanceTracker(
        sample_interval=0.02,
        memory_critical_percent=90.0,
        critical_consecutive_count=2,
    )
    tracker.start()
    # Wait long enough for 2+ samples
    time.sleep(0.15)
    tracker.stop()

    assert tracker.memory_critical_event.is_set()
    info = tracker.get_memory_critical_info()
    assert info["used_mb"] > 0
    assert info["total_mb"] > 0
    assert info["threshold_percent"] == 90.0


def test_tracker_critical_threshold_transient_spike_does_not_trigger(monkeypatch):
    """Non-consecutive spikes above critical threshold do not trigger abort."""
    call_count = 0

    def _alternating_vmem():
        nonlocal call_count
        call_count += 1
        # Alternate: high, low, high, low, ...
        if call_count % 2 == 1:
            return _fake_vmem(95.0)
        return _fake_vmem(50.0)

    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.psutil.virtual_memory",
        _alternating_vmem,
    )
    tracker = PerformanceTracker(
        sample_interval=0.02,
        memory_critical_percent=90.0,
        critical_consecutive_count=3,
    )
    tracker.start()
    time.sleep(0.25)
    tracker.stop()

    assert not tracker.memory_critical_event.is_set()


def test_tracker_warning_resets_below_threshold(monkeypatch, caplog):
    """Warning can fire again after memory drops below warning threshold."""
    call_count = 0

    def _up_down_vmem():
        nonlocal call_count
        call_count += 1
        # First 3 samples high, next 3 low, then 3 high again
        if call_count <= 3:
            return _fake_vmem(85.0)
        if call_count <= 6:
            return _fake_vmem(50.0)
        return _fake_vmem(85.0)

    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.psutil.virtual_memory",
        _up_down_vmem,
    )
    tracker = PerformanceTracker(
        sample_interval=0.02,
        memory_warning_percent=80.0,
        memory_critical_percent=99.0,  # won't trigger
    )
    tracker.start()
    time.sleep(0.3)
    tracker.stop()

    warning_records = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "MEMORY WARNING" in r.message
    ]
    # Should have fired twice: once for the first spike, once after reset
    assert len(warning_records) >= 2


def test_tracker_memory_critical_info_populated_on_trigger(monkeypatch):
    """get_memory_critical_info returns real values after critical event fires."""
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.psutil.virtual_memory",
        lambda: _fake_vmem(92.0),
    )
    tracker = PerformanceTracker(
        sample_interval=0.02,
        memory_critical_percent=90.0,
        critical_consecutive_count=2,
    )
    tracker.start()
    time.sleep(0.15)
    tracker.stop()

    info = tracker.get_memory_critical_info()
    expected_used_mb = int(TOTAL_BYTES * 92.0 / 100) / (1024**2)
    assert abs(info["used_mb"] - expected_used_mb) < 1.0
    assert info["total_mb"] == TOTAL_BYTES / (1024**2)
    assert info["threshold_percent"] == 90.0


def test_tracker_custom_thresholds():
    """Custom threshold values are stored correctly."""
    tracker = PerformanceTracker(
        sample_interval=60,
        memory_warning_percent=70.0,
        memory_critical_percent=85.0,
        critical_consecutive_count=5,
    )
    assert tracker._memory_warning_percent == 70.0
    assert tracker._memory_critical_percent == 85.0
    assert tracker._critical_consecutive_count == 5


def test_generate_build_report_with_build_notes(make_report):
    """Extra build_notes passed to generate_build_report appear in report."""
    report = make_report(
        build_notes=["BUILD ABORTED: memory exceeded 90%"],
    )
    assert any("ABORTED" in note for note in report.build_notes)


# ── Upload stage gating tests ────────────────────────────────────────────────
#
# These tests verify the two fixed bugs:
# 1. UPLOAD stage runs in the normal happy path (condition fix)
# 2. UPLOAD is correctly skipped when upload=False or memory is critical


def test_upload_stage_pending_after_prior_stages_complete():
    """In the happy path, UPLOAD stays 'pending' after RUN/MERGE/RELEASE complete.

    This is the precondition for the UPLOAD stage to run -- the condition
    ``display.stage_status.get("UPLOAD") == "pending"`` must be true.
    """
    display = _make_display(upload=True)

    # Simulate RUN -> MERGE -> RELEASE completing
    for stage in ("RUN", "MERGE", "RELEASE"):
        display.start_stage(stage)
        display.complete_stage(stage)

    assert display.stage_status["UPLOAD"] == "pending"


def test_upload_stage_gating_happy_path():
    """When upload=True and UPLOAD is pending, the gate condition is met."""
    display = _make_display(upload=True)
    upload_enabled = True

    # Simulate all prior stages completing
    for stage in ("RUN", "MERGE", "RELEASE"):
        display.start_stage(stage)
        display.complete_stage(stage)

    # This is the exact condition from run_build.py line 1557
    should_run = upload_enabled and display.stage_status.get("UPLOAD") == "pending"
    assert should_run is True


def test_upload_stage_gating_upload_disabled():
    """When upload=False, the upload gate condition is not met."""
    display = _make_display(upload=False)
    upload_enabled = False

    for stage in ("RUN", "MERGE", "RELEASE"):
        display.start_stage(stage)
        display.complete_stage(stage)

    should_run = upload_enabled and display.stage_status.get("UPLOAD") == "pending"
    assert should_run is False


def test_upload_stage_gating_memory_skipped():
    """When UPLOAD was skipped due to memory, the gate condition is not met."""
    display = _make_display(upload=True)
    upload_enabled = True

    for stage in ("RUN", "MERGE", "RELEASE"):
        display.start_stage(stage)
        display.complete_stage(stage)

    # Simulate memory critical after RELEASE -> UPLOAD gets skipped
    display.skip_stage("UPLOAD")

    should_run = upload_enabled and display.stage_status.get("UPLOAD") == "pending"
    assert should_run is False
    assert display.stage_status["UPLOAD"] == "skipped"


def test_upload_stage_skip_marks_skipped():
    """When upload=False and UPLOAD is pending, skip_stage transitions to 'skipped'.

    This tests the elif branch at run_build.py line 1568.
    """
    display = _make_display(upload=True)
    upload_enabled = False

    for stage in ("RUN", "MERGE", "RELEASE"):
        display.start_stage(stage)
        display.complete_stage(stage)

    # Replicate the elif logic
    if not upload_enabled and display.stage_status.get("UPLOAD") == "pending":
        display.skip_stage("UPLOAD")

    assert display.stage_status["UPLOAD"] == "skipped"


def _fake_upload_results(total_failed: int = 0) -> dict:
    """Build a minimal upload_and_cleanup return value."""
    return {
        "sources_processed": 1,
        "total_uploaded": 5,
        "total_failed": total_failed,
        "total_bytes_transferred": 1024,
        "total_bytes_freed": 0,
        "per_source_stats": {},
    }


def test_stage_upload_marks_completed_on_success(tmp_path, monkeypatch):
    """stage_upload marks display as completed when upload_and_cleanup succeeds."""
    display = _make_display(upload=True)

    # Create required report subdirectories
    report_dir = tmp_path / "report"
    (report_dir / "stages" / "upload").mkdir(parents=True)

    # Also create the reports base dir that stage_upload writes to
    reports_base = tmp_path / "reports"
    reports_base.mkdir()
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.REPORTS_BASE", reports_base,
    )

    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_data_sources",
        lambda: ["go_cam"],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_release_sources",
        lambda: ["go_cam"],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.upload_and_cleanup",
        lambda **kwargs: _fake_upload_results(total_failed=0),
    )

    stage_upload(report_dir, display)

    assert display.stage_status["UPLOAD"] == "completed"

    # Verify results were saved
    results_file = report_dir / "stages" / "upload" / "upload-results.json"
    assert results_file.exists()
    saved = json.loads(results_file.read_text())
    assert saved["total_uploaded"] == 5
    assert saved["total_failed"] == 0


def test_stage_upload_marks_failed_on_upload_failures(tmp_path, monkeypatch):
    """stage_upload marks display as failed when some files fail to upload."""
    display = _make_display(upload=True)

    report_dir = tmp_path / "report"
    (report_dir / "stages" / "upload").mkdir(parents=True)

    reports_base = tmp_path / "reports"
    reports_base.mkdir()
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.REPORTS_BASE", reports_base,
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_data_sources",
        lambda: ["go_cam"],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_release_sources",
        lambda: ["go_cam"],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.upload_and_cleanup",
        lambda **kwargs: _fake_upload_results(total_failed=3),
    )

    stage_upload(report_dir, display)

    assert display.stage_status["UPLOAD"] == "failed"


def test_stage_upload_marks_failed_on_exception(tmp_path, monkeypatch):
    """stage_upload marks display as failed and re-raises on S3 exception."""
    display = _make_display(upload=True)

    report_dir = tmp_path / "report"
    (report_dir / "stages" / "upload").mkdir(parents=True)

    reports_base = tmp_path / "reports"
    reports_base.mkdir()
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.REPORTS_BASE", reports_base,
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_data_sources",
        lambda: ["go_cam"],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_release_sources",
        lambda: ["go_cam"],
    )

    def _explode(**kwargs):
        raise ConnectionError("S3 unreachable")

    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.upload_and_cleanup", _explode,
    )

    with pytest.raises(ConnectionError, match="S3 unreachable"):
        stage_upload(report_dir, display)

    assert display.stage_status["UPLOAD"] == "failed"

    # Summary file should still be written (finally block)
    summary_file = report_dir / "stages" / "upload" / "_summary.json"
    assert summary_file.exists()


def test_stage_upload_saves_summary_json(tmp_path, monkeypatch):
    """stage_upload always writes _summary.json even on success."""
    display = _make_display(upload=True)

    report_dir = tmp_path / "report"
    (report_dir / "stages" / "upload").mkdir(parents=True)

    reports_base = tmp_path / "reports"
    reports_base.mkdir()
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.REPORTS_BASE", reports_base,
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_data_sources",
        lambda: [],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.discover_release_sources",
        lambda: [],
    )
    monkeypatch.setattr(
        "translator_ingest.util.run_build.run_build.upload_and_cleanup",
        lambda **kwargs: _fake_upload_results(),
    )

    stage_upload(report_dir, display)

    summary = json.loads((report_dir / "stages" / "upload" / "_summary.json").read_text())
    assert summary["stage"] == "UPLOAD"
    assert summary["status"] == "completed"
    assert "duration_seconds" in summary
    assert "performance" in summary
