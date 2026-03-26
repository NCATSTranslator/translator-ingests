"""
End-to-end pipeline build orchestrator for translator-ingests.

Runs all pipeline stages (run, merge, release, upload) with parallel source
execution, progress tracking, memory monitoring, error collection, and
automated report generation.

Usage:
    uv run python -m translator_ingest.util.run_build.run_build
    uv run python -m translator_ingest.util.run_build.run_build --sources "ctd go_cam"
    uv run python -m translator_ingest.util.run_build.run_build --no-upload
    uv run python -m translator_ingest.util.run_build.run_build --overwrite

Via Makefile:
    make build
    make build SOURCES="ctd go_cam"
    make build NO_UPLOAD=true
"""

import datetime
import io
import json
import logging
import os
import sys
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any

import click
import psutil

from translator_ingest import INGESTS_DATA_PATH, INGESTS_LOGS_PATH, INGESTS_PARSER_PATH
from translator_ingest.merging import (
    generate_merged_graph_release,
    is_merged_graph_release_current,
    merge,
)
from translator_ingest.pipeline import run_pipeline
from translator_ingest.release import generate_release_summary, release_ingest
from translator_ingest.util.logging_utils import get_logger, setup_worker_logging
from translator_ingest.util.run_build import REPORTS_BASE
from translator_ingest.util.run_build.build_report import (
    StageTimingReport,
    format_summary_report,
    generate_build_report,
    save_report,
)
from translator_ingest.util.run_build.utils import (
    BYTES_PER_GB,
    BYTES_PER_MB,
    MEMORY_CRITICAL_CONSECUTIVE_SAMPLES,
    MEMORY_CRITICAL_THRESHOLD_PERCENT,
    MEMORY_WARNING_THRESHOLD_PERCENT,
    STAGE_NAMES,
    STAGE_NAMES_LOWER,
    format_duration,
    update_latest_symlink,
)
from translator_ingest.util.storage.upload_s3 import discover_data_sources, discover_release_sources
from translator_ingest.util.storage.s3 import upload_and_cleanup

logger = get_logger(__name__)


# ── Report directory ──────────────────────────────────────────────────────────

LOGS_BASE = Path(INGESTS_LOGS_PATH)


def create_log_dirs(timestamp: str) -> tuple[dict[str, Path], Path]:
    """Create per-stage log directories and error log directory.

    Structure::

        logs/
        ├── run/{timestamp}/run.log
        ├── merge/{timestamp}/merge.log
        ├── release/{timestamp}/release.log
        ├── upload/{timestamp}/upload.log
        └── errors/{timestamp}/errors.log

    Args:
        timestamp: Shared timestamp string for this build

    Returns:
        Tuple of (stage_log_paths dict mapping stage name to log file Path,
                   error_log_path Path)
    """
    stage_log_paths: dict[str, Path] = {}
    for stage in STAGE_NAMES_LOWER:
        d = LOGS_BASE / stage / timestamp
        d.mkdir(parents=True, exist_ok=True)
        stage_log_paths[stage] = d / f"{stage}.log"

    errors_dir = LOGS_BASE / "errors" / timestamp
    errors_dir.mkdir(parents=True, exist_ok=True)
    error_log_path = errors_dir / "errors.log"

    # Update latest symlinks
    for stage in (*STAGE_NAMES_LOWER, "errors"):
        update_latest_symlink(LOGS_BASE / stage, timestamp)

    return stage_log_paths, error_log_path


def create_report_dir(timestamp: str | None = None) -> Path:
    """Create a timestamped report directory with stage subdirectories and a 'latest' symlink.

    Args:
        timestamp: Optional timestamp string (default: generate from current time)

    Returns:
        Path to the new report directory
    """
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y_%m_%d_%H%M%S")
    report_dir = REPORTS_BASE / timestamp
    report_dir.mkdir(parents=True, exist_ok=True)

    # Create stage subdirectories
    for stage in STAGE_NAMES_LOWER:
        (report_dir / "stages" / stage).mkdir(parents=True, exist_ok=True)

    update_latest_symlink(REPORTS_BASE, report_dir.name)

    return report_dir


# ── Performance tracking ─────────────────────────────────────────────────────

SAMPLE_INTERVAL = 2.0  # seconds between memory samples


def _get_memory_mb() -> float:
    """Get current process tree memory usage in MB (RSS)."""
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss
    for child in process.children(recursive=True):
        try:
            mem += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return mem / BYTES_PER_MB


def _get_cpu_percent() -> float:
    """Get system-wide CPU utilization percent.

    Uses system-wide measurement rather than per-process because the build
    pipeline spawns child processes via ProcessPoolExecutor, and per-process
    cpu_percent(interval=None) requires a prior call on each process to
    establish a baseline — child processes never get primed, returning 0.
    """
    return psutil.cpu_percent(interval=None)


def _get_disk_usage_gb() -> dict[str, float]:
    """Get disk usage for the data partition."""
    data_path = Path(INGESTS_DATA_PATH)
    if data_path.exists():
        usage = psutil.disk_usage(str(data_path))
        return {
            "total_gb": usage.total / BYTES_PER_GB,
            "used_gb": usage.used / BYTES_PER_GB,
            "free_gb": usage.free / BYTES_PER_GB,
            "percent": usage.percent,
        }
    return {"total_gb": 0, "used_gb": 0, "free_gb": 0, "percent": 0}


class PerformanceTracker:
    """Background thread that samples memory/CPU at regular intervals.

    Tracks peak, average, and minimum memory usage per stage and overall.
    """

    def __init__(
        self,
        sample_interval: float = SAMPLE_INTERVAL,
        memory_warning_percent: float = MEMORY_WARNING_THRESHOLD_PERCENT,
        memory_critical_percent: float = MEMORY_CRITICAL_THRESHOLD_PERCENT,
        critical_consecutive_count: int = MEMORY_CRITICAL_CONSECUTIVE_SAMPLES,
    ):
        self.sample_interval = sample_interval
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # Overall samples
        self._all_memory_samples: list[float] = []
        self._all_cpu_samples: list[float] = []

        # Per-stage samples: stage_name -> list of samples
        self._stage_memory_samples: dict[str, list[float]] = {}
        self._stage_cpu_samples: dict[str, list[float]] = {}
        self._current_stage: str | None = None

        # Disk snapshots
        self._disk_start: dict[str, float] = {}
        self._disk_end: dict[str, float] = {}

        # Memory guardian state
        self.memory_critical_event = threading.Event()
        self._memory_warning_percent = memory_warning_percent
        self._memory_critical_percent = memory_critical_percent
        self._critical_consecutive_count = critical_consecutive_count
        self._consecutive_critical_samples: int = 0
        self._warning_logged: bool = False
        self._critical_memory_mb: float = 0.0
        self._total_system_memory_mb: float = psutil.virtual_memory().total / BYTES_PER_MB

    def start(self) -> None:
        """Start the background sampling thread."""
        # Take initial disk snapshot
        self._disk_start = _get_disk_usage_gb()
        # Prime system-wide cpu_percent (first call always returns 0.0)
        psutil.cpu_percent(interval=None)
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the background sampling thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._disk_end = _get_disk_usage_gb()

    def begin_stage(self, stage: str) -> None:
        """Mark the beginning of a new stage for per-stage tracking."""
        with self._lock:
            self._current_stage = stage
            self._stage_memory_samples[stage] = []
            self._stage_cpu_samples[stage] = []

    def end_stage(self, stage: str) -> None:
        """Mark the end of a stage."""
        with self._lock:
            if self._current_stage == stage:
                self._current_stage = None

    def _check_memory_threshold(self) -> None:
        """Check system-wide memory against warning and critical thresholds.

        Uses ``psutil.virtual_memory()`` for the system-wide measurement
        because the kernel OOM killer operates on total system memory, not
        per-process RSS.
        """
        vm = psutil.virtual_memory()
        used_percent = vm.percent

        # Warning threshold — log once, reset when memory drops back down
        if used_percent >= self._memory_warning_percent:
            if not self._warning_logged:
                self._warning_logged = True
                logger.warning(
                    "MEMORY WARNING: System memory at %.1f%% (%d MB used / %d MB total). "
                    "Warning threshold: %.0f%%. Critical threshold: %.0f%%.",
                    used_percent,
                    vm.used / BYTES_PER_MB,
                    vm.total / BYTES_PER_MB,
                    self._memory_warning_percent,
                    self._memory_critical_percent,
                )
        else:
            self._warning_logged = False

        # Critical threshold — require consecutive samples to avoid transient spikes
        if used_percent >= self._memory_critical_percent:
            self._consecutive_critical_samples += 1
            if (
                self._consecutive_critical_samples >= self._critical_consecutive_count
                and not self.memory_critical_event.is_set()
            ):
                self._critical_memory_mb = vm.used / BYTES_PER_MB
                self.memory_critical_event.set()
                logger.error(
                    "MEMORY CRITICAL: System memory at %.1f%% (%d MB used / %d MB total) "
                    "for %d consecutive samples (%.0fs). "
                    "Threshold: %.0f%%. Initiating graceful shutdown.",
                    used_percent,
                    vm.used / BYTES_PER_MB,
                    vm.total / BYTES_PER_MB,
                    self._consecutive_critical_samples,
                    self._consecutive_critical_samples * self.sample_interval,
                    self._memory_critical_percent,
                )
        else:
            self._consecutive_critical_samples = 0

    def _sample_loop(self) -> None:
        """Sampling loop run in background thread."""
        while not self._stop_event.is_set():
            mem = _get_memory_mb()
            cpu = _get_cpu_percent()
            with self._lock:
                self._all_memory_samples.append(mem)
                self._all_cpu_samples.append(cpu)
                if self._current_stage:
                    self._stage_memory_samples.setdefault(self._current_stage, []).append(mem)
                    self._stage_cpu_samples.setdefault(self._current_stage, []).append(cpu)
            self._check_memory_threshold()
            self._stop_event.wait(self.sample_interval)

    def _stats_from_samples(self, samples: list[float]) -> dict[str, float]:
        """Compute peak/avg/min from a list of samples."""
        if not samples:
            return {"peak": 0.0, "avg": 0.0, "min": 0.0}
        return {
            "peak": max(samples),
            "avg": sum(samples) / len(samples),
            "min": min(samples),
        }

    def get_overall_stats(self) -> dict[str, Any]:
        """Get overall performance statistics."""
        with self._lock:
            mem_stats = self._stats_from_samples(self._all_memory_samples)
            cpu_stats = self._stats_from_samples(self._all_cpu_samples)
        return {
            "memory_mb": mem_stats,
            "cpu_percent": cpu_stats,
            "disk_start": self._disk_start,
            "disk_end": self._disk_end,
            "sample_count": len(self._all_memory_samples),
            "sample_interval_seconds": self.sample_interval,
        }

    def get_stage_stats(self, stage: str) -> dict[str, Any]:
        """Get performance statistics for a specific stage."""
        with self._lock:
            mem_samples = list(self._stage_memory_samples.get(stage, []))
            cpu_samples = list(self._stage_cpu_samples.get(stage, []))
        return {
            "memory_mb": self._stats_from_samples(mem_samples),
            "cpu_percent": self._stats_from_samples(cpu_samples),
            "sample_count": len(mem_samples),
        }

    def get_current_memory_mb(self) -> float:
        """Get the latest memory sample (or live reading if no samples)."""
        with self._lock:
            if self._all_memory_samples:
                return self._all_memory_samples[-1]
        return _get_memory_mb()

    def get_peak_memory_mb(self) -> float:
        """Get peak memory across all samples."""
        with self._lock:
            if self._all_memory_samples:
                return max(self._all_memory_samples)
        return 0.0

    def get_avg_memory_mb(self) -> float:
        """Get average memory across all samples."""
        with self._lock:
            if self._all_memory_samples:
                return sum(self._all_memory_samples) / len(self._all_memory_samples)
        return 0.0

    def get_memory_critical_info(self) -> dict[str, float]:
        """Return details about the memory critical event, if triggered.

        Returns:
            Dict with ``used_mb``, ``total_mb``, ``threshold_percent``.
            All zeros if the event has not been triggered.
        """
        if not self.memory_critical_event.is_set():
            return {"used_mb": 0.0, "total_mb": 0.0, "threshold_percent": 0.0}
        return {
            "used_mb": self._critical_memory_mb,
            "total_mb": self._total_system_memory_mb,
            "threshold_percent": self._memory_critical_percent,
        }


# ── Console display ───────────────────────────────────────────────────────────


DISPLAY_REFRESH_INTERVAL = 10.0  # seconds between display refreshes



class BuildDisplay:
    """Logs build progress as sequential lines to stderr.

    Each state change (source done/failed, stage transition) emits one line.
    The periodic refresh emits a compact status line so long-running stages
    still show signs of life.
    """

    def __init__(
        self,
        total_sources: int,
        upload_enabled: bool,
        perf: PerformanceTracker,
        max_workers: int = 4,
    ):
        self.total_sources = total_sources
        self.upload_enabled = upload_enabled
        self.max_workers = max_workers
        self.num_stages = 4 if upload_enabled else 3
        self.perf = perf
        self.current_stage_idx = 0
        self.stage_status: dict[str, str] = {s: "pending" for s in STAGE_NAMES}
        self.stage_durations: dict[str, float] = {}

        # RUN stage tracking
        self.run_done: list[str] = []
        self.run_failed: list[str] = []
        self.run_running: list[str] = []
        self.run_start_time: float = 0.0

        self._render_lock = threading.Lock()

    def print_header(self) -> None:
        """Print the build header."""
        logger.info("=" * 80)
        logger.info("TRANSLATOR-INGESTS PIPELINE BUILD")
        logger.info("=" * 80)

    def render(self) -> None:
        """Emit a compact progress line via the logger (thread-safe).

        Called periodically by the refresh thread. Logs a single line
        with the current RUN stage progress or the active non-RUN stage.
        Uses the logger so output is sequenced with other log messages
        instead of overwriting them.
        """
        with self._render_lock:
            current_mem = self.perf.get_current_memory_mb()
            avg_mem = self.perf.get_avg_memory_mb()
            peak_mem = self.perf.get_peak_memory_mb()

            # Find the currently running stage
            running_stage = None
            for stage in STAGE_NAMES:
                if self.stage_status[stage] == "running":
                    running_stage = stage
                    break

            mem_str = f"mem: {current_mem:.0f} MB  avg: {avg_mem:.0f} MB  peak: {peak_mem:.0f} MB"

            if running_stage == "RUN":
                complete = len(self.run_done) + len(self.run_failed)
                total = self.total_sources
                running_str = ", ".join(sorted(self.run_running)) if self.run_running else "none"
                logger.info(
                    "[RUN] %d/%d  running: %s  |  %s",
                    complete, total, running_str, mem_str,
                )
            elif running_stage:
                elapsed = time.time() - self.stage_durations.get(f"{running_stage}_start", time.time())
                logger.info(
                    "[%s] running (%s)  |  %s",
                    running_stage, format_duration(elapsed), mem_str,
                )

    def start_stage(self, stage: str) -> None:
        """Mark a stage as running."""
        self.current_stage_idx = STAGE_NAMES.index(stage)
        self.stage_status[stage] = "running"
        self.stage_durations[f"{stage}_start"] = time.time()
        self.perf.begin_stage(stage)
        stage_num = self.current_stage_idx + 1
        logger.info("[%d/%d] %s ... started", stage_num, self.num_stages, stage)

    def complete_stage(self, stage: str) -> None:
        """Mark a stage as completed."""
        start = self.stage_durations.get(f"{stage}_start", time.time())
        self.stage_durations[stage] = time.time() - start
        self.stage_status[stage] = "completed"
        self.perf.end_stage(stage)
        stage_num = STAGE_NAMES.index(stage) + 1
        stage_stats = self.perf.get_stage_stats(stage)
        mem_peak = stage_stats["memory_mb"]["peak"]
        logger.info(
            "[%d/%d] %s ... OK  (%s, peak %.0f MB)",
            stage_num, self.num_stages, stage,
            format_duration(self.stage_durations[stage]), mem_peak,
        )

    def fail_stage(self, stage: str, error: str = "") -> None:
        """Mark a stage as failed."""
        start = self.stage_durations.get(f"{stage}_start", time.time())
        self.stage_durations[stage] = time.time() - start
        self.stage_status[stage] = "failed"
        self.perf.end_stage(stage)
        stage_num = STAGE_NAMES.index(stage) + 1
        logger.error(
            "[%d/%d] %s ... FAILED  (%s)",
            stage_num, self.num_stages, stage,
            format_duration(self.stage_durations[stage]),
        )

    def skip_stage(self, stage: str) -> None:
        """Mark a stage as skipped."""
        self.stage_status[stage] = "skipped"
        stage_num = STAGE_NAMES.index(stage) + 1
        logger.info("[%d/%d] %s ... skipped", stage_num, self.num_stages, stage)

    def print_final_summary(self, total_duration: float) -> None:
        """Print final summary after all stages."""
        overall = self.perf.get_overall_stats()
        mem = overall["memory_mb"]

        logger.info("=" * 80)
        logger.info(
            "BUILD COMPLETE  |  Total: %s  |  Peak mem: %.0f MB  Avg: %.0f MB",
            format_duration(total_duration), mem["peak"], mem["avg"],
        )
        logger.info("=" * 80)

        logger.info(
            "  %-12s %-8s %10s   %8s   %8s   %8s",
            "STAGE", "STATUS", "TIME", "PEAK", "AVG", "MIN",
        )

        for stage in STAGE_NAMES:
            if not self.upload_enabled and stage == "UPLOAD":
                continue
            status = self.stage_status[stage]
            dur = self.stage_durations.get(stage, 0)
            icon = "OK" if status == "completed" else "FAIL" if status == "failed" else "SKIP"
            dur_str = format_duration(dur) if dur > 0 else "--"

            ss = self.perf.get_stage_stats(stage)
            sm = ss["memory_mb"]
            peak_s = f"{sm['peak']:.0f} MB" if sm["peak"] > 0 else "--"
            avg_s = f"{sm['avg']:.0f} MB" if sm["avg"] > 0 else "--"
            min_s = f"{sm['min']:.0f} MB" if sm["min"] > 0 else "--"

            logger.info(
                "  %-12s %-8s %10s   %8s   %8s   %8s",
                stage, icon, dur_str, peak_s, avg_s, min_s,
            )

        disk = overall.get("disk_end", {})
        if disk.get("free_gb"):
            logger.info("Disk: %.1f GB free / %.1f GB total", disk["free_gb"], disk["total_gb"])

        logger.info("=" * 80)


# ── Worker function for parallel source execution ─────────────────────────────

def _run_single_source(
    source: str,
    overwrite: bool,
    stage_log_path: str | None = None,
    error_log_path: str | None = None,
) -> dict[str, Any]:
    """Run the pipeline for a single source in a worker process.

    Captures all output, timing, memory peak/avg, and errors/warnings.
    Uses a background thread to sample memory every SAMPLE_INTERVAL seconds,
    providing granular per-source memory tracking.

    Logs are written live to the shared stage log file (``logs/run/{ts}/run.log``)
    and errors/warnings also go to the shared error log (``logs/errors/{ts}/errors.log``).

    Args:
        source: Source name to process
        overwrite: Whether to overwrite existing files
        stage_log_path: Path to the shared stage log file (append mode)
        error_log_path: Path to the shared error log file (append mode)

    Returns:
        Dict with source, status, duration, memory stats, errors, warnings
    """
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []

    # Background memory sampling in the worker process
    proc = psutil.Process()
    mem_samples: list[float] = []
    stop_sampling = threading.Event()

    def _sample_loop():
        while not stop_sampling.is_set():
            try:
                mem_samples.append(proc.memory_info().rss / BYTES_PER_MB)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            stop_sampling.wait(SAMPLE_INTERVAL)

    sampler = threading.Thread(target=_sample_loop, daemon=True)
    sampler.start()

    # Set up logging: write live to shared stage log and error log
    setup_worker_logging(
        source=source,
        stage_log_path=stage_log_path,
        error_log_path=error_log_path,
    )

    # Also capture WARNING+ into a buffer for per-source error/warning extraction
    log_buffer = io.StringIO()
    buffer_handler = logging.StreamHandler(log_buffer)
    buffer_handler.setLevel(logging.WARNING)
    buffer_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(buffer_handler)

    # Note: Exception does not catch SystemExit or KeyboardInterrupt (BaseException
    # subclasses), so those propagate normally.  Broad catch here is intentional —
    # one worker failure must not crash the entire ProcessPoolExecutor.
    status = "completed"
    try:
        run_pipeline(source, overwrite=overwrite)
    except Exception:
        status = "failed"
        tb = traceback.format_exc()
        errors.append(f"Pipeline failed:\n{tb}")
        logging.getLogger().exception("[RUN] %s: pipeline failed", source)

    # Stop memory sampling
    stop_sampling.set()
    sampler.join(timeout=5)

    # Extract warnings/errors from buffer
    log_output = log_buffer.getvalue()
    for line in log_output.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("ERROR:"):
            errors.append(line)
        elif line.startswith("WARNING:"):
            warnings.append(line)

    root_logger.removeHandler(buffer_handler)
    buffer_handler.close()

    duration = time.time() - start
    mem_stats = {
        "peak_mb": max(mem_samples) if mem_samples else 0,
        "avg_mb": sum(mem_samples) / len(mem_samples) if mem_samples else 0,
        "min_mb": min(mem_samples) if mem_samples else 0,
        "samples": len(mem_samples),
    }
    return {
        "source": source,
        "status": status,
        "duration_seconds": duration,
        "memory": mem_stats,
        "errors": errors,
        "warnings": warnings,
    }


# ── Stage runner helpers ──────────────────────────────────────────────────────


def _collect_future_result(future: Any, source: str) -> dict[str, Any]:
    """Collect the result from a completed future, handling exceptions."""
    try:
        return future.result()
    except Exception:
        return {
            "source": source,
            "status": "failed",
            "duration_seconds": 0,
            "memory": {"peak_mb": 0, "avg_mb": 0, "min_mb": 0, "samples": 0},
            "errors": [traceback.format_exc()],
            "warnings": [],
        }


def _make_cancelled_result(source: str, reason: str) -> dict[str, Any]:
    """Build a result dict for a cancelled or timed-out source.

    Args:
        source: Source name
        reason: Short reason string (e.g. ``"memory"``, ``"timeout_after_memory"``)
    """
    return {
        "source": source,
        "status": f"cancelled_{reason}",
        "duration_seconds": 0,
        "memory": {"peak_mb": 0, "avg_mb": 0, "min_mb": 0, "samples": 0},
        "errors": [f"Cancelled due to {reason}"],
        "warnings": [],
    }


def _log_and_record_result(
    result: dict[str, Any],
    source: str,
    display: "BuildDisplay",
    report_dir: Path,
) -> None:
    """Log a source result and write per-source JSON to the report directory."""
    source_result_path = report_dir / "stages" / "run" / f"{source}.json"
    with source_result_path.open("w") as f:
        json.dump(result, f, indent=2)

    logger.info(
        "SOURCE: %s  |  status: %s  |  %s  |  peak mem: %.0f MB",
        source, result["status"],
        format_duration(result["duration_seconds"]),
        result["memory"]["peak_mb"],
    )

    if result["status"] == "completed":
        display.run_done.append(source)
    else:
        display.run_failed.append(source)
        logger.error("[RUN] %s: FAILED", source)
        for err in result["errors"]:
            logger.error("  %s", err)

    for warn in result["warnings"]:
        logger.warning("[RUN] %s: %s", source, warn)

    display.render()


# ── Stage runners ─────────────────────────────────────────────────────────────

def stage_run(
    sources: list[str],
    overwrite: bool,
    display: BuildDisplay,
    report_dir: Path,
    stage_log_path: str | None,
    error_log_path: str | None,
    max_workers: int | None = None,
) -> dict[str, dict[str, Any]]:
    """Execute the RUN stage: parallel pipeline execution for all sources.

    Worker processes log live to the shared stage log file at
    ``logs/run/{timestamp}/run.log``.

    Args:
        sources: List of source names
        overwrite: Whether to overwrite existing files
        display: BuildDisplay instance for progress tracking
        report_dir: Report directory for per-source JSON results
        stage_log_path: Path string to the shared run stage log file
        error_log_path: Path string to the shared error log file
        max_workers: Max parallel workers (default: number of sources)

    Returns:
        Dict mapping source name -> result dict
    """
    display.start_stage("RUN")
    display.run_start_time = time.time()
    results: dict[str, dict[str, Any]] = {}

    if max_workers is None:
        max_workers = min(4, len(sources))

    # Background thread refreshes the display every DISPLAY_REFRESH_INTERVAL seconds
    # so the user sees live memory/ETA updates even when no source finishes.
    stop_refresh = threading.Event()

    def _refresh_loop() -> None:
        while not stop_refresh.is_set():
            stop_refresh.wait(DISPLAY_REFRESH_INTERVAL)
            if not stop_refresh.is_set():
                display.render()

    refresh_thread = threading.Thread(target=_refresh_loop, daemon=True)
    refresh_thread.start()

    memory_aborted = False

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_source: dict[Any, str] = {}
        for source in sources:
            display.run_running.append(source)
            future = executor.submit(
                _run_single_source, source, overwrite, stage_log_path, error_log_path,
            )
            future_to_source[future] = source

        display.render()

        pending = set(future_to_source.keys())

        while pending:
            # Check memory guardian before waiting for the next future
            if display.perf.memory_critical_event.is_set():
                memory_aborted = True
                break

            done_futures, pending = wait(
                pending, timeout=DISPLAY_REFRESH_INTERVAL, return_when=FIRST_COMPLETED,
            )

            for future in done_futures:
                source = future_to_source[future]
                if source in display.run_running:
                    display.run_running.remove(source)

                result = _collect_future_result(future, source)
                results[source] = result
                _log_and_record_result(result, source, display, report_dir)

            # Nothing completed this round — refresh thread handles periodic display

        if memory_aborted:
            mem_info = display.perf.get_memory_critical_info()
            cancelled_count = 0
            for future in pending:
                source = future_to_source[future]
                if future.cancel():
                    cancelled_count += 1
                    if source in display.run_running:
                        display.run_running.remove(source)
                    results[source] = _make_cancelled_result(source, "memory")

            logger.error(
                "BUILD ABORTED: Memory usage at %.0f MB / %.0f MB (%.0f%% threshold). "
                "Cancelled %d pending futures. Waiting for running workers to finish.",
                mem_info["used_mb"], mem_info["total_mb"],
                mem_info["threshold_percent"], cancelled_count,
            )

            # Wait for already-running futures to finish (up to 5 min timeout)
            still_running = {f for f in pending if not f.cancelled()}
            if still_running:
                done_remaining, timed_out = wait(still_running, timeout=300)
                for future in done_remaining:
                    source = future_to_source[future]
                    if source in display.run_running:
                        display.run_running.remove(source)
                    result = _collect_future_result(future, source)
                    results[source] = result
                    _log_and_record_result(result, source, display, report_dir)
                for future in timed_out:
                    source = future_to_source[future]
                    if source in display.run_running:
                        display.run_running.remove(source)
                    results[source] = _make_cancelled_result(source, "timeout_after_memory")

    # Stop the periodic refresh thread
    stop_refresh.set()
    refresh_thread.join(timeout=2)

    # Save stage summary
    stage_summary: dict[str, Any] = {
        "stage": "RUN",
        "total_sources": len(sources),
        "completed": len(display.run_done),
        "failed": len(display.run_failed),
        "memory_aborted": memory_aborted,
        "duration_seconds": display.stage_durations.get("RUN", 0),
        "per_source_durations": {s: r["duration_seconds"] for s, r in results.items()},
        "per_source_memory": {s: r["memory"] for s, r in results.items()},
        "per_source_status": {s: r["status"] for s, r in results.items()},
        "per_source_errors": {s: r["errors"] for s, r in results.items() if r["errors"]},
        "per_source_warnings": {s: r["warnings"] for s, r in results.items() if r["warnings"]},
    }
    with (report_dir / "stages" / "run" / "_summary.json").open("w") as f:
        json.dump(stage_summary, f, indent=2)

    if memory_aborted or display.run_failed:
        display.fail_stage("RUN")
    else:
        display.complete_stage("RUN")

    return results


def stage_merge(
    graph_id: str,
    sources: list[str],
    overwrite: bool,
    display: BuildDisplay,
    report_dir: Path,
) -> bool:
    """Execute the MERGE stage.

    Args:
        graph_id: Merged graph identifier
        sources: List of sources to merge
        overwrite: Whether to overwrite
        display: BuildDisplay instance
        report_dir: Report directory

    Returns:
        True if merge succeeded
    """
    display.start_stage("MERGE")
    logger.info("STAGE: MERGE  |  graph: %s  |  %d sources", graph_id, len(sources))

    merge_result = {"stage": "MERGE", "graph_id": graph_id, "sources": sources, "status": "pending"}

    try:
        merged_graph_metadata, _kgx_sources = merge(graph_id, sources=sources, overwrite=overwrite)

        if is_merged_graph_release_current(merged_graph_metadata) and not overwrite:
            logger.info("Merged graph release already current: %s", merged_graph_metadata.build_version)
        else:
            generate_merged_graph_release(merged_graph_metadata)

        merge_result["status"] = "completed"
        merge_result["build_version"] = merged_graph_metadata.build_version
        display.complete_stage("MERGE")
        logger.info("MERGE: completed successfully")
        ok = True
    except Exception:
        merge_result["status"] = "failed"
        display.fail_stage("MERGE")
        logger.exception("[MERGE] FAILED")
        ok = False

    merge_result["duration_seconds"] = display.stage_durations.get("MERGE", 0)
    merge_result["performance"] = display.perf.get_stage_stats("MERGE")

    with (report_dir / "stages" / "merge" / "_summary.json").open("w") as f:
        json.dump(merge_result, f, indent=2)

    return ok


def stage_release(
    sources: list[str],
    node_properties: list[str],
    display: BuildDisplay,
    report_dir: Path,
) -> bool:
    """Execute the RELEASE stage: create releases for each source.

    Args:
        sources: List of sources to release
        node_properties: Sources to exclude from release
        display: BuildDisplay instance
        report_dir: Report directory

    Returns:
        True if all releases succeeded
    """
    display.start_stage("RELEASE")
    logger.info("STAGE: RELEASE")

    releasable = [s for s in sources if s not in node_properties]
    per_source: dict[str, dict[str, Any]] = {}
    all_ok = True

    for source in releasable:
        src_start = time.time()
        try:
            release_ingest(source)
            per_source[source] = {"status": "completed", "duration_seconds": time.time() - src_start}
            logger.info("  %s: released (%s)", source, format_duration(time.time() - src_start))
        except Exception:
            all_ok = False
            per_source[source] = {"status": "failed", "duration_seconds": time.time() - src_start}
            logger.exception("[RELEASE] %s: FAILED", source)

    # Generate summary
    try:
        generate_release_summary()
        logger.info("  Release summary generated")
    except Exception:
        logger.exception("[RELEASE] summary generation failed")

    if all_ok:
        display.complete_stage("RELEASE")
    else:
        display.fail_stage("RELEASE")

    release_result = {
        "stage": "RELEASE",
        "status": "completed" if all_ok else "failed",
        "total_sources": len(releasable),
        "completed": sum(1 for v in per_source.values() if v["status"] == "completed"),
        "failed": sum(1 for v in per_source.values() if v["status"] == "failed"),
        "duration_seconds": display.stage_durations.get("RELEASE", 0),
        "performance": display.perf.get_stage_stats("RELEASE"),
        "per_source": per_source,
    }
    with (report_dir / "stages" / "release" / "_summary.json").open("w") as f:
        json.dump(release_result, f, indent=2)

    return all_ok


def stage_upload(
    report_dir: Path,
    display: BuildDisplay,
) -> dict[str, Any] | None:
    """Execute the UPLOAD stage: auto-discover and upload to S3.

    Auto-discovers data and release sources from /data and /releases directories,
    same behavior as ``make upload-all``.

    Args:
        report_dir: Report directory to save upload results
        display: BuildDisplay instance

    Returns:
        Upload results dict, or None if failed
    """
    display.start_stage("UPLOAD")
    logger.info("STAGE: UPLOAD")

    try:
        data_sources = discover_data_sources()
        release_sources = discover_release_sources()

        logger.info("  Data sources: %d", len(data_sources))
        logger.info("  Release sources: %d", len(release_sources))

        results = upload_and_cleanup(
            data_sources=data_sources,
            release_sources=release_sources,
            cleanup=True,
        )

        # Save to stage dir and standard location
        for path in (
            report_dir / "stages" / "upload" / "upload-results.json",
            REPORTS_BASE / "upload-results-latest.json",
        ):
            with path.open("w") as f:
                json.dump(results, f, indent=2)

        # Save stage summary with perf
        upload_summary = {
            "stage": "UPLOAD",
            "status": "completed" if results["total_failed"] == 0 else "failed",
            "duration_seconds": display.stage_durations.get("UPLOAD", 0),
            "performance": display.perf.get_stage_stats("UPLOAD"),
            "files_uploaded": results["total_uploaded"],
            "files_failed": results["total_failed"],
            "bytes_transferred": results.get("total_bytes_transferred", 0),
        }
        with (report_dir / "stages" / "upload" / "_summary.json").open("w") as f:
            json.dump(upload_summary, f, indent=2)

        logger.info("  Files uploaded: %d", results["total_uploaded"])
        logger.info("  Files failed: %d", results["total_failed"])

        if results["total_failed"] > 0:
            display.fail_stage("UPLOAD")
            logger.error("[UPLOAD] %d files failed to upload", results["total_failed"])
        else:
            display.complete_stage("UPLOAD")

        return results

    except Exception as exc:
        display.fail_stage("UPLOAD")
        logger.exception("[UPLOAD] FAILED")

        upload_summary = {
            "stage": "UPLOAD",
            "status": "failed",
            "error": str(exc),
            "duration_seconds": display.stage_durations.get("UPLOAD", 0),
            "performance": display.perf.get_stage_stats("UPLOAD"),
        }
        with (report_dir / "stages" / "upload" / "_summary.json").open("w") as f:
            json.dump(upload_summary, f, indent=2)

        return None


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run_full_build(
    sources: list[str],
    graph_id: str = "translator_kg",
    node_properties: list[str] | None = None,
    overwrite: bool = False,
    upload: bool = True,
    max_workers: int | None = None,
    memory_critical_percent: float | None = None,
) -> tuple[Path, Path, bool]:
    """Run the complete build pipeline end-to-end.

    Stages: RUN (parallel) -> MERGE -> RELEASE -> UPLOAD (optional)

    If system memory exceeds ``memory_critical_percent`` for several
    consecutive samples, pending work is cancelled and remaining stages
    are skipped.  A partial report is still generated.

    Logs are written live to per-stage log files::

        logs/run/{timestamp}/run.log
        logs/merge/{timestamp}/merge.log
        logs/release/{timestamp}/release.log
        logs/upload/{timestamp}/upload.log
        logs/errors/{timestamp}/errors.log

    JSON artifacts and build reports go to ``reports/{timestamp}/``.

    Args:
        sources: List of source names to process
        graph_id: Merged graph identifier
        node_properties: Sources that are node-properties only
        overwrite: Whether to overwrite existing files
        upload: Whether to upload to S3
        max_workers: Max parallel workers for RUN stage
        memory_critical_percent: System memory % that triggers abort
            (default: ``MEMORY_CRITICAL_THRESHOLD_PERCENT``)

    Returns:
        Tuple of (report_dir, error_log_path, memory_aborted)
    """
    if node_properties is None:
        node_properties = ["ncbi_gene"]

    build_start = time.time()
    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y_%m_%d_%H%M%S")
    report_dir = create_report_dir(timestamp)
    stage_log_paths, error_log_path = create_log_dirs(timestamp)

    # Resolve max_workers early so BuildDisplay gets the correct value
    if max_workers is None:
        max_workers = min(4, len(sources))

    # Start performance tracker (with optional memory threshold override)
    perf_kwargs: dict[str, Any] = {}
    if memory_critical_percent is not None:
        perf_kwargs["memory_critical_percent"] = memory_critical_percent
    perf = PerformanceTracker(**perf_kwargs)
    perf.start()

    display = BuildDisplay(
        total_sources=len(sources), upload_enabled=upload, perf=perf,
        max_workers=max_workers,
    )
    display.print_header()

    # ── Set up root logger for the orchestrator process ──
    #
    # Console handler: INFO (progress + source results appear in terminal)
    # Error file handler: WARNING+ to logs/errors/{ts}/errors.log (active entire build)
    # Stage file handler: INFO to logs/{stage}/{ts}/{stage}.log (swapped per stage)
    #
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    root_logger.addHandler(console_handler)

    log_fmt = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

    error_handler = logging.FileHandler(error_log_path, mode="a")
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(log_fmt)
    root_logger.addHandler(error_handler)

    # Suppress noisy third-party loggers
    for name in ("boto3", "botocore", "urllib3", "s3transfer"):
        logging.getLogger(name).setLevel(logging.WARNING)

    stage_timing_reports: list[StageTimingReport] = []
    source_durations: dict[str, float] = {}
    source_memory: dict[str, dict[str, Any]] = {}
    upload_results_path: Path | None = None

    def _add_stage_handler(stage: str) -> logging.FileHandler:
        """Add a FileHandler for the given stage to the root logger."""
        handler = logging.FileHandler(stage_log_paths[stage], mode="a")
        handler.setLevel(logging.INFO)
        handler.setFormatter(log_fmt)
        root_logger.addHandler(handler)
        return handler

    def _remove_handler(handler: logging.FileHandler) -> None:
        """Remove and close a handler."""
        root_logger.removeHandler(handler)
        handler.close()

    def _collect_stage_timing(stage: str) -> StageTimingReport:
        """Collect timing/perf for a completed stage."""
        stage_perf = perf.get_stage_stats(stage)
        return StageTimingReport(
            stage=stage,
            duration_seconds=display.stage_durations.get(stage, 0),
            peak_memory_mb=stage_perf["memory_mb"]["peak"],
            avg_memory_mb=stage_perf["memory_mb"]["avg"],
            min_memory_mb=stage_perf["memory_mb"]["min"],
            avg_cpu_percent=stage_perf["cpu_percent"]["avg"],
            status=display.stage_status[stage],
        )

    # ── STAGE 1: RUN ──
    run_handler = _add_stage_handler("run")
    logger.info("TRANSLATOR-INGESTS RUN STAGE")
    logger.info("Started: %s", datetime.datetime.now().isoformat())
    logger.info("Sources: %s", ", ".join(sources))

    run_results = stage_run(
        sources, overwrite, display, report_dir,
        str(stage_log_paths["run"]), str(error_log_path),
        max_workers,
    )

    stage_timing_reports.append(_collect_stage_timing("RUN"))

    for source, result in run_results.items():
        source_durations[source] = result["duration_seconds"]
        source_memory[source] = result["memory"]

    successful_sources = [s for s in sources if run_results.get(s, {}).get("status") == "completed"]
    failed_sources = [s for s in sources if run_results.get(s, {}).get("status") != "completed"]

    if failed_sources:
        logger.error("[SUMMARY] %d sources failed RUN: %s", len(failed_sources), ", ".join(failed_sources))
        logger.info(
            "%d sources failed (will use previous build), %d succeeded.",
            len(failed_sources), len(successful_sources),
        )
        for src in failed_sources:
            logger.info("  %s: using previous successful build via latest-build.json", src)

    _remove_handler(run_handler)

    memory_aborted = perf.memory_critical_event.is_set()

    # All sources proceed to MERGE/RELEASE -- failed sources fall back to
    # their previous successful build (latest-build.json is only updated on
    # success, so it still points to the last good version).

    # ── STAGE 2: MERGE ──
    if memory_aborted:
        display.skip_stage("MERGE")
    else:
        merge_handler = _add_stage_handler("merge")
        stage_merge(graph_id, sources, overwrite, display, report_dir)
        stage_timing_reports.append(_collect_stage_timing("MERGE"))
        _remove_handler(merge_handler)
        memory_aborted = perf.memory_critical_event.is_set()

    # ── STAGE 3: RELEASE ──
    if memory_aborted:
        display.skip_stage("RELEASE")
    else:
        release_handler = _add_stage_handler("release")
        stage_release(sources, node_properties, display, report_dir)
        stage_timing_reports.append(_collect_stage_timing("RELEASE"))
        _remove_handler(release_handler)
        memory_aborted = perf.memory_critical_event.is_set()

    # ── STAGE 4: UPLOAD ──
    if memory_aborted:
        display.skip_stage("UPLOAD")
    elif upload:
        upload_handler = _add_stage_handler("upload")

        upload_result = stage_upload(report_dir, display)
        if upload_result:
            upload_results_path = report_dir / "stages" / "upload" / "upload-results.json"

        stage_timing_reports.append(_collect_stage_timing("UPLOAD"))
        _remove_handler(upload_handler)
    else:
        display.skip_stage("UPLOAD")

    total_duration = time.time() - build_start

    # Clean up logging handlers
    root_logger.removeHandler(error_handler)
    error_handler.close()
    root_logger.removeHandler(console_handler)

    # Stop performance tracker
    perf.stop()
    overall_perf = perf.get_overall_stats()

    # ── Final display ──
    display.print_final_summary(total_duration)

    # ── Save overall performance data ──
    perf_data = {
        "overall": overall_perf,
        "per_stage": {stage: perf.get_stage_stats(stage) for stage in STAGE_NAMES},
        "per_source_durations": source_durations,
        "per_source_memory": source_memory,
    }
    with (report_dir / "performance.json").open("w") as f:
        json.dump(perf_data, f, indent=2)

    # ── Generate report ──
    logger.info("Generating build report...")

    # Surface memory abort in report
    build_notes: list[str] = []
    if memory_aborted:
        mem_info = perf.get_memory_critical_info()
        build_notes.append(
            f"BUILD ABORTED: System memory exceeded critical threshold "
            f"({mem_info['threshold_percent']:.0f}%). "
            f"Usage: {mem_info['used_mb']:.0f} MB / {mem_info['total_mb']:.0f} MB. "
            f"Some stages may be incomplete."
        )

    report = generate_build_report(
        sources=sources,
        graph_id=graph_id,
        node_properties=node_properties,
        upload_results_path=upload_results_path,
        stage_timings=stage_timing_reports,
        source_durations=source_durations,
        source_memory=source_memory,
        total_duration=total_duration,
        peak_memory_mb=overall_perf["memory_mb"]["peak"],
        avg_memory_mb=overall_perf["memory_mb"]["avg"],
        min_memory_mb=overall_perf["memory_mb"]["min"],
        disk_usage=overall_perf.get("disk_end"),
        failed_sources=failed_sources,
        build_notes=build_notes if build_notes else None,
    )

    text_path, summary_path, json_path = save_report(report, report_dir)

    logs_dir = LOGS_BASE
    stage_dirs = "stages/run/ stages/merge/ stages/release/"
    if upload:
        stage_dirs += " stages/upload/"
    logger.info("Reports: %s", report_dir)
    logger.info("  %s, %s, %s, performance.json", text_path.name, summary_path.name, json_path.name)
    logger.info("  %s", stage_dirs)
    logger.info("Logs: %s", logs_dir)
    for stage in STAGE_NAMES_LOWER:
        if stage == "upload" and not upload:
            continue
        logger.info("  %s/%s/%s.log", stage, timestamp, stage)
    logger.info("  errors/%s/errors.log", timestamp)

    # Print summary report to stdout for easy piping/sharing
    print(format_summary_report(report))

    return report_dir, error_log_path, memory_aborted


# ── CLI ───────────────────────────────────────────────────────────────────────

# Private prefixes to exclude from auto-discovery
_INGEST_EXCLUDE_PREFIXES = ("_", ".")


def discover_ingest_sources() -> list[str]:
    """Auto-discover available ingest sources from the ingests/ directory.

    Scans src/translator_ingest/ingests/ for subdirectories that contain a
    matching {source}.py file, excluding templates and internal directories.

    Returns:
        Sorted list of source names
    """
    ingests_dir = Path(INGESTS_PARSER_PATH)
    sources = []
    for item in sorted(ingests_dir.iterdir()):
        if not item.is_dir():
            continue
        if any(item.name.startswith(p) for p in _INGEST_EXCLUDE_PREFIXES):
            continue
        if item.name == "__pycache__":
            continue
        # Verify it has a transform module (not just a stray directory)
        if (item / f"{item.name}.py").exists():
            sources.append(item.name)
    return sources


@click.command()
@click.option("--sources", type=str, default=None, help="Space-separated list of sources (default: all)")
@click.option("--graph-id", type=str, default="translator_kg", help="Merged graph ID")
@click.option("--node-properties", type=str, default="ncbi_gene", help="Space-separated node-property-only sources")
@click.option("--overwrite", is_flag=True, help="Overwrite previously generated files")
@click.option("--no-upload", is_flag=True, help="Skip S3 upload stage")
@click.option("--max-workers", type=int, default=None, help="Max parallel workers for RUN stage")
@click.option(
    "--memory-threshold",
    type=float,
    default=None,
    help=(
        f"System memory critical threshold in percent "
        f"(default: {MEMORY_CRITICAL_THRESHOLD_PERCENT:.0f}). "
        f"Build aborts gracefully when exceeded."
    ),
)
def main(
    sources: str | None,
    graph_id: str,
    node_properties: str,
    overwrite: bool,
    no_upload: bool,
    max_workers: int | None,
    memory_threshold: float | None,
) -> None:
    """Run the full translator-ingests pipeline build end-to-end.

    Stages: RUN (parallel) -> MERGE -> RELEASE -> UPLOAD

    Per-stage logs are written live to logs/{stage}/{timestamp}/.
    JSON artifacts and build reports go to reports/{timestamp}/.
    """
    if sources and sources.strip():
        source_list = sources.split()
    else:
        source_list = discover_ingest_sources()
        logger.info("Auto-discovered %d sources from ingests/ directory", len(source_list))
    node_props = node_properties.split() if node_properties else ["ncbi_gene"]

    _report_dir, _error_log_path, memory_aborted = run_full_build(
        sources=source_list,
        graph_id=graph_id,
        node_properties=node_props,
        overwrite=overwrite,
        upload=not no_upload,
        max_workers=max_workers,
        memory_critical_percent=memory_threshold,
    )

    if memory_aborted:
        logger.error(
            "BUILD ABORTED: System memory exceeded critical threshold. "
            "See report for details."
        )
        sys.exit(2)

    # Exit non-zero if any source failed or any stage failed
    run_summary_path = _report_dir / "stages" / "run" / "_summary.json"
    has_failures = False
    if run_summary_path.exists():
        with run_summary_path.open() as f:
            run_summary = json.load(f)
        has_failures = run_summary.get("failed", 0) > 0

    if not has_failures:
        for stage in ("merge", "release", "upload"):
            stage_summary_path = _report_dir / "stages" / stage / "_summary.json"
            if stage_summary_path.exists():
                with stage_summary_path.open() as f:
                    stage_data = json.load(f)
                if stage_data.get("status") == "failed":
                    has_failures = True
                    break

    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
