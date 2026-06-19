
import io
import json
import logging
import os
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any

# psutil is used inside the worker subprocess to sample its own rss memory;
# the stdlib resource module can't read child-process rss without ptrace
import psutil

from translator_ingest.merging import (
    generate_merged_graph_release,
    is_merged_graph_release_current,
    merge,
)
from translator_ingest.pipeline import run_pipeline
from translator_ingest.release import generate_release_summary, release_ingest
from translator_ingest.util.logging_utils import get_logger, setup_worker_logging
from translator_ingest.util.run_build import REPORTS_BASE
from translator_ingest.util.run_build.display import (
    DISPLAY_REFRESH_INTERVAL,
    BuildDisplay,
    SourceProgress,
    _load_source_io_history,
    _save_source_io_history,
    _worker_pid_path,
)
from translator_ingest.util.run_build.tracking import SAMPLE_INTERVAL
from translator_ingest.util.run_build.utils import (
    BYTES_PER_MB,
    format_duration,
)
from translator_ingest.util.storage.s3 import upload_and_cleanup
from translator_ingest.util.storage.upload_s3 import (
    discover_data_sources,
    discover_release_sources,
)

logger = get_logger(__name__)


# ── Worker function for parallel source execution ─────────────────────────────

def _run_single_source(
    source: str,
    overwrite: bool,
    stage_log_path: str | None = None,
    error_log_path: str | None = None,
) -> dict[str, Any]:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []

    # Write our PID so the orchestrator can poll /proc/<pid>/io for progress
    _pid_path = _worker_pid_path(source)
    _pid_path.write_text(str(os.getpid()))

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

    # Clean up PID file — orchestrator reads absence as "source finished"
    _pid_path.unlink(missing_ok=True)

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
    sequential_sources: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    if sequential_sources is None:
        sequential_sources = ["ctd", "semmeddb"]

    display.start_stage("RUN")
    display.run_start_time = time.time()
    results: dict[str, dict[str, Any]] = {}

    # I/O history for ETA estimation; updated after each successful source
    io_history = _load_source_io_history()
    # Per-source progress objects shared with the display's render()
    source_progress: dict[str, SourceProgress] = {}
    display.source_progress = source_progress

    # Partition: heavy sources run sequentially first, rest run in parallel.
    # Preserve the original ordering within each group.
    sequential = [s for s in sources if s in sequential_sources]
    parallel = [s for s in sources if s not in sequential_sources]

    if max_workers is None:
        max_workers = min(4, len(parallel)) if parallel else 1

    if sequential:
        logger.info("[RUN] Sequential phase (%d source(s)): %s", len(sequential), ", ".join(sequential))
    if parallel:
        logger.info("[RUN] Parallel phase (%d source(s), %d worker(s)): %s", len(parallel), max_workers, ", ".join(parallel))

    # Background thread refreshes the display every DISPLAY_REFRESH_INTERVAL seconds.
    # Also polls /proc/<pid>/io for each running source so progress lines stay current.
    stop_refresh = threading.Event()

    def _refresh_loop() -> None:
        while not stop_refresh.is_set():
            stop_refresh.wait(DISPLAY_REFRESH_INTERVAL)
            if not stop_refresh.is_set():
                for src in list(display.run_running):
                    if src in source_progress:
                        source_progress[src].update()
                display.render()

    refresh_thread = threading.Thread(target=_refresh_loop, daemon=True)
    refresh_thread.start()

    memory_aborted = False

    # ── Phase 1: Sequential sources ────────────────────────────────────────────
    for source in sequential:
        if memory_aborted or display.perf.memory_critical_event.is_set():
            memory_aborted = True
            result = _make_cancelled_result(source, "memory")
            results[source] = result
            _log_and_record_result(result, source, display, report_dir)
            continue

        source_progress[source] = SourceProgress(source=source, expected_bytes=io_history.get(source))
        display.run_running.append(source)
        display.render()

        with ProcessPoolExecutor(max_workers=1) as seq_executor:
            future = seq_executor.submit(
                _run_single_source, source, overwrite, stage_log_path, error_log_path,
            )
            # Wait for completion, checking memory periodically.
            # If memory fires mid-run, let the current source finish (only one
            # running) then mark abort so remaining sources are skipped.
            while True:
                done_f, _ = wait([future], timeout=DISPLAY_REFRESH_INTERVAL, return_when=FIRST_COMPLETED)
                if done_f:
                    break
                if display.perf.memory_critical_event.is_set():
                    memory_aborted = True
                    # Stop polling after 5 min. This is a soft budget, not a
                    # hard timeout: the `with ProcessPoolExecutor` context
                    # manager will still block on exit until the worker
                    # subprocess finishes. A true hard timeout would require
                    # SIGTERM-ing the worker PID, which is platform-dependent
                    # and out of scope here.
                    wait([future], timeout=300)
                    break

        if source in display.run_running:
            display.run_running.remove(source)

        result = _collect_future_result(future, source)
        results[source] = result
        _log_and_record_result(result, source, display, report_dir)

        # Save final I/O total for future ETA estimation
        if result["status"] == "completed" and source_progress[source].total_bytes_read > 0:
            io_history[source] = source_progress[source].total_bytes_read
            _save_source_io_history(io_history)

    # ── Phase 2: Parallel sources ──────────────────────────────────────────────
    if memory_aborted:
        # Memory was critical after sequential phase — cancel all parallel sources.
        for source in parallel:
            result = _make_cancelled_result(source, "memory")
            results[source] = result
            _log_and_record_result(result, source, display, report_dir)
    elif parallel:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_source: dict[Any, str] = {}
            for source in parallel:
                source_progress[source] = SourceProgress(source=source, expected_bytes=io_history.get(source))
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

                    # Save final I/O total for future ETA estimation
                    if result["status"] == "completed" and source in source_progress and source_progress[source].total_bytes_read > 0:
                        io_history[source] = source_progress[source].total_bytes_read
                        _save_source_io_history(io_history)

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
                        result = _make_cancelled_result(source, "memory")
                        results[source] = result
                        # Record result so per-source JSON is written and
                        # display.run_failed is updated, consistent with the
                        # upfront cancel path at the top of Phase 2.
                        _log_and_record_result(result, source, display, report_dir)

                logger.error(
                    "BUILD ABORTED: Memory usage at %.0f MB / %.0f MB (%.0f%% threshold). "
                    "Cancelled %d pending futures. Waiting for running workers to finish.",
                    mem_info["used_mb"], mem_info["total_mb"],
                    mem_info["threshold_percent"], cancelled_count,
                )

                # Wait for already-running futures to finish (soft 5 min
                # deadline). Note that the ProcessPoolExecutor context manager
                # will still block on exit until each worker subprocess
                # actually returns -- wait() timing out only stops us polling.
                # Workers respond to cancellation between tasks, not mid-task,
                # so this is a best-effort budget, not a hard timeout.
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
                    # Timed-out futures: still write per-source JSON and
                    # update display.run_done/run_failed so the stage summary
                    # and build report reflect these sources.
                    for future in timed_out:
                        source = future_to_source[future]
                        if source in display.run_running:
                            display.run_running.remove(source)
                        result = _make_cancelled_result(source, "timeout_after_memory")
                        results[source] = result
                        _log_and_record_result(result, source, display, report_dir)

    # Stop the periodic refresh thread
    stop_refresh.set()
    refresh_thread.join(timeout=2)

    # Mark stage complete/failed BEFORE building the summary so that
    # display.stage_durations['RUN'] is populated and the persisted
    # duration_seconds matches what the display and final report show.
    if memory_aborted or display.run_failed:
        display.fail_stage("RUN")
    else:
        display.complete_stage("RUN")

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

    return results


def stage_merge(
    graph_id: str,
    sources: list[str],
    overwrite: bool,
    display: BuildDisplay,
    report_dir: Path,
) -> None:
    display.start_stage("MERGE")
    logger.info("STAGE: MERGE  |  graph: %s  |  %d sources", graph_id, len(sources))

    merge_result: dict[str, Any] = {
        "stage": "MERGE", "graph_id": graph_id, "sources": sources, "status": "failed",
    }
    _stage_start = time.time()

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
    except Exception:
        display.fail_stage("MERGE")
        logger.exception("[MERGE] FAILED")
        raise
    finally:
        merge_result["duration_seconds"] = display.stage_durations.get("MERGE") or (time.time() - _stage_start)
        merge_result["performance"] = display.perf.get_stage_stats("MERGE")
        with (report_dir / "stages" / "merge" / "_summary.json").open("w") as f:
            json.dump(merge_result, f, indent=2)


def stage_release(
    sources: list[str],
    node_properties: list[str],
    display: BuildDisplay,
    report_dir: Path,
) -> None:
    display.start_stage("RELEASE")
    logger.info("STAGE: RELEASE")

    releasable = [s for s in sources if s not in node_properties]
    per_source: dict[str, dict[str, Any]] = {}
    all_ok = True

    try:
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
            raise RuntimeError(
                f"Release failed for "
                f"{sum(1 for v in per_source.values() if v['status'] == 'failed')} source(s)"
            )
    finally:
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


def stage_upload(
    report_dir: Path,
    display: BuildDisplay,
) -> None:
    display.start_stage("UPLOAD")
    logger.info("STAGE: UPLOAD")

    upload_summary: dict[str, Any] = {"stage": "UPLOAD", "status": "failed"}
    _stage_start = time.time()

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

        logger.info("  Files uploaded: %d", results["total_uploaded"])
        logger.info("  Files failed: %d", results["total_failed"])

        if results["total_failed"] > 0:
            display.fail_stage("UPLOAD")
            logger.error("[UPLOAD] %d files failed to upload", results["total_failed"])
        else:
            display.complete_stage("UPLOAD")

        upload_summary = {
            "stage": "UPLOAD",
            "status": "completed" if results["total_failed"] == 0 else "failed",
            "files_uploaded": results["total_uploaded"],
            "files_failed": results["total_failed"],
            "bytes_transferred": results.get("total_bytes_transferred", 0),
        }
    except Exception:
        if display.stage_status.get("UPLOAD") == "running":
            display.fail_stage("UPLOAD")
        logger.exception("[UPLOAD] FAILED")
        raise
    finally:
        upload_summary["duration_seconds"] = display.stage_durations.get("UPLOAD") or (time.time() - _stage_start)
        upload_summary["performance"] = display.perf.get_stage_stats("UPLOAD")
        with (report_dir / "stages" / "upload" / "_summary.json").open("w") as f:
            json.dump(upload_summary, f, indent=2)
