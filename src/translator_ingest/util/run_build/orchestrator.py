
import datetime
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

# psutil is used to check whether system memory is still critical at the
# start of each stage — informs the orchestrator's skip-stage decisions
import psutil

from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.run_build.build_report import (
    StageTimingReport,
    format_summary_report,
    generate_build_report,
    save_report,
)
from translator_ingest.util.run_build.display import BuildDisplay
# fallback resolution decides per-source whether to use the fresh build, the
# prior LATEST_BUILD_FILE on disk, or hard-stop because no fallback exists
from translator_ingest.util.run_build.fallback import (
    format_missing_sources_error,
    partition_sources_after_run,
)
from translator_ingest.util.run_build.paths import (
    LOGS_BASE,
    create_log_dirs,
    create_report_dir,
    finalize_latest_copies,
)
from translator_ingest.util.run_build.stages import (
    stage_merge,
    stage_release,
    stage_run,
    stage_upload,
)
from translator_ingest.util.run_build.tracking import PerformanceTracker
from translator_ingest.util.run_build.utils import (
    STAGE_NAMES,
    STAGE_NAMES_LOWER,
)
from translator_ingest.util.storage.s3 import S3Uploader

logger = get_logger(__name__)


def run_full_build(
    sources: list[str],
    graph_id: str = "translator_kg",
    node_properties: list[str] | None = None,
    overwrite: bool = False,
    upload: bool = True,
    max_workers: int | None = None,
    memory_critical_percent: float | None = None,
    sequential_sources: list[str] | None = None,
) -> tuple[Path, Path, bool]:
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
        handler = logging.FileHandler(stage_log_paths[stage], mode="a")
        handler.setLevel(logging.INFO)
        handler.setFormatter(log_fmt)
        root_logger.addHandler(handler)
        return handler

    def _remove_handler(handler: logging.FileHandler) -> None:
        root_logger.removeHandler(handler)
        handler.close()

    # one shared uploader instance reused by every per-stage incremental
    # upload below; created lazily only when upload is enabled so a
    # --no-upload run never touches boto3 / IAM
    _stage_log_uploader: S3Uploader | None = None
    if upload:
        try:
            _stage_log_uploader = S3Uploader()
        except Exception:
            # missing credentials / boto3 misconfiguration shouldn't crash
            # the build itself — per-stage uploads will silently no-op
            logger.exception(
                "Could not initialize S3Uploader for per-stage log uploads (non-fatal)"
            )

    def _upload_stage_logs_safe(stage: str) -> None:
        # called in the finally block after each stage to push that stage's
        # log dir to S3. wrapped so an S3 hiccup never takes down the build —
        # the final consolidated upload at the end of the run re-uploads
        # anything that failed here (skip-if-unchanged keeps it cheap).
        if not upload or _stage_log_uploader is None:
            return
        try:
            _stage_log_uploader.upload_stage_logs(stage=stage, timestamp=timestamp)
        except Exception:
            logger.exception("Per-stage log upload for %s failed (non-fatal)", stage)

    def _collect_stage_timing(stage: str) -> StageTimingReport:
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
        sequential_sources=sequential_sources,
    )

    stage_timing_reports.append(_collect_stage_timing("RUN"))

    for source, result in run_results.items():
        source_durations[source] = result["duration_seconds"]
        source_memory[source] = result["memory"]

    failed_sources = [s for s in sources if run_results.get(s, {}).get("status") != "completed"]

    # ── Fallback resolution ──
    # bucket every source into fresh / fallback / missing based on this run's
    # outcome and whether a prior LATEST_BUILD_FILE survives on disk.
    # if any source ends up 'missing' (failed this run AND no prior build),
    # the orchestrator must hard-stop — we cannot ship a partial graph that
    # silently drops a source the operator declared in graphs.yaml
    partition = partition_sources_after_run(
        sources=sources,
        failed_this_run=failed_sources,
    )

    if failed_sources:
        logger.error(
            "[SUMMARY] %d sources failed RUN: %s",
            len(failed_sources), ", ".join(failed_sources),
        )
        if partition.fallback:
            logger.info(
                "Fallback: %d source(s) will use the previous successful build "
                "via latest-build.json: %s",
                len(partition.fallback), ", ".join(partition.fallback),
            )

    _remove_handler(run_handler)
    # push the RUN stage's log dir to S3 now (incremental upload). this is the
    # first place a real operator can see what happened — even if a later
    # stage crashes the orchestrator, RUN logs are already on S3.
    _upload_stage_logs_safe("run")

    # if any source has no fallback data, hard-stop here. mark every later
    # stage as skipped, log a loud named error, and skip directly to the
    # report-generation / log-upload tail so the operator sees what broke.
    if partition.has_missing:
        # build a per-source error map so the hard-stop message names which
        # source failed for what reason — saves the operator from cross-
        # referencing the per-source RUN summary file
        errors_by_source: dict[str, str] = {}
        for source_name in partition.missing:
            errs = run_results.get(source_name, {}).get("errors") or []
            # first line of the first captured error gives the most useful
            # signal (typically the exception class + message); full traceback
            # lives in the per-source JSON under reports/{ts}/stages/run/
            errors_by_source[source_name] = errs[0].splitlines()[0] if errs else "no error captured"

        logger.error(
            "BUILD HARD-STOP — missing-with-no-fallback sources detected:\n%s",
            format_missing_sources_error(partition, errors_by_source=errors_by_source),
        )
        for _skipped in ("MERGE", "RELEASE", "UPLOAD"):
            display.skip_stage(_skipped)
            stage_timing_reports.append(_collect_stage_timing(_skipped))
        # short-circuit: jump to the report/log-upload tail by setting a
        # flag the rest of the function checks. the existing memory-critical
        # path already supports skipped stages downstream, so we slot in here.
        _hard_stop_no_fallback = True
    else:
        _hard_stop_no_fallback = False

    # memory_critical_event is a sticky latch: it records whether the threshold
    # was ever hit. But we only skip a stage if memory is STILL critical right
    # now at stage-start (workers from RUN are done, memory may have recovered).
    def _memory_still_critical() -> bool:
        return perf.is_memory_critical()

    # all sources proceed to MERGE/RELEASE via partition.available —
    # fresh sources contribute their freshly-built data, fallback sources
    # contribute their prior LATEST_BUILD_FILE (which still points at the
    # last good version because it's only updated on success)
    mergeable_sources = partition.available

    # ── STAGE 2: MERGE ──
    if _hard_stop_no_fallback:
        # already handled above — skip the rest of the stage gating block
        pass
    elif _memory_still_critical():
        logger.error(
            "Memory still at %.1f%% after RUN — skipping MERGE/RELEASE/UPLOAD.",
            psutil.virtual_memory().percent,
        )
        display.skip_stage("MERGE")
        display.skip_stage("RELEASE")
        display.skip_stage("UPLOAD")
        # Record timing entries for skipped stages so build_report.generate_build_report
        # sees their 'skipped' status via stage_timings and does not fall back to
        # artifact inference (which might mark them 'completed' from stale artifacts).
        for _skipped in ("MERGE", "RELEASE", "UPLOAD"):
            stage_timing_reports.append(_collect_stage_timing(_skipped))
    else:
        # ── STAGE 2: MERGE ──
        merge_handler = _add_stage_handler("merge")
        try:
            # pass mergeable_sources (fresh + fallback) so a source whose
            # transform failed today still contributes via its prior build
            stage_merge(graph_id, mergeable_sources, overwrite, display, report_dir)
        except Exception:
            pass  # stage_merge already called fail_stage and logged the exception
        finally:
            stage_timing_reports.append(_collect_stage_timing("MERGE"))
            _remove_handler(merge_handler)
            # incremental S3 upload for MERGE — runs even if stage_merge raised
            _upload_stage_logs_safe("merge")

        # ── STAGE 3: RELEASE ──
        # Always proceeds regardless of MERGE outcome — failed sources fall back
        # to their previous successful build via latest-build.json.
        if _memory_still_critical():
            logger.error(
                "Memory at %.1f%% after MERGE — skipping RELEASE/UPLOAD.",
                psutil.virtual_memory().percent,
            )
            display.skip_stage("RELEASE")
            display.skip_stage("UPLOAD")
            for _skipped in ("RELEASE", "UPLOAD"):
                stage_timing_reports.append(_collect_stage_timing(_skipped))
        else:
            release_handler = _add_stage_handler("release")
            try:
                stage_release(mergeable_sources, node_properties, display, report_dir)
            except Exception:
                pass  # stage_release already called fail_stage and logged the exception
            finally:
                stage_timing_reports.append(_collect_stage_timing("RELEASE"))
                _remove_handler(release_handler)
                _upload_stage_logs_safe("release")

            # ── STAGE 4: UPLOAD ──
            if _memory_still_critical():
                logger.error(
                    "Memory at %.1f%% after RELEASE — skipping UPLOAD.",
                    psutil.virtual_memory().percent,
                )
                display.skip_stage("UPLOAD")
                stage_timing_reports.append(_collect_stage_timing("UPLOAD"))

    if upload and display.stage_status.get("UPLOAD") == "pending":
        upload_handler = _add_stage_handler("upload")
        try:
            stage_upload(report_dir, display)
        except Exception:
            pass  # stage_upload already called fail_stage and logged the exception
        finally:
            if display.stage_status.get("UPLOAD") == "completed":
                upload_results_path = report_dir / "stages" / "upload" / "upload-results.json"
            stage_timing_reports.append(_collect_stage_timing("UPLOAD"))
            _remove_handler(upload_handler)
            # UPLOAD stage's own logs still need pushing — the final
            # consolidated pass below catches everything else, but doing it
            # here too means a partial upload-stage failure still produces
            # visible logs at the per-stage S3 path
            _upload_stage_logs_safe("upload")
    elif not upload and display.stage_status.get("UPLOAD") == "pending":
        display.skip_stage("UPLOAD")
        # Record the skipped UPLOAD so the report does not fall back to
        # inferring 'completed' from a stale upload-results-latest.json.
        stage_timing_reports.append(_collect_stage_timing("UPLOAD"))

    total_duration = time.time() - build_start

    # NOTE: console_handler and error_handler are intentionally left attached
    # here. They are removed at the very end of this function (after the
    # final summary, report generation, finalize_latest_copies, and the
    # post-upload pass) so any log messages from those phases are still
    # captured in errors.log and visible on the terminal.

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

    # memory_critical_event is the sticky record of whether threshold was hit.
    # Some stages may still have run if memory recovered between stages.
    memory_aborted = perf.memory_critical_event.is_set()
    stages_skipped = [
        s for s in ("MERGE", "RELEASE")
        if display.stage_status.get(s) == "skipped"
    ]
    build_notes: list[str] = []
    if memory_aborted:
        mem_info = perf.get_memory_critical_info()
        if stages_skipped:
            build_notes.append(
                f"MEMORY WARNING: System memory hit critical threshold "
                f"({mem_info['threshold_percent']:.0f}%) during build — "
                f"peak usage {mem_info['used_mb']:.0f} MB / {mem_info['total_mb']:.0f} MB. "
                f"Stages skipped due to memory: {', '.join(stages_skipped)}."
            )
        else:
            build_notes.append(
                f"MEMORY WARNING: System memory hit critical threshold "
                f"({mem_info['threshold_percent']:.0f}%) during RUN stage — "
                f"peak usage {mem_info['used_mb']:.0f} MB / {mem_info['total_mb']:.0f} MB. "
                f"Memory recovered before subsequent stages."
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

    text_path, summary_path, json_path = save_report(
        report, report_dir, errors_log_path=error_log_path,
    )

    # ── Refresh 'latest' copies now that all files are written ──
    # Doing this here (not at build start) ensures reports/latest/ and
    # logs/{stage}/latest/ contain the final populated content instead of
    # empty shells created before any files existed. Wrapped in try/except
    # so a single failure (e.g. permissions on one log stage) does not
    # block the rest of the finalization or post-upload.
    try:
        finalize_latest_copies(timestamp)
    except Exception:
        logger.exception("finalize_latest_copies failed (non-fatal)")

    # ── POST-UPLOAD: re-upload reports and logs so the final files are on S3 ──
    # The main UPLOAD stage runs before save_report, so build-report.{txt,json},
    # build-summary.txt, performance.json, and the refreshed 'latest' copies
    # haven't been uploaded yet. The upload stage's log file is also still
    # being written at that point. This final pass catches those files.
    #
    # Gated only on the upload flag -- NOT on UPLOAD stage status. The main
    # UPLOAD stage is marked 'failed' if any single file failed (common with
    # transient S3 hiccups), but that should not prevent uploading the final
    # reports/logs and 'latest' copies. Skip-if-unchanged keeps this cheap.
    if upload:
        logger.info("Uploading final build report and logs to S3...")
        try:
            s3_uploader = S3Uploader()
            reports_final = s3_uploader.upload_reports()
            logs_final = s3_uploader.upload_logs()
            logger.info(
                "Final reports upload: %d uploaded, %d skipped, %d failed",
                reports_final.get("uploaded", 0),
                reports_final.get("skipped", 0),
                reports_final.get("failed", 0),
            )
            logger.info(
                "Final logs upload:    %d uploaded, %d skipped, %d failed",
                logs_final.get("uploaded", 0),
                logs_final.get("skipped", 0),
                logs_final.get("failed", 0),
            )
        except Exception:
            logger.exception("Post-upload of final reports/logs failed (non-fatal)")

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

    # Clean up logging handlers now that all final work is done. Deferred
    # from before print_final_summary() so the summary, report generation,
    # finalize_latest_copies, and post-upload log output is still captured
    # in errors.log and on the terminal.
    root_logger.removeHandler(error_handler)
    error_handler.close()
    root_logger.removeHandler(console_handler)

    return report_dir, error_log_path, memory_aborted
