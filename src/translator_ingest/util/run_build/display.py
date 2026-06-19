
import json
import threading
import time
from pathlib import Path

from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.run_build import REPORTS_BASE
from translator_ingest.util.run_build.tracking import PerformanceTracker
from translator_ingest.util.run_build.utils import (
    BYTES_PER_GB,
    BYTES_PER_MB,
    STAGE_NAMES,
    format_duration,
)

logger = get_logger(__name__)


DISPLAY_REFRESH_INTERVAL = 10.0  # seconds between display refreshes

# Consecutive DISPLAY_REFRESH_INTERVAL samples with zero I/O delta before flagging stalled
_STALL_SAMPLE_THRESHOLD = 3

# Per-source historical I/O totals — persisted across builds for ETA estimation
SOURCE_IO_HISTORY_FILE = REPORTS_BASE / "source_io_history.json"


def _read_proc_rchar(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            if line.startswith("rchar:"):
                return int(line.split()[1])
    except (OSError, ValueError):
        pass
    return None


def _worker_pid_path(source: str) -> Path:
    return Path(f"/tmp/translator_ingest_{source}.pid")


def _load_source_io_history() -> dict[str, int]:
    if SOURCE_IO_HISTORY_FILE.exists():
        try:
            with SOURCE_IO_HISTORY_FILE.open() as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            pass
    return {}


def _save_source_io_history(history: dict[str, int]) -> None:
    SOURCE_IO_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SOURCE_IO_HISTORY_FILE.open("w") as f:
        json.dump(history, f, indent=2, sort_keys=True)


class SourceProgress:

    def __init__(self, source: str, expected_bytes: int | None = None) -> None:
        self.source = source
        self.expected_bytes = expected_bytes
        self._start_time = time.time()
        self._pid: int | None = None
        self._bytes_read: int = 0
        self._prev_bytes_read: int = 0
        self._rate_bps: float = 0.0  # smoothed bytes per second
        self._stall_samples: int = 0

    def update(self) -> None:
        if self._pid is None:
            try:
                self._pid = int(_worker_pid_path(self.source).read_text().strip())
            except (OSError, ValueError):
                return  # process hasn't written its PID yet

        new_bytes = _read_proc_rchar(self._pid)
        if new_bytes is None:
            return  # process finished or not yet accessible

        delta = new_bytes - self._prev_bytes_read
        if delta > 0:
            self._stall_samples = 0
        else:
            self._stall_samples += 1

        new_rate = delta / DISPLAY_REFRESH_INTERVAL
        self._rate_bps = 0.7 * self._rate_bps + 0.3 * new_rate if self._rate_bps > 0 else new_rate

        self._prev_bytes_read = new_bytes
        self._bytes_read = new_bytes

    @property
    def elapsed(self) -> float:
        return time.time() - self._start_time

    @property
    def is_stalled(self) -> bool:
        return self._stall_samples >= _STALL_SAMPLE_THRESHOLD and self.elapsed > 30

    @property
    def progress_pct(self) -> float | None:
        if self.expected_bytes and self.expected_bytes > 0 and self._bytes_read > 0:
            return min(99.0, self._bytes_read / self.expected_bytes * 100)
        return None

    @property
    def eta_seconds(self) -> float | None:
        if self.expected_bytes and self._rate_bps > 1024:
            return max(0.0, self.expected_bytes - self._bytes_read) / self._rate_bps
        return None

    @property
    def total_bytes_read(self) -> int:
        return self._bytes_read

    def format_line(self) -> str:
        pct = self.progress_pct

        if pct is not None:
            # progress_pct only returns non-None when expected_bytes is truthy
            # (see the property body) — narrow the type for mypy
            assert self.expected_bytes is not None
            width = 16
            filled = int(pct / 100 * width)
            bar = "█" * filled + "░" * (width - filled)
            read_gb = self._bytes_read / BYTES_PER_GB
            total_gb = self.expected_bytes / BYTES_PER_GB
            progress = f"[{bar}] {pct:.0f}%  {read_gb:.2f}/{total_gb:.2f} GB"
        elif self._bytes_read > 0:
            progress = f"[no baseline]  {self._bytes_read / BYTES_PER_MB:.0f} MB read"
        else:
            progress = "[waiting...]"

        if self.is_stalled:
            stall_dur = format_duration(self._stall_samples * DISPLAY_REFRESH_INTERVAL)
            rate_timing = f"STALLED ({stall_dur} no I/O)"
        else:
            if self._rate_bps >= BYTES_PER_MB:
                rate = f"{self._rate_bps / BYTES_PER_MB:.1f} MB/s"
            elif self._rate_bps >= 1024:
                rate = f"{self._rate_bps / 1024:.0f} KB/s"
            else:
                rate = "measuring..."

            eta = self.eta_seconds
            timing = f"ETA ~{format_duration(eta)}" if eta is not None else f"{format_duration(self.elapsed)} elapsed"
            rate_timing = f"{rate}  |  {timing}"

        return f"  {self.source:<16}  {progress}  |  {rate_timing}"


class BuildDisplay:

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

        # Per-source I/O progress (populated by stage_run)
        self.source_progress: dict[str, "SourceProgress"] = {}

        self._render_lock = threading.Lock()

    def print_header(self) -> None:
        logger.info("=" * 80)
        logger.info("TRANSLATOR-INGESTS PIPELINE BUILD")
        logger.info("=" * 80)

    def render(self) -> None:
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
                # Per-source progress lines (rate, ETA, stall detection)
                for src in sorted(self.run_running):
                    if src in self.source_progress:
                        logger.info(self.source_progress[src].format_line())
            elif running_stage:
                elapsed = time.time() - self.stage_durations.get(f"{running_stage}_start", time.time())
                logger.info(
                    "[%s] running (%s)  |  %s",
                    running_stage, format_duration(elapsed), mem_str,
                )

    def start_stage(self, stage: str) -> None:
        self.current_stage_idx = STAGE_NAMES.index(stage)
        self.stage_status[stage] = "running"
        self.stage_durations[f"{stage}_start"] = time.time()
        self.perf.begin_stage(stage)
        stage_num = self.current_stage_idx + 1
        logger.info("[%d/%d] %s ... started", stage_num, self.num_stages, stage)

    def complete_stage(self, stage: str) -> None:
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
        self.stage_status[stage] = "skipped"
        stage_num = STAGE_NAMES.index(stage) + 1
        logger.info("[%d/%d] %s ... skipped", stage_num, self.num_stages, stage)

    def print_final_summary(self, total_duration: float) -> None:
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
