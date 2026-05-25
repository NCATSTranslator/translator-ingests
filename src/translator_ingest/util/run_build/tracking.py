
import threading
from pathlib import Path
from typing import Any

# psutil exposes process and system memory/cpu/disk counters; the stdlib
# resource module can't read container cgroup limits on linux, and we need
# system-wide stats to make memory-threshold decisions in containerized runs
import psutil

from translator_ingest import INGESTS_DATA_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.run_build.utils import (
    BYTES_PER_GB,
    BYTES_PER_MB,
    MEMORY_CRITICAL_CONSECUTIVE_SAMPLES,
    MEMORY_CRITICAL_THRESHOLD_PERCENT,
    MEMORY_WARNING_THRESHOLD_PERCENT,
)

logger = get_logger(__name__)


SAMPLE_INTERVAL = 2.0  # seconds between memory samples


def _get_memory_mb() -> float:
    return psutil.virtual_memory().used / BYTES_PER_MB


def _get_cpu_percent() -> float:
    return psutil.cpu_percent(interval=None)


def _get_disk_usage_gb() -> dict[str, float]:
    path = Path(INGESTS_DATA_PATH)
    for candidate in (path, *path.parents):
        if candidate.exists():
            usage = psutil.disk_usage(str(candidate))
            return {
                "total_gb": usage.total / BYTES_PER_GB,
                "used_gb": usage.used / BYTES_PER_GB,
                "free_gb": usage.free / BYTES_PER_GB,
                "percent": usage.percent,
            }
    return {"total_gb": 0, "used_gb": 0, "free_gb": 0, "percent": 0}


class PerformanceTracker:

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
        # Take initial disk snapshot
        self._disk_start = _get_disk_usage_gb()
        # Prime system-wide cpu_percent (first call always returns 0.0)
        psutil.cpu_percent(interval=None)
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._disk_end = _get_disk_usage_gb()

    def begin_stage(self, stage: str) -> None:
        with self._lock:
            self._current_stage = stage
            self._stage_memory_samples[stage] = []
            self._stage_cpu_samples[stage] = []

    def end_stage(self, stage: str) -> None:
        with self._lock:
            if self._current_stage == stage:
                self._current_stage = None

    def _check_memory_threshold(self, vm: Any) -> None:
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
        while not self._stop_event.is_set():
            # Single syscall per iteration — reused for both stats and threshold check.
            vm = psutil.virtual_memory()
            mem = vm.used / BYTES_PER_MB
            cpu = _get_cpu_percent()
            with self._lock:
                self._all_memory_samples.append(mem)
                self._all_cpu_samples.append(cpu)
                if self._current_stage:
                    self._stage_memory_samples.setdefault(self._current_stage, []).append(mem)
                    self._stage_cpu_samples.setdefault(self._current_stage, []).append(cpu)
            self._check_memory_threshold(vm)
            self._stop_event.wait(self.sample_interval)

    def _stats_from_samples(self, samples: list[float]) -> dict[str, float]:
        if not samples:
            return {"peak": 0.0, "avg": 0.0, "min": 0.0}
        return {
            "peak": max(samples),
            "avg": sum(samples) / len(samples),
            "min": min(samples),
        }

    def get_overall_stats(self) -> dict[str, Any]:
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
        with self._lock:
            mem_samples = list(self._stage_memory_samples.get(stage, []))
            cpu_samples = list(self._stage_cpu_samples.get(stage, []))
        return {
            "memory_mb": self._stats_from_samples(mem_samples),
            "cpu_percent": self._stats_from_samples(cpu_samples),
            "sample_count": len(mem_samples),
        }

    def get_current_memory_mb(self) -> float:
        with self._lock:
            if self._all_memory_samples:
                return self._all_memory_samples[-1]
        return _get_memory_mb()

    def get_peak_memory_mb(self) -> float:
        with self._lock:
            if self._all_memory_samples:
                return max(self._all_memory_samples)
        return 0.0

    def get_avg_memory_mb(self) -> float:
        with self._lock:
            if self._all_memory_samples:
                return sum(self._all_memory_samples) / len(self._all_memory_samples)
        return 0.0

    def get_memory_critical_info(self) -> dict[str, float]:
        if not self.memory_critical_event.is_set():
            return {"used_mb": 0.0, "total_mb": 0.0, "threshold_percent": 0.0}
        return {
            "used_mb": self._critical_memory_mb,
            "total_mb": self._total_system_memory_mb,
            "threshold_percent": self._memory_critical_percent,
        }

    def is_memory_critical(self) -> bool:
        return psutil.virtual_memory().percent >= self._memory_critical_percent
