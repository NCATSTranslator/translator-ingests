"""Shared utilities for the run_build package.

Contains helpers used by both run_build.py and build_report.py to avoid
duplication: duration formatting, symlink management, report directory
creation, and shared constants.
"""

import shutil
from pathlib import Path


def format_duration(seconds: float, precise: bool = False) -> str:
    """Format seconds into a human-readable duration string.

    Args:
        seconds: Duration in seconds
        precise: If True, use one decimal place for sub-minute durations

    Returns:
        Formatted duration string

    Examples:
        >>> format_duration(5)
        '5s'
        >>> format_duration(5, precise=True)
        '5.0s'
        >>> format_duration(65)
        '1m 5s'
        >>> format_duration(3661)
        '1h 1m'
    """
    if seconds < 60:
        return f"{seconds:.1f}s" if precise else f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"


def update_latest_copy(parent_dir: Path, target_name: str) -> None:
    """Create or update a 'latest' directory in parent_dir as a copy of target_name.

    This mirrors the pattern used by ``release.atomic_copy_directory``: the
    'latest' entry is a real directory tree, not a symlink. This keeps the
    layout consistent across /data, /releases, /reports, and /logs so
    downstream tools (S3 upload, web UI, directory walkers) do not need any
    symlink-aware handling.

    Uses a temp directory + atomic rename so 'latest' is always valid (either
    the old copy or the new one), never half-updated.

    Args:
        parent_dir: Directory containing the 'latest' entry
        target_name: Name of the sibling subdirectory whose contents to copy

    Examples:
        >>> import tempfile
        >>> from pathlib import Path
        >>> with tempfile.TemporaryDirectory() as d:
        ...     p = Path(d)
        ...     (p / "2026_03_16").mkdir()
        ...     _ = (p / "2026_03_16" / "report.json").write_text("{}")
        ...     update_latest_copy(p, "2026_03_16")
        ...     (p / "latest").is_dir() and not (p / "latest").is_symlink()
        True
    """
    source = parent_dir / target_name
    if not source.exists():
        return

    latest = parent_dir / "latest"
    latest_tmp = parent_dir / "latest_new"

    # Clear any leftover state so copytree's destination-must-not-exist rule holds
    if latest_tmp.exists() or latest_tmp.is_symlink():
        if latest_tmp.is_symlink():
            latest_tmp.unlink()
        else:
            shutil.rmtree(latest_tmp)

    # Copy into temp location, then atomic swap onto the real name
    shutil.copytree(source, latest_tmp, symlinks=False)
    if latest.is_symlink():
        latest.unlink()
    elif latest.exists():
        shutil.rmtree(latest)
    latest_tmp.rename(latest)


# Stage names used throughout the pipeline
STAGE_NAMES = ("RUN", "MERGE", "RELEASE", "UPLOAD")
STAGE_NAMES_LOWER = tuple(s.lower() for s in STAGE_NAMES)

# Byte conversion constants
BYTES_PER_MB = 1024**2
BYTES_PER_GB = 1024**3

# Memory guardian thresholds (system-wide RAM usage percentage)
MEMORY_WARNING_THRESHOLD_PERCENT = 85.0
MEMORY_CRITICAL_THRESHOLD_PERCENT = 95.0

# Number of consecutive samples above critical threshold before triggering shutdown.
# At the default 2-second sample interval this means ~6 seconds of sustained pressure,
# avoiding false positives from transient GC-delay spikes.
MEMORY_CRITICAL_CONSECUTIVE_SAMPLES = 3
