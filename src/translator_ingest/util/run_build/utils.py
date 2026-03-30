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


def update_latest_symlink(parent_dir: Path, target_name: str) -> None:
    """Create or update a 'latest' symlink in parent_dir pointing to target_name.

    If a 'latest' symlink or directory already exists, it is removed first.

    Args:
        parent_dir: Directory containing the symlink
        target_name: Name of the subdirectory to point to (relative, not absolute)

    Examples:
        >>> import tempfile
        >>> from pathlib import Path
        >>> with tempfile.TemporaryDirectory() as d:
        ...     p = Path(d)
        ...     (p / "2026_03_16").mkdir()
        ...     update_latest_symlink(p, "2026_03_16")
        ...     (p / "latest").is_symlink()
        True
    """
    latest = parent_dir / "latest"
    if latest.is_symlink():
        latest.unlink()
    elif latest.exists():
        # Remove a real directory that was created instead of a symlink
        # (e.g. by a tool that doesn't preserve symlinks).
        shutil.rmtree(latest)
    latest.symlink_to(target_name)


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
