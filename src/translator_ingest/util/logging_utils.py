"""Centralized logging configuration for translator-ingests.

This module provides consistent logging setup across all modules in the project.

Usage:
    >>> from translator_ingest.util.logging_utils import get_logger, setup_logging
    >>>
    >>> # In any main/CLI entry point:
    >>> setup_logging()
    >>>
    >>> # In any module:
    >>> logger = get_logger(__name__)
    >>> logger.info("This is a log message")

    >>> # For source-specific logging with file output:
    >>> setup_logging(source="go_cam")  # Creates /logs/go_cam/{timestamp}/
    >>> # Or with explicit log directory (used by build orchestrator):
    >>> setup_logging(source="go_cam", log_dir=Path("reports/latest/stages/run"))
"""

import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

from loguru import logger as loguru_logger

from translator_ingest import INGESTS_LOGS_PATH


# Global variable to track current log directory
_current_log_dir: Path | None = None

# Mapping from loguru level names to stdlib logging levels.
# Loguru does not export a public ``Message`` type, so the sink callback
# below uses ``Any`` for the message parameter.
_LOGURU_TO_STDLIB: dict[str, int] = {
    "TRACE": logging.DEBUG,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "SUCCESS": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger for the given module name.

    This creates a logger that will work properly once setup_logging()
    has been called in the main entry point.

    Args:
        name: The module name (typically __name__)

    Returns:
        A configured logger instance

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Starting process...")
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def get_current_log_dir() -> Path | None:
    """Get the current log directory path.

    Returns:
        Path to current log directory, or None if not set up with source
    """
    return _current_log_dir


def setup_logging(
    level: int = logging.INFO,
    # Renamed from ``format`` to avoid shadowing the built-in.
    log_format: str = "%(asctime)s - %(levelname)s: %(message)s",
    source: str | None = None,
    log_dir: Path | None = None,
) -> Path | None:
    """Configure the root logger for console and optional file output.

    This should be called once at the start of each CLI entry point (main function).
    It configures the root logger with a StreamHandler that outputs to stderr.
    Also suppresses noisy third-party library warnings (linkml).

    If log_dir is provided, logs are written to ``log_dir/{source}.log``
    (or ``log_dir/run.log`` if no source). If only source is provided,
    logs go to ``/logs/{source}/{timestamp}/run.log``.

    Args:
        level: The logging level (default: logging.INFO)
        log_format: The log message format string
        source: Optional source name for file logging (e.g., "go_cam")
        log_dir: Optional explicit directory for log files

    Returns:
        Path to the log directory if file logging was set up, None otherwise

    Example:
        >>> setup_logging()
        >>> # Or with source-specific file logging:
        >>> log_dir = setup_logging(source="go_cam")
        >>> print(f"Logs written to: {log_dir}")
    """
    global _current_log_dir
    _current_log_dir = None

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(log_format)

    # Console handler (always)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (if source or log_dir specified)
    resolved_log_dir = None
    if log_dir:
        resolved_log_dir = Path(log_dir)
        resolved_log_dir.mkdir(parents=True, exist_ok=True)
        log_file = resolved_log_dir / (f"{source}.log" if source else "run.log")
    elif source:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        resolved_log_dir = Path(INGESTS_LOGS_PATH) / source / timestamp
        resolved_log_dir.mkdir(parents=True, exist_ok=True)
        log_file = resolved_log_dir / "run.log"

    if resolved_log_dir:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        _current_log_dir = resolved_log_dir
        root_logger.info("Logging to: %s", log_file)

    # Suppress some particularly noisy linkml warnings project-wide
    warnings.filterwarnings("ignore", message=".*namespace is already mapped.*")
    warnings.filterwarnings("ignore", message=".*Importing.*from source.*")

    return resolved_log_dir


class _StreamToLogger:
    """Redirect a stream (stdout/stderr) to a stdlib logger.

    Each ``write()`` call is buffered by line; complete lines are emitted as
    log records so they flow through all attached handlers (console + file).
    """

    def __init__(self, log: logging.Logger, level: int, original: TextIO) -> None:
        self._log = log
        self._level = level
        self._original = original
        self._buf = ""

    def write(self, msg: str) -> int:
        self._buf += msg
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                self._log.log(self._level, "%s", line.rstrip())
        return len(msg)

    def flush(self) -> None:
        if self._buf.strip():
            self._log.log(self._level, "%s", self._buf.rstrip())
            self._buf = ""
        self._original.flush()

    def fileno(self) -> int:
        return self._original.fileno()

    def isatty(self) -> bool:
        return False


def _intercept_loguru(source: str) -> None:
    """Route loguru output through stdlib logging.

    Removes loguru's default stderr sink and replaces it with a sink
    that forwards each message to a stdlib logger named after the source.
    This ensures Koza's loguru output appears in both terminal and log files.
    """
    loguru_logger.remove()

    stdlib_log = logging.getLogger(f"loguru.{source}")

    def _sink(message: Any) -> None:
        record = message.record
        level = _LOGURU_TO_STDLIB.get(record["level"].name, logging.INFO)
        stdlib_log.log(level, "%s", record["message"].rstrip())

    loguru_logger.add(_sink, level="DEBUG", format="{message}")


def setup_worker_logging(
    source: str,
    stage_log_path: str | None = None,
    error_log_path: str | None = None,
) -> None:
    """Configure logging for a build worker process.

    Sets up file handlers that write to the shared stage log and error log.
    Worker output is prefixed with [source] so interleaved lines from parallel
    sources can be distinguished.

    Also intercepts loguru (used by Koza) and stdout (used by download
    utilities) so that *all* worker output flows through stdlib logging
    and appears in both terminal and log files.

    .. note::
        This function is called once per worker *process* (via
        ``ProcessPoolExecutor``), never from multiple threads in the same
        process.  The global ``_current_log_dir`` mutation is therefore safe.

    Args:
        source: Source name (used as prefix in log lines)
        stage_log_path: Path to the stage log file (opened in append mode)
        error_log_path: Path to the error log file (opened in append mode, WARNING+)
    """
    global _current_log_dir

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    fmt = logging.Formatter(f"%(asctime)s [{source}] %(levelname)s: %(message)s")

    # Console handler (INFO — matches file handlers so terminal and logs are identical)
    console = logging.StreamHandler(sys.__stderr__)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root_logger.addHandler(console)

    # Stage log file handler (append mode, shared with other workers)
    if stage_log_path:
        stage_handler = logging.FileHandler(stage_log_path, mode="a")
        stage_handler.setLevel(logging.INFO)
        stage_handler.setFormatter(fmt)
        root_logger.addHandler(stage_handler)
        _current_log_dir = Path(stage_log_path).parent

    # Error log file handler (append mode, WARNING+ only)
    if error_log_path:
        error_handler = logging.FileHandler(error_log_path, mode="a")
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(fmt)
        root_logger.addHandler(error_handler)

    # Intercept loguru (Koza) → stdlib logging
    _intercept_loguru(source)

    # Capture stdout (download utility print output) → stdlib logging.
    # stdout is intentionally not restored: worker processes exit after
    # processing a single source, so cleanup is unnecessary.
    stdout_logger = logging.getLogger(f"stdout.{source}")
    sys.stdout = _StreamToLogger(stdout_logger, logging.INFO, sys.__stdout__)

    # Suppress noisy linkml warnings
    warnings.filterwarnings("ignore", message=".*namespace is already mapped.*")
    warnings.filterwarnings("ignore", message=".*Importing.*from source.*")
