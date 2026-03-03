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
"""

import logging
import warnings
from datetime import datetime
from pathlib import Path

from translator_ingest import INGESTS_LOGS_PATH


# Global variable to track current log directory
_current_log_dir: Path | None = None


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
    format: str = "%(asctime)s - %(levelname)s: %(message)s",
    source: str | None = None
) -> Path | None:
    """Configure the root logger for console and optional file output.

    This should be called once at the start of each CLI entry point (main function).
    It configures the root logger with a StreamHandler that outputs to stderr.
    Also suppresses noisy third-party library warnings (linkml).

    If a source is specified, logs are also written to:
        /logs/{source}/{timestamp}/run.log

    Args:
        level: The logging level (default: logging.INFO)
        format: The log message format string
        source: Optional source name for file logging (e.g., "go_cam")

    Returns:
        Path to the log directory if source was specified, None otherwise

    Example:
        >>> setup_logging()
        >>> # Or with source-specific file logging:
        >>> log_dir = setup_logging(source="go_cam")
        >>> print(f"Logs written to: {log_dir}")
    """
    global _current_log_dir

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(format)

    # Console handler (always)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (if source specified)
    log_dir = None
    if source:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = Path(INGESTS_LOGS_PATH) / source / timestamp
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / "run.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        _current_log_dir = log_dir
        root_logger.info(f"Logging to: {log_file}")

    # Suppress some particularly noisy linkml warnings project-wide
    warnings.filterwarnings("ignore", message=".*namespace is already mapped.*")
    warnings.filterwarnings("ignore", message=".*Importing.*from source.*")

    return log_dir
