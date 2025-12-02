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
"""

import logging
import warnings


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


def setup_logging(level: int = logging.INFO, format: str = "%(asctime)s - %(levelname)s: %(message)s") -> None:
    """Configure the root logger for console output.

    This should be called once at the start of each CLI entry point (main function).
    It configures the root logger with a StreamHandler that outputs to stderr.
    Also suppresses noisy third-party library warnings (linkml).

    Args:
        level: The logging level (default: logging.INFO)
        format: The log message format string (default: "%(asctime)s - %(levelname)s: %(message)s")

    Example:
        >>> setup_logging()
        >>> # Or with custom settings:1
        >>> setup_logging(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")
    """
    logging.basicConfig(level=level, format=format)

    # If we decide we really don't want any linkml warnings we could turn them all off like this
    # Suppress all linkml warnings using the logging level
    # logging.getLogger('linkml_runtime').setLevel(logging.ERROR)
    # logging.getLogger('linkml').setLevel(logging.ERROR)

    # Suppress some particularly noisy linkml warnings project-wide
    warnings.filterwarnings("ignore", message=".*namespace is already mapped.*")
    warnings.filterwarnings("ignore", message=".*Importing.*from source.*")
