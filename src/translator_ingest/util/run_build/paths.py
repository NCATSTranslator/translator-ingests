
import datetime
from pathlib import Path

from translator_ingest import INGESTS_LOGS_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.run_build import REPORTS_BASE
from translator_ingest.util.run_build.utils import (
    STAGE_NAMES_LOWER,
    update_latest_copy,
)

logger = get_logger(__name__)


# root directory under which per-stage timestamped log dirs live
LOGS_BASE = Path(INGESTS_LOGS_PATH)


def create_log_dirs(timestamp: str) -> tuple[dict[str, Path], Path]:
    stage_log_paths: dict[str, Path] = {}
    for stage in STAGE_NAMES_LOWER:
        d = LOGS_BASE / stage / timestamp
        d.mkdir(parents=True, exist_ok=True)
        stage_log_paths[stage] = d / f"{stage}.log"

    errors_dir = LOGS_BASE / "errors" / timestamp
    errors_dir.mkdir(parents=True, exist_ok=True)
    error_log_path = errors_dir / "errors.log"

    # NOTE: 'latest' copies for logs are refreshed at the END of the build
    # (see finalize_latest_copies()), not here. At build start the log files
    # are empty, so copying them now would leave reports/logs/{stage}/latest/
    # as empty shells that never get populated with actual content.

    return stage_log_paths, error_log_path


def create_report_dir(timestamp: str | None = None) -> Path:
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y_%m_%d_%H%M%S")
    report_dir = REPORTS_BASE / timestamp
    report_dir.mkdir(parents=True, exist_ok=True)

    # Create stage subdirectories
    for stage in STAGE_NAMES_LOWER:
        (report_dir / "stages" / stage).mkdir(parents=True, exist_ok=True)

    return report_dir


def finalize_latest_copies(timestamp: str) -> None:
    logger.info("Finalizing 'latest' copies for reports and logs...")

    reports_src = REPORTS_BASE / timestamp
    if reports_src.exists():
        update_latest_copy(REPORTS_BASE, timestamp)
        logger.info("  reports/latest refreshed from %s", reports_src)
    else:
        logger.warning("  reports/%s does not exist, skipping reports/latest refresh", timestamp)

    for stage in (*STAGE_NAMES_LOWER, "errors"):
        stage_base = LOGS_BASE / stage
        stage_src = stage_base / timestamp
        if stage_src.exists():
            update_latest_copy(stage_base, timestamp)
            logger.info("  logs/%s/latest refreshed from %s", stage, stage_src)
        else:
            logger.debug("  logs/%s/%s does not exist, skipping", stage, timestamp)
