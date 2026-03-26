"""Build orchestration and reporting for the translator-ingests pipeline."""

from pathlib import Path

from translator_ingest import INGESTS_DATA_PATH

REPORTS_BASE = Path(INGESTS_DATA_PATH).parent / "reports"
