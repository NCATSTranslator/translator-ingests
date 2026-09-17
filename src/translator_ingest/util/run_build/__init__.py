"""Build orchestration and reporting for the translator-ingests pipeline."""

from translator_ingest import INGESTS_REPORTS_PATH

# REPORTS_BASE is exported under this name (rather than the raw path constant)
# because it is imported throughout run_build/ and s3.py as the established
# name for "where build reports live". It now resolves directly from
# INGESTS_REPORTS_PATH instead of being derived from INGESTS_DATA_PATH, so it
# no longer silently follows the data directory if that path is reconfigured.
REPORTS_BASE = INGESTS_REPORTS_PATH
