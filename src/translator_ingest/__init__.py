"""
Translator Ingest Globally Shared Code and parameters
"""
import os
from pathlib import Path

TRANSLATOR_INGEST_PATH = Path(__file__).parent
TRANSLATOR_INGEST_DIR = TRANSLATOR_INGEST_PATH.absolute()

INGESTS_DATA_PATH = TRANSLATOR_INGEST_PATH / ".." / ".." / "data"
INGESTS_DATA_DIR = INGESTS_DATA_PATH.absolute()

INGESTS_RELEASES_PATH = TRANSLATOR_INGEST_PATH / ".." / ".." / "releases"

INGESTS_LOGS_PATH = TRANSLATOR_INGEST_PATH / ".." / ".." / "logs"

INGESTS_PARSER_PATH = TRANSLATOR_INGEST_PATH / "ingests"
INGEST_PARSER_DIR = INGESTS_PARSER_PATH.absolute()

# Default public HTTPS endpoints for KGX storage (browser view format)
INGESTS_STORAGE_URL = os.environ.get("INGESTS_STORAGE_URL", "https://kgx-storage.rtx.ai/?path=data")
INGESTS_RELEASES_URL = os.environ.get("INGESTS_RELEASES_URL", "https://kgx-storage.rtx.ai/?path=releases")