"""
Translator Ingest Globally Shared Code and parameters
"""
from os.path import abspath
from pathlib import Path

TRANSLATOR_INGEST_PATH = Path(__file__).parent
TRANSLATOR_INGEST_DIR = abspath(TRANSLATOR_INGEST_PATH)

INGESTS_DATA_PATH = TRANSLATOR_INGEST_PATH / ".." / ".." / "data"
INGESTS_DATA_DIR = abspath(INGESTS_DATA_PATH)

INGESTS_PARSER_PATH = TRANSLATOR_INGEST_PATH / "ingests"
INGEST_PARSER_DIR = abspath(INGESTS_PARSER_PATH)
