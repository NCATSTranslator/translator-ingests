"""Tests for how the SIGNOR ingest acquires and versions its source data (issue #303)."""

from datetime import datetime, timezone
from itertools import islice
from typing import Any

import pytest
import requests
import yaml
from kghub_downloader.model import DownloadableResource

from translator_ingest import INGESTS_PARSER_PATH
from translator_ingest.ingests.signor.signor import get_latest_version

SIGNOR_INGEST_DIR = INGESTS_PARSER_PATH / "signor"


def load_download_resources() -> list[DownloadableResource]:
    """Parse the SIGNOR download.yaml with kghub-downloader's own resource model."""
    with (SIGNOR_INGEST_DIR / "download.yaml").open() as handle:
        return [DownloadableResource(**entry) for entry in yaml.safe_load(handle)]


def load_reader_config() -> dict[str, Any]:
    """Return the Koza reader configuration of the SIGNOR transform."""
    with (SIGNOR_INGEST_DIR / "signor.yaml").open() as handle:
        return yaml.safe_load(handle)["readers"]["signor_parsing"]


def test_downloaded_file_is_the_file_the_reader_consumes():
    """kghub-downloader names the file by local_name; the Koza reader must read that same file."""
    (resource,) = load_download_resources()
    assert [str(resource.path)] == load_reader_config()["files"]


def test_version_is_the_utc_download_date():
    """Reading the clock on both sides of the call tolerates a run that crosses midnight UTC."""
    before = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    version = get_latest_version()
    after = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    assert version in {before, after}


# Network-dependent: streams the first rows of SIGNOR's live export.
# Skipped to keep CI hermetic, matching the convention in test_string.py and test_panther.py.
@pytest.mark.skip(reason="hits signor.uniroma2.it; run manually to verify the export still matches signor.yaml")
def test_live_export_matches_the_reader_contract():
    """The export has the configured columns and the DIRECT and SCORE encodings that signor.py relies on."""
    (resource,) = load_download_resources()
    sample_size = 500
    with requests.get(resource.url, stream=True, timeout=60) as response:
        response.raise_for_status()
        lines = [line.decode("utf-8") for line in islice(response.iter_lines(), sample_size + 1)]
    header, *rows = (line.split("\t") for line in lines)
    assert header == load_reader_config()["columns"]

    records = [dict(zip(header, row, strict=True)) for row in rows]
    assert len(records) == sample_size
    assert {record["DIRECT"] for record in records} <= {"YES", "NO", "UNK"}
    scores = [float(record["SCORE"]) for record in records if record["SCORE"]]
    assert scores
    assert all(0 <= score <= 1 for score in scores)
