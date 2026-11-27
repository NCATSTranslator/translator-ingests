"""Fixtures shared by the text-location build tests: the full fixture set, and the build run on it."""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.unit.ingests.semmeddb.text_location_fixtures import TOTAL_ROWS, argv_for, full_inputs
from translator_ingest.ingests.semmeddb.analysis.build_text_locations import main


@pytest.fixture(name="inputs")
def fixture_inputs(tmp_path: Path) -> dict[str, Path]:
    """The full fixture set: two chunks with rows, one empty chunk, and all KG2 records."""
    return full_inputs(tmp_path)


@pytest.fixture(name="built")
def fixture_built(inputs: dict[str, Path]) -> dict[str, Any]:
    """Run the build on the full fixture set and return the manifest."""
    assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    return json.loads(inputs["out_manifest"].read_text())
