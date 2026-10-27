"""Tests for the environment configuration that importing translator_ingest sets up for ORION."""
import os
import subprocess
import sys

import pytest


def node_normalization_url_after_import(environment_value: str | None) -> str:
    """
    Import translator_ingest in a fresh interpreter, whose environment has NODE_NORMALIZATION_URL set to
    environment_value (or unset, if None), and return the NODE_NORMALIZATION_URL it leaves for ORION.
    """
    env = {k: v for k, v in os.environ.items() if k != "NODE_NORMALIZATION_URL"}
    if environment_value is not None:
        env["NODE_NORMALIZATION_URL"] = environment_value
    result = subprocess.run(
        [sys.executable, "-c", "import os, translator_ingest; print(os.environ['NODE_NORMALIZATION_URL'])"],
        env=env, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


@pytest.mark.parametrize(
    "environment_value, expected",
    [
        (None, "https://nodenorm-es.ci.transltr.io"),
        ("https://nodenormalization-sri.renci.org", "https://nodenormalization-sri.renci.org"),
        ("https://nodenormalization-sri.renci.org/", "https://nodenormalization-sri.renci.org"),
    ],
)
def test_node_normalization_url_has_no_trailing_slash(environment_value, expected):
    """ORION appends "/<endpoint>" to the URL, so a trailing slash would make it request e.g. "//status", which 404s."""
    assert node_normalization_url_after_import(environment_value) == expected
