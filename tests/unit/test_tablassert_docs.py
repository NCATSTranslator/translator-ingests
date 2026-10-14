"""Tests that the Tablassert author documentation in INGESTERS_README.md stays valid.

The YAML example is loaded from the README itself rather than duplicated into a fixture, so the
declaration model, the field reference, and the example cannot drift apart silently.
"""

import re
from pathlib import Path

import pytest
import yaml

from translator_ingest.util.tablassert import TablassertIngest

README_PATH = Path(__file__).parents[2] / "INGESTERS_README.md"


def extract_tablassert_example(readme_text: str) -> dict:
    """Pull the first ```yaml block that declares Tablassert nodes/edges/rig out of the README."""
    for match in re.finditer(r"```yaml\n(.*?)```", readme_text, flags=re.DOTALL):
        parsed = yaml.safe_load(match.group(1))
        if isinstance(parsed, dict) and {"nodes", "edges", "rig"} <= set(parsed):
            return parsed
    raise AssertionError("No Tablassert yaml example found in INGESTERS_README.md")


@pytest.mark.parametrize("required_section", [
    "## Choosing an ingest kind",
    "## Writing a Tablassert ingest",
    "### Execution, warnings, and cache behavior",
    "### Testing and running",
])
def test_readme_documents_the_tablassert_kind(required_section):
    readme_text = README_PATH.read_text(encoding="utf-8")
    assert required_section in readme_text, f"INGESTERS_README.md is missing the {required_section!r} section"


def test_readme_example_is_a_valid_tablassert_declaration():
    declaration = extract_tablassert_example(README_PATH.read_text(encoding="utf-8"))
    config = TablassertIngest(**declaration)
    assert config.version == "latest"
    assert config.nodes.path.endswith("nodes.jsonl")
    assert config.edges.path.endswith("edges.jsonl")
    assert config.rig.path.endswith(".yaml")
    assert "{version}" in config.nodes.url


def test_readme_field_reference_mentions_every_declared_field():
    readme_text = README_PATH.read_text(encoding="utf-8")
    field_table = readme_text[readme_text.index("## Writing a Tablassert ingest"):
                              readme_text.index("### Execution, warnings, and cache behavior")]
    for field in ("version", "nodes", "edges", "rig", "url", "path"):
        assert f"`{field}`" in field_table, f"The field reference does not document {field!r}"
