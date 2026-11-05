"""Tests for the DAKP Tablassert passthrough ingest (offline).

DAKP ships no transform code: the declaration and the version helper are the whole ingest. The
live network behavior (release discovery, file retrieval) is covered by tests/integration.
"""

from pathlib import Path

import yaml

from translator_ingest.util.tablassert import declares, load
from translator_ingest.util.hf_releases import DATASET_RELEASE_PATTERN
from translator_ingest.util.tablassert import resolve_dataset_version_placeholder

INGEST_DIR = Path(__file__).parents[4] / "src" / "translator_ingest" / "ingests" / "dakp"


def test_dakp_declares_the_tablassert_kind():
    assert declares("dakp")
    config = load("dakp")
    assert config.version == "latest"
    assert config.nodes.path == "drug_approvals_kp_nodes.jsonl"
    assert config.edges.path == "drug_approvals_kp_edges.jsonl"
    assert config.rig.path == "drug_approvals_kp_rig.yaml"


def test_dakp_declaration_matches_the_readme_example():
    """The ingest's YAML and the README's example must not drift apart."""
    readme = (Path(__file__).parents[4] / "INGESTERS_README.md").read_text(encoding="utf-8")
    example_block = readme[readme.index("```yaml") + 7:readme.index("```", readme.index("```yaml") + 7)]
    assert yaml.safe_load((INGEST_DIR / "tablassert.yaml").read_text()) == yaml.safe_load(example_block)


def test_dakp_ships_no_transform_code_or_committed_rig():
    """The passthrough contract: no koza config, no manifest, no committed RIG, no rewrite hook."""
    present = {path.name for path in INGEST_DIR.iterdir()}
    assert "dakp.yaml" not in present, "DAKP must not declare a koza config alongside tablassert.yaml"
    assert "download.yaml" not in present, "DAKP's files are declared in tablassert.yaml"
    assert "dakp_rig.yaml" not in present, "The DAKP RIG is fetched from upstream, never committed"
    source_text = (INGEST_DIR / "dakp.py").read_text(encoding="utf-8")
    assert "koza" not in source_text, "DAKP must not run koza transform code"
    assert "rewrite_record" not in source_text, "DAKP performs no record rewriting"


def test_dakp_version_helper_delegates_to_the_shared_hf_helper():
    import inspect
    import translator_ingest.ingests.dakp.dakp as dakp

    assert dakp.DATASET_REPO_ID == "SkyeAv/drug-approvals-kp"
    assert callable(dakp.get_latest_version)
    # The helper itself is exercised offline for ordering, and live in tests/integration; here we
    # bind the contract: the ingest function delegates straight to the shared discovery helper.
    source_text = inspect.getsource(dakp)
    assert "get_latest_dataset_version" in source_text


def test_dakp_urls_resolve_against_a_dataset_version():
    """Every declared URL must be a complete template that a release version fills in."""
    config = load("dakp")
    for declared in (config.nodes, config.edges, config.rig):
        resolved = resolve_dataset_version_placeholder(declared.url, "1.23.5")
        assert "1.23.5" in resolved and "{" not in resolved
        assert "/resolve/main/1.23.5/" in resolved
    # And the shape of a release version is what the placeholder expects.
    assert DATASET_RELEASE_PATTERN.match("1.23.5")
