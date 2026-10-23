"""Tests for the MOKG Tablassert passthrough ingest (offline).

MOKG ships no transform code: the declaration and the version helper are the whole ingest. The
live network behavior (release discovery, file retrievability) is covered by tests/integration,
which never downloads the graph itself (multiomics-kp is ~85 GB upstream).
"""

from pathlib import Path

from translator_ingest.util.tablassert import declares, load
from translator_ingest.util.hf_releases import DATASET_RELEASE_PATTERN
from translator_ingest.util.tablassert import resolve_dataset_version_placeholder

INGEST_DIR = Path(__file__).parents[4] / "src" / "translator_ingest" / "ingests" / "mokg"

def test_mokg_declares_the_tablassert_kind():
    assert declares("mokg")
    config = load("mokg")
    assert config.version == "latest"
    assert config.nodes.path == "multiomics_kp_nodes.jsonl"
    assert config.edges.path == "multiomics_kp_edges.jsonl"
    assert config.rig.path == "multiomics_kp_rig.yaml"

def test_mokg_ships_no_transform_code_or_committed_rig():
    """The passthrough contract: no koza config, no manifest, no committed RIG, no rewrite hook."""
    present = {path.name for path in INGEST_DIR.iterdir()}
    assert "mokg.yaml" not in present, "MOKG must not declare a koza config alongside tablassert.yaml"
    assert "download.yaml" not in present, "MOKG's files are declared in tablassert.yaml"
    assert "mokg_rig.yaml" not in present, "The MOKG RIG is fetched from upstream, never committed"
    source_text = (INGEST_DIR / "mokg.py").read_text(encoding="utf-8")
    assert "koza" not in source_text, "MOKG must not run koza transform code"
    assert "rewrite_record" not in source_text, "MOKG performs no record rewriting"

def test_mokg_version_helper_delegates_to_the_shared_hf_helper():
    import inspect
    import translator_ingest.ingests.mokg.mokg as mokg

    assert mokg.DATASET_REPO_ID == "SkyeAv/multiomics-kp"
    assert callable(mokg.get_latest_version)
    # The helper itself is exercised offline for ordering, and live in tests/integration; here we
    # bind the contract: the ingest function delegates straight to the shared discovery helper.
    source_text = inspect.getsource(mokg)
    assert "get_latest_dataset_version" in source_text

def test_mokg_urls_resolve_against_a_dataset_version():
    """Every declared URL must be a complete template that a release version fills in."""
    config = load("mokg")
    for declared in (config.nodes, config.edges, config.rig):
        resolved = resolve_dataset_version_placeholder(declared.url, "1.23.5")
        assert "1.23.5" in resolved and "{" not in resolved
        assert "/resolve/main/1.23.5/" in resolved
    # And the shape of a release version is what the placeholder expects.
    assert DATASET_RELEASE_PATTERN.match("1.23.5")
