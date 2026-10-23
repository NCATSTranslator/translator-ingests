"""Live integration coverage for the MOKG Tablassert passthrough.

Real network, but never the graph itself: multiomics-kp release 3.1.3 is ~85 GB upstream, so
retrievability is proven with HEAD requests only. These tests resolve MOKG's latest dataset
release through the ingest's own `get_latest_version()`, confirm every declared URL (including
the RIG) is retrievable, then stage the real MOKG RIG (a small YAML) through the shared staging
script. The full local run is performed manually per the plan's verification backlog.
"""

import requests

from src.docs.scripts import stage_rigs as stage_rigs_script
from translator_ingest.pipeline import get_ingest_module
from translator_ingest.util.tablassert import load, resolve_dataset_version_placeholder

def test_mokg_latest_version_matches_the_current_dataset_tree():
    version = get_ingest_module("mokg").get_latest_version()
    assert version and version.count(".") == 2
    # The dataset tree currently exposes exactly this release directory.
    tree_url = "https://huggingface.co/api/datasets/SkyeAv/multiomics-kp/tree/main"
    response = requests.get(tree_url, timeout=30)
    response.raise_for_status()
    directories = {entry["path"] for entry in response.json() if entry.get("type") == "directory"}
    assert version in directories

def test_every_declared_mokg_file_is_retrievable_for_the_latest_release():
    """HEAD only: the nodes/edges payloads are far too large to download in a test."""
    config = load("mokg")
    version = get_ingest_module("mokg").get_latest_version()
    for declared in (config.nodes, config.edges, config.rig):
        url = resolve_dataset_version_placeholder(declared.url, version)
        response = requests.head(url, timeout=30, allow_redirects=True)
        assert response.status_code == 200, f"Expected {url} to be retrievable for release {version}"

def test_staging_fetches_the_real_mokg_rig(tmp_path):
    target = tmp_path / "rigs"
    target.mkdir()
    staged = stage_rigs_script.stage_rigs(target)
    staged_names = {path.name for path in staged}
    assert "mokg_rig.yaml" in staged_names, "The live staging run must fetch the remote MOKG RIG"
    mokg_rig = target / "mokg_rig.yaml"
    assert mokg_rig.read_bytes().startswith(b"name:"), "The staged MOKG RIG must be the upstream YAML"
    # Every committed RIG is staged alongside the fetched ones, in one run.
    assert staged_names >= {path.name for path in stage_rigs_script.committed_rig_paths()}
