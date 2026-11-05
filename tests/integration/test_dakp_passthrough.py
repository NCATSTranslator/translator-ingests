"""Live integration coverage for the DAKP Tablassert passthrough.

Real network: resolves DAKP's latest dataset release through the ingest's own
`get_latest_version()` and confirms every declared URL (including the RIG) is retrievable, then
stages the real DAKP RIG through the shared staging script. No full-graph download happens here;
the full local run is performed manually per the plan's verification backlog.
"""

import requests

from src.docs.scripts import stage_rigs as stage_rigs_script
from translator_ingest.pipeline import get_ingest_module
from translator_ingest.util.tablassert import load, resolve_dataset_version_placeholder


def test_dakp_latest_version_matches_the_current_dataset_tree():
    version = get_ingest_module("dakp").get_latest_version()
    assert version and version.count(".") == 2
    # The dataset tree currently exposes exactly this release directory.
    tree_url = "https://huggingface.co/api/datasets/SkyeAv/drug-approvals-kp/tree/main"
    response = requests.get(tree_url, timeout=30)
    response.raise_for_status()
    directories = {entry["path"] for entry in response.json() if entry.get("type") == "directory"}
    assert version in directories


def test_every_declared_dakp_file_is_retrievable_for_the_latest_release():
    config = load("dakp")
    version = get_ingest_module("dakp").get_latest_version()
    for declared in (config.nodes, config.edges, config.rig):
        url = resolve_dataset_version_placeholder(declared.url, version)
        response = requests.head(url, timeout=30, allow_redirects=True)
        assert response.status_code == 200, f"Expected {url} to be retrievable for release {version}"


def test_staging_fetches_the_real_dakp_rig(tmp_path):
    target = tmp_path / "rigs"
    target.mkdir()
    staged = stage_rigs_script.stage_rigs(target)
    staged_names = {path.name for path in staged}
    assert "dakp_rig.yaml" in staged_names, "The live staging run must fetch the remote DAKP RIG"
    dakp_rig = target / "dakp_rig.yaml"
    assert dakp_rig.read_bytes().startswith(b"name:"), "The staged DAKP RIG must be the upstream YAML"
    # Every committed RIG is staged alongside the fetched one, in one run.
    assert staged_names >= {path.name for path in stage_rigs_script.committed_rig_paths()}
