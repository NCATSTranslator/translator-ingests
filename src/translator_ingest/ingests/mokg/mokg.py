"""MOKG: Multiomics Knowledge Provider, ingested as a Tablassert passthrough.

Upstream builds the complete KGX graph and the generated Resource Ingest Guide; this ingest
declares those files in `tablassert.yaml` and contributes no transform code. The Tablassert CLI
validates the graph, prunes non-compliant edges, and the pipeline publishes the result as-is,
snapshotting the RIG for the build. See `tablassert.yaml` for the declared upstream files.
"""

from translator_ingest.util.hf_releases import get_latest_dataset_version

DATASET_REPO_ID = "SkyeAv/multiomics-kp"

def get_latest_version() -> str:
    """Return the latest complete release of the upstream multiomics-kp dataset."""
    return get_latest_dataset_version(DATASET_REPO_ID)
