"""Helpers for discovering the available releases of a HuggingFace dataset.

Release discovery requests the dataset's root tree from the HF API and keeps only complete
`X.Y.Z` release directories. Historical release directories are routinely removed from HF, so
callers must never assume a specific old release is still retrievable: discovery reports what
exists now, ordering is a pure numeric comparison, and every other pipeline stage pins exactly one
resolved version.
"""

import re
from collections.abc import Iterable

import requests

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

HF_API_ROOT = "https://huggingface.co/api/datasets"
# Complete strict-semver releases only: no leading zeros, exactly three numeric components.
DATASET_RELEASE_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30


def semver_key(version: str) -> tuple[int, ...]:
    """Return the numeric sort key for a complete `X.Y.Z` version string.

    Numeric component comparison keeps `1.23.5` ordered after `1.9.0`, which string comparison
    would get wrong.

    Raises:
        ValueError: when the version is not a complete three-component numeric release.
    """
    if not DATASET_RELEASE_PATTERN.match(version):
        raise ValueError(f"Not a complete X.Y.Z release version: {version!r}")
    return tuple(int(component) for component in version.split("."))


def latest_release(versions: Iterable[str]) -> str:
    """Return the highest complete `X.Y.Z` release from the given versions.

    Raises:
        ValueError: when no valid release version is present.
    """
    valid = sorted(set(versions), key=semver_key)
    if not valid:
        raise ValueError("No complete X.Y.Z release versions were available to choose from.")
    return valid[-1]


def list_dataset_release_directories(repo_id: str, timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS) -> list[str]:
    """List the complete `X.Y.Z` release directories currently in a HuggingFace dataset's root tree.

    Follows the API's pagination until every page has been read. HTTP errors propagate to the
    caller: a listing that cannot be retrieved is a hard failure, never an empty result.

    Args:
        repo_id: HuggingFace dataset repository id, e.g. `SkyeAv/drug-approvals-kp`.
        timeout_seconds: per-request timeout in seconds.

    Returns:
        The sorted list of complete release directory names.
    """
    url = f"{HF_API_ROOT}/{repo_id}/tree/main"
    releases: set[str] = set()
    while url:
        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        for entry in response.json():
            if entry.get("type") == "directory" and DATASET_RELEASE_PATTERN.match(entry.get("path", "")):
                releases.add(entry["path"])
        url = response.links.get("next", {}).get("url")
    return sorted(releases, key=semver_key)


def get_latest_dataset_version(repo_id: str) -> str:
    """Return the highest complete `X.Y.Z` release directory currently available for a dataset.

    Raises:
        ValueError: when the dataset exposes no complete release directory.
        requests.HTTPError: when the dataset tree cannot be retrieved.
    """
    releases = list_dataset_release_directories(repo_id)
    latest = latest_release(releases)
    logger.info(f"Latest release for {repo_id}: {latest} (available: {releases})")
    return latest
