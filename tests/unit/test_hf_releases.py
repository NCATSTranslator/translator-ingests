"""Tests for the HF dataset release discovery helpers (offline: ordering and shape only).

The live HuggingFace discovery run is covered by tests/integration.
"""

import pytest

from translator_ingest.util import hf_releases


@pytest.mark.parametrize("version", ["1.23.5", "0.0.1", "10.20.30"])
def test_semver_key_accepts_complete_releases(version):
    assert hf_releases.semver_key(version) == tuple(int(part) for part in version.split("."))


@pytest.mark.parametrize("version", ["1.23", "v1.2.3", "1.2.3.4", "latest", "", "01.2.3"])
def test_semver_key_rejects_incomplete_or_decorated_versions(version):
    with pytest.raises(ValueError, match="X.Y.Z"):
        hf_releases.semver_key(version)


def test_latest_release_orders_numerically_not_lexically():
    # Lexical ordering would pick 1.9.0 here; 1.23.5 is the true latest.
    assert hf_releases.latest_release(["1.9.0", "1.23.5", "1.10.0"]) == "1.23.5"


def test_latest_release_ignores_duplicates_and_reports_none_when_empty():
    assert hf_releases.latest_release(["2.0.0", "2.0.0", "1.0.0"]) == "2.0.0"
    with pytest.raises(ValueError, match="No complete"):
        hf_releases.latest_release([])


def test_latest_release_rejects_malformed_entries():
    with pytest.raises(ValueError, match="X.Y.Z"):
        hf_releases.latest_release(["not-a-version", "also-bad"])
