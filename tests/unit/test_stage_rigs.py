"""Tests for the shared RIG staging used by `make validate-rigs` and `docs-setup`.

These tests are offline: the remote-fetch boundary (`fetch_remote_rig`) is stubbed so staging
behavior is verified without network, and the real fetch is exercised live in
tests/integration/test_dakp_passthrough.py.
"""

import pytest
import requests

from src.docs.scripts import stage_rigs as stage_rigs_script
from src.docs.scripts.stage_rigs import stage_rigs

STUB_RIG_BYTES = b"name: stubbed remote rig\nsource_info: {}\n"


@pytest.fixture
def offline_fetch(monkeypatch):
    """Replace the network boundary with fixed bytes; live fetches are covered by integration."""
    fetched_sources = []
    def fake_fetch(source: str) -> bytes:
        fetched_sources.append(source)
        return STUB_RIG_BYTES
    monkeypatch.setattr(stage_rigs_script, "fetch_remote_rig", fake_fetch)
    return fetched_sources


def committed_rig_count() -> int:
    return len(stage_rigs_script.committed_rig_paths())


def remote_source_count() -> int:
    return len(stage_rigs_script.remote_rig_sources())


def test_staging_copies_committed_rigs_byte_for_byte(tmp_path, offline_fetch):
    target = tmp_path / "rigs"
    target.mkdir()
    staged = stage_rigs(target)
    committed = stage_rigs_script.committed_rig_paths()
    assert len(staged) == committed_rig_count() + remote_source_count()
    assert staged == sorted(target.iterdir())
    for source_rig in committed:
        assert (target / source_rig.name).read_bytes() == source_rig.read_bytes()
    # The remote set is exactly the Tablassert sources without a committed RIG (DAKP today).
    assert set(offline_fetch) == set(stage_rigs_script.remote_rig_sources())
    for source in offline_fetch:
        assert (target / f"{source}_rig.yaml").read_bytes() == STUB_RIG_BYTES


def test_staging_refuses_foreign_files_instead_of_wiping(tmp_path, offline_fetch):
    target = tmp_path / "rigs"
    target.mkdir()
    (target / "someone_elses_file.txt").write_text("do not delete me")
    with pytest.raises(Exception, match="does not manage"):
        stage_rigs(target)
    assert (target / "someone_elses_file.txt").read_text() == "do not delete me"


def test_staging_refuses_symlinks_among_managed_names(tmp_path, offline_fetch):
    """A symlink named like a staged file must be refused, not followed by write_bytes."""
    target = tmp_path / "rigs"
    target.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("do not clobber")
    (target / "dakp_rig.yaml").symlink_to(outside)
    with pytest.raises(Exception, match="does not manage"):
        stage_rigs(target)
    assert outside.read_text() == "do not clobber"


def test_staging_refuses_a_non_directory(tmp_path, offline_fetch):
    not_a_dir = tmp_path / "file.yaml"
    not_a_dir.write_text("x")
    with pytest.raises(Exception, match="not a directory"):
        stage_rigs(not_a_dir)


def test_staging_fails_loudly_when_a_remote_rig_cannot_be_fetched(tmp_path, monkeypatch):
    target = tmp_path / "rigs"
    target.mkdir()

    def failing_fetch(source: str) -> bytes:
        raise requests.HTTPError(f"upstream unavailable for {source}")

    monkeypatch.setattr(stage_rigs_script, "fetch_remote_rig", failing_fetch)
    with pytest.raises(requests.HTTPError, match="upstream unavailable"):
        stage_rigs(target)
    # The failure propagates: a source whose RIG cannot be fetched is never silently dropped.


def test_expected_names_cover_every_committed_and_remote_rig():
    expected = stage_rigs_script.expected_staged_names()
    assert {path.name for path in stage_rigs_script.committed_rig_paths()} <= expected
    # DAKP is a remote (Tablassert) source: it is staged even though nothing is committed.
    assert "dakp_rig.yaml" in expected
