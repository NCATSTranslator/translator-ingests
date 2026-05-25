"""
Tests for the per-source fallback resolution that runs between the RUN stage
and the MERGE stage of the build orchestrator.

Pins the contract:
  * A source that succeeded this run is classified as 'fresh'.
  * A source that failed this run but has a LATEST_BUILD_FILE on disk
    (from a prior successful build) is classified as 'fallback'.
  * A source that failed this run AND has no LATEST_BUILD_FILE is
    classified as 'missing'. The orchestrator must hard-stop when any
    source is in this state.
  * The human-readable error message names every missing source and
    includes its captured error.
"""

import json
from pathlib import Path

from translator_ingest.util.run_build.fallback import (
    SourcePartition,
    format_missing_sources_error,
    partition_sources_after_run,
)
from translator_ingest.util.storage.local import IngestFileName


# ── helpers ─────────────────────────────────────────────────────────────────


def _write_latest_build(base_path: Path, source: str, *, source_version: str = "v1") -> None:
    """Create a minimal latest-build.json under base_path/{source}/.

    Only the file's existence matters for the partition logic; contents must be
    valid JSON so future readers (merge) don't blow up.
    """
    source_dir = base_path / source
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / IngestFileName.LATEST_BUILD_FILE).write_text(
        json.dumps({"source_version": source_version})
    )


# ── partition tests ─────────────────────────────────────────────────────────


def test_partition_all_succeeded_are_fresh(tmp_path):
    # baseline: every source succeeded this run, no fallback or missing
    partition = partition_sources_after_run(
        sources=["alliance", "bgee", "ctd"],
        failed_this_run=[],
        base_path=tmp_path,
    )
    assert partition.fresh == ["alliance", "bgee", "ctd"]
    assert partition.fallback == []
    assert partition.missing == []
    assert partition.has_missing is False
    assert partition.available == ["alliance", "bgee", "ctd"]


def test_partition_failed_with_prior_build_is_fallback(tmp_path):
    # source failed this run but has a LATEST_BUILD_FILE from a prior run
    _write_latest_build(tmp_path, "chembl")

    partition = partition_sources_after_run(
        sources=["alliance", "chembl"],
        failed_this_run=["chembl"],
        base_path=tmp_path,
    )
    assert partition.fresh == ["alliance"]
    assert partition.fallback == ["chembl"]
    assert partition.missing == []
    assert partition.has_missing is False


def test_partition_failed_without_prior_build_is_missing(tmp_path):
    # source failed and never had a successful build (no LATEST_BUILD_FILE)
    partition = partition_sources_after_run(
        sources=["alliance", "chembl"],
        failed_this_run=["chembl"],
        base_path=tmp_path,
    )
    assert partition.fresh == ["alliance"]
    assert partition.fallback == []
    assert partition.missing == ["chembl"]
    assert partition.has_missing is True


def test_partition_mixed_realistic(tmp_path):
    # production-shaped mix: most succeed, one falls back, one is missing
    _write_latest_build(tmp_path, "semmeddb")  # prior build exists
    # chembl has never built — no LATEST_BUILD_FILE

    partition = partition_sources_after_run(
        sources=["alliance", "ctd", "semmeddb", "chembl"],
        failed_this_run=["semmeddb", "chembl"],
        base_path=tmp_path,
    )
    assert partition.fresh == ["alliance", "ctd"]
    assert partition.fallback == ["semmeddb"]
    assert partition.missing == ["chembl"]
    assert partition.has_missing is True
    # available is fresh + fallback (everything mergeable, in declared order)
    assert partition.available == ["alliance", "ctd", "semmeddb"]


def test_partition_preserves_source_order_within_each_bucket(tmp_path):
    # callers (e.g. merge) sometimes depend on the declared order for
    # determinism — the partition must not reorder within each bucket
    _write_latest_build(tmp_path, "b")
    _write_latest_build(tmp_path, "d")

    partition = partition_sources_after_run(
        sources=["a", "b", "c", "d", "e"],
        failed_this_run=["b", "c", "d"],
        base_path=tmp_path,
    )
    assert partition.fresh == ["a", "e"]
    assert partition.fallback == ["b", "d"]
    assert partition.missing == ["c"]


def test_partition_failed_list_with_unknown_source_is_ignored(tmp_path):
    # a stale or wrong failed_this_run entry that's not in sources should
    # not show up in any bucket — the partition is anchored on `sources`
    partition = partition_sources_after_run(
        sources=["alliance"],
        failed_this_run=["alliance", "nonexistent_source"],
        base_path=tmp_path,
    )
    assert partition.fresh == []
    assert partition.fallback == []
    assert partition.missing == ["alliance"]


def test_partition_default_base_path_uses_ingests_data_path(monkeypatch, tmp_path):
    # when no base_path is passed, the function reads from the global
    # INGESTS_DATA_PATH — verify by patching the global and observing the
    # function picks up the override
    monkeypatch.setattr(
        "translator_ingest.util.run_build.fallback.INGESTS_DATA_PATH",
        str(tmp_path),
    )
    _write_latest_build(tmp_path, "ctd")

    partition = partition_sources_after_run(
        sources=["ctd"],
        failed_this_run=["ctd"],
    )
    assert partition.fallback == ["ctd"]
    assert partition.missing == []


# ── error message tests ─────────────────────────────────────────────────────


def test_format_missing_sources_error_names_every_missing_source():
    # the orchestrator's hard-stop log must call out each missing source by
    # name so the operator can immediately see what's broken
    partition = SourcePartition(
        fresh=["alliance"],
        fallback=["semmeddb"],
        missing=["chembl", "gtopdb"],
    )
    message = format_missing_sources_error(partition)
    assert "chembl" in message
    assert "gtopdb" in message
    # fresh/fallback sources are NOT in the error — only the missing ones
    assert "alliance" not in message
    assert "semmeddb" not in message


def test_format_missing_sources_error_includes_per_source_errors():
    # when the caller has per-source error context (e.g. from the RUN stage
    # summary), the formatted message should include it so the operator does
    # not have to cross-reference another file
    partition = SourcePartition(missing=["chembl"])
    errors = {"chembl": "pydantic ValidationError on ChemicalAffectsGeneAssociation"}
    message = format_missing_sources_error(partition, errors_by_source=errors)
    assert "chembl" in message
    assert "ValidationError" in message


def test_format_missing_sources_error_handles_no_error_context():
    # if no error context was captured (e.g. a worker crashed before producing
    # output), the message must still be readable rather than crashing
    partition = SourcePartition(missing=["chembl"])
    message = format_missing_sources_error(partition)
    assert "chembl" in message
    # caller didn't pass errors_by_source; message should not crash
    assert "no error captured" in message or "chembl" in message
