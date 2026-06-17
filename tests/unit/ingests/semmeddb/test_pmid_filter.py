"""Tests for the SemMedDB post-normalization PMID-checker filter (Rule B)."""

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from translator_ingest.ingests.semmeddb.filtering import pmid_filter
from translator_ingest.ingests.semmeddb.filtering.pmid_filter import (
    VERDICT_ARTIFACT_FILENAME,
    DropSet,
    _cap_by_recency,
    _pmid_number,
    filter_edge,
    filter_normalized_kgx,
    load_drop_set,
)
from translator_ingest.pipeline import get_filter_code_version, get_source_filter

DROP_SET: DropSet = {("A", "p", "B"): {"PMID:1"}, ("C", "p", "D"): {"PMID:3", "PMID:4"}}


@pytest.mark.parametrize(
    "edge, expected_publications",
    [
        # one rejected, one kept -> survives with the kept PMID
        ({"subject": "A", "predicate": "p", "object": "B", "publications": ["PMID:1", "PMID:2"]}, ["PMID:2"]),
        # no verdict for this edge -> untouched
        ({"subject": "X", "predicate": "p", "object": "Y", "publications": ["PMID:9"]}, ["PMID:9"]),
        # rejected PMID mixed with an unevaluated one (not in drop set) -> unevaluated kept
        ({"subject": "A", "predicate": "p", "object": "B", "publications": ["PMID:1", "PMID:7"]}, ["PMID:7"]),
    ],
)
def test_filter_edge_keeps_non_rejected(edge: dict[str, Any], expected_publications: list[str]) -> None:
    result = filter_edge(edge, DROP_SET)
    assert result is not None
    assert result["publications"] == expected_publications


def test_filter_edge_drops_when_all_rejected() -> None:
    # every publication is a "no" verdict -> the edge is dropped entirely
    edge = {"subject": "C", "predicate": "p", "object": "D", "publications": ["PMID:3", "PMID:4"]}
    assert filter_edge(edge, DROP_SET) is None


def test_filter_edge_prunes_supporting_studies() -> None:
    edge = {
        "subject": "A",
        "predicate": "p",
        "object": "B",
        "publications": ["PMID:1", "PMID:2"],
        "has_supporting_studies": {
            "study1": {
                "id": "study1",
                "category": ["biolink:Study"],
                "has_study_results": [
                    {"id": "r1", "xref": ["PMID:1"], "supporting_text": ["rejected"]},
                    {"id": "r2", "xref": ["PMID:2"], "supporting_text": ["kept"]},
                ],
            }
        },
    }
    result = filter_edge(edge, DROP_SET)
    assert result is not None
    kept_results = result["has_supporting_studies"]["study1"]["has_study_results"]
    assert [r["xref"] for r in kept_results] == [["PMID:2"]]


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")


def _write_artifact(path: Path, rows: list[tuple[str, str, str, str, str]]) -> None:
    # mirrors the LLM PMID-checker results parquet schema
    pl.DataFrame(
        rows, schema=["subject_curie", "predicate", "object_curie", "PMID", "support"], orient="row"
    ).write_parquet(path)


def test_load_drop_set_keeps_no_and_maybe(tmp_path: Path) -> None:
    artifact = tmp_path / VERDICT_ARTIFACT_FILENAME
    _write_artifact(
        artifact,
        [
            ("A", "p", "B", "PMID:1", "no"),
            ("A", "p", "B", "PMID:2", "yes"),
            ("C", "p", "D", "PMID:3", "no"),
            ("A", "p", "E", "PMID:5", "maybe"),
        ],
    )
    drop_set = load_drop_set(artifact)
    # both "no" and "maybe" are dropped; "yes" is kept
    assert drop_set == {
        ("A", "p", "B"): {"PMID:1"},
        ("C", "p", "D"): {"PMID:3"},
        ("A", "p", "E"): {"PMID:5"},
    }


def test_filter_normalized_kgx_end_to_end(tmp_path: Path) -> None:
    source_data_dir = tmp_path / "source_data"
    source_data_dir.mkdir()
    _write_artifact(
        source_data_dir / VERDICT_ARTIFACT_FILENAME,
        [
            ("A", "p", "B", "PMID:1", "no"),
            ("A", "p", "B", "PMID:2", "yes"),
            ("C", "p", "D", "PMID:3", "no"),
            ("C", "p", "D", "PMID:4", "no"),
            # PMID:5 has no verdict row (e.g. a paper the checker could not read) -> kept
        ],
    )

    nodes_file = tmp_path / "normalized_nodes.jsonl"
    edges_file = tmp_path / "normalized_edges.jsonl"
    _write_jsonl(nodes_file, [{"id": node_id} for node_id in ("A", "B", "C", "D", "E")])
    _write_jsonl(
        edges_file,
        [
            {
                "subject": "A", "predicate": "p", "object": "B",
                "publications": ["PMID:1", "PMID:2"],
                "has_supporting_studies": {
                    "study1": {
                        "id": "study1",
                        "category": ["biolink:Study"],
                        "has_study_results": [
                            {"id": "r1", "xref": ["PMID:1"], "supporting_text": ["rejected"]},
                            {"id": "r2", "xref": ["PMID:2"], "supporting_text": ["kept"]},
                        ],
                    }
                },
            },
            # edge with all-rejected publications -> dropped (orphans C and D)
            {"subject": "C", "predicate": "p", "object": "D", "publications": ["PMID:3", "PMID:4"]},
            # edge resting only on a no-abstract publication -> survives (Rule B)
            {"subject": "A", "predicate": "p", "object": "E", "publications": ["PMID:5"]},
        ],
    )

    stats = filter_normalized_kgx(nodes_file, edges_file, source_data_dir)

    assert stats == {
        "edges_before": 3,
        "edges_after": 2,
        "edges_dropped": 1,
        "publications_before": 5,
        "publications_after": 2,
        "publications_removed": 3,
        "nodes_before": 5,
        "nodes_after": 3,
        "nodes_pruned": 2,
    }

    surviving_edges = [json.loads(line) for line in edges_file.read_text().splitlines() if line.strip()]
    edge_keys = {(edge["subject"], edge["object"]) for edge in surviving_edges}
    assert edge_keys == {("A", "B"), ("A", "E")}

    surviving_nodes = {json.loads(line)["id"] for line in nodes_file.read_text().splitlines() if line.strip()}
    assert surviving_nodes == {"A", "B", "E"}

    # the rejected PMID:1 study-result is pruned; the kept PMID:2 one survives
    a_b_edge = next(edge for edge in surviving_edges if edge["object"] == "B")
    kept_xrefs = [r["xref"] for r in a_b_edge["has_supporting_studies"]["study1"]["has_study_results"]]
    assert kept_xrefs == [["PMID:2"]]


def test_filter_missing_artifact_raises(tmp_path: Path) -> None:
    (tmp_path / "source_data").mkdir()
    nodes_file = tmp_path / "normalized_nodes.jsonl"
    edges_file = tmp_path / "normalized_edges.jsonl"
    _write_jsonl(nodes_file, [{"id": "A"}])
    _write_jsonl(edges_file, [{"subject": "A", "predicate": "p", "object": "B", "publications": ["PMID:1"]}])
    with pytest.raises(FileNotFoundError):
        filter_normalized_kgx(nodes_file, edges_file, tmp_path / "source_data")


def test_source_filter_discovery() -> None:
    # semmeddb opts in via its filtering subpackage; a source without one returns None
    assert callable(get_source_filter("semmeddb"))
    assert get_source_filter("ctd") is None


def test_filter_code_version_present_only_for_filtered_source() -> None:
    semmeddb_version = get_filter_code_version("semmeddb")
    assert isinstance(semmeddb_version, str) and len(semmeddb_version) == 8
    assert get_filter_code_version("ctd") is None


@pytest.mark.parametrize(
    "publications, limit, expected",
    [
        # keep the 2 highest PMIDs (9, 5), preserving original order
        (["PMID:5", "PMID:1", "PMID:9", "PMID:3"], 2, ["PMID:5", "PMID:9"]),
        # under the limit -> untouched
        (["PMID:1", "PMID:2"], 5, ["PMID:1", "PMID:2"]),
        (["PMID:10", "PMID:2", "PMID:30"], 1, ["PMID:30"]),
    ],
)
def test_cap_by_recency(publications: list[str], limit: int, expected: list[str]) -> None:
    assert _cap_by_recency(publications, limit) == expected


def test_pmid_number_handles_non_numeric() -> None:
    assert _pmid_number("PMID:42") == 42
    assert _pmid_number("PMID:abc") == -1


def test_filter_edge_caps_oversized_edge(monkeypatch: pytest.MonkeyPatch) -> None:
    # with a low cap, an oversized edge keeps only the most recent PMIDs and prunes their studies
    monkeypatch.setattr(pmid_filter, "MAX_PUBLICATIONS_PER_EDGE", 2)
    edge = {
        "subject": "S", "predicate": "p", "object": "O",
        "publications": ["PMID:10", "PMID:30", "PMID:20"],
        "has_supporting_studies": {
            "study1": {
                "id": "study1",
                "has_study_results": [
                    {"id": "r10", "xref": ["PMID:10"], "supporting_text": ["old"]},
                    {"id": "r30", "xref": ["PMID:30"], "supporting_text": ["new"]},
                    {"id": "r20", "xref": ["PMID:20"], "supporting_text": ["mid"]},
                ],
            }
        },
    }
    result = filter_edge(edge, {})
    assert result is not None
    # PMID:10 (lowest) dropped; the two most recent kept in original order
    assert result["publications"] == ["PMID:30", "PMID:20"]
    kept_xrefs = [r["xref"] for r in result["has_supporting_studies"]["study1"]["has_study_results"]]
    assert kept_xrefs == [["PMID:30"], ["PMID:20"]]


def test_filter_edge_cap_can_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    # SEMMEDDB_UNCAPPED disables the cap, so an oversized edge keeps every publication
    monkeypatch.setattr(pmid_filter, "CAP_ENABLED", False)
    monkeypatch.setattr(pmid_filter, "MAX_PUBLICATIONS_PER_EDGE", 2)
    edge = {
        "subject": "S", "predicate": "p", "object": "O",
        "publications": ["PMID:10", "PMID:30", "PMID:20"],
    }
    result = filter_edge(edge, {})
    assert result is not None
    assert result["publications"] == ["PMID:10", "PMID:30", "PMID:20"]
