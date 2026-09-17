"""Tests for the SemMedDB pre-normalization PMID-checker filter (Rule B)."""

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

import translator_ingest.util.storage.local as local_storage
from translator_ingest.ingests.semmeddb.filtering import pmid_filter
from translator_ingest.ingests.semmeddb.filtering.pmid_filter import (
    MIN_EDGE_COVERAGE,
    VERDICT_ARTIFACT_FILENAME,
    DropSet,
    _cap_by_recency,
    _pmid_number,
    filter_edge,
    filter_transform_kgx,
    load_covered_edge_keys,
    load_drop_set,
)
from translator_ingest.pipeline import (
    get_filter_code_version,
    get_normalization_input_kgx_files,
    get_source_filter,
    is_source_filter_complete,
)
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import (
    IngestFileName,
    IngestFileType,
    get_filter_directory,
    get_normalization_directory,
    get_transform_directory,
    get_versioned_file_paths,
)

DROP_SET: DropSet = {("A", "p", "B"): {"PMID:1"}, ("C", "p", "D"): {"PMID:3", "PMID:4"}}

# Version fields needed to resolve an on-disk directory tree without running the pipeline.
BASE_METADATA: dict[str, Any] = dict(
    source="testsrc",
    source_version="v1",
    transform_version="tv1",
    babel_version="b1",
    node_normalizer_version="nn1",
    normalization_code_version="nc1",
    normalization_conflation=True,
    normalization_strict=True,
)


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
    # mirrors the LLM PMID-checker results parquet schema, keyed by raw kg2.10.3 ids
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
            # a future no-abstract verdict must be treated as keep
            ("A", "p", "F", "PMID:6", "no_abstract"),
        ],
    )
    drop_set = load_drop_set(artifact)
    # both "no" and "maybe" are dropped; "yes" and "no_abstract" are kept
    assert drop_set == {
        ("A", "p", "B"): {"PMID:1"},
        ("C", "p", "D"): {"PMID:3"},
        ("A", "p", "E"): {"PMID:5"},
    }


def test_load_covered_edge_keys_includes_every_support_value(tmp_path: Path) -> None:
    artifact = tmp_path / VERDICT_ARTIFACT_FILENAME
    _write_artifact(
        artifact,
        [
            ("A", "p", "B", "PMID:1", "no"),
            ("A", "p", "B", "PMID:2", "yes"),
            ("C", "p", "D", "PMID:3", "no_abstract"),
        ],
    )
    # coverage is about the join, not the verdict, so a kept edge still counts as covered
    assert load_covered_edge_keys(artifact) == {("A", "p", "B"), ("C", "p", "D")}


def _write_end_to_end_inputs(tmp_path: Path, artifact_rows: list[tuple[str, str, str, str, str]]) -> tuple[Path, Path]:
    """Write the verdict artifact plus transform nodes/edges, returning (nodes_file, edges_file)."""
    source_data_dir = tmp_path / "source_data"
    source_data_dir.mkdir()
    _write_artifact(source_data_dir / VERDICT_ARTIFACT_FILENAME, artifact_rows)

    nodes_file = tmp_path / "nodes.jsonl"
    edges_file = tmp_path / "edges.jsonl"
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
    return nodes_file, edges_file


def test_filter_transform_kgx_end_to_end(tmp_path: Path) -> None:
    nodes_file, edges_file = _write_end_to_end_inputs(
        tmp_path,
        [
            ("A", "p", "B", "PMID:1", "no"),
            ("A", "p", "B", "PMID:2", "yes"),
            ("C", "p", "D", "PMID:3", "no"),
            ("C", "p", "D", "PMID:4", "no"),
            # PMID:5 has no verdict row (e.g. a paper the checker could not read) -> kept,
            # but the A-p-E edge key is covered so the coverage guard stays satisfied
            ("A", "p", "E", "PMID:8", "yes"),
        ],
    )
    nodes_before = nodes_file.read_bytes()
    edges_before = edges_file.read_bytes()

    output_nodes_file = tmp_path / "filtered_nodes.jsonl"
    output_edges_file = tmp_path / "filtered_edges.jsonl"
    stats = filter_transform_kgx(
        nodes_file=nodes_file,
        edges_file=edges_file,
        output_nodes_file=output_nodes_file,
        output_edges_file=output_edges_file,
        source_data_dir=tmp_path / "source_data",
    )

    assert stats == {
        "edges_before": 3,
        "edges_after": 2,
        "edges_dropped": 1,
        "edges_with_verdicts": 3,
        "edge_coverage": 1.0,
        "publications_before": 5,
        "publications_after": 2,
        "publications_removed": 3,
        "nodes_before": 5,
        "nodes_after": 3,
        "nodes_pruned": 2,
    }

    # the transform output is an input only, never rewritten
    assert nodes_file.read_bytes() == nodes_before
    assert edges_file.read_bytes() == edges_before

    surviving_edges = [json.loads(line) for line in output_edges_file.read_text().splitlines() if line.strip()]
    edge_keys = {(edge["subject"], edge["object"]) for edge in surviving_edges}
    assert edge_keys == {("A", "B"), ("A", "E")}

    surviving_nodes = {json.loads(line)["id"] for line in output_nodes_file.read_text().splitlines() if line.strip()}
    assert surviving_nodes == {"A", "B", "E"}

    # the rejected PMID:1 study-result is pruned; the kept PMID:2 one survives
    a_b_edge = next(edge for edge in surviving_edges if edge["object"] == "B")
    kept_xrefs = [r["xref"] for r in a_b_edge["has_supporting_studies"]["study1"]["has_study_results"]]
    assert kept_xrefs == [["PMID:2"]]


def test_coverage_guard_raises_when_artifact_keys_do_not_match(tmp_path: Path) -> None:
    # an artifact keyed on identifiers unrelated to the edges: every edge looks like "no verdict",
    # which silently means keep, so the guard has to turn this into a hard failure
    nodes_file, edges_file = _write_end_to_end_inputs(
        tmp_path,
        [
            ("UNRELATED:1", "p", "UNRELATED:2", "PMID:1", "no"),
            ("UNRELATED:3", "p", "UNRELATED:4", "PMID:3", "no"),
        ],
    )
    with pytest.raises(RuntimeError, match="verdict coverage"):
        filter_transform_kgx(
            nodes_file=nodes_file,
            edges_file=edges_file,
            output_nodes_file=tmp_path / "filtered_nodes.jsonl",
            output_edges_file=tmp_path / "filtered_edges.jsonl",
            source_data_dir=tmp_path / "source_data",
        )


def test_coverage_guard_message_names_rate_and_threshold(tmp_path: Path) -> None:
    nodes_file, edges_file = _write_end_to_end_inputs(
        tmp_path, [("UNRELATED:1", "p", "UNRELATED:2", "PMID:1", "no")]
    )
    with pytest.raises(RuntimeError) as raised:
        filter_transform_kgx(
            nodes_file=nodes_file,
            edges_file=edges_file,
            output_nodes_file=tmp_path / "filtered_nodes.jsonl",
            output_edges_file=tmp_path / "filtered_edges.jsonl",
            source_data_dir=tmp_path / "source_data",
        )
    message = str(raised.value)
    assert "0.0000" in message
    assert str(MIN_EDGE_COVERAGE) in message
    assert "0 of 3 edges" in message


def test_coverage_guard_passes_at_full_coverage(tmp_path: Path) -> None:
    nodes_file, edges_file = _write_end_to_end_inputs(
        tmp_path,
        [
            ("A", "p", "B", "PMID:1", "no"),
            ("C", "p", "D", "PMID:3", "yes"),
            ("A", "p", "E", "PMID:5", "yes"),
        ],
    )
    stats = filter_transform_kgx(
        nodes_file=nodes_file,
        edges_file=edges_file,
        output_nodes_file=tmp_path / "filtered_nodes.jsonl",
        output_edges_file=tmp_path / "filtered_edges.jsonl",
        source_data_dir=tmp_path / "source_data",
    )
    assert stats["edge_coverage"] == 1.0
    assert stats["edges_with_verdicts"] == stats["edges_before"] == 3


def test_filter_missing_artifact_raises(tmp_path: Path) -> None:
    (tmp_path / "source_data").mkdir()
    nodes_file = tmp_path / "nodes.jsonl"
    edges_file = tmp_path / "edges.jsonl"
    _write_jsonl(nodes_file, [{"id": "A"}])
    _write_jsonl(edges_file, [{"subject": "A", "predicate": "p", "object": "B", "publications": ["PMID:1"]}])
    with pytest.raises(FileNotFoundError):
        filter_transform_kgx(
            nodes_file=nodes_file,
            edges_file=edges_file,
            output_nodes_file=tmp_path / "filtered_nodes.jsonl",
            output_edges_file=tmp_path / "filtered_edges.jsonl",
            source_data_dir=tmp_path / "source_data",
        )


def test_source_filter_discovery() -> None:
    # semmeddb opts in via its filtering subpackage; a source without one returns None
    semmeddb_filter = get_source_filter("semmeddb")
    assert semmeddb_filter is not None
    assert semmeddb_filter.__name__ == "filter_transform_kgx"
    assert get_source_filter("ctd") is None


def test_filter_code_version_present_only_for_filtered_source() -> None:
    semmeddb_version = get_filter_code_version("semmeddb")
    assert isinstance(semmeddb_version, str) and len(semmeddb_version) == 8
    assert get_filter_code_version("ctd") is None


@pytest.fixture
def data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the storage module's data path at tmp_path so directory helpers resolve there."""
    data_path = tmp_path / "data"
    monkeypatch.setattr(local_storage, "INGESTS_DATA_PATH", data_path)
    return data_path


@pytest.mark.parametrize("filter_code_version", [None, "fc123456"])
def test_normalization_directory_nests_under_filter_dir_only_when_filtered(
    data_root: Path, filter_code_version: str | None
) -> None:
    pipeline_metadata = PipelineMetadata(**BASE_METADATA, filter_code_version=filter_code_version)
    transform_dir = get_transform_directory(pipeline_metadata)
    normalization_dir = get_normalization_directory(pipeline_metadata)

    assert transform_dir == data_root / "testsrc" / "v1" / "transform_tv1"
    expected_parent = transform_dir if filter_code_version is None else transform_dir / "filter_fc123456"
    assert normalization_dir.parent == expected_parent
    assert normalization_dir.name == f"normalization_{pipeline_metadata.get_composite_normalization_version()}"


def test_filtered_files_and_filter_metadata_resolve_into_filter_directory(data_root: Path) -> None:
    pipeline_metadata = PipelineMetadata(**BASE_METADATA, filter_code_version="fc123456")
    filter_dir = get_filter_directory(pipeline_metadata)
    assert filter_dir == get_transform_directory(pipeline_metadata) / "filter_fc123456"

    filtered_nodes, filtered_edges = get_versioned_file_paths(
        file_type=IngestFileType.FILTERED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    assert filtered_nodes == filter_dir / IngestFileName.FILTERED_NODES
    assert filtered_edges == filter_dir / IngestFileName.FILTERED_EDGES

    filter_metadata = get_versioned_file_paths(
        file_type=IngestFileType.FILTER_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    assert filter_metadata == filter_dir / IngestFileName.FILTER_METADATA


def test_normalization_reads_filtered_files_only_for_a_filtered_source(data_root: Path) -> None:
    transform_dir = get_transform_directory(PipelineMetadata(**BASE_METADATA))
    transform_dir.mkdir(parents=True)
    (transform_dir / "testsrc_nodes.jsonl").write_text("")
    (transform_dir / "testsrc_edges.jsonl").write_text("")
    # the filter subdirectory sits inside the transform directory; the transform file scan matches
    # on "nodes.jsonl"/"edges.jsonl" substrings, so it must not pick this directory up
    (transform_dir / "filter_fc123456").mkdir()

    unfiltered_inputs = get_normalization_input_kgx_files(PipelineMetadata(**BASE_METADATA))
    assert unfiltered_inputs == (transform_dir / "testsrc_nodes.jsonl", transform_dir / "testsrc_edges.jsonl")

    filtered_metadata = PipelineMetadata(**BASE_METADATA, filter_code_version="fc123456")
    filtered_inputs = get_normalization_input_kgx_files(filtered_metadata)
    assert filtered_inputs == get_versioned_file_paths(
        file_type=IngestFileType.FILTERED_KGX_FILES, pipeline_metadata=filtered_metadata
    )


def test_source_filter_complete_requires_filtered_files(data_root: Path) -> None:
    pipeline_metadata = PipelineMetadata(**BASE_METADATA, filter_code_version="fc123456")
    filter_dir = get_filter_directory(pipeline_metadata)
    filter_dir.mkdir(parents=True)
    metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.FILTER_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    metadata_path.write_text(json.dumps({"filter_code_version": "fc123456"}))

    # metadata alone is not enough: the filtered output has to be on disk too
    assert is_source_filter_complete(pipeline_metadata) is False

    filtered_nodes, filtered_edges = get_versioned_file_paths(
        file_type=IngestFileType.FILTERED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    filtered_nodes.write_text("")
    filtered_edges.write_text("")
    assert is_source_filter_complete(pipeline_metadata) is True

    # a source without a filter has nothing to do and is always complete
    assert is_source_filter_complete(PipelineMetadata(**BASE_METADATA)) is True


def test_source_filter_not_complete_when_filter_code_version_changed(data_root: Path) -> None:
    pipeline_metadata = PipelineMetadata(**BASE_METADATA, filter_code_version="fc123456")
    get_filter_directory(pipeline_metadata).mkdir(parents=True)
    metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.FILTER_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    metadata_path.write_text(json.dumps({"filter_code_version": "stale123"}))
    filtered_nodes, filtered_edges = get_versioned_file_paths(
        file_type=IngestFileType.FILTERED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    filtered_nodes.write_text("")
    filtered_edges.write_text("")
    assert is_source_filter_complete(pipeline_metadata) is False


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
