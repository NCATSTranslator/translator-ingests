"""Tests for the SemMedDB PMID-checker verdict re-key script.

The toy fixtures below model the three cases that matter on the real data:

- conflation: ``UniProtKB:P01137`` and ``NCBIGene:7040`` both normalize to ``NCBIGene:7040``,
  and ``UMLS:C0011847`` and ``MONDO:0005015`` both normalize to ``MONDO:0005015``, so one
  verdict fans out to four raw rows;
- passthrough: a verdict on ids absent from the normalization map survives unchanged;
- drift: the normalized-keyed verdicts miss raw edges that the re-keyed ones reach.
"""

import doctest
import gzip
import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from translator_ingest.ingests.semmeddb.analysis import rekey_pmid_verdicts
from translator_ingest.ingests.semmeddb.analysis.rekey_pmid_verdicts import (
    OUTPUT_COLUMNS,
    VERDICT_COLUMNS,
    VERDICT_KEY_COLUMNS,
    compute_coverage,
    iter_edge_publication_keys,
    load_edge_keys,
    load_inverse_normalization_map,
    main,
    parse_args,
    rekey_stats,
    rekey_verdicts,
    run,
    sha256_file,
)

NORMALIZATION_MAP: dict[str, list[str]] = {
    "NCBIGene:7040": ["NCBIGene:7040"],
    "UniProtKB:P01137": ["NCBIGene:7040"],
    "MONDO:0005015": ["MONDO:0005015"],
    "UMLS:C0011847": ["MONDO:0005015"],
}

VERDICT_ROWS: dict[str, list[str]] = {
    "subject_curie": ["NCBIGene:7040", "CHEBI:9999"],
    "predicate": ["biolink:affects", "biolink:treats"],
    "object_curie": ["MONDO:0005015", "UMLS:C0000001"],
    "PMID": ["PMID:1", "PMID:2"],
    "support": ["no", "yes"],
}

EDGE_RECORDS: list[dict[str, Any]] = [
    # raw ids that normalize onto the verdict's key; unreachable without the re-key
    {
        "subject": "UniProtKB:P01137",
        "predicate": "biolink:affects",
        "object": "UMLS:C0011847",
        "publications": ["PMID:1"],
    },
    # raw ids that happen to equal the normalized ones; reachable either way
    {
        "subject": "NCBIGene:7040",
        "predicate": "biolink:affects",
        "object": "MONDO:0005015",
        "publications": ["PMID:1"],
    },
    # not in the map at all, and PMID:3 was never evaluated by the checker
    {
        "subject": "CHEBI:9999",
        "predicate": "biolink:treats",
        "object": "UMLS:C0000001",
        "publications": ["PMID:2", "PMID:3"],
    },
]


@pytest.fixture
def normalization_map_file(tmp_path: Path) -> Path:
    """Write the toy normalization map in the CATRAX file shape."""
    path = tmp_path / "normalization_map.json"
    path.write_text(json.dumps({"normalization_map": NORMALIZATION_MAP}), encoding="utf-8")
    return path


@pytest.fixture
def verdicts_parquet(tmp_path: Path) -> Path:
    """Write the toy normalized-keyed verdict parquet."""
    path = tmp_path / "results.parquet"
    pl.DataFrame(VERDICT_ROWS).write_parquet(path)
    return path


@pytest.fixture
def edges_file(tmp_path: Path) -> Path:
    """Write the toy raw transform-stage edges JSONL."""
    path = tmp_path / "semmeddb_edges.jsonl"
    path.write_text("".join(f"{json.dumps(record)}\n" for record in EDGE_RECORDS), encoding="utf-8")
    return path


def _rekeyed(normalization_map_file: Path, verdicts_parquet: Path) -> pl.DataFrame:
    """Run the re-key over the toy fixtures and collect the result."""
    inverse_map = load_inverse_normalization_map(normalization_map_file)
    verdicts = pl.scan_parquet(verdicts_parquet).select(VERDICT_COLUMNS)
    return rekey_verdicts(verdicts, inverse_map).collect()


def test_module_doctests_pass() -> None:
    results = doctest.testmod(rekey_pmid_verdicts)
    assert results.failed == 0
    assert results.attempted > 0


def test_load_inverse_normalization_map_inverts_and_explodes(normalization_map_file: Path) -> None:
    inverse_map = load_inverse_normalization_map(normalization_map_file)
    assert inverse_map.columns == ["normalized_curie", "raw_curie"]
    assert inverse_map.rows() == [
        ("MONDO:0005015", "MONDO:0005015"),
        ("MONDO:0005015", "UMLS:C0011847"),
        ("NCBIGene:7040", "NCBIGene:7040"),
        ("NCBIGene:7040", "UniProtKB:P01137"),
    ]


def test_load_inverse_normalization_map_explodes_multiple_normalized_ids(tmp_path: Path) -> None:
    # a raw id that maps to two normalized ids must produce one row per pair
    path = tmp_path / "split_map.json"
    path.write_text(json.dumps({"normalization_map": {"UMLS:C1": ["MONDO:1", "MONDO:2"]}}), encoding="utf-8")
    assert load_inverse_normalization_map(path).rows() == [("MONDO:1", "UMLS:C1"), ("MONDO:2", "UMLS:C1")]


def test_rekey_verdicts_output_shape(normalization_map_file: Path, verdicts_parquet: Path) -> None:
    rekeyed = _rekeyed(normalization_map_file, verdicts_parquet)
    assert rekeyed.columns == OUTPUT_COLUMNS
    # 1 verdict fanning out to 2 subjects x 2 objects, plus 1 passthrough verdict
    assert rekeyed.height == 5


@pytest.mark.parametrize(
    "subject_curie, object_curie, support, subject_normalized, object_normalized",
    [
        ("NCBIGene:7040", "MONDO:0005015", "no", "NCBIGene:7040", "MONDO:0005015"),
        ("NCBIGene:7040", "UMLS:C0011847", "no", "NCBIGene:7040", "MONDO:0005015"),
        ("UniProtKB:P01137", "MONDO:0005015", "no", "NCBIGene:7040", "MONDO:0005015"),
        ("UniProtKB:P01137", "UMLS:C0011847", "no", "NCBIGene:7040", "MONDO:0005015"),
        # neither id is in the map: carried through unchanged, normalized columns echo the input
        ("CHEBI:9999", "UMLS:C0000001", "yes", "CHEBI:9999", "UMLS:C0000001"),
    ],
)
def test_rekey_verdicts_fan_out_and_passthrough(
    normalization_map_file: Path,
    verdicts_parquet: Path,
    subject_curie: str,
    object_curie: str,
    support: str,
    subject_normalized: str,
    object_normalized: str,
) -> None:
    rekeyed = _rekeyed(normalization_map_file, verdicts_parquet)
    match = rekeyed.filter(
        (pl.col("subject_curie") == subject_curie) & (pl.col("object_curie") == object_curie)
    )
    assert match.height == 1
    row = match.row(0, named=True)
    assert row["support"] == support
    assert row["subject_curie_normalized"] == subject_normalized
    assert row["object_curie_normalized"] == object_normalized


def test_rekey_verdicts_drops_duplicate_rows(tmp_path: Path, normalization_map_file: Path) -> None:
    # the same verdict twice in the input must collapse to one output row per raw pair
    path = tmp_path / "duplicated.parquet"
    pl.DataFrame(
        {
            "subject_curie": ["NCBIGene:7040", "NCBIGene:7040"],
            "predicate": ["biolink:affects", "biolink:affects"],
            "object_curie": ["MONDO:0005015", "MONDO:0005015"],
            "PMID": ["PMID:1", "PMID:1"],
            "support": ["no", "no"],
        }
    ).write_parquet(path)
    assert _rekeyed(normalization_map_file, path).height == 4


def test_rekey_stats_counts_renames(normalization_map_file: Path, verdicts_parquet: Path) -> None:
    before = pl.scan_parquet(verdicts_parquet).select(VERDICT_COLUMNS)
    after = _rekeyed(normalization_map_file, verdicts_parquet)
    assert rekey_stats(before, after) == {
        "input_rows": 2,
        "output_rows": 5,
        "fan_out_rows_added": 3,
        # 2 of the 4 fanned-out rows carry a subject id different from the normalized one
        "subjects_renamed": 2,
        "objects_renamed": 2,
    }


@pytest.mark.parametrize("chunk_size, expected_chunk_heights", [(1, [1, 1, 2]), (2, [2, 2]), (100, [4])])
def test_iter_edge_publication_keys_chunking(
    edges_file: Path, chunk_size: int, expected_chunk_heights: list[int]
) -> None:
    chunks = list(iter_edge_publication_keys(edges_file, chunk_size=chunk_size))
    assert [chunk.height for chunk in chunks] == expected_chunk_heights
    combined = pl.concat(chunks)
    assert combined.columns == ["subject", "predicate", "object", "PMID"]
    assert combined.rows() == [
        ("UniProtKB:P01137", "biolink:affects", "UMLS:C0011847", "PMID:1"),
        ("NCBIGene:7040", "biolink:affects", "MONDO:0005015", "PMID:1"),
        ("CHEBI:9999", "biolink:treats", "UMLS:C0000001", "PMID:2"),
        ("CHEBI:9999", "biolink:treats", "UMLS:C0000001", "PMID:3"),
    ]


def test_iter_edge_publication_keys_reads_gzip(tmp_path: Path) -> None:
    path = tmp_path / "edges.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write("".join(f"{json.dumps(record)}\n" for record in EDGE_RECORDS))
    assert load_edge_keys(path).height == 4


def test_load_edge_keys_on_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    edge_keys = load_edge_keys(path)
    assert edge_keys.height == 0
    assert edge_keys.columns == ["subject", "predicate", "object", "PMID"]


def test_compute_coverage_before_and_after_rekey(
    normalization_map_file: Path, verdicts_parquet: Path, edges_file: Path
) -> None:
    edge_keys = load_edge_keys(edges_file)
    before = compute_coverage(edge_keys, pl.read_parquet(verdicts_parquet, columns=VERDICT_KEY_COLUMNS))
    after = compute_coverage(
        edge_keys, _rekeyed(normalization_map_file, verdicts_parquet).select(VERDICT_KEY_COLUMNS)
    )

    # normalized keys reach only the edge whose raw ids happen to equal the normalized ones
    assert before == {
        "pmid_pairs_total": 4,
        "pmid_pairs_with_verdict": 2,
        "pmid_coverage": 0.5,
        "edges_total": 3,
        "edges_with_verdict": 2,
        "edge_coverage": round(2 / 3, 6),
    }
    # re-keyed verdicts reach every edge; PMID:3 has no verdict at all, so pair coverage stays < 1
    assert after == {
        "pmid_pairs_total": 4,
        "pmid_pairs_with_verdict": 3,
        "pmid_coverage": 0.75,
        "edges_total": 3,
        "edges_with_verdict": 3,
        "edge_coverage": 1.0,
    }


@pytest.mark.parametrize("verdict_pmid", ["PMID:1", "1", " pmid:1 "])
def test_compute_coverage_normalizes_pmid_representations(verdict_pmid: str) -> None:
    edge_keys = pl.DataFrame(
        {"subject": ["A"], "predicate": ["p"], "object": ["B"], "PMID": ["PMID:1"]}
    )
    verdict_keys = pl.DataFrame(
        {"subject_curie": ["A"], "predicate": ["p"], "object_curie": ["B"], "PMID": [verdict_pmid]}
    )
    assert compute_coverage(edge_keys, verdict_keys)["pmid_coverage"] == 1.0


def test_compute_coverage_on_empty_edge_table() -> None:
    edge_keys = pl.DataFrame(schema={"subject": pl.String(), "predicate": pl.String(),
                                     "object": pl.String(), "PMID": pl.String()})
    verdict_keys = pl.DataFrame(
        {"subject_curie": ["A"], "predicate": ["p"], "object_curie": ["B"], "PMID": ["PMID:1"]}
    )
    coverage = compute_coverage(edge_keys, verdict_keys)
    assert coverage["pmid_coverage"] == 0.0
    assert coverage["edge_coverage"] == 0.0


def test_run_writes_parquet_and_manifest(
    tmp_path: Path, normalization_map_file: Path, verdicts_parquet: Path, edges_file: Path
) -> None:
    out_parquet = tmp_path / "out" / "results_raw_keyed.parquet"
    out_manifest = tmp_path / "out" / "manifest.json"
    args = parse_args(
        [
            "--results-parquet", str(verdicts_parquet),
            "--normalization-map", str(normalization_map_file),
            "--transform-edges", str(edges_file),
            "--out-parquet", str(out_parquet),
            "--out-manifest", str(out_manifest),
        ]
    )
    manifest = run(args)

    written = pl.read_parquet(out_parquet)
    assert written.columns == OUTPUT_COLUMNS
    assert written.height == 5

    assert manifest == json.loads(out_manifest.read_text(encoding="utf-8"))
    assert manifest["rekey"]["output_rows"] == 5
    assert manifest["coverage_before"]["edge_coverage"] == round(2 / 3, 6)
    assert manifest["coverage_after"]["edge_coverage"] == 1.0
    assert manifest["transform_edges"] == str(edges_file)
    assert manifest["generated_at"].endswith("+00:00")
    assert manifest["notes"]

    for key, path in [
        ("results_parquet", verdicts_parquet),
        ("normalization_map", normalization_map_file),
        ("out_parquet", out_parquet),
    ]:
        assert manifest[key]["path"] == str(path)
        assert manifest[key]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_main_without_transform_edges_leaves_coverage_null(
    tmp_path: Path, normalization_map_file: Path, verdicts_parquet: Path
) -> None:
    out_parquet = tmp_path / "results_raw_keyed.parquet"
    out_manifest = tmp_path / "manifest.json"
    exit_code = main(
        [
            "--results-parquet", str(verdicts_parquet),
            "--normalization-map", str(normalization_map_file),
            "--out-parquet", str(out_parquet),
            "--out-manifest", str(out_manifest),
        ]
    )
    assert exit_code == 0

    manifest = json.loads(out_manifest.read_text(encoding="utf-8"))
    assert manifest["coverage_before"] is None
    assert manifest["coverage_after"] is None
    assert manifest["transform_edges"] is None
    assert manifest["rekey"] == {
        "input_rows": 2,
        "output_rows": 5,
        "fan_out_rows_added": 3,
        "subjects_renamed": 2,
        "objects_renamed": 2,
    }


def test_sha256_file_matches_hashlib(tmp_path: Path) -> None:
    path = tmp_path / "blob.bin"
    path.write_bytes(b"semmeddb" * 1000)
    assert sha256_file(path) == hashlib.sha256(path.read_bytes()).hexdigest()
