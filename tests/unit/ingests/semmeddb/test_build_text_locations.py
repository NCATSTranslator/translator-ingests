"""Tests for the SemMedDB text-location build script, its KG2.10.3 converter mirror and the transform.

The fixtures, and what each PMID in them exercises, are in ``text_location_fixtures.py``.
"""

import doctest
import hashlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import polars as pl
import pytest
from biolink_model.datamodel.pydanticmodel_v2 import Association
from koza.runner import KozaRunner, KozaTransformHooks

from tests.unit.ingests import MockKozaWriter
from tests.unit.ingests.semmeddb.text_location_fixtures import (
    CHUNK_A_ROWS,
    CHUNK_B,
    CHUNK_B_ROWS,
    EXPECTED_ROWS,
    KEPT_PUBLICATION_COUNT,
    KG2_RECORDS,
    OUTPUT_COLUMNS_CONTRACT,
    PADDING_PMIDS,
    R16_KG2_ID,
    R20_KG2_ID,
    R22_KG2_ID,
    R26_KG2_ID,
    R28_KG2_ID,
    R29_KG2_ID,
    R30_KG2_ID,
    R30_SECOND_RECORD_SCORES,
    R32_KG2_ID,
    S1,
    S15,
    S16_ABSTRACT,
    S16_TITLE,
    S18,
    S2,
    S20,
    S21,
    S22,
    S24,
    S25,
    S26,
    S27,
    S28,
    S29,
    S30,
    S31,
    S32,
    SKIPPED_RECORDS,
    Spans,
    StudyLocation,
    TOTAL_ROWS,
    TREATS,
    argv_for,
    chunk_first_id,
    chunk_rows,
    evidence,
    kg2_id_of,
    full_inputs,
    leftover_build_files,
    output_rows,
    kg2_record,
    literal,
    span,
    write_chunk,
    write_inputs,
)
from translator_ingest.ingests.semmeddb.analysis import build_text_locations, text_location_report, work_dir
from translator_ingest.ingests.semmeddb.analysis.text_location_report import REPO_ROOT
from translator_ingest.ingests.semmeddb.analysis.build_text_locations import (
    main,
    span_matches,
)
from translator_ingest.ingests.semmeddb.analysis.semmeddb_export import (
    PREDICATION_ID_RANGE_SIZE,
)
from translator_ingest.ingests.semmeddb.semmeddb import (
    PREDICATE_REMAP,
    VERDICT_ARTIFACT_COLUMNS,
    VERDICT_ARTIFACT_FILENAME,
    on_begin_filter_edges,
    transform_semmeddb_edge,
)
from translator_ingest.ingests.semmeddb.text_locations import (
    TEXT_LOCATION_ARTIFACT_FILENAME,
    evidence_fingerprint,
)


@pytest.mark.parametrize("module", [build_text_locations, text_location_report, work_dir])
def test_module_doctests_pass(module: ModuleType) -> None:
    """The doctests of the pure helpers pass."""
    results = doctest.testmod(module)
    assert results.failed == 0
    assert results.attempted > 0


@pytest.mark.parametrize(
    "sentence, start, end, text, matches",
    [
        ("Aspirin treats pain.", 0, 7, "Aspirin", True),
        ("Aspirin treats pain.", 1, 8, "Aspirin", False),
        ("Aspirin treats pain.", -4, 0, "ain.", False),
        ("Aspirin treats pain.", 15, 40, "pain.", False),
        ("Aspirin treats pain.", 3, 3, "", False),
        ("Ä 😀 β-blocker", 4, 13, "β-blocker", True),
        (None, 0, 1, "a", False),
        ("abc", None, 1, "a", False),
    ],
)
def test_span_matches(sentence: str | None, start: int | None, end: int, text: str, matches: bool) -> None:
    """The span check slices by code point, like Python, and rejects out-of-range or empty spans."""
    frame = pl.DataFrame(
        {"sentence": [sentence], "start": [start], "end": [end], "text": [text]},
        schema={"sentence": pl.String(), "start": pl.Int64(), "end": pl.Int64(), "text": pl.String()},
    )
    assert frame.select(span_matches("sentence", "start", "end", "text")).item() is matches


# argparse exits with this status on a bad command line.
ARGPARSE_ERROR_STATUS = 2


@pytest.mark.parametrize(
    "damage, message",
    [
        (lambda directory: (directory / "chunk_002.status").write_text("1\n"), "status not 0"),
        (lambda directory: (directory / "chunk_002.status").unlink(), "chunk without status"),
        (lambda directory: (directory / "chunk_004.tsv.zst.part").write_bytes(b""), "partial files left"),
        (lambda directory: (directory / "chunk_002.rows").unlink(), "chunk without .rows"),
        (lambda directory: write_chunk(directory, "chunk_14", []), "chunk names not of the form chunk_NNN"),
        (lambda directory: write_chunk(directory, "chunk_0014", []), "chunk names not of the form chunk_NNN"),
        (
            lambda directory: write_chunk(directory, "chunk_\u0660\u0661\u0664", []),
            "chunk names not of the form chunk_NNN",
        ),
        (
            lambda directory: write_chunk(directory, "chunk_\uff10\uff11\uff14", []),
            "chunk names not of the form chunk_NNN",
        ),
        (
            lambda directory: shutil.move(directory / "chunk_002.tsv.zst", directory / "elsewhere"),
            "status without chunk",
        ),
    ],
)
def test_incomplete_export_stops_the_build(
    damage: Callable[[Path], object], message: str, inputs: dict[str, Path]
) -> None:
    """A failed, missing or partial export range stops the build before anything is written."""
    damage(inputs["semmeddb_dir"])
    with pytest.raises(ValueError, match=message):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["work_dir"].exists()
    assert not inputs["out_parquet"].parent.exists()


@pytest.mark.parametrize(
    "chunk_002_rows, message",
    [
        (chunk_rows(CHUNK_B, chunk_first_id(7)), "PREDICATION_IDs outside"),
        ([*CHUNK_B_ROWS, CHUNK_B_ROWS[0]], "1 duplicate"),
    ],
)
def test_chunk_ids_must_fit_the_chunk_name(chunk_002_rows: list[list[str]], message: str, tmp_path: Path) -> None:
    """A chunk holding ids from another chunk's range, or one id twice, stops the build."""
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS, "chunk_002": chunk_002_rows}, KG2_RECORDS)
    with pytest.raises(ValueError, match=f"chunk_002.tsv.zst: .*{message}"):
        main(argv_for(paths, len(CHUNK_A_ROWS) + len(chunk_002_rows)))


def test_row_total_other_than_the_source_count_stops_the_build(inputs: dict[str, Path]) -> None:
    """The .rows files must sum to --expected-rows, the source table's COUNT(*)."""
    with pytest.raises(ValueError, match=f"expected {TOTAL_ROWS + 1:,}"):
        main(argv_for(inputs, TOTAL_ROWS + 1))


def test_output_rows_schema_and_order(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """The output holds exactly the expected rows and dtypes, strictly ascending on the key."""
    output = pl.read_parquet(inputs["out_parquet"])
    assert dict(output.schema) == OUTPUT_COLUMNS_CONTRACT
    assert set(output.rows()) == EXPECTED_ROWS
    keys = output.select("kg2_id", "evidence_fingerprint").rows()
    assert keys == sorted(set(keys))
    assert built["output"]["rows"] == len(EXPECTED_ROWS)
    assert built["output"]["sha256"] == hashlib.sha256(inputs["out_parquet"].read_bytes()).hexdigest()


def spans_of(inputs: dict[str, Path], kg2_id: tuple[str, str, str], pmid: str, sentence: str) -> list[Spans]:
    """Return the artifact's spans for one kg2_id and one PMID's evidence."""
    key = (kg2_id_of(*kg2_id), evidence_fingerprint(pmid, evidence(sentence)))
    return [(row[2], row[3], row[4], row[5]) for row in output_rows(inputs) if (row[0], row[1]) == key]


def test_gene_edge_gets_the_gene_only_row_not_the_cui_led_one(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """PMID 18: a ``CUI|gene`` field makes no gene relation, so NCBIGene:5594 gets "ERK", not "p38 MAPK"."""
    gene_edge = ("NCBIGene:5594", "coexists_with", "UMLS:C0164852")
    assert spans_of(inputs, gene_edge, "PMID:18", S18) == [(*span(S18, "ERK"), *span(S18, "JNK"))]
    cui_edge = ("UMLS:C0164786", "coexists_with", "UMLS:C0164852")
    assert spans_of(inputs, cui_edge, "PMID:18", S18) == [(*span(S18, "p38 MAPK"), *span(S18, "JNK"))]
    assert built["semmeddb"]["rows_without_relation"] == 1 + len(PADDING_PMIDS)


@pytest.mark.usefixtures("built")
def test_scores_pick_the_row_kg2_kept(inputs: dict[str, Path]) -> None:
    """PMID 15: the earlier "Tau" row is scored 694, the record kept 1000/1000, so the later one wins."""
    tau_edge = ("UMLS:C0248337", "interacts_with", "UMLS:C0041365")
    assert S15.index("Tau") < S15.index("Tau", 1) == 23
    assert spans_of(inputs, tau_edge, "PMID:15", S15) == [(23, 26, *span(S15, "tubulin"))]


def test_scores_matching_no_row_give_no_location(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """PMID 24: the record kept scores no SemMedDB row has, so it gets no row."""
    caffeine_edge = ("UMLS:C0006644", "affects", "UMLS:C0002022")
    assert spans_of(inputs, caffeine_edge, "PMID:24", S24) == []
    assert built["matching"]["pairs_lost_to_scores"] == 1


def test_negated_rows_are_never_evidence(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """KG2 dropped negated edges: a NEG_-only pair gets no row, and a positive twin beats an earlier NEG_."""
    assert spans_of(inputs, ("UMLS:C0004057", "treats", "UMLS:C0018099"), "PMID:21", S21) == []
    positive = (*span(S25, "aspirin"), *span(S25, "stroke"))
    assert span(S25, "Aspirin") < span(S25, "aspirin")
    assert spans_of(inputs, ("UMLS:C0004057", "treats", "UMLS:C0038454"), "PMID:25", S25) == [positive]
    assert built["semmeddb"]["rows_negated_dropped"] == 2


def test_one_kg2_id_keeps_one_row_per_evidence(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """Two records under one kg2_id: different evidence gives two rows, the same evidence one."""
    assert len(spans_of(inputs, R16_KG2_ID, "PMID:16", S16_TITLE)) == 1
    assert len(spans_of(inputs, R16_KG2_ID, "PMID:16", S16_ABSTRACT)) == 1
    assert len(spans_of(inputs, R20_KG2_ID, "PMID:20", S20)) == 1
    assert built["matching"]["pairs_sharing_evidence_merged"] == 1


@pytest.mark.usefixtures("built")
def test_only_the_covering_group_supplies_spans(inputs: dict[str, Path]) -> None:
    """PMID 26: the plain-CUI group has the earlier span but no row at PMID 27, so it is not the kept group."""
    assert span(S26, "Nicotine") < span(S26, "nicotine")
    assert spans_of(inputs, R26_KG2_ID, "PMID:26", S26) == [(*span(S26, "nicotine"), *span(S26, "blood pressure"))]
    assert spans_of(inputs, R26_KG2_ID, "PMID:27", S27) == [(*span(S27, "Nicotine"), *span(S27, "blood pressure"))]


def test_covering_groups_that_disagree_drop_the_pair(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """PMID 28: both raw groups cover the record and pick different spans, so the pair gets none."""
    assert spans_of(inputs, R28_KG2_ID, "PMID:28", S28) == []
    assert built["matching"]["pairs_groups_disagree_dropped"] == 1


def test_a_failing_kept_group_drops_the_pair(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """PMID 32: both groups are kept; one's row fails the text check, so the other's span does not stand in."""
    assert spans_of(inputs, R32_KG2_ID, "PMID:32", S32) == []
    assert built["matching"]["pairs_kept_group_text_fail_dropped"] == 1


def test_kept_group_must_hold_the_stored_evidence_and_has_no_stand_in(
    built: dict[str, Any], inputs: dict[str, Path]
) -> None:
    """PMIDs 30/31: both groups cover the record, only the plain-CUI one has its scores at PMID 31.

    That group is the kept one. Its PMID 30 row fails the text check, so PMID 30 gets no span, even
    though the other group's PMID 30 row would pass.
    """
    assert spans_of(inputs, R30_KG2_ID, "PMID:30", S30) == []
    assert spans_of(inputs, R30_KG2_ID, "PMID:31", S31) == [(*span(S31, "Ethanol"), *span(S31, "liver"))]
    assert built["kept_group"]["records_several_covering_groups"] > built["kept_group"]["records_several_kept_groups"]


def test_a_key_some_record_got_no_span_for_is_dropped(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """PMID 30: the second R30 record gets a span, the first none; the transform would share it, so none."""
    assert spans_of(inputs, R30_KG2_ID, "PMID:30", S30) == []
    second = (kg2_id_of(*R30_KG2_ID), evidence_fingerprint("PMID:31", evidence(S31, R30_SECOND_RECORD_SCORES)))
    assert [row[2:] for row in output_rows(inputs) if row[:2] == second] == [
        (*span(S31, "Ethanol"), *span(S31, "liver"))
    ]
    assert built["matching"]["pairs_dropped_unresolved_key"] == 1


def test_record_without_covering_group_gets_no_spans(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """PMID 29: a valid row exists, but no raw group has every publication of the record."""
    assert spans_of(inputs, R29_KG2_ID, "PMID:29", S29) == []
    assert built["matching"]["pairs_lost_to_kept_group_rule"] == 1


def test_reversed_predicates_swap_spans_also_for_canonicalized_records(
    built: dict[str, Any], inputs: dict[str, Path]
) -> None:
    """For part_of / location_of the subject columns hold the SemMedDB object span."""
    assert spans_of(inputs, R22_KG2_ID, "PMID:22", S22) == [(*span(S22, "Albumin"), *span(S22, "plasma"))]
    assert built["orientation"]["location_of"]["derived"] == "flipped"
    assert built["orientation"]["location_of"]["undetermined"] == 1
    assert built["orientation"]["part_of"]["derived"] == "flipped"


def test_manifest_counts(built: dict[str, Any]) -> None:
    """The manifest counts every case of the fixture set."""
    kept = len(KG2_RECORDS)
    assert built["kg2"] == {
        "kg2_ids_malformed": 0,
        "pairs": kept + 3,
        "pairs_with_sentence": kept + 2,
        "pairs_without_sentence": 1,
        "publications": kept * KEPT_PUBLICATION_COUNT + 1,
        "records": kept + len(SKIPPED_RECORDS),
        "records_kept": kept,
        "records_skipped": {
            "bte_excluded_predicate_skipped": 1,
            "domain_range_exclusion_skipped": 1,
            "kg2_id_count_not_one_skipped": 1,
            "low_publication_count_skipped": 1,
        },
    }
    semmeddb = built["semmeddb"]
    assert semmeddb["rows"] == TOTAL_ROWS
    # The padding rows of the zinc deficiency group carry its tab, and those of the SMAD group make no relation.
    assert (semmeddb["rows_null_aux"], semmeddb["rows_escaped_sentence"], semmeddb["rows_negated_dropped"]) == (
        1,
        1 + len(PADDING_PMIDS),
        2,
    )
    assert semmeddb["rows_without_relation"] == 1 + len(PADDING_PMIDS)
    assert (semmeddb["rows_subject_text_fail"], semmeddb["rows_object_text_fail"]) == (4, 1)
    matching = built["matching"]
    assert matching["pairs_with_sentence"] == kept + 2
    assert matching["pairs_matched"] == 22
    assert matching["pairs_dropped_unresolved_key"] == 1
    assert matching["pairs_located"] == 21
    assert matching["pairs_located_only_via_mrcui"] == 1
    assert matching["pairs_lost_to_scores"] == 1
    assert matching["pairs_lost_to_kept_group_rule"] == 1
    assert matching["pairs_lost_to_text_check"] == 2
    assert matching["pairs_groups_disagree_dropped"] == 1
    assert matching["pairs_kept_group_text_fail_dropped"] == 1
    assert matching["pairs_without_kept_group"] == 6
    assert matching["pairs_several_spans"] == 1
    assert matching["output_rows"] == len(EXPECTED_ROWS)
    assert matching["coverage"] == round(21 / (kept + 2), 6)
    assert built["kept_group"] == {
        "records_one_covering_group": 18,
        "records_several_covering_groups": 6,
        "records_no_covering_group": 6,
        "records_one_kept_group": 19,
        "records_several_kept_groups": 4,
        "records_no_kept_group": 7,
    }
    assert built["coverage_by"]["semmed_predicate"]["xref"] == {"pairs": 1, "matched": 0, "coverage": 0.0}
    # Coverage counts only located pairs: the R30 pair whose shared key was dropped is not matched anywhere.
    for table in built["coverage_by"].values():
        assert sum(row["matched"] for row in table.values()) == matching["pairs_located"]
    assert built["inputs"]["semmeddb_export"] == {
        "chunks": 3,
        "statuses_zero": 3,
        "partial_files": 0,
        "rows": TOTAL_ROWS,
        "expected_rows": TOTAL_ROWS,
        "predication_id_range_size": PREDICATION_ID_RANGE_SIZE,
    }


def test_manifest_records_chunk_provenance(built: dict[str, Any], inputs: dict[str, Path]) -> None:
    """The manifest lists every chunk with its sha256 and row count, and every export file with its sha256."""
    files = built["inputs"]["semmeddb_files"]
    on_disk = sorted(inputs["semmeddb_dir"].iterdir())
    assert [entry["path"] for entry in files] == [path.name for path in on_disk]
    assert [entry["sha256"] for entry in files] == [hashlib.sha256(path.read_bytes()).hexdigest() for path in on_disk]
    chunks = built["inputs"]["semmeddb_chunks"]
    assert [chunk["rows"] for chunk in chunks] == [len(CHUNK_A_ROWS), len(CHUNK_B_ROWS), 0]
    assert [chunk["min_predication_id"] for chunk in chunks] == [chunk_first_id(1), chunk_first_id(2), None]
    for chunk in chunks:
        digest = hashlib.sha256((inputs["semmeddb_dir"] / chunk["name"]).read_bytes()).hexdigest()
        assert chunk["sha256"] == digest


# The repo files the build runs, as the manifest's code section names them.
CODE_FILES = [
    f"src/translator_ingest/ingests/semmeddb/{name}.py"
    for name in (
        "analysis/build_text_locations",
        "analysis/kg2_orientation",
        "analysis/kg2_records",
        "analysis/kg2_semmeddb_converter",
        "analysis/output_guards",
        "analysis/rekey_pmid_verdicts",
        "analysis/semmeddb_export",
        "analysis/text_location_report",
        "analysis/work_dir",
        "semmeddb",
        "text_locations",
    )
]


def test_manifest_records_the_code_that_built_it(built: dict[str, Any]) -> None:
    """The manifest names each repo file the build runs, relative to the repo root, with its sha256 on disk."""
    assert [entry["path"] for entry in built["code"]] == CODE_FILES
    for entry in built["code"]:
        assert entry["sha256"] == hashlib.sha256((REPO_ROOT / entry["path"]).read_bytes()).hexdigest()


@pytest.mark.parametrize("value", ["0", "-1"])
@pytest.mark.parametrize("flag", ["--batch-pairs", "--buckets"])
def test_counts_below_one_are_refused(
    flag: str, value: str, inputs: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """--batch-pairs and --buckets must be at least 1; argparse stops before anything is written."""
    with pytest.raises(SystemExit) as stopped:
        main(argv_for(inputs, TOTAL_ROWS, flag, value))
    assert stopped.value.code == ARGPARSE_ERROR_STATUS
    assert f"argument {flag}: must be at least 1, got {value}" in capsys.readouterr().err
    assert not inputs["work_dir"].exists()
    assert not leftover_build_files(inputs)


@pytest.mark.parametrize("batch_pairs, buckets", [(1, 1), (1, 3), (3, 2), (3, 5)])
def test_batches_and_buckets_do_not_change_the_result(
    batch_pairs: int, buckets: int, built: dict[str, Any], inputs: dict[str, Path], tmp_path: Path
) -> None:
    """Many small KG2 batches and other bucket counts give the same rows and the same counts."""
    variant = full_inputs(tmp_path / "variant")
    argv = argv_for(variant, TOTAL_ROWS, "--batch-pairs", str(batch_pairs), "--buckets", str(buckets), "--keep-work")
    assert main(argv) == 0
    assert len(list((variant["work_dir"] / "records").glob("*.parquet"))) > 1
    assert output_rows(variant) == output_rows(inputs)
    manifest = json.loads(variant["out_manifest"].read_text())
    counted = ["kg2", "semmeddb", "orientation", "kept_group", "matching", "coverage_by"]
    assert {key: manifest[key] for key in counted} == {key: built[key] for key in counted}
    assert manifest["parameters"]["buckets"] == buckets


@pytest.mark.parametrize("kg2_ids", [None, "missing"])
def test_record_without_kg2_ids_is_skipped(kg2_ids: str | None, tmp_path: Path) -> None:
    """A missing or null kg2_ids field counts as no kg2_id: the record is skipped, not a crash."""
    record = {key: value for key, value in KG2_RECORDS[0].items() if key != "kg2_ids"}
    if kg2_ids is None:
        record["kg2_ids"] = None
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, [KG2_RECORDS[1], record])
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    manifest = json.loads(paths["out_manifest"].read_text())
    assert manifest["kg2"]["records_skipped"] == {"kg2_id_count_not_one_skipped": 1}


@pytest.mark.parametrize("missing", ["subject", "object", "predicate"])
def test_record_without_subject_object_or_predicate_is_skipped(missing: str, tmp_path: Path) -> None:
    """The transform counts such a record as an invalid edge; the build skips it the same way."""
    absent = {key: value for key, value in KG2_RECORDS[0].items() if key != missing}
    empty = {**KG2_RECORDS[0], missing: None}
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, [KG2_RECORDS[1], absent, empty])
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    manifest = json.loads(paths["out_manifest"].read_text())
    assert manifest["kg2"]["records_skipped"] == {"invalid_edge_skipped": 2}
    assert manifest["kg2"]["records_kept"] == 1


@pytest.mark.parametrize(
    "change, reason, malformed",
    [
        ({"subject": None, "kg2_ids": ["malformed"]}, "invalid_edge_skipped", 0),
        ({"kg2_ids": ["malformed"]}, "malformed_kg2_id_skipped", 1),
        ({"kg2_ids": ["UMLS:C1---SEMMEDDB:treats---UMLS:C2"]}, "malformed_kg2_id_skipped", 1),
        ({"kg2_ids": [*KG2_RECORDS[0]["kg2_ids"], "malformed"]}, "kg2_id_count_not_one_skipped", 1),
        # A JSON null kg2_id, on records the transform's filters reject and on one they keep.
        ({"kg2_ids": [None], "domain_range_exclusion": True}, "domain_range_exclusion_skipped", 0),
        ({"kg2_ids": [None], "publications": KG2_RECORDS[0]["publications"][:1]}, "low_publication_count_skipped", 0),
        ({"kg2_ids": [None]}, "malformed_kg2_id_skipped", 1),
    ],
)
def test_record_with_a_malformed_kg2_id_is_skipped_in_the_transform_order(
    change: dict[str, Any], reason: str, malformed: int, tmp_path: Path
) -> None:
    """A kg2_id that does not parse skips the record instead of stopping the build.

    The checks run in the transform's order, so a record that is also filtered out or an invalid
    edge is skipped for that, and its kg2_ids are not even parsed, so they are not counted as
    malformed; a record with several kg2_ids is skipped for that, its malformed one counted.
    """
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, [KG2_RECORDS[1], {**KG2_RECORDS[0], **change}])
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    manifest = json.loads(paths["out_manifest"].read_text())
    assert manifest["kg2"]["records_skipped"] == {reason: 1}
    assert (manifest["kg2"]["records_kept"], manifest["kg2"]["kg2_ids_malformed"]) == (1, malformed)


# Ends of the two part_of records of the orientation tests, as KG2 writes CUIs.
PART_OF_WHOLE, PART_OF_PART, OTHER_WHOLE, OTHER_PART = (f"UMLS:C900000{digit}" for digit in range(1, 5))


def part_of_records() -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a record whose kg2_id is flipped (part_of reversed to has_part) and one that is literal."""
    flipped = kg2_record(
        (PART_OF_WHOLE, "biolink:has_part", PART_OF_PART),
        (PART_OF_PART, "part_of", PART_OF_WHOLE),
        {"PMID:2": evidence(S2)},
    )
    return flipped, literal((OTHER_WHOLE, "biolink:has_part", OTHER_PART), "part_of", "PMID:2", S2)


@pytest.mark.parametrize(
    "change, reason",
    [
        ({"predicate": None}, "invalid_edge_skipped"),
        ({"publications": ["PMID:2"]}, "low_publication_count_skipped"),
        ({"domain_range_exclusion": True}, "domain_range_exclusion_skipped"),
    ],
)
def test_only_used_records_vote_on_orientation(change: dict[str, Any], reason: str, tmp_path: Path) -> None:
    """A skipped record with a well-formed flipped part_of kg2_id does not vote, so it cannot stop the build."""
    flipped, literal_record = part_of_records()
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, [{**flipped, **change}, literal_record])
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    manifest = json.loads(paths["out_manifest"].read_text())
    assert manifest["kg2"]["records_skipped"] == {reason: 1}
    assert manifest["orientation"]["part_of"] == {
        "same": 1,
        "flipped": 0,
        "self_loop": 0,
        "undetermined": 0,
        "derived": "same",
    }


@pytest.mark.parametrize(
    "change, reason",
    [
        ({"kg2_ids": 1, "domain_range_exclusion": True}, "domain_range_exclusion_skipped"),
        ({"kg2_ids": 1}, "kg2_id_count_not_one_skipped"),
        ({"kg2_ids": "UMLS:C9000001---SEMMEDDB:part_of"}, "kg2_id_count_not_one_skipped"),
    ],
)
def test_kg2_ids_that_are_not_a_list_count_as_none(change: dict[str, Any], reason: str, tmp_path: Path) -> None:
    """As in the transform, a kg2_ids that is not a list holds no kg2_id; the record is skipped, not a crash."""
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, [KG2_RECORDS[1], {**KG2_RECORDS[0], **change}])
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    assert json.loads(paths["out_manifest"].read_text())["kg2"]["records_skipped"] == {reason: 1}


@pytest.mark.parametrize(
    "malform",
    [
        lambda kg2_id: kg2_id.removesuffix("SEMMEDDB:") + "other:",
        lambda kg2_id: kg2_id.replace("SEMMEDDB:part_of---None---None", "SEMMEDDB:PART_OF---biolink:x---bogus"),
    ],
)
@pytest.mark.parametrize("malformed", [0, 1])
def test_a_malformed_kg2_id_does_not_vote_on_orientation(
    malformed: int, malform: Callable[[str], str], tmp_path: Path
) -> None:
    """A malformed kg2_id casts no orientation vote, so it cannot conflict with a well-formed record's vote.

    One of a flipped and a literal part_of record gets a kg2_id KG2 cannot write (a wrong last part,
    or an uppercase predicate with a bogus qualifier). Without the strict parser its vote would
    stop the build; with it, the record is skipped and part_of takes the other record's orientation.
    """
    records = list(part_of_records())
    records[malformed]["kg2_ids"] = [malform(records[malformed]["kg2_ids"][0])]
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, records)
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    manifest = json.loads(paths["out_manifest"].read_text())
    assert manifest["kg2"]["records_skipped"] == {"malformed_kg2_id_skipped": 1}
    assert manifest["kg2"]["kg2_ids_malformed"] == 1
    assert manifest["orientation"]["part_of"]["derived"] == ["same", "flipped"][malformed]


def test_records_with_an_unknown_predicate_do_not_vote(tmp_path: Path) -> None:
    """Two records whose kg2_id has a predicate KG2.10.3 does not know, in opposite orientations.

    Both kg2_ids are malformed, so neither record votes and the build goes on instead of stopping.
    """
    records = list(part_of_records())
    for record in records:
        record["kg2_ids"] = [record["kg2_ids"][0].replace("SEMMEDDB:part_of", "SEMMEDDB:unknown_predicate")]
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, records)
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    manifest = json.loads(paths["out_manifest"].read_text())
    assert manifest["kg2"]["records_skipped"] == {"malformed_kg2_id_skipped": 2}
    assert manifest["kg2"]["kg2_ids_malformed"] == 2
    assert "unknown_predicate" not in manifest["orientation"]


def test_predicate_seen_both_ways_stops_the_build(tmp_path: Path) -> None:
    """A SemMedDB predicate seen both same and flipped in KG2 stops the build."""
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, list(part_of_records()))
    with pytest.raises(ValueError, match="both same and flipped"):
        main(argv_for(paths, len(CHUNK_A_ROWS)))
    assert not leftover_build_files(paths)


def test_matched_predicate_without_orientation_stops_the_build(tmp_path: Path) -> None:
    """A matched predicate with no orientation vote stops the build."""
    records = [
        kg2_record(
            ("CHEBI:15365", TREATS, "UMLS:C0018681"),
            ("UMLS:C0004057", "treats", "UMLS:C0018681"),
            {"PMID:1": evidence(S1)},
        )
    ]
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS, "chunk_002": CHUNK_B_ROWS}, records)
    with pytest.raises(ValueError, match="no orientation"):
        main(argv_for(paths, TOTAL_ROWS))


def transform_locations(records: list[dict[str, Any]], artifact: Path, input_dir: Path) -> list[StudyLocation]:
    """Run the transform hooks on ``records`` with ``artifact`` and return every study result's location.

    Each entry is (subject, predicate, object, PMID, sentence, subject location, object location).
    """
    input_dir.mkdir()
    shutil.copy(artifact, input_dir / TEXT_LOCATION_ARTIFACT_FILENAME)
    no_verdicts = pl.DataFrame(schema=dict.fromkeys(VERDICT_ARTIFACT_COLUMNS, pl.String()))
    no_verdicts.write_parquet(input_dir / VERDICT_ARTIFACT_FILENAME)
    writer = MockKozaWriter()
    hooks = KozaTransformHooks(on_data_begin=[on_begin_filter_edges], transform_record=[transform_semmeddb_edge])
    KozaRunner(data=records, writer=writer, hooks=hooks, input_files_dir=input_dir).run()
    found: list[StudyLocation] = []
    for association in (item for item in writer.items if isinstance(item, Association)):
        for study in (association.has_supporting_studies or {}).values():
            for result in study.has_study_results:
                found.append(
                    (
                        association.subject,
                        association.predicate,
                        association.object,
                        result.xref[0],
                        result.supporting_text[0],
                        result.subject_location_in_text,
                        result.object_location_in_text,
                    )
                )
    return sorted(found, key=repr)


def test_transform_attaches_the_built_spans(built: dict[str, Any], inputs: dict[str, Path], tmp_path: Path) -> None:
    """End to end: the transform, loading the built artifact, puts each record's own spans on it."""
    assert built["output"]["rows"] == len(EXPECTED_ROWS)
    spans_by_key = {row[:2]: row[2:] for row in EXPECTED_ROWS}
    wanted: list[StudyLocation] = []
    for record in [*KG2_RECORDS, SKIPPED_RECORDS[-1]]:
        for pmid, info in record["publications_info"].items():
            if not info["sentence"]:
                continue
            key = (record["kg2_ids"][0], evidence_fingerprint(pmid, info))
            spans = spans_by_key.get(key) if len(record["kg2_ids"]) == 1 else None
            locations = (None, None) if spans is None else (list(spans[:2]), list(spans[2:]))
            predicate = PREDICATE_REMAP.get(record["predicate"], record["predicate"])
            wanted.append((record["subject"], predicate, record["object"], pmid, info["sentence"], *locations))
    assert transform_locations(KG2_RECORDS + SKIPPED_RECORDS, inputs["out_parquet"], tmp_path / "in") == sorted(
        wanted, key=repr
    )
