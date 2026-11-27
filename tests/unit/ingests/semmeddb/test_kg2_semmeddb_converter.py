"""Tests for the KG2.10.3 converter mirror (``analysis/kg2_semmeddb_converter.py``) and orientation votes.

The build runs these rules end to end in ``test_build_text_locations.py``; these tests pin each rule
on its own: which kg2_ids parse, which relations a SemMedDB row yields, the MRCUI remap, and how a
kg2_id's ends vote on orientation.
"""

import doctest
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest

from tests.unit.ingests.semmeddb.text_location_fixtures import MRCUI_LINES, kg2_id_of
from translator_ingest.ingests.semmeddb.analysis import kg2_orientation, kg2_semmeddb_converter
from translator_ingest.ingests.semmeddb.analysis.kg2_semmeddb_converter import (
    KG2_ID_QUALIFIERS,
    KG2_SEMMEDDB_PREDICATES,
    kg2_relations,
    load_cui_remap,
    parse_kg2_id,
)

# The 31 SemMedDB predicates of the kg2_ids of kg2.10.3-semmeddb-edges.jsonl.gz, from a scan of all of them.
REAL_FILE_PREDICATES = (
    "administered_to affects associated_with augments causes coexists_with compared_with complicates "
    "converts_to diagnoses disrupts higher_than inhibits interacts_with isa location_of lower_than "
    "manifestation_of measures occurs_in part_of precedes predisposes prevents process_of produces same_as "
    "stimulates treats uses xref"
).split()


@pytest.mark.parametrize("module", [kg2_orientation, kg2_semmeddb_converter])
def test_module_doctests_pass(module: ModuleType) -> None:
    """The doctests of the converter mirror and the orientation votes pass."""
    results = doctest.testmod(module)
    assert results.failed == 0
    assert results.attempted > 0


QUALIFIED_KG2_ID = (
    "UMLS:C0000001---SEMMEDDB:stimulates---biolink:causes---activity---increased---NCBIGene:2---SEMMEDDB:"
)


@pytest.mark.parametrize(
    "kg2_id, ends",
    [
        (QUALIFIED_KG2_ID, ("UMLS:C0000001", "stimulates", "NCBIGene:2")),
        (kg2_id_of("UMLS:C0000001", "part_of", "UMLS:C0000002"), ("UMLS:C0000001", "part_of", "UMLS:C0000002")),
        # make_rel lowercased every predicate, so an uppercase one is not KG2's.
        (kg2_id_of("UMLS:C0000001", "PART_OF", "UMLS:C0000002"), None),
        # Qualifiers are exactly the predicate's in KG2.10.3: none for part_of, its own for inhibits.
        (
            kg2_id_of("UMLS:C0000001", "part_of", "UMLS:C0000002").replace(
                "None---None---None", "biolink:foo---bogus---invented"
            ),
            None,
        ),
        (kg2_id_of("UMLS:C0000001", "inhibits", "UMLS:C0000002"), ("UMLS:C0000001", "inhibits", "UMLS:C0000002")),
        (
            kg2_id_of("UMLS:C0000001", "inhibits", "UMLS:C0000002")
            .replace("activity---decreased", "None---None")
            .replace("biolink:causes", "None"),
            None,
        ),
        (QUALIFIED_KG2_ID.replace("increased", "decreased"), None),
        (QUALIFIED_KG2_ID.replace("stimulates", "stimulates2"), None),
        # Only predicates KG2.10.3 keeps: not an unknown one, nor one predicate-remap.yaml deletes.
        (kg2_id_of("UMLS:C0000001", "unknown_predicate", "UMLS:C0000002"), None),
        (kg2_id_of("UMLS:C0000001", "nom", "UMLS:C0000002"), None),
        # Qualifiers: all None or biolink:<snake_case> with two snake_case values.
        (QUALIFIED_KG2_ID.replace("biolink:causes---activity", "None---activity"), None),
        (QUALIFIED_KG2_ID.replace("biolink:causes", "causes"), None),
        (QUALIFIED_KG2_ID.replace("biolink:causes", "biolink:Causes"), None),
        (QUALIFIED_KG2_ID.replace("increased", "bogus value"), None),
        # Ends: UMLS:C and 7 digits, or NCBIGene: and digits, as in every real kg2_id.
        (QUALIFIED_KG2_ID.replace("UMLS:C0000001", "UMLS:A"), None),
        (QUALIFIED_KG2_ID.replace("UMLS:C0000001", ""), None),
        (QUALIFIED_KG2_ID.replace("NCBIGene:2", "NCBIGene:"), None),
        # The shape itself.
        (QUALIFIED_KG2_ID.removesuffix("SEMMEDDB:") + "other:", None),
        (QUALIFIED_KG2_ID + "---SEMMEDDB:", None),
        (QUALIFIED_KG2_ID.rsplit("---", 1)[0], None),
        (QUALIFIED_KG2_ID.replace("SEMMEDDB:stimulates", "SEMMEDDB:"), None),
        (QUALIFIED_KG2_ID.replace("SEMMEDDB:stimulates", "stimulates"), None),
        (None, None),
        (7, None),
    ],
)
def test_parse_kg2_id_accepts_only_shapes_kg2_writes(kg2_id: object, ends: tuple[str, str, str] | None) -> None:
    """Only ids of the shapes every real kg2_id has parse; the predicate is taken as written, not lowercased."""
    assert parse_kg2_id(kg2_id) == ends


@pytest.mark.parametrize(
    "subject_field, predicate, object_field, relations",
    [
        ("C1", "TREATS", "C2", [("UMLS:C1", "treats", "UMLS:C2")]),
        ("C0164786|5594", "COEXISTS_WITH", "C3", [("UMLS:C0164786", "coexists_with", "UMLS:C3")]),
        (
            "2048|5594",
            "COEXISTS_WITH",
            "C3",
            [("NCBIGene:2048", "coexists_with", "UMLS:C3"), ("NCBIGene:5594", "coexists_with", "UMLS:C3")],
        ),
        ("C1", "AFFECTS", "7|8", [("UMLS:C1", "affects", "NCBIGene:7"), ("UMLS:C1", "affects", "NCBIGene:8")]),
        (
            "6",
            "INTERACTS_WITH",
            "7|8",
            [("NCBIGene:6", "interacts_with", "NCBIGene:7"), ("NCBIGene:6", "interacts_with", "NCBIGene:8")],
        ),
        (
            "6|9",
            "INTERACTS_WITH",
            "7",
            [("NCBIGene:6", "interacts_with", "NCBIGene:7"), ("NCBIGene:9", "interacts_with", "NCBIGene:7")],
        ),
        ("6|9", "INTERACTS_WITH", "7|8", []),
        ("C0000002|5", "CAUSES", "C2", [("UMLS:C0007404", "causes", "UMLS:C2")]),
        ("C0000002|5", "NEG_CAUSES", "C2", []),
        ("c1", "TREATS", "C2", [("UMLS:c1", "treats", "UMLS:C2")]),
        ("C1", "SAME_AS", "C1", []),
        ("C1", "TREATS", "C1", [("UMLS:C1", "treats", "UMLS:C1")]),
    ],
)
def test_kg2_relations_follow_the_kg2_converter(
    subject_field: str, predicate: str, object_field: str, relations: list[tuple[str, str, str]], tmp_path: Path
) -> None:
    """Rows yield the relations KG2.10.3's get_rels_to_make_for_row made, without xrefs or negations."""
    mrcui = tmp_path / "MRCUI.RRF"
    mrcui.write_text("".join(line + "\n" for line in MRCUI_LINES))
    remap, _ = load_cui_remap(mrcui)
    rows = pl.DataFrame({"subject_cui": [subject_field], "object_cui": [object_field], "predicate": [predicate]})
    found = kg2_relations(rows, remap)
    assert found.select("kg2_subject", "semmed_predicate", "kg2_object").rows() == relations


def test_load_cui_remap_keeps_current_synonyms_with_a_target(tmp_path: Path) -> None:
    """Only REL = SY, current Y rows with a non-empty new CUI become remap pairs."""
    mrcui = tmp_path / "MRCUI.RRF"
    mrcui.write_text("".join(line + "\n" for line in MRCUI_LINES))
    remap, stats = load_cui_remap(mrcui)
    assert remap.rows() == [("C0000002", "C0007404")]
    assert stats["mrcui_rows"] == len(MRCUI_LINES)


@pytest.mark.parametrize(
    "kg2_ends, record_ends, orientation",
    [
        (("A", "B"), ("A", "B"), "same"),
        (("A", "B"), ("B", "A"), "flipped"),
        (("A", "A"), ("A", "A"), "self_loop"),
        (("UMLS:C1", "B"), ("MONDO:1", "B"), "undetermined"),
    ],
)
def test_kg2_orientation(kg2_ends: tuple[str, str], record_ends: tuple[str, str], orientation: str) -> None:
    """Orientation votes use literal equality of kg2_id ends and record ends."""
    assert kg2_orientation.kg2_orientation(*kg2_ends, *record_ends) == orientation


def test_the_known_predicates_are_those_of_the_real_file() -> None:
    """KG2.10.3's keep or invert SemMedDB predicates are the real file's; every qualified one is among them."""
    assert sorted(KG2_SEMMEDDB_PREDICATES) == sorted(REAL_FILE_PREDICATES)
    assert set(KG2_ID_QUALIFIERS) <= KG2_SEMMEDDB_PREDICATES


@pytest.mark.parametrize("predicate", REAL_FILE_PREDICATES)
def test_every_known_predicate_parses_with_its_qualifiers(predicate: str) -> None:
    """A kg2_id written the way KG2.10.3 writes it parses for each of the predicates it knows."""
    assert parse_kg2_id(kg2_id_of("UMLS:C0000001", predicate, "NCBIGene:2")) == (
        "UMLS:C0000001",
        predicate,
        "NCBIGene:2",
    )
