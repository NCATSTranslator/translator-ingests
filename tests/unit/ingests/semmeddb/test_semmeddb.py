import doctest
import importlib
import tempfile
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import polars as pl
import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    CausalGeneToDiseaseAssociation,
    ChemicalAffectsGeneAssociation,
    ChemicalAffectsBiologicalEntityAssociation,
    ChemicalEntity,
    Disease,
    Gene,
    GeneAffectsChemicalAssociation,
    GeneToGeneAssociation,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    NamedThing,
    Protein,
    RetrievalSource,
    ResourceRoleEnum,
    Study,
    TextMiningStudyResult,
)
from koza.runner import KozaRunner, KozaTransformHooks
from koza.transform import KozaTransform, Record

from tests.unit.ingests import MockKozaWriter
from translator_ingest.ingests.semmeddb import semmeddb as semmeddb_module
from translator_ingest.ingests.semmeddb import text_locations as text_locations_module
from translator_ingest.ingests.semmeddb.semmeddb import (
    _STATE_DEFAULTS,
    MAX_PUBLICATIONS_PER_EDGE,
    MIN_TEXT_LOCATION_COVERAGE,
    VERDICT_ARTIFACT_COLUMNS,
    VERDICT_ARTIFACT_FILENAME,
    _apply_filters,
    _extract_supporting_studies,
    _has_bte_excluded_predicate,
    _make_node,
    get_latest_version,
    load_verdicts,
    on_begin_filter_edges,
    on_end_filter_edges,
    record_filter_reason,
    transform_semmeddb_edge,
)
from translator_ingest.ingests.semmeddb.text_locations import (
    TEXT_LOCATION_ARTIFACT_FILENAME,
    TEXT_LOCATION_ARTIFACT_SCHEMA,
    TEXT_LOCATION_FINGERPRINT_COLUMN,
    TEXT_LOCATION_KG2_ID_COLUMN,
    TextSpans,
    evidence_fingerprint,
    load_text_locations,
)

FOUR_PUBS = ["PMID:11111111", "PMID:22222222", "PMID:33333333", "PMID:44444444"]

# What a run writes: the subject and object nodes and the association.
Entities = list[NamedThing | Association]
VerdictRow = dict[str, str]
TextLocationRow = dict[str, str | int]

# Column order of the LLM PMID-checker results parquet the transform reads.
VERDICT_SCHEMA: dict[str, pl.DataType] = {column: pl.String() for column in VERDICT_ARTIFACT_COLUMNS}
TEXT_LOCATION_SORT_COLUMNS = [TEXT_LOCATION_KG2_ID_COLUMN, TEXT_LOCATION_FINGERPRINT_COLUMN]


def _verdict(
    pmid: str,
    support: str = "no",
    subject: str = "CHEBI:15365",
    predicate: str = "biolink:treats_or_applied_or_studied_to_treat",
    obj: str = "MONDO:0005148",
) -> VerdictRow:
    """Return one verdict row, keyed like _base_record's edge by default."""
    return {
        "subject_curie": subject,
        "predicate": predicate,
        "object_curie": obj,
        "PMID": pmid,
        "support": support,
    }


def _write_verdict_artifact(directory: Path, verdicts: list[VerdictRow]) -> None:
    """Write the verdict parquet the transform loads from its input files directory."""
    pl.DataFrame(verdicts, schema=VERDICT_SCHEMA).write_parquet(directory / VERDICT_ARTIFACT_FILENAME)


# A KG2 record's single kg2_ids entry, as KG2 writes it, for _base_record's edge.
BASE_KG2_ID = "CHEBI:15365---SEMMEDDB:treats---None---None---None---MONDO:0005148---SEMMEDDB:"
# Another kg2_id that emits the same edge key: KG2 maps SemMedDB PREVENTS to
# preventative_for_condition, which the transform remaps to treats_or_applied_or_studied_to_treat.
PREVENTS_KG2_ID = "CHEBI:15365---SEMMEDDB:prevents---None---None---None---MONDO:0005148---SEMMEDDB:"

# Subject "Aspirin" is [0, 7] and object "diabetes" is [15, 23]: 0-based, end exclusive.
LOCATED_SENTENCE = "Aspirin treats diabetes in adults."
LOCATED_EVIDENCE: dict[str, str] = {
    "sentence": LOCATED_SENTENCE,
    "subject score": "1000",
    "object score": "888",
}
SUBJECT_SPAN = (0, 7)
OBJECT_SPAN = (15, 23)

# The same claim in another sentence: "aspirin" is [11, 18] and "diabetes" is [26, 34].
OTHER_SENTENCE = "In adults, aspirin treats diabetes."
OTHER_EVIDENCE: dict[str, str] = {**LOCATED_EVIDENCE, "sentence": OTHER_SENTENCE}
OTHER_SUBJECT_SPAN = (11, 18)
OTHER_OBJECT_SPAN = (26, 34)

# The first sentence with other scores, which makes it the evidence of another predication.
RESCORED_EVIDENCE: dict[str, str] = {**LOCATED_EVIDENCE, "object score": "1000"}
# A sentence the record does not carry, as in an artifact built from older KG2 data.
STALE_SENTENCE = "Aspirin treats diabetes in rats."
STALE_EVIDENCE: dict[str, str] = {**LOCATED_EVIDENCE, "sentence": STALE_SENTENCE}
UNKNOWN_KG2_ID = "CHEBI:1---SEMMEDDB:treats---None---None---None---MONDO:1---SEMMEDDB:"


def _location(
    pmid: str,
    subject_span: tuple[int, int] = SUBJECT_SPAN,
    object_span: tuple[int, int] = OBJECT_SPAN,
    kg2_id: str = BASE_KG2_ID,
    evidence: Mapping[str, str] | None = None,
) -> TextLocationRow:
    """Return one text-location row for a PMID's evidence, LOCATED_EVIDENCE unless given."""
    return {
        TEXT_LOCATION_KG2_ID_COLUMN: kg2_id,
        TEXT_LOCATION_FINGERPRINT_COLUMN: evidence_fingerprint(
            pmid, LOCATED_EVIDENCE if evidence is None else evidence
        ),
        "subject_start": subject_span[0],
        "subject_end": subject_span[1],
        "object_start": object_span[0],
        "object_end": object_span[1],
    }


def _text_location_frame(locations: list[TextLocationRow]) -> pl.DataFrame:
    """Return the rows as a frame with the artifact's schema, in the order they were given."""
    return pl.DataFrame(locations, schema=TEXT_LOCATION_ARTIFACT_SCHEMA)


def _write_text_location_artifact(directory: Path, locations: list[TextLocationRow]) -> None:
    """Write the text-location parquet the transform loads, sorted like the build writes it."""
    _text_location_frame(locations).sort(TEXT_LOCATION_SORT_COLUMNS).write_parquet(
        directory / TEXT_LOCATION_ARTIFACT_FILENAME
    )


def _run_transform(
    record: Record,
    verdicts: list[VerdictRow] | None = None,
    check_coverage: bool = False,
    write_artifact: bool = True,
    text_locations: list[TextLocationRow] | None = None,
) -> tuple[Entities, dict[str, Any]]:
    """Run a single record through the transform, returning emitted entities and transform metadata.

    The transform loads the PMID-checker verdicts and the text locations in its on_data_begin hook,
    so every run needs both artifacts. With no ``verdicts`` the verdict artifact is empty, meaning
    no publication is rejected, and with no ``text_locations`` no study result gets a location.
    The on_data_end hook, which records the metadata and enforces both coverage guards, only runs
    when ``check_coverage`` is set. ``write_artifact`` is set to False to run without a verdict
    artifact, which is what an unfiltered run does.
    """
    writer = MockKozaWriter()
    hooks = KozaTransformHooks(
        on_data_begin=[on_begin_filter_edges],
        transform_record=[transform_semmeddb_edge],
        on_data_end=[on_end_filter_edges] if check_coverage else [],
    )
    with tempfile.TemporaryDirectory() as input_files_dir:
        if write_artifact:
            _write_verdict_artifact(Path(input_files_dir), verdicts or [])
        _write_text_location_artifact(Path(input_files_dir), text_locations or [])
        runner = KozaRunner(
            data=[record],
            writer=writer,
            hooks=hooks,
            input_files_dir=Path(input_files_dir),
        )
        runner.run()
    return writer.items, runner.transform_metadata


def _create_test_runner(
    record: Record,
    verdicts: list[VerdictRow] | None = None,
    check_coverage: bool = False,
    write_artifact: bool = True,
    text_locations: list[TextLocationRow] | None = None,
) -> Entities:
    """Run a single record through the transform and return the emitted entities.

    See _run_transform for the arguments.
    """
    entities, _ = _run_transform(record, verdicts, check_coverage, write_artifact, text_locations)
    return entities


def _base_record(**overrides: object) -> Record:
    """Return a minimal valid KG2 edge record, with optional field overrides."""
    record: Record = {
        "subject": "CHEBI:15365",
        "object": "MONDO:0005148",
        "predicate": "biolink:treats_or_applied_or_studied_to_treat",
        "publications": list(FOUR_PUBS),
        "domain_range_exclusion": False,
    }
    record.update(overrides)
    return record


# ---------------------------------------------------------------------------
# Basic edge transformation
# ---------------------------------------------------------------------------

def test_therapeutic_edge_entities():
    """Therapeutic edge creates Chemical, Disease, and Association."""
    entities = _create_test_runner(_base_record())
    assert len(entities) == 3

    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.predicate == "biolink:treats_or_applied_or_studied_to_treat"
    assert association.subject == "CHEBI:15365"
    assert association.object == "MONDO:0005148"
    assert association.publications == FOUR_PUBS
    assert association.knowledge_level == KnowledgeLevelEnum.not_provided
    assert association.agent_type == AgentTypeEnum.text_mining_agent

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "CHEBI:15365"

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MONDO:0005148"


# ---------------------------------------------------------------------------
# _make_node
# ---------------------------------------------------------------------------

def test_make_node_function():
    """_make_node creates correct types by prefix and rejects malformed IDs."""
    gene_node = _make_node("NCBIGene:123")
    assert isinstance(gene_node, Gene)
    assert gene_node.id == "NCBIGene:123"

    protein_node = _make_node("UniProtKB:P12345")
    assert isinstance(protein_node, Protein)

    chemical_node = _make_node("CHEBI:15365")
    assert isinstance(chemical_node, ChemicalEntity)

    unknown_node = _make_node("UNKNOWN:123")
    assert isinstance(unknown_node, NamedThing)
    assert unknown_node.id == "UNKNOWN:123"

    assert _make_node("malformed_id") is None


# ---------------------------------------------------------------------------
# Filters: domain_range_exclusion, publication count
# ---------------------------------------------------------------------------

def test_domain_range_exclusion_filters_out():
    """Records with domain_range_exclusion=True are dropped."""
    entities = _create_test_runner(_base_record(domain_range_exclusion=True))
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 0


def test_low_publication_count_filters_out():
    """Records with <=3 publications are dropped."""
    entities = _create_test_runner(_base_record(publications=[]))
    assert [e for e in entities if isinstance(e, Association)] == []

    entities = _create_test_runner(_base_record(publications=["PMID:1", "PMID:2", "PMID:3"]))
    assert [e for e in entities if isinstance(e, Association)] == []


# ---------------------------------------------------------------------------
# BTE-excluded predicate filtering via kg2_ids
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("original_pred", [
    "compared_with",
    "isa",
    "measures",
    "higher_than",
    "lower_than",
])
def test_bte_excluded_predicate_filtered(original_pred: str):
    """Records whose kg2_ids contain a BTE-excluded original predicate are dropped."""
    kg2_id = f"UMLS:C0---SEMMEDDB:{original_pred}---UMLS:C1"
    record = _base_record(
        predicate="biolink:related_to",
        kg2_ids=[kg2_id],
    )
    entities = _create_test_runner(record)
    assert [e for e in entities if isinstance(e, Association)] == []


def test_bte_included_predicate_passes_through():
    """Records with a non-excluded kg2_ids original predicate are kept."""
    record = _base_record(
        predicate="biolink:related_to",
        kg2_ids=["UMLS:C0---SEMMEDDB:associated_with---UMLS:C1"],
    )
    entities = _create_test_runner(record)
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 1


def test_missing_kg2_ids_passes_through():
    """Records without kg2_ids are not filtered by BTE exclusion."""
    entities = _create_test_runner(_base_record())
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 1


def test_empty_kg2_ids_passes_through():
    """Records with empty kg2_ids list are not filtered."""
    entities = _create_test_runner(_base_record(kg2_ids=[]))
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 1


# ---------------------------------------------------------------------------
# record_filter_reason / _apply_filters (unit)
# ---------------------------------------------------------------------------

ISA_KG2_ID = "UMLS:C0---SEMMEDDB:isa---UMLS:C1"


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"domain_range_exclusion": True}, "domain_range_exclusion_skipped"),
        (
            {"domain_range_exclusion": True, "publications": [], "kg2_ids": [ISA_KG2_ID]},
            "domain_range_exclusion_skipped",
        ),
        ({"publications": ["PMID:1", "PMID:2", "PMID:3"]}, "low_publication_count_skipped"),
        ({"publications": [], "kg2_ids": [ISA_KG2_ID]}, "low_publication_count_skipped"),
        ({"kg2_ids": [ISA_KG2_ID]}, "bte_excluded_predicate_skipped"),
        ({"kg2_ids": ["UMLS:C0---SEMMEDDB:treats---UMLS:C1"]}, None),
        ({"kg2_ids": []}, None),
        ({}, None),
    ],
)
def test_record_filter_reason_and_apply_filters_agree(overrides: dict[str, object], reason: str | None) -> None:
    """The first rejecting filter names the reason, and _apply_filters counts exactly that key."""
    record = _base_record(**overrides)
    assert record_filter_reason(record) == reason
    state: dict[str, Any] = dict(_STATE_DEFAULTS)
    publications = _apply_filters(record, state)
    changed = {key: value for key, value in state.items() if value != _STATE_DEFAULTS[key]}
    if reason is None:
        assert publications == record["publications"]
        assert changed == {}
    else:
        assert publications is None
        assert changed == {reason: 1}


# ---------------------------------------------------------------------------
# _has_bte_excluded_predicate (unit)
# ---------------------------------------------------------------------------

def test_has_bte_excluded_predicate_unit():
    """Direct unit tests for _has_bte_excluded_predicate."""
    assert _has_bte_excluded_predicate(["UMLS:C0---SEMMEDDB:isa---UMLS:C1"]) is True
    assert _has_bte_excluded_predicate(["UMLS:C0---SEMMEDDB:treats---UMLS:C1"]) is False
    assert _has_bte_excluded_predicate([]) is False
    assert _has_bte_excluded_predicate(["malformed_string"]) is False
    assert _has_bte_excluded_predicate(["UMLS:C0---isa---UMLS:C1"]) is True


# ---------------------------------------------------------------------------
# Qualifier pass-through
# ---------------------------------------------------------------------------

def test_qualifier_chemical_affects_gene():
    """Chemical subject + Gene object with qualifiers -> ChemicalAffectsGeneAssociation."""
    record = _base_record(
        subject="CHEBI:15365",
        object="NCBIGene:100",
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 1

    assoc = associations[0]
    assert isinstance(assoc, ChemicalAffectsGeneAssociation)
    assert assoc.qualified_predicate == "biolink:causes"
    assert assoc.object_aspect_qualifier == "activity"
    assert assoc.object_direction_qualifier == "increased"


def test_qualifier_gene_affects_chemical():
    """Gene subject + Chemical object with qualifiers -> GeneAffectsChemicalAssociation."""
    record = _base_record(
        subject="NCBIGene:100",
        object="CHEBI:15365",
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity",
        qualified_object_direction="decreased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert isinstance(assoc, GeneAffectsChemicalAssociation)
    assert assoc.object_direction_qualifier == "decreased"


def test_qualifier_gene_affects_gene():
    """Gene subject + Gene object with qualifiers -> GeneToGeneAssociation."""
    record = _base_record(
        subject="NCBIGene:100",
        object="HGNC:200",
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity_or_abundance",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert isinstance(assoc, GeneToGeneAssociation)
    assert assoc.predicate == "biolink:affects"
    assert assoc.qualified_predicate == "biolink:causes"
    assert assoc.object_aspect_qualifier == "activity_or_abundance"
    assert assoc.object_direction_qualifier == "increased"


def test_qualifier_stripped_when_endpoints_untyped():
    """NamedThing-on-both-sides with activity aspect strips the qualifier triple.

    Regression for NCATSTranslator/Feedback#1213. Activity-like aspects are
    only valid when both endpoints are Gene/Protein/ChemicalEntity, so an
    untyped subject and object cannot bear them. The edge is downgraded to a
    plain ``biolink:affects`` Association without qualifier fields.
    """
    record = _base_record(
        subject="UMLS:C0011847",
        object="UMLS:C0012345",
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert type(assoc) is Association
    assert assoc.predicate == "biolink:affects"
    dumped = assoc.model_dump(mode="json", exclude_none=True)
    assert "qualified_predicate" not in dumped
    assert "object_aspect_qualifier" not in dumped
    assert "object_direction_qualifier" not in dumped


def test_issue_1213_chemical_to_phenotype_strips_qualifier():
    """Regression for NCATSTranslator/Feedback#1213.

    The literal failing example from the issue: D-glucose (CHEBI:17234) ->
    "injury" (UMLS:C3263723) with ``causes activity_or_abundance increased``.
    Aragorn was chaining the qualifier semantics to produce "X has increased
    activity caused by glucose" inferences. After this fix, the qualifier
    triple is stripped so the chain cannot match.
    """
    record = _base_record(
        subject="CHEBI:17234",
        object="UMLS:C3263723",
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity_or_abundance",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert type(assoc) is Association
    assert assoc.predicate == "biolink:affects"
    dumped = assoc.model_dump(mode="json", exclude_none=True)
    assert "qualified_predicate" not in dumped
    assert "object_aspect_qualifier" not in dumped
    assert "object_direction_qualifier" not in dumped


@pytest.mark.parametrize("subject_id,object_id", [
    ("CHEBI:15365", "MONDO:0005148"),
    ("CHEBI:15365", "HP:0000118"),
    ("CHEBI:15365", "UBERON:0000001"),
    ("CHEBI:15365", "UMLS:C0012345"),
    ("UMLS:C0011847", "NCBIGene:100"),
    ("MONDO:0005148", "CHEBI:15365"),
])
def test_activity_qualifier_stripped_for_non_activity_bearing_endpoints(
    subject_id: str,
    object_id: str,
):
    """Activity-like qualifiers require Gene/Protein/Chemical on BOTH sides."""
    record = _base_record(
        subject=subject_id,
        object=object_id,
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity_or_abundance",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert type(assoc) is Association
    assert assoc.predicate == "biolink:affects"
    dumped = assoc.model_dump(mode="json", exclude_none=True)
    assert "qualified_predicate" not in dumped
    assert "object_aspect_qualifier" not in dumped
    assert "object_direction_qualifier" not in dumped


@pytest.mark.parametrize("subject_id,object_id", [
    ("CHEBI:15365", "NCBIGene:100"),
    ("CHEBI:15365", "UniProtKB:P12345"),
    ("CHEBI:15365", "CHEBI:99999"),
    ("NCBIGene:100", "CHEBI:15365"),
    ("NCBIGene:100", "HGNC:200"),
    ("UniProtKB:P12345", "NCBIGene:100"),
])
def test_activity_qualifier_kept_for_activity_bearing_endpoints(
    subject_id: str,
    object_id: str,
):
    """Activity-like qualifiers pass through when both endpoints are Gene/Protein/Chemical."""
    record = _base_record(
        subject=subject_id,
        object=object_id,
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert assoc.qualified_predicate == "biolink:causes"
    assert assoc.object_aspect_qualifier == "activity"
    assert assoc.object_direction_qualifier == "increased"


def test_qualifier_fields_in_model_dump():
    """Qualifier fields appear in model_dump() output for serialization."""
    record = _base_record(
        subject="CHEBI:15365",
        object="NCBIGene:100",
        predicate="biolink:affects",
        qualified_predicate="biolink:causes",
        qualified_object_aspect="activity",
        qualified_object_direction="increased",
    )
    entities = _create_test_runner(record)
    assoc = [e for e in entities if isinstance(e, Association)][0]
    dumped = assoc.model_dump(mode="json", exclude_none=True)
    assert dumped["qualified_predicate"] == "biolink:causes"
    assert dumped["object_aspect_qualifier"] == "activity"
    assert dumped["object_direction_qualifier"] == "increased"


def test_no_qualifier_on_plain_edge():
    """Records without qualifier fields produce a plain Association."""
    entities = _create_test_runner(_base_record())
    assoc = [e for e in entities if isinstance(e, Association)][0]
    assert type(assoc) is Association
    dumped = assoc.model_dump(mode="json", exclude_none=True)
    assert "qualified_predicate" not in dumped
    assert "object_aspect_qualifier" not in dumped
    assert "object_direction_qualifier" not in dumped


# ---------------------------------------------------------------------------
# Supporting studies
# ---------------------------------------------------------------------------

def test_extract_supporting_studies():
    """_extract_supporting_studies creates Study with TextMiningStudyResults."""
    publications_info = {
        "PMID:12345678": {
            "sentence": "This drug treats the disease effectively.",
            "publication date": "2020 Jan",
            "subject score": "1000",
            "object score": "900",
        },
        "PMID:87654321": {
            "sentence": "Further studies confirmed the therapeutic effect.",
            "publication date": "2021 Mar",
            "subject score": "950",
            "object score": "850",
        },
    }
    result = _extract_supporting_studies(publications_info, {}, dict(_STATE_DEFAULTS))
    assert result is not None
    assert len(result) == 1

    study = list(result.values())[0]
    assert isinstance(study, Study)
    assert study.has_study_results is not None
    assert len(study.has_study_results) == 2

    all_sentences = []
    for tm_result in study.has_study_results:
        assert isinstance(tm_result, TextMiningStudyResult)
        if tm_result.supporting_text:
            all_sentences.extend(tm_result.supporting_text)

    assert "This drug treats the disease effectively." in all_sentences
    assert "Further studies confirmed the therapeutic effect." in all_sentences


def test_extract_supporting_studies_empty():
    """_extract_supporting_studies returns None for empty/None input."""
    assert _extract_supporting_studies({}, {}, dict(_STATE_DEFAULTS)) is None
    assert _extract_supporting_studies(None, {}, dict(_STATE_DEFAULTS)) is None


def test_edge_with_publications_info():
    """Transform attaches supporting studies from publications_info."""
    record = _base_record(
        publications_info={
            "PMID:12345678": {
                "sentence": "Aspirin effectively reduces inflammation in diabetic patients.",
                "publication date": "2020 Jan",
                "subject score": "1000",
                "object score": "900",
            },
        },
    )
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]

    assert association.has_supporting_studies is not None
    assert len(association.has_supporting_studies) == 1

    study = list(association.has_supporting_studies.values())[0]
    assert len(study.has_study_results) == 1
    assert "Aspirin effectively reduces inflammation in diabetic patients." in study.has_study_results[0].supporting_text


# ---------------------------------------------------------------------------
# PMID-checker verdict filtering
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "support,expected_publications",
    [
        ("no", [p for p in FOUR_PUBS if p != FOUR_PUBS[0]]),
        ("maybe", [p for p in FOUR_PUBS if p != FOUR_PUBS[0]]),
        ("yes", FOUR_PUBS),
        ("no_abstract", FOUR_PUBS),
        # support values are compared case- and whitespace-insensitively
        (" NO ", [p for p in FOUR_PUBS if p != FOUR_PUBS[0]]),
    ],
)
def test_verdict_support_values(support: str, expected_publications: list[str]):
    """Only 'no' and 'maybe' drop a publication; other verdicts keep it."""
    entities = _create_test_runner(_base_record(), verdicts=[_verdict(FOUR_PUBS[0], support)])
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == expected_publications


def test_publication_without_a_verdict_is_kept():
    """A PMID the checker never judged has no verdict row, and no verdict means keep."""
    entities = _create_test_runner(_base_record(), verdicts=[_verdict("PMID:99999999", "no")])
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == FOUR_PUBS


BASE_EDGE_KEY = ("CHEBI:15365", "biolink:treats_or_applied_or_studied_to_treat", "MONDO:0005148")


def test_load_verdicts_groups_rejected_pmids_per_edge(tmp_path):
    """Both rejecting verdicts of an edge land in its set, and another edge keeps its own."""
    _write_verdict_artifact(tmp_path, [
        _verdict(FOUR_PUBS[0], "no"),
        _verdict(FOUR_PUBS[1], "maybe"),
        _verdict(FOUR_PUBS[2], "yes"),
        _verdict(FOUR_PUBS[3], "no", subject="CHEBI:999999"),
    ])

    index = load_verdicts(tmp_path / VERDICT_ARTIFACT_FILENAME)

    assert index[BASE_EDGE_KEY] == {FOUR_PUBS[0], FOUR_PUBS[1]}
    assert index[("CHEBI:999999", BASE_EDGE_KEY[1], BASE_EDGE_KEY[2])] == {FOUR_PUBS[3]}


def test_load_verdicts_marks_an_edge_without_rejections_as_covered(tmp_path):
    """An edge whose verdicts all keep still counts as covered, mapped to None."""
    _write_verdict_artifact(tmp_path, [_verdict(FOUR_PUBS[0], "yes")])

    assert load_verdicts(tmp_path / VERDICT_ARTIFACT_FILENAME) == {BASE_EDGE_KEY: None}


def test_unfiltered_run_keeps_rejected_publications_and_needs_no_verdict_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEMMEDDB_UNFILTERED regenerates the pre-filter edge set: nothing is dropped, no verdicts are read."""
    monkeypatch.setattr("translator_ingest.ingests.semmeddb.semmeddb.PMID_FILTER_ENABLED", False)

    entities = _create_test_runner(
        _base_record(),
        verdicts=[_verdict(pmid, "no") for pmid in FOUR_PUBS],
        check_coverage=True,
        write_artifact=False,
    )

    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == FOUR_PUBS


def test_unfiltered_run_reports_its_own_source_version(monkeypatch):
    """A filtered and an unfiltered build must never share a directory or a build version."""
    assert get_latest_version() == "semmeddb-2023-kg2.10.3"

    monkeypatch.setattr("translator_ingest.ingests.semmeddb.semmeddb.PMID_FILTER_ENABLED", False)

    assert get_latest_version() == "semmeddb-2023-kg2.10.3-unfiltered"


def test_verdicts_for_another_edge_do_not_apply():
    """Verdicts are keyed per edge, so the same PMID on a different edge is untouched."""
    entities = _create_test_runner(
        _base_record(), verdicts=[_verdict(FOUR_PUBS[0], "no", subject="CHEBI:999999")]
    )
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == FOUR_PUBS


def test_edge_dropped_when_every_publication_is_rejected():
    """An edge that loses all of its publications is not emitted, and neither are its nodes."""
    entities = _create_test_runner(
        _base_record(), verdicts=[_verdict(pmid, "no") for pmid in FOUR_PUBS]
    )
    assert entities == []


def test_verdicts_match_the_remapped_predicate():
    """Verdicts were produced after PREDICATE_REMAP, so they key on the emitted predicate."""
    record = _base_record(predicate="biolink:preventative_for_condition")
    entities = _create_test_runner(
        record,
        verdicts=[_verdict(FOUR_PUBS[0], "no", predicate="biolink:treats_or_applied_or_studied_to_treat")],
    )
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == [p for p in FOUR_PUBS if p != FOUR_PUBS[0]]


def test_rejected_publication_drops_its_supporting_study():
    """A rejected PMID is removed from publications_info, so no study result is built for it."""
    pub_info = {pmid: {"sentence": f"Sentence for {pmid}"} for pmid in FOUR_PUBS}
    record = _base_record(publications_info=pub_info)
    entities = _create_test_runner(record, verdicts=[_verdict(FOUR_PUBS[0], "no")])
    association = [e for e in entities if isinstance(e, Association)][0]

    study = list(association.has_supporting_studies.values())[0]
    supporting_pmids = {result.xref[0] for result in study.has_study_results}
    assert supporting_pmids == set(FOUR_PUBS[1:])


def test_coverage_guard_raises_when_no_edge_has_a_verdict():
    """An artifact whose keys do not match the emitted edges must fail, not silently pass."""
    with pytest.raises(RuntimeError, match="verdict coverage"):
        _create_test_runner(
            _base_record(),
            verdicts=[_verdict(FOUR_PUBS[0], "no", subject="CHEBI:999999")],
            check_coverage=True,
        )


def test_coverage_guard_passes_when_the_edge_is_covered():
    """A covered edge satisfies the guard even when its verdicts keep every publication."""
    entities = _create_test_runner(
        _base_record(), verdicts=[_verdict(FOUR_PUBS[0], "yes")], check_coverage=True
    )
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == FOUR_PUBS


def test_missing_verdict_artifact_raises():
    """The transform cannot run without the artifact; failing fast beats filtering nothing."""
    writer = MockKozaWriter()
    with tempfile.TemporaryDirectory() as input_files_dir:
        runner = KozaRunner(
            data=[_base_record()],
            writer=writer,
            hooks=KozaTransformHooks(
                on_data_begin=[on_begin_filter_edges], transform_record=[transform_semmeddb_edge]
            ),
            input_files_dir=Path(input_files_dir),
        )
        with pytest.raises(FileNotFoundError, match=VERDICT_ARTIFACT_FILENAME):
            runner.run()


# ---------------------------------------------------------------------------
# Text locations
# ---------------------------------------------------------------------------

def _located_record(**overrides: object) -> Record:
    """Return _base_record with kg2_ids [BASE_KG2_ID] and LOCATED_EVIDENCE for every publication."""
    located: Record = {
        "kg2_ids": [BASE_KG2_ID],
        "publications_info": {pmid: dict(LOCATED_EVIDENCE) for pmid in FOUR_PUBS},
    }
    return _base_record(**{**located, **overrides})


def _study_results(entities: Entities) -> list[TextMiningStudyResult]:
    """Return the study results of the single association among the emitted entities."""
    association = [e for e in entities if isinstance(e, Association)][0]
    study = list(association.has_supporting_studies.values())[0]
    return study.has_study_results


def _locations_by_pmid(entities: Entities) -> dict[str, tuple[list[int] | None, list[int] | None]]:
    """Return each study result's (subject, object) location, keyed by its PMID."""
    return {
        result.xref[0]: (result.subject_location_in_text, result.object_location_in_text)
        for result in _study_results(entities)
    }


def test_text_locations_attached_for_the_matching_evidence() -> None:
    """Each study result gets its evidence's spans as [start, end], 0-based and end exclusive."""
    record = _located_record(publications_info={
        FOUR_PUBS[0]: dict(LOCATED_EVIDENCE),
        FOUR_PUBS[1]: dict(OTHER_EVIDENCE),
    })
    entities = _create_test_runner(record, text_locations=[
        _location(FOUR_PUBS[0]),
        _location(FOUR_PUBS[1], OTHER_SUBJECT_SPAN, OTHER_OBJECT_SPAN, evidence=OTHER_EVIDENCE),
    ])

    assert _locations_by_pmid(entities) == {
        FOUR_PUBS[0]: (list(SUBJECT_SPAN), list(OBJECT_SPAN)),
        FOUR_PUBS[1]: (list(OTHER_SUBJECT_SPAN), list(OTHER_OBJECT_SPAN)),
    }
    for result in _study_results(entities):
        sentence = result.supporting_text[0]
        subject_start, subject_end = result.subject_location_in_text
        object_start, object_end = result.object_location_in_text
        assert sentence[subject_start:subject_end].lower() == "aspirin"
        assert sentence[object_start:object_end] == "diabetes"


@pytest.mark.parametrize(
    "text_locations",
    [
        [],
        [_location(pmid, kg2_id=PREVENTS_KG2_ID) for pmid in FOUR_PUBS],
        [_location("PMID:99999999")],
        # a stale artifact: the spans were found in a sentence the record no longer carries
        [_location(pmid, evidence=STALE_EVIDENCE) for pmid in FOUR_PUBS],
    ],
    ids=["no rows", "rows for another kg2_id", "rows for another PMID", "rows for an old sentence"],
)
def test_study_result_without_a_location_row_gets_none(text_locations: list[TextLocationRow]) -> None:
    """A study result whose (kg2_id, evidence) has no row in the artifact gets no location."""
    entities = _create_test_runner(_located_record(), text_locations=text_locations)

    assert _locations_by_pmid(entities) == {pmid: (None, None) for pmid in FOUR_PUBS}


@pytest.mark.parametrize(
    "predicate,kg2_id,subject_span",
    [
        ("biolink:treats_or_applied_or_studied_to_treat", BASE_KG2_ID, SUBJECT_SPAN),
        ("biolink:preventative_for_condition", PREVENTS_KG2_ID, (0, 3)),
    ],
    ids=["treats record", "prevents record"],
)
def test_records_with_the_same_edge_key_get_the_spans_of_their_own_kg2_id(
    predicate: str, kg2_id: str, subject_span: tuple[int, int]
) -> None:
    """Both records emit the same edge with the same PMID and sentence, but keep their own spans."""
    text_locations = [_location(pmid, kg2_id=BASE_KG2_ID) for pmid in FOUR_PUBS] + [
        _location(pmid, subject_span=(0, 3), kg2_id=PREVENTS_KG2_ID) for pmid in FOUR_PUBS
    ]
    record = _located_record(predicate=predicate, kg2_ids=[kg2_id])

    entities = _create_test_runner(record, text_locations=text_locations)

    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.predicate == "biolink:treats_or_applied_or_studied_to_treat"
    assert _locations_by_pmid(entities) == {
        pmid: (list(subject_span), list(OBJECT_SPAN)) for pmid in FOUR_PUBS
    }


@pytest.mark.parametrize(
    "evidence,subject_span,object_span",
    [
        (LOCATED_EVIDENCE, SUBJECT_SPAN, OBJECT_SPAN),
        (OTHER_EVIDENCE, OTHER_SUBJECT_SPAN, OTHER_OBJECT_SPAN),
        (RESCORED_EVIDENCE, SUBJECT_SPAN, (15, 20)),
    ],
    ids=["first evidence", "another sentence", "same sentence, other scores"],
)
def test_records_sharing_a_kg2_id_get_the_spans_of_their_own_evidence(
    evidence: dict[str, str], subject_span: tuple[int, int], object_span: tuple[int, int]
) -> None:
    """Records sharing one kg2_id each get the spans of their own evidence for a PMID."""
    text_locations = [
        _location(FOUR_PUBS[0]),
        _location(FOUR_PUBS[0], OTHER_SUBJECT_SPAN, OTHER_OBJECT_SPAN, evidence=OTHER_EVIDENCE),
        _location(FOUR_PUBS[0], SUBJECT_SPAN, (15, 20), evidence=RESCORED_EVIDENCE),
    ]
    record = _located_record(publications_info={FOUR_PUBS[0]: dict(evidence)})

    entities = _create_test_runner(record, text_locations=text_locations)

    assert _locations_by_pmid(entities) == {FOUR_PUBS[0]: (list(subject_span), list(object_span))}


def test_null_evidence_score_emits_the_study_without_a_location() -> None:
    """A null score does not stop the transform; that evidence just finds no span."""
    text_locations = [_location(pmid) for pmid in FOUR_PUBS]
    unscored: dict[str, str | None] = {**LOCATED_EVIDENCE, "subject score": None}
    publications_info: dict[str, dict[str, str | None]] = {
        pmid: dict(LOCATED_EVIDENCE) for pmid in FOUR_PUBS
    }
    publications_info[FOUR_PUBS[0]] = unscored

    entities = _create_test_runner(
        _located_record(publications_info=publications_info), text_locations=text_locations
    )

    located = (list(SUBJECT_SPAN), list(OBJECT_SPAN))
    assert _locations_by_pmid(entities) == {
        FOUR_PUBS[0]: (None, None),
        **{pmid: located for pmid in FOUR_PUBS[1:]},
    }


@pytest.mark.parametrize(
    "kg2_ids",
    [None, 1, [], [None], [BASE_KG2_ID, PREVENTS_KG2_ID]],
    ids=["null kg2_ids", "scalar kg2_ids", "no kg2_id", "null kg2_id entry", "two kg2_ids"],
)
def test_record_without_exactly_one_kg2_id_gets_no_location(kg2_ids: list[str | None] | int | None) -> None:
    """Spans are keyed by the record's single kg2_id, so a record with none or several gets none."""
    text_locations = [
        _location(pmid, kg2_id=kg2_id)
        for pmid in FOUR_PUBS
        for kg2_id in (kg2_ids if isinstance(kg2_ids, list) else [])
        if kg2_id is not None
    ]

    entities = _create_test_runner(_located_record(kg2_ids=kg2_ids), text_locations=text_locations)

    assert _locations_by_pmid(entities) == {pmid: (None, None) for pmid in FOUR_PUBS}


@pytest.mark.parametrize(
    "subject_span,object_span",
    [
        ((0, len(LOCATED_SENTENCE) + 1), OBJECT_SPAN),
        (SUBJECT_SPAN, (-1, OBJECT_SPAN[1])),
        ((SUBJECT_SPAN[1], SUBJECT_SPAN[1]), OBJECT_SPAN),
        (SUBJECT_SPAN, (OBJECT_SPAN[1], OBJECT_SPAN[0])),
    ],
    ids=["end past the sentence", "negative start", "empty span", "start after end"],
)
def test_out_of_bounds_span_is_skipped_and_counted(
    subject_span: tuple[int, int], object_span: tuple[int, int]
) -> None:
    """A span outside the sentence attaches neither location, and only that result is counted."""
    state: dict[str, Any] = dict(_STATE_DEFAULTS)
    publications_info = {pmid: dict(LOCATED_EVIDENCE) for pmid in FOUR_PUBS[:2]}
    first, second = (evidence_fingerprint(pmid, LOCATED_EVIDENCE) for pmid in FOUR_PUBS[:2])
    text_spans = {
        first: TextSpans(*subject_span, *object_span),
        second: TextSpans(*SUBJECT_SPAN, *OBJECT_SPAN),
    }

    studies = _extract_supporting_studies(publications_info, text_spans, state)

    assert studies is not None
    results = {result.xref[0]: result for result in list(studies.values())[0].has_study_results}
    assert results[FOUR_PUBS[0]].subject_location_in_text is None
    assert results[FOUR_PUBS[0]].object_location_in_text is None
    assert results[FOUR_PUBS[1]].subject_location_in_text == list(SUBJECT_SPAN)
    assert results[FOUR_PUBS[1]].object_location_in_text == list(OBJECT_SPAN)
    assert state["text_mining_results_emitted"] == 2
    assert state["text_mining_results_with_locations"] == 1
    assert state["text_locations_out_of_bounds"] == 1


def test_pmid_rejected_by_the_verdicts_gets_no_study_result_despite_a_location() -> None:
    """A rejected PMID builds no study result, so it gets no location and is not counted."""
    entities, metadata = _run_transform(
        _located_record(),
        verdicts=[_verdict(FOUR_PUBS[0], "no")],
        text_locations=[_location(pmid) for pmid in FOUR_PUBS],
        check_coverage=True,
    )

    assert _locations_by_pmid(entities) == {
        pmid: (list(SUBJECT_SPAN), list(OBJECT_SPAN)) for pmid in FOUR_PUBS[1:]
    }
    assert metadata["text_locations"]["text_mining_results_emitted"] == len(FOUR_PUBS) - 1
    assert metadata["text_locations"]["text_mining_results_with_locations"] == len(FOUR_PUBS) - 1


OUT_OF_BOUNDS_OBJECT_SPAN = (0, len(LOCATED_SENTENCE) + 1)


@pytest.mark.parametrize(
    "text_locations,error",
    [
        ([_location(pmid, kg2_id=PREVENTS_KG2_ID) for pmid in FOUR_PUBS], "only 0 of 4"),
        (
            [_location(pmid, object_span=OUT_OF_BOUNDS_OBJECT_SPAN) for pmid in FOUR_PUBS],
            "4 had a span outside",
        ),
    ],
    ids=["keys do not match", "every span out of bounds"],
)
def test_text_location_coverage_guard_raises_when_no_study_result_is_located(
    text_locations: list[TextLocationRow], error: str
) -> None:
    """An artifact whose keys or spans do not fit the emitted results must fail, not pass."""
    with pytest.raises(RuntimeError, match=f"Text-location coverage.*{error}"):
        _create_test_runner(
            _located_record(),
            verdicts=[_verdict(FOUR_PUBS[0], "yes")],
            text_locations=text_locations,
            check_coverage=True,
        )


def test_both_coverage_guards_report_together() -> None:
    """When both artifacts stop matching, one error names both guards, so neither hides the other."""
    with pytest.raises(RuntimeError) as raised:
        _create_test_runner(
            _located_record(),
            verdicts=[_verdict(FOUR_PUBS[0], "no", subject="CHEBI:999999")],
            text_locations=[],
            check_coverage=True,
        )

    assert "Text-location coverage" in str(raised.value)
    assert "PMID-checker verdict coverage" in str(raised.value)


def test_text_location_coverage_guard_passes_and_records_the_counts() -> None:
    """With every study result located the guard passes, and the counts land in the metadata."""
    _, metadata = _run_transform(
        _located_record(),
        verdicts=[_verdict(FOUR_PUBS[0], "yes")],
        text_locations=[_location(pmid) for pmid in FOUR_PUBS],
        check_coverage=True,
    )

    assert metadata["text_locations"] == {
        "text_mining_results_emitted": len(FOUR_PUBS),
        "text_mining_results_with_locations": len(FOUR_PUBS),
        "text_locations_out_of_bounds": 0,
        "location_coverage": 1.0,
        "min_location_coverage": MIN_TEXT_LOCATION_COVERAGE,
    }


@pytest.mark.parametrize("pmid_filter_enabled", [True, False], ids=["filtered", "unfiltered"])
def test_missing_text_location_artifact_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, pmid_filter_enabled: bool
) -> None:
    """Locations are loaded with or without the PMID filter, and the transform needs them."""
    monkeypatch.setattr(
        "translator_ingest.ingests.semmeddb.semmeddb.PMID_FILTER_ENABLED", pmid_filter_enabled
    )
    _write_verdict_artifact(tmp_path, [])
    transform = KozaTransform(
        extra_fields={}, writer=MockKozaWriter(), mappings={}, input_files_dir=tmp_path
    )

    with pytest.raises(FileNotFoundError, match=TEXT_LOCATION_ARTIFACT_FILENAME):
        on_begin_filter_edges(transform)


def test_load_text_locations_maps_each_kg2_id_to_its_evidence_spans(tmp_path: Path) -> None:
    """Each kg2_id maps to the spans of its own rows, keyed by evidence fingerprint."""
    _write_text_location_artifact(tmp_path, [
        _location(FOUR_PUBS[1], subject_span=(1, 2), object_span=(3, 4)),
        _location(FOUR_PUBS[0], kg2_id=PREVENTS_KG2_ID),
        _location(FOUR_PUBS[0]),
    ])

    index = load_text_locations(tmp_path / TEXT_LOCATION_ARTIFACT_FILENAME)

    located = TextSpans(*SUBJECT_SPAN, *OBJECT_SPAN)
    first, second = (evidence_fingerprint(pmid, LOCATED_EVIDENCE) for pmid in FOUR_PUBS[:2])
    assert len(index) == 2
    assert index.spans_for(BASE_KG2_ID) == {first: located, second: TextSpans(1, 2, 3, 4)}
    assert index.spans_for(PREVENTS_KG2_ID) == {first: located}
    assert index.spans_for(UNKNOWN_KG2_ID) == {}


def test_loaders_read_the_exact_path_not_a_glob_match(tmp_path: Path) -> None:
    """A bracketed directory name is read literally, never as a glob that matches a decoy sibling."""
    literal, decoy = tmp_path / "in[1]", tmp_path / "in1"
    literal.mkdir()
    decoy.mkdir()
    _write_text_location_artifact(literal, [_location(FOUR_PUBS[0])])
    _write_text_location_artifact(decoy, [_location(FOUR_PUBS[0], kg2_id=PREVENTS_KG2_ID)])
    _write_verdict_artifact(literal, [_verdict(FOUR_PUBS[0], "yes")])
    _write_verdict_artifact(decoy, [_verdict(FOUR_PUBS[0], "no")])

    index = load_text_locations(literal / TEXT_LOCATION_ARTIFACT_FILENAME)

    assert index.spans_for(BASE_KG2_ID) != {}
    assert index.spans_for(PREVENTS_KG2_ID) == {}
    assert load_verdicts(literal / VERDICT_ARTIFACT_FILENAME) == {BASE_EDGE_KEY: None}


ONE_LOCATION = [_location(FOUR_PUBS[0])]
INT64_SPAN_SCHEMA = {**TEXT_LOCATION_ARTIFACT_SCHEMA, "subject_start": pl.Int64()}
# Rows that break the (kg2_id, evidence_fingerprint) order: by kg2_id, then by fingerprint.
KG2_IDS_OUT_OF_ORDER = sorted(
    (_location(FOUR_PUBS[0], kg2_id=kg2_id) for kg2_id in (BASE_KG2_ID, PREVENTS_KG2_ID)),
    key=lambda row: row[TEXT_LOCATION_KG2_ID_COLUMN],
    reverse=True,
)
FINGERPRINTS_OUT_OF_ORDER = sorted(
    (_location(pmid) for pmid in FOUR_PUBS[:2]),
    key=lambda row: row[TEXT_LOCATION_FINGERPRINT_COLUMN],
    reverse=True,
)


@pytest.mark.parametrize(
    "artifact,error",
    [
        (_text_location_frame(ONE_LOCATION * 2), "more than one row"),
        (_text_location_frame(KG2_IDS_OUT_OF_ORDER), "not sorted"),
        (_text_location_frame(FINGERPRINTS_OUT_OF_ORDER), "not sorted"),
        (pl.DataFrame(ONE_LOCATION, schema=INT64_SPAN_SCHEMA), "has columns"),
        (_text_location_frame(ONE_LOCATION).drop("object_end"), "has columns"),
        (
            _text_location_frame(ONE_LOCATION).with_columns(subject_end=pl.lit(None, pl.Int32)),
            "null values",
        ),
    ],
    ids=[
        "duplicate (kg2_id, fingerprint)",
        "kg2_ids out of order",
        "fingerprints out of order",
        "wrong dtype",
        "missing column",
        "null span",
    ],
)
def test_load_text_locations_rejects_an_artifact_that_breaks_its_contract(
    tmp_path: Path, artifact: pl.DataFrame, error: str
) -> None:
    """The loader fails fast instead of attaching locations from a malformed artifact."""
    artifact.write_parquet(tmp_path / TEXT_LOCATION_ARTIFACT_FILENAME)

    with pytest.raises(ValueError, match=error):
        load_text_locations(tmp_path / TEXT_LOCATION_ARTIFACT_FILENAME)


# ---------------------------------------------------------------------------
# Publication capping
# ---------------------------------------------------------------------------

def test_publications_under_cap_are_unchanged():
    """Edges at or under MAX_PUBLICATIONS_PER_EDGE keep every publication."""
    pubs = [f"PMID:{i}" for i in range(1, MAX_PUBLICATIONS_PER_EDGE + 1)]
    entities = _create_test_runner(_base_record(publications=pubs))
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == pubs


def test_publications_over_cap_trimmed_to_most_recent():
    """Oversized edges keep the highest-numbered (most recent) PMIDs, in their original order."""
    pubs = [f"PMID:{i}" for i in range(1, MAX_PUBLICATIONS_PER_EDGE + 101)]
    entities = _create_test_runner(_base_record(publications=pubs))
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == pubs[100:]


def test_cap_applies_after_verdict_filtering():
    """Rejected publications are removed first, so they do not use up the cap."""
    pubs = [f"PMID:{i}" for i in range(1, MAX_PUBLICATIONS_PER_EDGE + 101)]
    # reject the 100 most recent, leaving exactly the cap, so nothing is trimmed
    verdicts = [_verdict(pmid, "no") for pmid in pubs[-100:]]
    entities = _create_test_runner(_base_record(publications=pubs), verdicts=verdicts)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == pubs[:-100]


def test_capped_publications_drop_their_supporting_studies():
    """Capped-out PMIDs are removed from publications_info like rejected ones."""
    pubs = [f"PMID:{i}" for i in range(1, MAX_PUBLICATIONS_PER_EDGE + 101)]
    pub_info = {pmid: {"sentence": f"Sentence for {pmid}"} for pmid in pubs}
    entities = _create_test_runner(_base_record(publications=pubs, publications_info=pub_info))
    association = [e for e in entities if isinstance(e, Association)][0]

    study = list(association.has_supporting_studies.values())[0]
    assert len(study.has_study_results) == MAX_PUBLICATIONS_PER_EDGE


def test_uncapped_mode_skips_capping(monkeypatch: pytest.MonkeyPatch):
    """Setting SEMMEDDB_UNCAPPED=1 disables publication capping entirely."""
    monkeypatch.setenv("SEMMEDDB_UNCAPPED", "1")
    importlib.reload(semmeddb_module)

    pubs = [f"PMID:{i}" for i in range(1, MAX_PUBLICATIONS_PER_EDGE + 101)]
    writer = MockKozaWriter()
    with tempfile.TemporaryDirectory() as input_files_dir:
        _write_verdict_artifact(Path(input_files_dir), [])
        _write_text_location_artifact(Path(input_files_dir), [])
        runner = KozaRunner(
            data=[_base_record(publications=pubs)],
            writer=writer,
            hooks=KozaTransformHooks(
                on_data_begin=[semmeddb_module.on_begin_filter_edges],
                transform_record=[semmeddb_module.transform_semmeddb_edge],
            ),
            input_files_dir=Path(input_files_dir),
        )
        runner.run()
    association = [e for e in writer.items if isinstance(e, Association)][0]
    assert association.publications == pubs

    monkeypatch.delenv("SEMMEDDB_UNCAPPED")
    importlib.reload(semmeddb_module)


# ---------------------------------------------------------------------------
# Predicate remapping
# ---------------------------------------------------------------------------

def test_preventative_predicate_remapped():
    """preventative_for_condition is remapped to treats_or_applied_or_studied_to_treat."""
    record = _base_record(predicate="biolink:preventative_for_condition")
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.predicate == "biolink:treats_or_applied_or_studied_to_treat"


# ---------------------------------------------------------------------------
# Causal gene -> disease / phenotype variant qualifier
# ---------------------------------------------------------------------------

def test_gene_causes_disease_gets_variant_qualifier():
    """Gene -> Disease with biolink:causes gets CausalGeneToDiseaseAssociation."""
    record = _base_record(
        subject="NCBIGene:100",
        object="MONDO:0005148",
        predicate="biolink:causes",
    )
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert isinstance(association, CausalGeneToDiseaseAssociation)
    assert association.subject_form_or_variant_qualifier == "genetic_variant_form"


def test_protein_causes_phenotype_gets_variant_qualifier():
    """Protein -> PhenotypicFeature with biolink:causes gets GeneToPhenotypicFeatureAssociation."""
    record = _base_record(
        subject="PR:P12345",
        object="HP:0000118",
        predicate="biolink:causes",
    )
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert isinstance(association, GeneToPhenotypicFeatureAssociation)
    assert association.subject_form_or_variant_qualifier == "genetic_variant_form"


def test_causes_without_disease_or_phenotype_no_variant_qualifier():
    """Gene -> NamedThing with biolink:causes stays as plain Association."""
    record = _base_record(
        subject="NCBIGene:100",
        object="UMLS:C0011847",
        predicate="biolink:causes",
    )
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert type(association) is Association


# ---------------------------------------------------------------------------
# Pydantic roundtrip edge fixtures
# ---------------------------------------------------------------------------

TEST_SOURCES = [
    RetrievalSource(
        id="infores:semmeddb",
        resource_id="infores:semmeddb",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": Association,
        "params": {
            "id": "uuid:test-semmeddb-1",
            "subject": "CHEBI:15365",
            "predicate": "biolink:treats_or_applied_or_studied_to_treat",
            "object": "MONDO:0005148",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "uuid:test-semmeddb-2",
            "subject": "CHEBI:15365",
            "predicate": "biolink:affects",
            "object": "NCBIGene:100",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": "activity",
            "object_direction_qualifier": "increased",
        },
    },
    {
        "association_class": GeneAffectsChemicalAssociation,
        "params": {
            "id": "uuid:test-semmeddb-3",
            "subject": "NCBIGene:100",
            "predicate": "biolink:affects",
            "object": "CHEBI:15365",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": "activity",
            "object_direction_qualifier": "decreased",
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "uuid:test-semmeddb-4",
            "subject": "NCBIGene:100",
            "predicate": "biolink:affects",
            "object": "HGNC:200",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": "activity_or_abundance",
            "object_direction_qualifier": "increased",
        },
    },
    {
        "association_class": CausalGeneToDiseaseAssociation,
        "params": {
            "id": "uuid:test-semmeddb-5",
            "subject": "NCBIGene:100",
            "predicate": "biolink:causes",
            "object": "MONDO:0005148",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
        },
    },
    {
        "association_class": GeneToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:test-semmeddb-6",
            "subject": "NCBIGene:100",
            "predicate": "biolink:causes",
            "object": "HP:0000118",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalAffectsBiologicalEntityAssociation,
        "params": {
            "id": "uuid:test-semmeddb-7",
            "subject": "CHEBI:15365",
            "predicate": "biolink:affects",
            "object": "UBERON:0000001",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
            "sources": TEST_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f["association_class"].__name__,
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj


@pytest.mark.parametrize("module", [semmeddb_module, text_locations_module])
def test_module_doctests_pass(module: ModuleType) -> None:
    """The doctests of the transform and of the artifact contract pass, including the pinned
    evidence fingerprint that the built artifact is keyed by."""
    results = doctest.testmod(module)
    assert results.failed == 0
    assert results.attempted > 0
