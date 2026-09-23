import tempfile
from pathlib import Path

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

from tests.unit.ingests import MockKozaWriter
from translator_ingest.ingests.semmeddb.semmeddb import (
    MAX_PUBLICATIONS_PER_EDGE,
    VERDICT_ARTIFACT_COLUMNS,
    VERDICT_ARTIFACT_FILENAME,
    _extract_supporting_studies,
    _has_bte_excluded_predicate,
    _make_node,
    get_latest_version,
    on_begin_filter_edges,
    on_end_filter_edges,
    transform_semmeddb_edge,
)

FOUR_PUBS = ["PMID:11111111", "PMID:22222222", "PMID:33333333", "PMID:44444444"]

# Column order of the LLM PMID-checker results parquet the transform reads.
VERDICT_SCHEMA: dict[str, pl.DataType] = {column: pl.String() for column in VERDICT_ARTIFACT_COLUMNS}


def _verdict(
    pmid: str,
    support: str = "no",
    subject: str = "CHEBI:15365",
    predicate: str = "biolink:treats_or_applied_or_studied_to_treat",
    obj: str = "MONDO:0005148",
) -> dict[str, str]:
    """Return one verdict row, keyed like _base_record's edge by default."""
    return {
        "subject_curie": subject,
        "predicate": predicate,
        "object_curie": obj,
        "PMID": pmid,
        "support": support,
    }


def _write_verdict_artifact(directory: Path, verdicts: list[dict[str, str]]) -> None:
    """Write the verdict parquet the transform loads from its input files directory."""
    pl.DataFrame(verdicts, schema=VERDICT_SCHEMA).write_parquet(directory / VERDICT_ARTIFACT_FILENAME)


def _create_test_runner(
    record: dict,
    verdicts: list[dict[str, str]] | None = None,
    check_coverage: bool = False,
    write_artifact: bool = True,
) -> list:
    """Run a single record through the transform and return emitted entities.

    The transform loads the PMID-checker verdicts in its on_data_begin hook, so every run needs
    an artifact; with no ``verdicts`` it is empty, meaning no publication is rejected. The
    on_data_end hook, which enforces the coverage guard, only runs when ``check_coverage`` is set.
    ``write_artifact`` is set to False to run without an artifact at all, which is what an
    unfiltered run does.
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
        runner = KozaRunner(
            data=[record],
            writer=writer,
            hooks=hooks,
            input_files_dir=Path(input_files_dir),
        )
        runner.run()
    return writer.items


def _base_record(**overrides: object) -> dict:
    """Return a minimal valid KG2 edge record, with optional field overrides."""
    record: dict = {
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
    result = _extract_supporting_studies(publications_info)
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
    assert _extract_supporting_studies({}) is None
    assert _extract_supporting_studies(None) is None


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


def test_unfiltered_run_keeps_rejected_publications_and_needs_no_artifact(monkeypatch):
    """SEMMEDDB_UNFILTERED regenerates the pre-filter edge set, so nothing is dropped or required."""
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
    import importlib
    import translator_ingest.ingests.semmeddb.semmeddb as semmeddb_mod
    importlib.reload(semmeddb_mod)

    pubs = [f"PMID:{i}" for i in range(1, MAX_PUBLICATIONS_PER_EDGE + 101)]
    writer = MockKozaWriter()
    with tempfile.TemporaryDirectory() as input_files_dir:
        _write_verdict_artifact(Path(input_files_dir), [])
        runner = KozaRunner(
            data=[_base_record(publications=pubs)],
            writer=writer,
            hooks=KozaTransformHooks(
                on_data_begin=[semmeddb_mod.on_begin_filter_edges],
                transform_record=[semmeddb_mod.transform_semmeddb_edge],
            ),
            input_files_dir=Path(input_files_dir),
        )
        runner.run()
    association = [e for e in writer.items if isinstance(e, Association)][0]
    assert association.publications == pubs

    monkeypatch.delenv("SEMMEDDB_UNCAPPED")
    importlib.reload(semmeddb_mod)


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
