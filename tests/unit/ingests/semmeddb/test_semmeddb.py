import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    CausalGeneToDiseaseAssociation,
    ChemicalAffectsBiologicalEntityAssociation,
    ChemicalAffectsGeneAssociation,
    ChemicalEntity,
    Disease,
    Gene,
    GeneAffectsChemicalAssociation,
    GeneToPhenotypicFeatureAssociation,
    GeneRegulatesGeneAssociation,
    KnowledgeLevelEnum,
    NamedThing,
    Protein,
    Study,
    TextMiningStudyResult,
)
from koza.runner import KozaRunner, KozaTransformHooks

from tests.unit.ingests import MockKozaWriter
from translator_ingest.ingests.semmeddb.semmeddb import (
    PUBLICATIONS_CAP_THRESHOLD,
    _cap_publications,
    _extract_supporting_studies,
    _has_bte_excluded_predicate,
    _make_node,
    transform_semmeddb_edge,
)

FOUR_PUBS = ["PMID:11111111", "PMID:22222222", "PMID:33333333", "PMID:44444444"]


def _create_test_runner(record: dict) -> list:
    """Run a single record through the transform and return emitted entities."""
    writer = MockKozaWriter()
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_semmeddb_edge]),
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
    """Gene subject + Gene object with qualifiers -> GeneRegulatesGeneAssociation."""
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
    assert isinstance(assoc, GeneRegulatesGeneAssociation)
    assert assoc.object_aspect_qualifier == "activity_or_abundance"


def test_qualifier_fallback_named_thing():
    """NamedThing subject + NamedThing object -> ChemicalAffectsBiologicalEntityAssociation."""
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
    assert isinstance(assoc, ChemicalAffectsBiologicalEntityAssociation)
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
# Publication capping
# ---------------------------------------------------------------------------

def test_cap_publications_under_threshold():
    """Publications lists at or under PUBLICATIONS_CAP_THRESHOLD are returned unchanged."""
    pubs = [f"PMID:{i}" for i in range(PUBLICATIONS_CAP_THRESHOLD)]
    info = {p: {"subject score": "500", "object score": "500"} for p in pubs}
    result_pubs, result_info = _cap_publications(pubs, info)
    assert result_pubs == pubs
    assert result_info == info


def test_cap_publications_over_threshold():
    """Publications lists over the threshold are trimmed by score + recency union."""
    count = PUBLICATIONS_CAP_THRESHOLD * 3
    pubs = [f"PMID:{i}" for i in range(count)]
    info = {
        p: {
            "subject score": str(i),
            "object score": str(i),
            "publication date": f"{2000 + (i % 26)} Jan",
        }
        for i, p in enumerate(pubs)
    }
    result_pubs, result_info = _cap_publications(pubs, info)
    assert len(result_pubs) <= 200
    assert len(result_pubs) > 0
    assert len(result_pubs) < count
    assert set(result_info.keys()) == set(result_pubs)


def test_cap_publications_integration():
    """Transform caps publications for edges exceeding PUBLICATIONS_CAP_THRESHOLD."""
    count = PUBLICATIONS_CAP_THRESHOLD + 100
    many_pubs = [f"PMID:{i}" for i in range(count)]
    pub_info = {
        p: {
            "sentence": f"Sentence for {p}",
            "subject score": str(i * 10),
            "object score": str(i * 10),
            "publication date": f"{2000 + (i % 26)} Jan",
        }
        for i, p in enumerate(many_pubs)
    }
    record = _base_record(
        publications=many_pubs,
        publications_info=pub_info,
    )
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert len(association.publications) <= 200
    assert len(association.publications) < count


def test_uncapped_mode_skips_capping(monkeypatch: pytest.MonkeyPatch):
    """Setting SEMMEDDB_UNCAPPED=1 disables publication capping entirely."""
    monkeypatch.setenv("SEMMEDDB_UNCAPPED", "1")
    import importlib
    import translator_ingest.ingests.semmeddb.semmeddb as semmeddb_mod
    importlib.reload(semmeddb_mod)

    count = PUBLICATIONS_CAP_THRESHOLD + 100
    many_pubs = [f"PMID:{i}" for i in range(count)]
    pub_info = {
        p: {
            "sentence": f"Sentence for {p}",
            "subject score": str(i * 10),
            "object score": str(i * 10),
            "publication date": f"{2000 + (i % 26)} Jan",
        }
        for i, p in enumerate(many_pubs)
    }
    record = _base_record(
        publications=many_pubs,
        publications_info=pub_info,
    )
    entities = _create_test_runner(record)
    association = [e for e in entities if isinstance(e, Association)][0]
    assert len(association.publications) == count

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
