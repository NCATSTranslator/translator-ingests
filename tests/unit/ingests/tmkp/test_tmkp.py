"""Tests for TMKP ingest validation functions and transforms."""

import json

import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsGeneAssociation,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    CorrelatedGeneToDiseaseAssociation,
    GeneRegulatesGeneAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    Study,
    TextMiningStudyResult,
)
from bmt.pydantic import entity_id
from koza.runner import KozaRunner, KozaTransformHooks

from tests.unit.ingests import MockKozaWriter
from translator_ingest.ingests.tmkp.tmkp import (
    _get_id_prefix,
    _get_valid_prefixes_for_class,
    _get_predicate_domain_range_prefixes,
    _validate_edge_prefixes,
    _reset_module_state,
    _warned_unmapped_attrs,
    get_skipped_edges_summary,
    _skipped_edges_by_prefix,
    parse_attributes,
    transform_tmkp_edge,
    INFORES_TEXT_MINING_KP,
    PREDICATE_REMAP,
    MODIFIED_FORM,
)


@pytest.fixture(autouse=True)
def clean_module_state():
    """Reset module-level mutable state before each test."""
    _reset_module_state()


class TestGetIdPrefix:
    """Tests for _get_id_prefix function."""

    @pytest.mark.parametrize(
        "curie,expected",
        [
            ("MONDO:0008315", "MONDO"),
            ("DRUGBANK:DB01248", "DRUGBANK"),
            ("CHEBI:15365", "CHEBI"),
            ("NCBIGene:1234", "NCBIGene"),
            ("HP:0001234", "HP"),
        ],
    )
    def test_extracts_prefix_from_valid_curie(self, curie: str, expected: str):
        assert _get_id_prefix(curie) == expected

    @pytest.mark.parametrize(
        "curie",
        [
            "invalid",
            "nocolon",
            "",
        ],
    )
    def test_returns_empty_string_for_invalid_curie(self, curie: str):
        assert _get_id_prefix(curie) == ""


class TestGetValidPrefixesForClass:
    """Tests for _get_valid_prefixes_for_class function."""

    def test_chemical_entity_includes_drugbank(self):
        prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        assert "DRUGBANK" in prefixes

    def test_chemical_entity_includes_chebi(self):
        prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        assert "CHEBI" in prefixes

    def test_disease_or_phenotypic_feature_includes_mondo(self):
        prefixes = _get_valid_prefixes_for_class("DiseaseOrPhenotypicFeature")
        assert "MONDO" in prefixes

    def test_disease_or_phenotypic_feature_includes_hp(self):
        prefixes = _get_valid_prefixes_for_class("DiseaseOrPhenotypicFeature")
        assert "HP" in prefixes

    def test_gene_or_gene_product_includes_hgnc(self):
        prefixes = _get_valid_prefixes_for_class("GeneOrGeneProduct")
        assert "HGNC" in prefixes

    def test_gene_or_gene_product_includes_ncbigene(self):
        prefixes = _get_valid_prefixes_for_class("GeneOrGeneProduct")
        assert "NCBIGene" in prefixes

    def test_includes_descendant_prefixes(self):
        """Prefixes from subclasses should be included via descendant traversal."""
        # SmallMolecule is a descendant of ChemicalEntity
        chemical_prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        small_mol_prefixes = _get_valid_prefixes_for_class("SmallMolecule")
        # All SmallMolecule prefixes should appear in ChemicalEntity's set
        assert small_mol_prefixes.issubset(chemical_prefixes)

    def test_returns_frozenset(self):
        prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        assert isinstance(prefixes, frozenset)

    def test_unknown_class_raises_value_error(self):
        # BMT raises ValueError for invalid Biolink classes
        with pytest.raises(ValueError, match="not a valid biolink component"):
            _get_valid_prefixes_for_class("NonExistentClass")


class TestGetPredicateDomainRangePrefixes:
    """Tests for _get_predicate_domain_range_prefixes function."""

    def test_treats_returns_chemical_domain_and_disease_range(self):
        result = _get_predicate_domain_range_prefixes("biolink:treats")
        assert result is not None
        domain_prefixes, range_prefixes = result
        assert "DRUGBANK" in domain_prefixes
        assert "MONDO" in range_prefixes

    def test_gene_associated_with_condition_has_gene_domain(self):
        result = _get_predicate_domain_range_prefixes("biolink:gene_associated_with_condition")
        assert result is not None
        domain_prefixes, _ = result
        assert "HGNC" in domain_prefixes

    def test_unknown_predicate_returns_none(self):
        result = _get_predicate_domain_range_prefixes("biolink:nonexistent_predicate")
        assert result is None

    def test_returns_tuple_of_frozensets(self):
        result = _get_predicate_domain_range_prefixes("biolink:treats")
        assert result is not None
        domain_prefixes, range_prefixes = result
        assert isinstance(domain_prefixes, frozenset)
        assert isinstance(range_prefixes, frozenset)


class TestValidateEdgePrefixes:
    """Tests for _validate_edge_prefixes function."""

    def test_valid_treats_edge_chemical_to_disease(self):
        # Chemical treating disease is valid
        assert _validate_edge_prefixes(
            "DRUGBANK:DB01248", "MONDO:0008315", "biolink:treats"
        )

    def test_invalid_treats_edge_disease_to_chemical(self):
        # Disease treating chemical is invalid (reversed)
        assert not _validate_edge_prefixes(
            "MONDO:0008315", "DRUGBANK:DB01248", "biolink:treats"
        )

    def test_valid_treats_edge_chebi_to_mondo(self):
        # CHEBI chemical treating MONDO disease is valid
        assert _validate_edge_prefixes(
            "CHEBI:15365", "MONDO:0005148", "biolink:treats"
        )

    def test_valid_gene_associated_with_condition(self):
        # Gene associated with disease is valid
        assert _validate_edge_prefixes(
            "HGNC:11477", "MONDO:0005148", "biolink:gene_associated_with_condition"
        )

    def test_invalid_gene_associated_with_condition_reversed(self):
        # Disease associated with gene in subject position is invalid
        assert not _validate_edge_prefixes(
            "MONDO:0005148", "HGNC:11477", "biolink:gene_associated_with_condition"
        )

    def test_valid_treats_edge_chebi_to_hp(self):
        # Chemical treating phenotypic feature is valid (HP is in range of treats)
        assert _validate_edge_prefixes(
            "CHEBI:15365", "HP:0001234", "biolink:treats"
        )

    def test_unknown_predicate_returns_true(self):
        # Unknown predicates should pass (no constraints to check)
        assert _validate_edge_prefixes(
            "FOO:123", "BAR:456", "biolink:nonexistent_predicate"
        )

    def test_unknown_prefix_in_subject_fails_validation(self):
        # Unknown prefix in subject position for constrained predicate
        assert not _validate_edge_prefixes(
            "UNKNOWN:123", "MONDO:0008315", "biolink:treats"
        )

    def test_unknown_prefix_in_object_fails_validation(self):
        # Unknown prefix in object position for constrained predicate
        assert not _validate_edge_prefixes(
            "DRUGBANK:DB01248", "UNKNOWN:456", "biolink:treats"
        )

    def test_invalid_curie_without_colon_fails(self):
        # Subject with no colon has empty prefix, should fail for constrained predicate
        assert not _validate_edge_prefixes(
            "invalid", "MONDO:0008315", "biolink:treats"
        )


class TestGetSkippedEdgesSummary:
    """Tests for get_skipped_edges_summary function."""

    def test_empty_when_no_skipped_edges(self):
        summary = get_skipped_edges_summary()
        assert summary == {}

    def test_counts_skipped_edges_by_prefix_pattern(self):
        _skipped_edges_by_prefix.add(
            ("MONDO:0008315", "biolink:treats", "DRUGBANK:DB01248", "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation")
        )
        _skipped_edges_by_prefix.add(
            ("MONDO:0005148", "biolink:treats", "CHEBI:15365", "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation")
        )
        summary = get_skipped_edges_summary()
        # Both have pattern "MONDO biolink:treats <prefix> (relation)"
        assert len(summary) >= 1
        # Each unique (subject, pred, object, relation) tuple is one entry in the set
        total = sum(summary.values())
        assert total == 2


# ---------------------------------------------------------------------------
# parse_attributes unit tests
# ---------------------------------------------------------------------------

def _make_association(**overrides) -> Association:
    """Create a minimal Association for testing parse_attributes."""
    defaults = {
        "id": entity_id(),
        "subject": "DRUGBANK:DB00001",
        "predicate": "biolink:treats",
        "object": "MONDO:0005148",
        "knowledge_level": KnowledgeLevelEnum.not_provided,
        "agent_type": AgentTypeEnum.text_mining_agent,
    }
    defaults.update(overrides)
    return Association(**defaults)


class TestParseAttributes:
    """Direct unit tests for parse_attributes."""

    def test_text_mining_study_result_creation(self):
        """Full nested attribute parsing produces a Study with TextMiningStudyResult."""
        attributes = [
            {
                "attribute_type_id": "biolink:supporting_study_result",
                "value": "tmkp:result_1",
                "attributes": [
                    {"attribute_type_id": "biolink:supporting_text", "value": "Drug X treats disease Y."},
                    {"attribute_type_id": "biolink:supporting_document", "value": "PMID:12345"},
                    {"attribute_type_id": "biolink:supporting_text_located_in", "value": "abstract"},
                    {"attribute_type_id": "biolink:extraction_confidence_score", "value": "0.95"},
                    {"attribute_type_id": "biolink:subject_location_in_text", "value": "42|50"},
                    {"attribute_type_id": "biolink:object_location_in_text", "value": "60|70"},
                    {"attribute_type_id": "biolink:supporting_document_year", "value": "2021"},
                ],
            }
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        assert assoc.has_supporting_studies is not None
        study = list(assoc.has_supporting_studies.values())[0]
        assert isinstance(study, Study)

        result = study.has_study_results[0]
        assert isinstance(result, TextMiningStudyResult)
        assert result.id == "tmkp:result_1"
        assert result.supporting_text == ["Drug X treats disease Y."]
        assert result.xref == ["PMID:12345"]
        assert result.supporting_text_section_type == "abstract"
        assert result.extraction_confidence_score == 0.95
        assert result.subject_location_in_text == [42, 50]
        assert result.object_location_in_text == [60, 70]
        assert result.supporting_document_year == 2021

    def test_pipe_delimited_location_parsing(self):
        """'42|50' parses to [42, 50]."""
        attributes = [
            {
                "attribute_type_id": "biolink:supporting_study_result",
                "value": "tmkp:loc_test",
                "attributes": [
                    {"attribute_type_id": "biolink:subject_location_in_text", "value": "42|50"},
                ],
            }
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        result = list(assoc.has_supporting_studies.values())[0].has_study_results[0]
        assert result.subject_location_in_text == [42, 50]

    def test_tmkp_to_biolink_attribute_mapping(self):
        """supporting_publications maps to publications via TMKP_TO_BIOLINK_SLOT_MAP."""
        attributes = [
            {"attribute_type_id": "supporting_publications", "value": "PMID:111|PMID:222"},
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        assert assoc.publications == ["PMID:111", "PMID:222"]

    def test_publications_accumulation(self):
        """Multiple attributes mapping to publications merge into one list."""
        attributes = [
            {"attribute_type_id": "supporting_publications", "value": "PMID:111"},
            {"attribute_type_id": "supporting_document", "value": "PMID:222"},
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        assert "PMID:111" in assoc.publications
        assert "PMID:222" in assoc.publications

    def test_knowledge_source_extraction(self):
        """Primary + supporting sources from attributes populate association.sources."""
        attributes = [
            {"attribute_type_id": "biolink:primary_knowledge_source", "value": "infores:custom-kp"},
            {"attribute_type_id": "biolink:supporting_data_source", "value": "infores:upstream"},
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        assert assoc.sources is not None
        source_ids = [s.resource_id for s in assoc.sources]
        assert "infores:custom-kp" in source_ids
        assert "infores:upstream" in source_ids

    def test_default_sources_when_no_source_attributes(self):
        """Falls back to INFORES_TEXT_MINING_KP and pubmed when no source attrs present."""
        attributes = [
            {"attribute_type_id": "has_evidence_count", "value": 5},
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        source_ids = [s.resource_id for s in assoc.sources]
        assert INFORES_TEXT_MINING_KP in source_ids
        assert "infores:pubmed" in source_ids

    def test_empty_attributes_list(self):
        """Empty list still sets default sources."""
        assoc = _make_association()
        parse_attributes([], assoc)

        assert assoc.sources is not None
        source_ids = [s.resource_id for s in assoc.sources]
        assert INFORES_TEXT_MINING_KP in source_ids

    def test_multiple_study_results_in_single_study(self):
        """Two supporting_study_result attrs create two results under one Study."""
        attributes = [
            {
                "attribute_type_id": "biolink:supporting_study_result",
                "value": "tmkp:r1",
                "attributes": [
                    {"attribute_type_id": "biolink:supporting_text", "value": "First sentence."},
                ],
            },
            {
                "attribute_type_id": "biolink:supporting_study_result",
                "value": "tmkp:r2",
                "attributes": [
                    {"attribute_type_id": "biolink:supporting_text", "value": "Second sentence."},
                ],
            },
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        assert len(assoc.has_supporting_studies) == 1
        study = list(assoc.has_supporting_studies.values())[0]
        assert len(study.has_study_results) == 2

    def test_unknown_attribute_warning_logged_once(self):
        """Unknown attr_type is tracked in _warned_unmapped_attrs."""
        attributes = [
            {"attribute_type_id": "totally_unknown_attr", "value": "whatever"},
            {"attribute_type_id": "totally_unknown_attr", "value": "again"},
        ]
        assoc = _make_association()
        parse_attributes(attributes, assoc)

        assert "totally_unknown_attr" in _warned_unmapped_attrs


# ---------------------------------------------------------------------------
# transform_tmkp_edge integration tests via KozaRunner
# ---------------------------------------------------------------------------

def _run_edge_transform(record: dict) -> list:
    """Helper: run transform_tmkp_edge through KozaRunner, return collected items."""
    writer = MockKozaWriter()
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_tmkp_edge]),
    )
    runner.run()
    return writer.items


class TestTransformTmkpEdge:
    """Integration tests for transform_tmkp_edge via KozaRunner."""

    def test_basic_edge_with_attributes(self):
        """Full round-trip: edge with JSON attributes produces association + nodes.

        Source 'biolink:treats' is remapped to 'biolink:treats_or_applied_or_studied_to_treat'
        and knowledge_level is set to 'knowledge_assertion' for this predicate.
        """
        attributes = [
            {
                "attribute_type_id": "biolink:supporting_study_result",
                "value": "tmkp:result_1",
                "attributes": [
                    {"attribute_type_id": "biolink:supporting_text", "value": "Drug treats disease."},
                    {"attribute_type_id": "biolink:supporting_document", "value": "PMID:99999"},
                ],
            }
        ]
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:treats",
            "object": "MONDO:0008315",
            "relation": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
            "_attributes": json.dumps(attributes),
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1

        assoc = associations[0]
        assert isinstance(assoc, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)
        assert assoc.subject == "DRUGBANK:DB01248"
        assert assoc.object == "MONDO:0008315"
        assert assoc.predicate == "biolink:treats_or_applied_or_studied_to_treat"
        assert assoc.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
        assert assoc.agent_type == AgentTypeEnum.text_mining_agent

        # Verify supporting studies populated
        assert assoc.has_supporting_studies is not None
        study = list(assoc.has_supporting_studies.values())[0]
        assert len(study.has_study_results) == 1

        # Verify nodes created
        nodes = [i for i in items if isinstance(i, NamedThing) and not isinstance(i, Association)]
        assert len(nodes) == 2

    def test_edge_without_attributes_gets_default_sources(self):
        """Edge without _attributes still gets default sources and remapped predicate."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:treats",
            "object": "MONDO:0008315",
            "relation": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1

        assoc = associations[0]
        assert assoc.predicate == "biolink:treats_or_applied_or_studied_to_treat"
        source_ids = [s.resource_id for s in assoc.sources]
        assert INFORES_TEXT_MINING_KP in source_ids
        assert "infores:pubmed" in source_ids

    def test_edge_with_invalid_prefix_is_skipped(self):
        """Domain/range validation filters out reversed edges."""
        record = {
            "subject": "MONDO:0008315",
            "predicate": "biolink:treats",
            "object": "DRUGBANK:DB01248",
            "relation": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 0

    def test_edge_missing_required_fields_returns_none(self):
        """Missing subject/predicate/object produces no output."""
        record = {
            "subject": "DRUGBANK:DB01248",
            # no predicate
            "object": "MONDO:0008315",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 0

    def test_gene_regulates_gene_without_qualifiers_is_skipped(self):
        """GeneRegulatesGeneAssociation without required qualifiers is skipped."""
        record = {
            "subject": "NCBIGene:100",
            "predicate": "biolink:affects",
            "object": "NCBIGene:200",
            "relation": "biolink:GeneRegulatoryRelationship",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 0

    def test_gene_regulates_gene_with_qualifiers(self):
        """GeneRegulatesGeneAssociation succeeds when required qualifiers are present."""
        record = {
            "subject": "NCBIGene:100",
            "predicate": "biolink:affects",
            "object": "NCBIGene:200",
            "relation": "biolink:GeneRegulatoryRelationship",
            "object_aspect_qualifier": "activity_or_abundance",
            "object_direction_qualifier": "increased",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1
        assert isinstance(associations[0], GeneRegulatesGeneAssociation)
        assert associations[0].object_aspect_qualifier == "activity_or_abundance"
        assert associations[0].object_direction_qualifier == "increased"

    @pytest.mark.parametrize(
        "relation,expected_class",
        [
            ("biolink:ChemicalToGeneAssociation", ChemicalAffectsGeneAssociation),
            ("biolink:GeneToDiseaseAssociation", CorrelatedGeneToDiseaseAssociation),
            ("biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation", ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation),
        ],
    )
    def test_association_class_mapping(self, relation: str, expected_class: type):
        """Relation maps to the correct Association subclass."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:treats",
            "object": "MONDO:0008315",
            "relation": relation,
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1
        assert isinstance(associations[0], expected_class)

    def test_treats_predicate_is_remapped(self):
        """Source 'biolink:treats' is remapped to 'biolink:treats_or_applied_or_studied_to_treat'."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:treats",
            "object": "MONDO:0008315",
            "relation": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1
        assert associations[0].predicate == "biolink:treats_or_applied_or_studied_to_treat"

    def test_non_treats_predicate_not_remapped(self):
        """Predicates not in PREDICATE_REMAP pass through unchanged."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P12345",
            "relation": "biolink:ChemicalToGeneAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1
        assert associations[0].predicate == "biolink:affects"

    def test_treats_edge_gets_knowledge_assertion(self):
        """Remapped treats edges get knowledge_level=knowledge_assertion."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:treats",
            "object": "MONDO:0008315",
            "relation": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert associations[0].knowledge_level == KnowledgeLevelEnum.knowledge_assertion

    def test_non_treats_edge_gets_not_provided(self):
        """Non-treats edges keep knowledge_level=not_provided."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P12345",
            "relation": "biolink:ChemicalToGeneAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert associations[0].knowledge_level == KnowledgeLevelEnum.not_provided

    def test_gene_disease_contributes_to_gets_epc_pattern(self):
        """Gene-disease 'contributes_to' is transformed to canonical EPC pattern.

        Primary predicate becomes 'affects', qualified_predicate becomes 'contributes_to',
        and subject_form_or_variant_qualifier is set to 'modified_form'.
        """
        record = {
            "subject": "UniProtKB:P12345",
            "predicate": "biolink:contributes_to",
            "object": "MONDO:0008315",
            "relation": "biolink:GeneToDiseaseAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1

        assoc = associations[0]
        assert isinstance(assoc, CorrelatedGeneToDiseaseAssociation)
        assert assoc.predicate == "biolink:affects"
        assert assoc.qualified_predicate == "biolink:contributes_to"
        assert assoc.subject_form_or_variant_qualifier == MODIFIED_FORM

    def test_chemical_disease_contributes_to_not_transformed(self):
        """Chemical-disease 'contributes_to' is NOT transformed (only gene-disease gets EPC)."""
        record = {
            "subject": "DRUGBANK:DB01248",
            "predicate": "biolink:contributes_to",
            "object": "MONDO:0008315",
            "relation": "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1

        assoc = associations[0]
        assert isinstance(assoc, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)
        assert assoc.predicate == "biolink:contributes_to"

    def test_gene_disease_affects_not_transformed(self):
        """Gene-disease 'affects' is NOT transformed (only contributes_to triggers EPC)."""
        record = {
            "subject": "UniProtKB:P12345",
            "predicate": "biolink:affects",
            "object": "MONDO:0008315",
            "relation": "biolink:GeneToDiseaseAssociation",
        }
        items = _run_edge_transform(record)

        associations = [i for i in items if isinstance(i, Association)]
        assert len(associations) == 1

        assoc = associations[0]
        assert isinstance(assoc, CorrelatedGeneToDiseaseAssociation)
        assert assoc.predicate == "biolink:affects"
        assert not hasattr(assoc, "qualified_predicate") or assoc.qualified_predicate is None


class TestPredicateRemap:
    """Tests for the PREDICATE_REMAP constant."""

    def test_treats_is_remapped(self):
        assert PREDICATE_REMAP["biolink:treats"] == "biolink:treats_or_applied_or_studied_to_treat"

    def test_only_treats_is_remapped(self):
        assert len(PREDICATE_REMAP) == 1
