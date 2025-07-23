import pytest
from unittest.mock import patch
from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    GeneToGoTermAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    OntologyClass,
)

from src.translator_ingest.ingests.goa.goa import (
    transform_record,
    ASPECT_TO_PREDICATE,
    ASPECT_TO_ASSOCIATION,
    EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE,
)


class TestGOATransform:
    """Test the GOA transform functionality."""

    def test_transform_record_basic(self):
        """Test basic record transformation."""
        record = {
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "P",
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "",
            "DB_Reference": "PMID:12345|PMID:67890",
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
        }

        nodes, associations = transform_record(record)

        # Check nodes
        assert len(nodes) == 2
        gene_node = next(n for n in nodes if isinstance(n, Gene))
        go_node = next(n for n in nodes if isinstance(n, NamedThing) and n.id.startswith("GO:"))

        assert gene_node.id == "UniProtKB:P12345"
        assert gene_node.name == "APAF1"
        assert gene_node.category == ["biolink:Gene"]
        assert gene_node.in_taxon == ["NCBITaxon:9606"]

        assert go_node.id == "GO:0006915"
        assert go_node.category == ["biolink:NamedThing"]

        # Check association
        assert len(associations) == 1
        association = associations[0]
        assert isinstance(association, GeneToGoTermAssociation)
        assert association.subject == "UniProtKB:P12345"
        assert association.object == "GO:0006915"
        assert association.predicate == "biolink:participates_in"
        assert association.negated is False
        assert association.has_evidence == ["ECO:EXP"]
        assert association.publications == ["PMID:12345", "PMID:67890"]
        assert association.primary_knowledge_source == "infores:goa"
        assert association.aggregator_knowledge_source == ["infores:biolink"]
        assert association.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
        assert association.agent_type == AgentTypeEnum.manual_agent

    def test_transform_record_negated(self):
        """Test record transformation with negation."""
        record = {
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "P",
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "NOT",
            "DB_Reference": "PMID:12345",
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
        }

        nodes, associations = transform_record(record)
        association = associations[0]
        assert association.negated is True

    def test_transform_record_different_aspects(self):
        """Test record transformation for different GO aspects."""
        test_cases = [
            ("P", "biolink:participates_in"),
            ("F", "biolink:enables"),
            ("C", "biolink:located_in"),
        ]

        for aspect, expected_predicate in test_cases:
            record = {
                "DB_Object_ID": "P12345",
                "GO_ID": "GO:0006915",
                "Aspect": aspect,
                "DB_Object_Symbol": "APAF1",
                "Qualifier": "",
                "DB_Reference": "PMID:12345",
                "Evidence_Code": "EXP",
                "Taxon": "taxon:9606",
                "Date": "20230101",
            }

            nodes, associations = transform_record(record)
            association = associations[0]
            assert association.predicate == expected_predicate

    def test_transform_record_unknown_aspect(self):
        """Test record transformation with unknown aspect."""
        record = {
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "X",  # Unknown aspect
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "",
            "DB_Reference": "PMID:12345",
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
        }

        nodes, associations = transform_record(record)
        assert len(nodes) == 0
        assert len(associations) == 0

    def test_transform_record_different_evidence_codes(self):
        """Test record transformation with different evidence codes."""
        test_cases = [
            ("EXP", KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
            ("IEA", KnowledgeLevelEnum.prediction, AgentTypeEnum.automated_agent),
            ("ND", KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided),
            ("UNKNOWN", KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided),
        ]

        for evidence_code, expected_knowledge_level, expected_agent_type in test_cases:
            record = {
                "DB_Object_ID": "P12345",
                "GO_ID": "GO:0006915",
                "Aspect": "P",
                "DB_Object_Symbol": "APAF1",
                "Qualifier": "",
                "DB_Reference": "PMID:12345",
                "Evidence_Code": evidence_code,
                "Taxon": "taxon:9606",
                "Date": "20230101",
            }

            nodes, associations = transform_record(record)
            association = associations[0]
            assert association.knowledge_level == expected_knowledge_level
            assert association.agent_type == expected_agent_type

    def test_transform_record_empty_publications(self):
        """Test record transformation with empty publications."""
        record = {
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "P",
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "",
            "DB_Reference": "",  # Empty publications
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
        }

        nodes, associations = transform_record(record)
        association = associations[0]
        assert association.publications == []

    def test_mappings_consistency(self):
        """Test that mappings are consistent and complete."""
        # Test aspect to predicate mapping
        assert "P" in ASPECT_TO_PREDICATE
        assert "F" in ASPECT_TO_PREDICATE
        assert "C" in ASPECT_TO_PREDICATE
        assert ASPECT_TO_PREDICATE["P"] == "biolink:participates_in"
        assert ASPECT_TO_PREDICATE["F"] == "biolink:enables"
        assert ASPECT_TO_PREDICATE["C"] == "biolink:located_in"

        # Test aspect to association mapping
        assert "P" in ASPECT_TO_ASSOCIATION
        assert "F" in ASPECT_TO_ASSOCIATION
        assert "C" in ASPECT_TO_ASSOCIATION
        assert ASPECT_TO_ASSOCIATION["P"] == GeneToGoTermAssociation
        assert ASPECT_TO_ASSOCIATION["F"] == GeneToGoTermAssociation
        assert ASPECT_TO_ASSOCIATION["C"] == GeneToGoTermAssociation

        # Test evidence code mapping
        assert "EXP" in EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE
        assert "IEA" in EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE
        assert "ND" in EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE 