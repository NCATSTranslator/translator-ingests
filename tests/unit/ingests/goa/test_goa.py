import pytest
from unittest.mock import patch, MagicMock
from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Protein,
    GeneToGoTermAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    Association,
    BiologicalProcess,
    MolecularActivity,
    CellularComponent,
)

from src.translator_ingest.ingests.goa.goa import (
    transform_record,
    QUALIFIER_TO_PREDICATE,
    ASPECT_TO_PREDICATE,
    EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE,
)


class TestGOATransform:
    """Test the GOA transform functionality."""

    def test_transform_record_basic(self):
        """Test basic record transformation."""
        # Create a mock Koza context
        mock_koza = MagicMock()
        
        record = {
            "DB": "UniProtKB",  # Database source determines biolink class
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "P",
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "",
            "DB_Reference": "PMID:12345|PMID:67890",
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
            "DB_Object_Name": "Test Gene",
        }

        nodes, associations = transform_record(mock_koza, record)

        # Check nodes
        assert len(nodes) == 2
        protein_node = next(n for n in nodes if isinstance(n, Protein))
        go_node = next(n for n in nodes if isinstance(n, BiologicalProcess) and n.id.startswith("GO:"))

        assert protein_node.id == "UniProtKB:P12345"
        assert protein_node.name == "APAF1"
        assert protein_node.category == ["biolink:Protein"]
        assert protein_node.in_taxon == ["NCBITaxon:9606"]

        assert go_node.id == "GO:0006915"
        assert go_node.category == ["biolink:BiologicalProcess"]

        # Check association
        assert len(associations) == 1
        association = associations[0]
        assert isinstance(association, Association)  # Generic Association for Protein
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
        # Create a mock Koza context
        mock_koza = MagicMock()
        
        record = {
            "DB": "MGI",  # Use MGI to get Gene class
            "DB_Object_ID": "MGI:12345",
            "GO_ID": "GO:0006915",
            "Aspect": "P",
            "DB_Object_Symbol": "Apaf1",
            "Qualifier": "NOT",
            "DB_Reference": "PMID:12345",
            "Evidence_Code": "EXP",
            "Taxon": "taxon:10090",
            "Date": "20230101",
            "DB_Object_Name": "Test Gene",
        }

        nodes, associations = transform_record(mock_koza, record)
        association = associations[0]
        assert association.negated is True
        assert isinstance(association, GeneToGoTermAssociation)  # Specific association for Gene

    def test_transform_record_different_aspects(self):
        """Test record transformation for different GO aspects."""
        # Create a mock Koza context
        mock_koza = MagicMock()
        
        test_cases = [
            ("P", "biolink:participates_in", BiologicalProcess),
            ("F", "biolink:enables", MolecularActivity),
            ("C", "biolink:located_in", CellularComponent),
        ]

        for aspect, expected_predicate, expected_go_class in test_cases:
            record = {
                "DB": "UniProtKB",
                "DB_Object_ID": "P12345",
                "GO_ID": "GO:0006915",
                "Aspect": aspect,
                "DB_Object_Symbol": "APAF1",
                "Qualifier": "",
                "DB_Reference": "PMID:12345",
                "Evidence_Code": "EXP",
                "Taxon": "taxon:9606",
                "Date": "20230101",
                "DB_Object_Name": "Test Gene",
            }

            nodes, associations = transform_record(mock_koza, record)
            
            # Check GO term node has correct class
            go_node = next(n for n in nodes if n.id.startswith("GO:"))
            assert isinstance(go_node, expected_go_class)
            
            # Check predicate
            association = associations[0]
            assert association.predicate == expected_predicate

    def test_transform_record_unknown_aspect(self):
        """Test record transformation with unknown aspect."""
        # Create a mock Koza context
        mock_koza = MagicMock()
        
        record = {
            "DB": "UniProtKB",
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "X",  # Unknown aspect
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "",
            "DB_Reference": "PMID:12345",
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
            "DB_Object_Name": "Test Gene",
        }

        nodes, associations = transform_record(mock_koza, record)
        
        # Should return empty lists for unknown aspect
        assert nodes == []
        assert associations == []

    def test_transform_record_different_evidence_codes(self):
        """Test record transformation with different evidence codes."""
        # Create a mock Koza context
        mock_koza = MagicMock()
        
        test_cases = [
            ("EXP", KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
            ("IEA", KnowledgeLevelEnum.prediction, AgentTypeEnum.automated_agent),
            ("ND", KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided),
            ("UNKNOWN", KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided),
        ]

        for evidence_code, expected_knowledge_level, expected_agent_type in test_cases:
            record = {
                "DB": "UniProtKB",
                "DB_Object_ID": "P12345",
                "GO_ID": "GO:0006915",
                "Aspect": "P",
                "DB_Object_Symbol": "APAF1",
                "Qualifier": "",
                "DB_Reference": "PMID:12345",
                "Evidence_Code": evidence_code,
                "Taxon": "taxon:9606",
                "Date": "20230101",
                "DB_Object_Name": "Test Gene",
            }

            nodes, associations = transform_record(mock_koza, record)
            association = associations[0]
            assert association.knowledge_level == expected_knowledge_level
            assert association.agent_type == expected_agent_type

    def test_transform_record_empty_publications(self):
        """Test record transformation with empty publications."""
        # Create a mock Koza context
        mock_koza = MagicMock()
        
        record = {
            "DB": "UniProtKB",
            "DB_Object_ID": "P12345",
            "GO_ID": "GO:0006915",
            "Aspect": "P",
            "DB_Object_Symbol": "APAF1",
            "Qualifier": "",
            "DB_Reference": "",  # Empty publications
            "Evidence_Code": "EXP",
            "Taxon": "taxon:9606",
            "Date": "20230101",
            "DB_Object_Name": "Test Gene",
        }

        nodes, associations = transform_record(mock_koza, record)
        association = associations[0]
        assert association.publications == []  # Should be empty list

    def test_mappings_consistency(self):
        """Test that mappings are consistent."""
        # Test qualifier to predicate mapping
        assert QUALIFIER_TO_PREDICATE["enables"] == "biolink:enables"
        assert QUALIFIER_TO_PREDICATE["part_of"] == "biolink:part_of"
        assert QUALIFIER_TO_PREDICATE["located_in"] == "biolink:located_in"
        assert QUALIFIER_TO_PREDICATE["involved_in"] == "biolink:participates_in"
        assert QUALIFIER_TO_PREDICATE["contributes_to"] == "biolink:contributes_to"
        assert QUALIFIER_TO_PREDICATE["colocalizes_with"] == "biolink:colocalizes_with"
        
        # Test aspect to predicate mapping (fallback)
        assert ASPECT_TO_PREDICATE["P"] == "biolink:participates_in"
        assert ASPECT_TO_PREDICATE["F"] == "biolink:enables"
        assert ASPECT_TO_PREDICATE["C"] == "biolink:located_in"
        
        # Test evidence code mapping
        assert EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE["EXP"] == (
            KnowledgeLevelEnum.knowledge_assertion, 
            AgentTypeEnum.manual_agent
        )
        assert EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE["IEA"] == (
            KnowledgeLevelEnum.prediction, 
            AgentTypeEnum.automated_agent
        ) 