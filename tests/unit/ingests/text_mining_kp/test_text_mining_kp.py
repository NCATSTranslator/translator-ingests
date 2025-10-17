from typing import Iterable
import json
import pytest
from unittest.mock import patch, MagicMock
import tempfile
import os

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    NamedThing,
    StudyResult,
    RetrievalSource,
    KnowledgeGraph,
    ResourceRoleEnum,
    AgentTypeEnum,
    KnowledgeLevelEnum
)
from koza.io.writer.writer import KozaWriter
from koza.transform import KozaTransform
from src.translator_ingest.ingests.text_mining_kp.text_mining_kp import (
    transform_text_mining_kp,
    parse_attributes_json,
    map_attribute_to_biolink_slot,
    extract_tar_gz,
    get_latest_version,
    TMKP_INFORES
)


class MockWriter(KozaWriter):
    def __init__(self):
        self.items = []

    def write(self, entities):
        if isinstance(entities, list):
            self.items.extend(entities)
        else:
            self.items.append(entities)

    def write_nodes(self, nodes: Iterable):
        self.items.extend(nodes)

    def write_edges(self, edges: Iterable):
        self.items.extend(edges)

    def finalize(self):
        pass


@pytest.fixture
def mock_koza():
    writer = MockWriter()
    return KozaTransform(writer=writer, extra_fields={}, mappings={})


def test_get_latest_version():
    version = get_latest_version()
    assert isinstance(version, str)
    assert len(version) == 10  # YYYY-MM-DD format
    assert version.count('-') == 2


def test_map_attribute_to_biolink_slot(mock_koza):
    # Test valid mapping
    result = map_attribute_to_biolink_slot('confidence_score', mock_koza)
    assert result == 'biolink:has_confidence_level'
    
    # Test unknown attribute
    result = map_attribute_to_biolink_slot('unknown_attribute', mock_koza)
    assert result is None
    
    # Test text mining specific attribute
    result = map_attribute_to_biolink_slot('supporting_text', mock_koza)
    assert result == 'biolink:supporting_text'


def test_parse_attributes_json_empty(mock_koza):
    # Test empty string
    result = parse_attributes_json('', mock_koza)
    assert result == {}
    
    # Test empty list
    result = parse_attributes_json('[]', mock_koza)
    assert result == {}


def test_parse_attributes_json_confidence(mock_koza):
    attributes = json.dumps([
        {
            "attribute_type_id": "biolink:confidence_score",
            "value": 0.95
        }
    ])
    
    result = parse_attributes_json(attributes, mock_koza)
    assert result['biolink:has_confidence_level'] == 0.95


def test_parse_attributes_json_supporting_study_result(mock_koza):
    attributes = json.dumps([
        {
            "attribute_type_id": "biolink:supporting_study_result",
            "value": "PMID:12345678",
            "attributes": [
                {
                    "attribute_type_id": "biolink:supporting_text",
                    "value": "This is supporting text"
                },
                {
                    "attribute_type_id": "biolink:extraction_confidence_score",
                    "value": 0.85
                }
            ]
        }
    ])
    
    result = parse_attributes_json(attributes, mock_koza)
    assert 'biolink:has_supporting_study_result' in result
    assert len(result['biolink:has_supporting_study_result']) == 1
    assert result['biolink:supporting_text'] == "This is supporting text"
    assert result['biolink:extraction_confidence_score'] == 0.85


@patch('tarfile.open')
@patch('tempfile.mkdtemp')
def test_extract_tar_gz(mock_mkdtemp, mock_tarfile_open, mock_koza):
    mock_mkdtemp.return_value = '/tmp/test_extract'
    mock_tar = MagicMock()
    mock_tarfile_open.return_value.__enter__.return_value = mock_tar
    
    result = extract_tar_gz('/path/to/test.tar.gz', mock_koza)
    
    assert result == '/tmp/test_extract'
    mock_tarfile_open.assert_called_once_with('/path/to/test.tar.gz', 'r:gz')
    mock_tar.extractall.assert_called_once_with('/tmp/test_extract')


@pytest.fixture
def text_mining_kp_output(mock_koza):
    # Create test data that mimics the structure after prepare_data
    test_data = [
        {
            "_record_type": "node",
            "id": "HGNC:1234",
            "category": "biolink:Gene",
            "name": "Test Gene"
        },
        {
            "_record_type": "node",
            "id": "MONDO:5678",
            "category": "biolink:Disease",
            "name": "Test Disease"
        },
        {
            "_record_type": "edge",
            "id": "edge_1",
            "subject": "HGNC:1234",
            "predicate": "biolink:associated_with",
            "object": "MONDO:5678",
            "_attributes": json.dumps([
                {
                    "attribute_type_id": "biolink:confidence_score",
                    "value": 0.95
                },
                {
                    "attribute_type_id": "biolink:supporting_study_result",
                    "value": "PMID:12345678",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:supporting_text",
                            "value": "Gene X is associated with disease Y"
                        }
                    ]
                }
            ])
        }
    ]
    
    # Transform the data
    result = transform_text_mining_kp(mock_koza, test_data)
    return result


def test_text_mining_kp_nodes(text_mining_kp_output):
    kg = text_mining_kp_output
    assert isinstance(kg, KnowledgeGraph)
    
    # Check nodes
    assert len(kg.nodes) == 2
    
    gene = next(node for node in kg.nodes if node.id == "HGNC:1234")
    assert gene.name == "Test Gene"
    assert gene.category == ["biolink:Gene"]
    
    disease = next(node for node in kg.nodes if node.id == "MONDO:5678")
    assert disease.name == "Test Disease"
    assert disease.category == ["biolink:Disease"]


def test_text_mining_kp_edges(text_mining_kp_output):
    kg = text_mining_kp_output
    assert isinstance(kg, KnowledgeGraph)
    
    # Check edges
    assert len(kg.edges) == 1
    
    association = kg.edges[0]
    assert isinstance(association, Association)
    assert association.subject == "HGNC:1234"
    assert association.predicate == "biolink:associated_with"
    assert association.object == "MONDO:5678"
    
    # Check knowledge level and agent type
    assert association.knowledge_level == KnowledgeLevelEnum.statistical_association
    assert association.agent_type == AgentTypeEnum.text_mining_agent
    
    # Check sources
    assert association.sources is not None
    assert len(association.sources) >= 1
    primary_source = next(s for s in association.sources if s.resource_role == ResourceRoleEnum.primary_knowledge_source)
    assert primary_source.resource_id == TMKP_INFORES
    
    # Check attributes
    assert hasattr(association, 'has_confidence_level')
    assert association.has_confidence_level == 0.95
    
    # Check supporting study result and text
    assert hasattr(association, 'has_supporting_study_result')
    assert len(association.has_supporting_study_result) == 1
    assert hasattr(association, 'supporting_text')
    assert association.supporting_text == "Gene X is associated with disease Y"


def test_text_mining_kp_invalid_edge(mock_koza):
    # Test edge without required fields
    test_data = [
        {
            "_record_type": "edge",
            "id": "edge_1",
            "subject": "HGNC:1234",
            # Missing predicate and object
            "_attributes": "[]"
        }
    ]
    
    result = transform_text_mining_kp(mock_koza, test_data)
    assert isinstance(result, KnowledgeGraph)
    assert len(result.edges) == 0  # Invalid edge should be skipped


def test_text_mining_kp_character_offsets(mock_koza):
    test_data = [
        {
            "_record_type": "edge",
            "id": "edge_1",
            "subject": "HGNC:1234",
            "predicate": "biolink:associated_with",
            "object": "MONDO:5678",
            "_attributes": json.dumps([
                {
                    "attribute_type_id": "biolink:supporting_study_result",
                    "value": "PMID:12345678",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:subject_location_in_text",
                            "value": "10|20"
                        },
                        {
                            "attribute_type_id": "biolink:object_location_in_text",
                            "value": "30|40"
                        }
                    ]
                }
            ])
        }
    ]
    
    result = transform_text_mining_kp(mock_koza, test_data)
    association = result.edges[0]
    
    # Check that character offsets are parsed as integer lists
    assert hasattr(association, 'subject_location_in_text')
    assert association.subject_location_in_text == [10, 20]
    assert hasattr(association, 'object_location_in_text')
    assert association.object_location_in_text == [30, 40]