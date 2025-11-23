import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from koza.model.graphs import KnowledgeGraph

from tests.unit.ingests import MockKozaWriter, MockKozaTransform

from translator_ingest.ingests.ubergraph.ubergraph import (
    on_begin_redundant_graph,
    on_end_redundant_graph,
    transform_redundant_graph,
    INFORES_UBERGRAPH,
    EXTRACTED_ONTOLOGY_PREFIXES,
    OBO_MISSING_MAPPINGS,
    BIOLINK_MAPPING_CHANGES,
)


def test_extracted_ontology_prefixes():
    assert "UBERON" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "GO" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "MONDO" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "CHEBI" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "HPO" in EXTRACTED_ONTOLOGY_PREFIXES
    assert len(EXTRACTED_ONTOLOGY_PREFIXES) > 0


def test_obo_missing_mappings():
    assert OBO_MISSING_MAPPINGS["NCBIGene"] == "http://purl.obolibrary.org/obo/NCBIGene_"
    assert OBO_MISSING_MAPPINGS["HGNC"] == "http://purl.obolibrary.org/obo/HGNC_"
    assert OBO_MISSING_MAPPINGS["SGD"] == "http://purl.obolibrary.org/obo/SGD_"


def test_biolink_mapping_changes():
    assert BIOLINK_MAPPING_CHANGES["KEGG"] == "http://identifiers.org/kegg/"
    assert BIOLINK_MAPPING_CHANGES["NCBIGene"] == "https://identifiers.org/ncbigene/"


def test_infores_ubergraph():
    assert INFORES_UBERGRAPH == "infores:ubergraph"


class MockKozaState:
    def __init__(self):
        self.state = {}
        self.transform_metadata = {}

    def __getitem__(self, key):
        return self.state[key]

    def __setitem__(self, key, value):
        self.state[key] = value

    def log(self, message, level="INFO"):
        pass


def test_on_begin_redundant_graph():
    mock_koza = MockKozaState()

    on_begin_redundant_graph(mock_koza)

    assert mock_koza.state["record_counter"] == 0
    assert mock_koza.state["skipped_record_counter"] == 0
    assert mock_koza.state["node_curies"] == {}
    assert mock_koza.state["edge_curies"] == {}
    assert mock_koza.transform_metadata["redundant_graph"]["num_source_lines"] == 0
    assert mock_koza.transform_metadata["redundant_graph"]["unusable_source_lines"] == 0


def test_on_end_redundant_graph():
    mock_koza = MockKozaState()
    mock_koza.state = {
        "record_counter": 100,
        "skipped_record_counter": 10
    }
    mock_koza.transform_metadata = {"redundant_graph": {}}

    on_end_redundant_graph(mock_koza)

    assert mock_koza.transform_metadata["redundant_graph"]["num_source_lines"] == 100
    assert mock_koza.transform_metadata["redundant_graph"]["unusable_source_lines"] == 10


@pytest.fixture
def mock_koza_with_state():
    writer = MockKozaWriter()
    mock_koza = MockKozaTransform(extra_fields={}, writer=writer, mappings={})
    mock_koza.state = {
        "record_counter": 0,
        "skipped_record_counter": 0,
        "node_curies": {
            "n1": "GO:0008150",
            "n2": "UBERON:0001062",
            "n3": "MONDO:0000001",
        },
        "edge_curies": {
            "e1": "rdfs:subClassOf",
        }
    }
    return mock_koza


def test_transform_creates_nodes_and_edges(mock_koza_with_state):
    data = [
        {"subject_id": "n1", "predicate_id": "e1", "object_id": "n2"},
    ]

    results = list(transform_redundant_graph(mock_koza_with_state, iter(data)))

    assert len(results) == 1
    kg = results[0]
    assert isinstance(kg, KnowledgeGraph)
    nodes = list(kg.nodes)
    edges = list(kg.edges)
    assert len(nodes) == 2
    assert len(edges) == 1

    node_ids = [node.id for node in kg.nodes]
    assert "GO:0008150" in node_ids
    assert "UBERON:0001062" in node_ids

    edge = edges[0]
    assert isinstance(edge, Association)
    assert edge.subject == "GO:0008150"
    assert edge.object == "UBERON:0001062"
    assert edge.predicate == "rdfs:subClassOf"
    assert edge.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert len(edge.sources) == 1
    assert edge.sources[0].resource_id == INFORES_UBERGRAPH


def test_transform_skips_missing_node_curies(mock_koza_with_state):
    data = [
        {"subject_id": "n99", "predicate_id": "e1", "object_id": "n2"},
    ]

    results = list(transform_redundant_graph(mock_koza_with_state, iter(data)))

    assert len(results) == 0
    assert mock_koza_with_state.state["skipped_record_counter"] == 1


def test_transform_skips_missing_edge_curies(mock_koza_with_state):
    data = [
        {"subject_id": "n1", "predicate_id": "e99", "object_id": "n2"},
    ]

    results = list(transform_redundant_graph(mock_koza_with_state, iter(data)))

    assert len(results) == 0
    assert mock_koza_with_state.state["skipped_record_counter"] == 1


def test_transform_deduplicates_nodes(mock_koza_with_state):
    data = [
        {"subject_id": "n1", "predicate_id": "e1", "object_id": "n2"},
        {"subject_id": "n1", "predicate_id": "e1", "object_id": "n3"},
    ]

    results = list(transform_redundant_graph(mock_koza_with_state, iter(data)))

    assert len(results) == 1
    kg = results[0]
    assert len(kg.edges) == 2
    assert len(kg.nodes) == 3

    node_ids = [node.id for node in kg.nodes]
    assert node_ids.count("GO:0008150") == 1
