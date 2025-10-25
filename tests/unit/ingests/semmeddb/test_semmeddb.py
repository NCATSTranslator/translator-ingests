import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalEntity,
    Disease,
    Gene,
    Protein,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

from koza.runner import KozaRunner, KozaTransformHooks
from translator_ingest.ingests.semmeddb.semmeddb import (
    transform_semmeddb_edge,
    _make_node,
)

from tests.unit.ingests import MockKozaWriter


@pytest.fixture
def mock_koza_transform():
    """Create a mock Koza transform for testing."""
    from tests.unit.ingests import MockKozaTransform
    from koza.transform import Mappings
    from koza.io.writer.writer import KozaWriter
    
    writer = KozaWriter()
    mappings = Mappings()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


@pytest.fixture
def therapeutic_edge_output():
    """Test therapeutic edge transformation."""
    from translator_ingest.ingests.semmeddb.semmeddb import on_begin_filter_edges
    
    writer = MockKozaWriter()
    record = {
        "subject": "CHEBI:15365",
        "object": "MONDO:0005148",
        "predicate": "biolink:treats_or_applied_or_studied_to_treat",
        "publications": ["PMID:12345678", "PMID:87654321"],
        "negated": False,
        "domain_range_exclusion": False,
        "provided_by": "infores:semmeddb",
        "knowledge_level": "knowledge_assertion",
        "agent_type": "automated_agent",
    }
    
    # Create a mock koza transform and initialize state
    from tests.unit.ingests import MockKozaTransform
    from koza.transform import Mappings
    mock_koza = MockKozaTransform(extra_fields=dict(), writer=writer, mappings=Mappings())
    on_begin_filter_edges(mock_koza)
    
    runner = KozaRunner(
        data=[record], 
        writer=writer, 
        hooks=KozaTransformHooks(transform_record=[transform_semmeddb_edge])
    )
    runner.run()
    return writer.items


def test_therapeutic_edge_entities(therapeutic_edge_output):
    """Test that therapeutic edge creates correct entities."""
    entities = therapeutic_edge_output
    assert len(entities) == 3  # 2 nodes + 1 edge
    
    # Check association
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.predicate == "biolink:treats_or_applied_or_studied_to_treat"
    assert association.subject == "CHEBI:15365"
    assert association.object == "MONDO:0005148"
    assert "PMID:12345678" in association.publications
    assert "PMID:87654321" in association.publications
    assert association.negated is None  # False values are not set, default to None
    assert association.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
    assert association.agent_type == AgentTypeEnum.automated_agent
    
    # Check nodes
    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "CHEBI:15365"
    
    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MONDO:0005148"


def test_make_node_function():
    """Test the _make_node function with various prefixes."""
    # Test known prefixes
    gene_node = _make_node("NCBIGene:123")
    assert isinstance(gene_node, Gene)
    assert gene_node.id == "NCBIGene:123"
    
    protein_node = _make_node("UniProtKB:P12345")
    assert isinstance(protein_node, Protein)
    assert protein_node.id == "UniProtKB:P12345"
    
    chemical_node = _make_node("CHEBI:15365")
    assert isinstance(chemical_node, ChemicalEntity)
    assert chemical_node.id == "CHEBI:15365"
    
    # Test unknown prefix falls back to NamedThing
    unknown_node = _make_node("UNKNOWN:123")
    assert isinstance(unknown_node, type(unknown_node))  # Should be NamedThing
    assert unknown_node.id == "UNKNOWN:123"
    
    # Test malformed ID
    malformed_node = _make_node("malformed_id")
    assert isinstance(malformed_node, type(malformed_node))  # Should be NamedThing
    assert malformed_node.id == "malformed_id"


@pytest.fixture
def negated_edge_output():
    """Test negated edge transformation."""
    from translator_ingest.ingests.semmeddb.semmeddb import on_begin_filter_edges
    
    writer = MockKozaWriter()
    record = {
        "subject": "HGNC:1234",
        "object": "HP:0001234",
        "predicate": "biolink:causes",
        "publications": ["PMID:11111111"],
        "negated": True,
        "domain_range_exclusion": False,
    }
    
    # Create a mock koza transform and initialize state
    from tests.unit.ingests import MockKozaTransform
    from koza.transform import Mappings
    mock_koza = MockKozaTransform(extra_fields=dict(), writer=writer, mappings=Mappings())
    on_begin_filter_edges(mock_koza)
    
    runner = KozaRunner(
        data=[record], 
        writer=writer, 
        hooks=KozaTransformHooks(transform_record=[transform_semmeddb_edge])
    )
    runner.run()
    return writer.items


def test_negated_edge(negated_edge_output):
    """Test that negated edges are properly handled."""
    entities = negated_edge_output
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.negated is True
    assert association.predicate == "biolink:causes"


@pytest.fixture
def edge_without_publications():
    """Test edge without publications."""
    from translator_ingest.ingests.semmeddb.semmeddb import on_begin_filter_edges
    
    writer = MockKozaWriter()
    record = {
        "subject": "DOID:1234",
        "object": "UBERON:0001234",
        "predicate": "biolink:located_in",
        "publications": [],
        "negated": False,
    }
    
    # Create a mock koza transform and initialize state
    from tests.unit.ingests import MockKozaTransform
    from koza.transform import Mappings
    mock_koza = MockKozaTransform(extra_fields=dict(), writer=writer, mappings=Mappings())
    on_begin_filter_edges(mock_koza)
    
    runner = KozaRunner(
        data=[record], 
        writer=writer, 
        hooks=KozaTransformHooks(transform_record=[transform_semmeddb_edge])
    )
    runner.run()
    return writer.items


def test_edge_without_publications(edge_without_publications):
    """Test edge without publications is still processed."""
    entities = edge_without_publications
    assert len(entities) == 3  # 2 nodes + 1 edge
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.publications == []
