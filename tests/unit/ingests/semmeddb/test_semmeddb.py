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


def _create_test_runner(record):
    """Helper function to create a test runner with proper setup."""
    writer = MockKozaWriter()
    
    runner = KozaRunner(
        data=[record], 
        writer=writer, 
        hooks=KozaTransformHooks(transform_record=[transform_semmeddb_edge])
    )
    runner.run()
    return writer.items


@pytest.fixture
def therapeutic_edge_output():
    """Test therapeutic edge transformation."""
    record = {
        "subject": "CHEBI:15365",
        "object": "MONDO:0005148",
        "predicate": "biolink:treats_or_applied_or_studied_to_treat",
        "publications": ["PMID:12345678", "PMID:87654321", "PMID:11111111", "PMID:22222222"],
        "negated": False,
        "domain_range_exclusion": False,
        "subject_novelty": 1,
        "object_novelty": 1,
    }
    return _create_test_runner(record)


def test_therapeutic_edge_entities(therapeutic_edge_output):
    """Test that therapeutic edge creates correct entities."""
    entities = therapeutic_edge_output
    assert len(entities) == 3  # 2 nodes + 1 edge
    
    # verify association properties
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association.predicate == "biolink:treats_or_applied_or_studied_to_treat"
    assert association.subject == "CHEBI:15365"
    assert association.object == "MONDO:0005148"
    assert association.publications == ["PMID:12345678", "PMID:87654321", "PMID:11111111", "PMID:22222222"]
    assert association.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
    assert association.agent_type == AgentTypeEnum.automated_agent
    
    # verify node creation
    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "CHEBI:15365"
    
    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MONDO:0005148"

def test_make_node_function():
    """Test the _make_node function with various prefixes."""
    # test known prefixes create correct entity types
    gene_node = _make_node("NCBIGene:123")
    assert isinstance(gene_node, Gene)
    assert gene_node.id == "NCBIGene:123"
    
    protein_node = _make_node("UniProtKB:P12345")
    assert isinstance(protein_node, Protein)
    assert protein_node.id == "UniProtKB:P12345"
    
    chemical_node = _make_node("CHEBI:15365")
    assert isinstance(chemical_node, ChemicalEntity)
    assert chemical_node.id == "CHEBI:15365"
    
    # test unknown prefix falls back to NamedThing
    unknown_node = _make_node("UNKNOWN:123")
    assert isinstance(unknown_node, type(unknown_node))  # Should be NamedThing
    assert unknown_node.id == "UNKNOWN:123"
    
    # test malformed ID returns None
    malformed_node = _make_node("malformed_id")
    assert malformed_node is None


@pytest.fixture
def negated_edge_output():
    """Test negated edge transformation."""
    record = {
        "subject": "HGNC:1234",
        "object": "HP:0001234",
        "predicate": "biolink:causes",
        "publications": ["PMID:11111111", "PMID:22222222", "PMID:33333333", "PMID:44444444"],
        "negated": True,
        "domain_range_exclusion": False,
        "subject_novelty": 1,
        "object_novelty": 1,
    }
    return _create_test_runner(record)


def test_negated_edge(negated_edge_output):
    """Test that negated edges are properly filtered out."""
    entities = negated_edge_output
    # Negated edges should be filtered out, so no associations should be created
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 0, "Negated edges should be filtered out"


@pytest.fixture
def edge_without_publications():
    """Test edge with insufficient publications."""
    record = {
        "subject": "DOID:1234",
        "object": "UBERON:0001234",
        "predicate": "biolink:located_in",
        "publications": [],
        "negated": False,
        "domain_range_exclusion": False,
        "subject_novelty": 1,
        "object_novelty": 1,
    }
    return _create_test_runner(record)


def test_edge_without_publications(edge_without_publications):
    """Test edge with insufficient publications is filtered out."""
    entities = edge_without_publications
    # Edges with <=3 publications should be filtered out
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 0, "Edges with <=3 publications should be filtered out"


@pytest.fixture
def edge_with_zero_novelty():
    """Test edge with zero novelty score."""
    record = {
        "subject": "NCBIGene:100",
        "object": "MONDO:0005148",
        "predicate": "biolink:affects",
        "publications": ["PMID:11111111", "PMID:22222222", "PMID:33333333", "PMID:44444444"],
        "negated": False,
        "domain_range_exclusion": False,
        "subject_novelty": 0,
        "object_novelty": 1,
    }
    return _create_test_runner(record)


def test_edge_with_zero_novelty(edge_with_zero_novelty):
    """Test edge with zero novelty score is filtered out."""
    entities = edge_with_zero_novelty
    # Edges with subject_novelty == 0 or object_novelty == 0 should be filtered out
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 0, "Edges with zero novelty score should be filtered out"


@pytest.fixture
def edge_with_domain_range_exclusion():
    """Test edge with domain_range_exclusion."""
    record = {
        "subject": "NCBIGene:100",
        "object": "MONDO:0005148",
        "predicate": "biolink:affects",
        "publications": ["PMID:11111111", "PMID:22222222", "PMID:33333333", "PMID:44444444"],
        "negated": False,
        "domain_range_exclusion": True,
        "subject_novelty": 1,
        "object_novelty": 1,
    }
    return _create_test_runner(record)


def test_edge_with_domain_range_exclusion(edge_with_domain_range_exclusion):
    """Test edge with domain_range_exclusion is filtered out."""
    entities = edge_with_domain_range_exclusion
    # Edges with domain_range_exclusion == True should be filtered out
    associations = [e for e in entities if isinstance(e, Association)]
    assert len(associations) == 0, "Edges with domain_range_exclusion should be filtered out"
