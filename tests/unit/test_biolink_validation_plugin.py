"""
Tests for BiolinkValidationPlugin validation methods
"""

import pytest
from src.translator_ingest.util.biolink_validation_plugin import BiolinkValidationPlugin
from src.translator_ingest.util.biolink import get_biolink_schema
from linkml.validator.validation_context import ValidationContext


@pytest.fixture
def plugin():
    return BiolinkValidationPlugin()


VALID_CURIES = [
    "MONDO:0005148",
    "HP:123",
    "NCBIGene:1234",
    "foo_bar.baz-123:test_45.67-89",
    "biolink:NamedThing",
    "biolink:related_to",
    "a:b",
    "A:B",
    "GO:0008150",
    "CHEBI:12345",
    "UniProtKB:P12345",
    "PUBCHEM.COMPOUND:12345",
    "VALID-id:234235",
    "1:2",
    "EC_NUMBER:2.2.3"
]


INVALID_CURIES = [
    "not-a-curie",
    "MONDO:",
    "MONDO 123:456",
    "MONDO::123",
    "MONDO:123/456",
    "http://example.com/MONDO:123",
    ":123",
    "",
    "MONDO",
    "MONDO:123:456",
    "MONDO:123 extra",
    " MONDO:123",
    "MONDO:123 ",
    ":a",
    "a:",
    "_:_",
    "-:-",
    ".:..",
]


@pytest.mark.parametrize("curie", VALID_CURIES)
def test_valid_curie(plugin, curie):
    assert plugin._is_valid_curie(curie), f"Expected {curie} to be valid"


@pytest.mark.parametrize("curie", INVALID_CURIES)
def test_invalid_curie(plugin, curie):
    assert not plugin._is_valid_curie(curie), f"Expected {curie} to be invalid"


@pytest.mark.parametrize("value", [None, 123, [], {}])
def test_non_string_input(plugin, value):
    assert not plugin._is_valid_curie(value)


@pytest.fixture
def schema_plugin():
    """Plugin with biolink schema for domain/range validation tests"""
    schema = get_biolink_schema()
    return BiolinkValidationPlugin(schema_view=schema)


def test_category_matches_constraint_drug_domain(schema_plugin):
    """Test that Drug category matches chemical or drug or treatment domain constraint"""
    categories = ['biolink:Drug']
    constraint = 'chemical or drug or treatment'
    
    assert schema_plugin._category_matches_constraint(categories, constraint)


def test_category_matches_constraint_chemical_entity_domain(schema_plugin):
    """Test that ChemicalEntity category matches chemical or drug or treatment domain constraint"""
    categories = ['biolink:ChemicalEntity']
    constraint = 'chemical or drug or treatment'
    
    assert schema_plugin._category_matches_constraint(categories, constraint)


def test_category_matches_constraint_gene_does_not_match_drug_domain(schema_plugin):
    """Test that Gene category does not match chemical or drug or treatment domain constraint"""
    categories = ['biolink:Gene']
    constraint = 'chemical or drug or treatment'
    
    assert not schema_plugin._category_matches_constraint(categories, constraint)


def test_category_matches_constraint_disease_range(schema_plugin):
    """Test that Disease category matches disease or phenotypic feature range constraint"""
    categories = ['biolink:Disease']
    constraint = 'disease or phenotypic feature'
    
    assert schema_plugin._category_matches_constraint(categories, constraint)


def test_category_matches_constraint_phenotypic_feature_range(schema_plugin):
    """Test that PhenotypicFeature category matches disease or phenotypic feature range constraint"""
    categories = ['biolink:PhenotypicFeature']
    constraint = 'disease or phenotypic feature'
    
    assert schema_plugin._category_matches_constraint(categories, constraint)


def test_category_matches_constraint_empty_inputs(schema_plugin):
    """Test that empty inputs return True (permissive)"""
    assert schema_plugin._category_matches_constraint([], 'some constraint')
    assert schema_plugin._category_matches_constraint(['biolink:Drug'], '')
    assert schema_plugin._category_matches_constraint([], '')


def test_domain_range_validation_valid_treats_edge():
    """Test that valid treats edge (Drug treats Disease) passes domain/range validation"""
    test_data = {
        'nodes': [
            {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
            {'id': 'MONDO:5678', 'category': ['biolink:Disease'], 'name': 'Test Disease'}
        ],
        'edges': [
            {'subject': 'CHEBI:1234', 'predicate': 'biolink:treats', 'object': 'MONDO:5678', 
             'sources': [{'resource_id': 'infores:test'}]}
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    domain_range_violations = [r for r in results if 'domain constraint' in r.message or 'range constraint' in r.message]
    
    assert len(domain_range_violations) == 0


def test_domain_range_validation_invalid_treats_domain():
    """Test that invalid treats edge (Gene treats Disease) fails domain validation"""
    test_data = {
        'nodes': [
            {'id': 'HGNC:1111', 'category': ['biolink:Gene'], 'name': 'Test Gene'},
            {'id': 'MONDO:5678', 'category': ['biolink:Disease'], 'name': 'Test Disease'}
        ],
        'edges': [
            {'subject': 'HGNC:1111', 'predicate': 'biolink:treats', 'object': 'MONDO:5678',
             'sources': [{'resource_id': 'infores:test'}]}
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    domain_violations = [r for r in results if 'domain constraint' in r.message]
    
    assert len(domain_violations) == 1
    assert 'expects domain \'chemical or drug or treatment\' but subject has categories [\'biolink:Gene\']' in domain_violations[0].message


def test_domain_range_validation_invalid_treats_range():
    """Test that invalid treats edge (Drug treats Gene) fails range validation"""
    test_data = {
        'nodes': [
            {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
            {'id': 'HGNC:1111', 'category': ['biolink:Gene'], 'name': 'Test Gene'}
        ],
        'edges': [
            {'subject': 'CHEBI:1234', 'predicate': 'biolink:treats', 'object': 'HGNC:1111',
             'sources': [{'resource_id': 'infores:test'}]}
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    range_violations = [r for r in results if 'range constraint' in r.message]
    
    assert len(range_violations) == 1
    assert 'expects range \'disease or phenotypic feature\' but object has categories [\'biolink:Gene\']' in range_violations[0].message


def test_domain_range_validation_treats_phenotypic_feature():
    """Test that Drug treats PhenotypicFeature is valid (satisfies range constraint)"""
    test_data = {
        'nodes': [
            {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
            {'id': 'HP:9999', 'category': ['biolink:PhenotypicFeature'], 'name': 'Test Phenotype'}
        ],
        'edges': [
            {'subject': 'CHEBI:1234', 'predicate': 'biolink:treats', 'object': 'HP:9999',
             'sources': [{'resource_id': 'infores:test'}]}
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    domain_range_violations = [r for r in results if 'domain constraint' in r.message or 'range constraint' in r.message]
    
    assert len(domain_range_violations) == 0
