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


@pytest.mark.parametrize("categories,constraint,expected", [
    # Domain constraint tests
    (['biolink:Drug'], 'chemical or drug or treatment', True),
    (['biolink:ChemicalEntity'], 'chemical or drug or treatment', True),
    (['biolink:Gene'], 'chemical or drug or treatment', False),
    # Range constraint tests
    (['biolink:Disease'], 'disease or phenotypic feature', True),
    (['biolink:PhenotypicFeature'], 'disease or phenotypic feature', True),
    # Empty input tests (permissive)
    ([], 'some constraint', True),
    (['biolink:Drug'], '', True),
    ([], '', True),
])
def test_category_matches_constraint(schema_plugin, categories, constraint, expected):
    """Test that categories match or don't match their respective constraints"""
    assert schema_plugin._category_matches_constraint(categories, constraint) == expected


@pytest.mark.parametrize("subject_node,object_node", [
    # Valid: Drug treats Disease
    (
        {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
        {'id': 'MONDO:5678', 'category': ['biolink:Disease'], 'name': 'Test Disease'}
    ),
    # Valid: Drug treats PhenotypicFeature
    (
        {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
        {'id': 'HP:9999', 'category': ['biolink:PhenotypicFeature'], 'name': 'Test Phenotype'}
    ),
])
def test_domain_range_validation_no_violations(subject_node, object_node):
    """Test that valid treats edges pass domain/range validation without violations"""
    test_data = {
        'nodes': [subject_node, object_node],
        'edges': [
            {
                'subject': subject_node['id'], 
                'predicate': 'biolink:treats', 
                'object': object_node['id'],
                'sources': [{'resource_id': 'infores:test'}]
            }
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    
    # Check no domain/range violations
    violations = [r for r in results if 'domain constraint' in r.message or 'range constraint' in r.message]
    assert len(violations) == 0


@pytest.mark.parametrize("subject_node,object_node,violation_type,expected_message_parts", [
    # Invalid domain: Gene treats Disease
    (
        {'id': 'HGNC:1111', 'category': ['biolink:Gene'], 'name': 'Test Gene'},
        {'id': 'MONDO:5678', 'category': ['biolink:Disease'], 'name': 'Test Disease'},
        'domain',
        ["expects domain 'chemical or drug or treatment'", "subject has categories ['biolink:Gene']"]
    ),
    # Invalid range: Drug treats Gene
    (
        {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
        {'id': 'HGNC:1111', 'category': ['biolink:Gene'], 'name': 'Test Gene'},
        'range',
        ["expects range 'disease or phenotypic feature'", "object has categories ['biolink:Gene']"]
    ),
])
def test_domain_range_validation_with_violations(subject_node, object_node, violation_type, expected_message_parts):
    """Test that invalid treats edges fail domain/range validation with expected messages"""
    test_data = {
        'nodes': [subject_node, object_node],
        'edges': [
            {
                'subject': subject_node['id'], 
                'predicate': 'biolink:treats', 
                'object': object_node['id'],
                'sources': [{'resource_id': 'infores:test'}]
            }
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    
    violations = [r for r in results if f'{violation_type} constraint' in r.message]
    assert len(violations) == 1
    
    # Check that all expected message parts are present
    for expected_part in expected_message_parts:
        assert expected_part in violations[0].message


def test_domain_range_validation_missing_nodes_in_cache():
    """Test that domain/range validation is skipped when nodes are not in cache"""
    # Create edges without corresponding nodes
    test_data = {
        'nodes': [],  # Empty nodes list
        'edges': [
            {
                'subject': 'MISSING:1234', 
                'predicate': 'biolink:treats', 
                'object': 'MISSING:5678',
                'sources': [{'resource_id': 'infores:test'}]
            }
        ]
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    
    # Should have errors about missing nodes, but no domain/range constraint violations
    domain_range_violations = [r for r in results if 'domain constraint' in r.message or 'range constraint' in r.message]
    assert len(domain_range_violations) == 0
    
    # Should have errors about missing node references
    missing_node_errors = [r for r in results if 'non-existent' in r.message]
    assert len(missing_node_errors) == 2  # One for subject, one for object


def test_mixin_categories_are_valid():
    """Test that mixin categories like GenomicEntity are recognized as valid.
    
    This test addresses the issue where GenomicEntity (a mixin) was incorrectly
    flagged as invalid. Mixins can be used as node categories even though they
    don't appear directly in get_descendants() results.
    """
    # Create a node with GenomicEntity as category (a common mixin)
    test_data = {
        'nodes': [
            {
                'id': 'HGNC:1234',
                'category': ['biolink:GenomicEntity'],
                'name': 'Test Genomic Entity'
            }
        ],
        'edges': []
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    
    # Check that GenomicEntity is NOT flagged as invalid category
    invalid_category_warnings = [r for r in results if 'potentially invalid category' in r.message.lower()]
    assert len(invalid_category_warnings) == 0, \
        f"GenomicEntity should be a valid category, but got warnings: {[r.message for r in invalid_category_warnings]}"


def test_multiple_mixin_categories():
    """Test that nodes can have multiple categories including mixins."""
    # Gene is a concrete class that uses multiple mixins including GenomicEntity
    test_data = {
        'nodes': [
            {
                'id': 'HGNC:1234',
                'category': ['biolink:Gene', 'biolink:GenomicEntity'],
                'name': 'Test Gene'
            }
        ],
        'edges': []
    }
    
    schema = get_biolink_schema()
    plugin = BiolinkValidationPlugin(schema_view=schema)
    context = ValidationContext(target_class='KnowledgeGraph', schema=schema.schema)
    
    results = list(plugin.process(test_data, context))
    
    # Neither category should be flagged as invalid
    invalid_category_warnings = [r for r in results if 'potentially invalid category' in r.message.lower()]
    assert len(invalid_category_warnings) == 0, \
        f"Both Gene and GenomicEntity should be valid categories, but got warnings: {[r.message for r in invalid_category_warnings]}"
