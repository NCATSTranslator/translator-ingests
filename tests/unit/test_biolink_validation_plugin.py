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


@pytest.mark.parametrize("subject_node,object_node,expected_violations,violation_type", [
    # Valid: Drug treats Disease
    (
        {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
        {'id': 'MONDO:5678', 'category': ['biolink:Disease'], 'name': 'Test Disease'},
        0,
        None
    ),
    # Invalid domain: Gene treats Disease
    (
        {'id': 'HGNC:1111', 'category': ['biolink:Gene'], 'name': 'Test Gene'},
        {'id': 'MONDO:5678', 'category': ['biolink:Disease'], 'name': 'Test Disease'},
        1,
        'domain'
    ),
    # Invalid range: Drug treats Gene
    (
        {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
        {'id': 'HGNC:1111', 'category': ['biolink:Gene'], 'name': 'Test Gene'},
        1,
        'range'
    ),
    # Valid: Drug treats PhenotypicFeature
    (
        {'id': 'CHEBI:1234', 'category': ['biolink:Drug'], 'name': 'Test Drug'},
        {'id': 'HP:9999', 'category': ['biolink:PhenotypicFeature'], 'name': 'Test Phenotype'},
        0,
        None
    ),
])
def test_domain_range_validation(subject_node, object_node, expected_violations, violation_type):
    """Test domain and range validation for treats predicate"""
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
    
    if violation_type:
        violations = [r for r in results if f'{violation_type} constraint' in r.message]
        assert len(violations) == expected_violations
        if expected_violations > 0:
            if violation_type == 'domain':
                assert 'expects domain \'chemical or drug or treatment\'' in violations[0].message
                assert f"subject has categories {subject_node['category']}" in violations[0].message
            elif violation_type == 'range':
                assert 'expects range \'disease or phenotypic feature\'' in violations[0].message
                assert f"object has categories {object_node['category']}" in violations[0].message
    else:
        # Check no domain/range violations
        violations = [r for r in results if 'domain constraint' in r.message or 'range constraint' in r.message]
        assert len(violations) == expected_violations
