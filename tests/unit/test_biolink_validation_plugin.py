"""
Tests for BiolinkValidationPlugin._is_valid_curie method
"""

import pytest
from src.translator_ingest.util.biolink_validation_plugin import BiolinkValidationPlugin


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
