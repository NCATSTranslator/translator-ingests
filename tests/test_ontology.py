import pytest

from translator_ingest.util.ontology import lookup_uberon

@pytest.mark.parametrize(
    "name,expected",
    [
        ("blood", "UBERON:0000178"),
        ("saliva", "UBERON:0001836"),
        ("cerebrospinal fluid", "UBERON:0001359"),
        ("csf", "UBERON:0001359"),  # normalized to 'cerebrospinal fluid'
        ("urine", "UBERON:0001088"),
        ("feces", "UBERON:0001988"),
        ("stool", "UBERON:0001988"),  # alias of 'feces' in Uberon
    ],
)
def test_uberon_lookup(name:str, expected:str):
    entry: dict = lookup_uberon(name)
    assert entry["uberon_id"] == expected, f"Entry '{entry!s}' returned for query name: '{name}'"
