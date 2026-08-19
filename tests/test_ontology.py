import pytest

from translator_ingest.util.ontology import lookup_uberon

@pytest.mark.parametrize(
    "name,expected",
    [
        ("Blood", "UBERON:0000178"),
        ("Saliva", "UBERON:0001836"),
        ("Urine", "UBERON:0001088"),
        ("Feces", "UBERON:0001988"),

        # alias of 'feces' in Uberon
        ("Stool", "UBERON:0001988"),

        # various other aliases may be arcane to some sources
        # Here, we have some normalized to 'cerebrospinal fluid'
        ("Cerebrospinal Fluid", "UBERON:0001359"),
        ("CSF", "UBERON:0001359"),
        ("Cerebrospinal Fluid (CSF)", "UBERON:0001359"),  # "UBERON:0001359"),

        # Gross anatomical structures
        ("Placenta", "UBERON:0001987"),
        ("Skeletal muscle", "UBERON:0004857"),
    ],
)
def test_uberon_lookup(name:str, expected:str):
    entry: dict = lookup_uberon(name)
    assert entry["uberon_id"] == expected, f"Entry '{entry!s}' returned for query name: '{name}'"
