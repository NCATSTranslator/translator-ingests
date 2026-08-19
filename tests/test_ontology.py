import pytest

from translator_ingest.util.ontology import lookup, lookup_go, lookup_mondo, lookup_uberon

@pytest.mark.parametrize(
    "name,ontology,expected",
    [
        ("Glucose", "CHEBI","CHEBI:17234"),
    ],
)
def test_generic_lookup(name:str, ontology:str, expected:str):
    entry: dict = lookup(name,ontology)
    assert entry["id"] == expected,\
        f"Entry '{entry!s}' returned for query name: '{name}' did not match expected term '{expected}'"


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Cytoplasm", "GO:0005737"),
        ("cell proliferation","GO:1904898"),
        ("apoptotic process","GO:0006915"),
        ("mitochondrion","GO:0005739"),
        ("DNA binding","GO:0003677")
    ],
)
def test_go_lookup(name:str, expected:str):
    entry: dict = lookup_go(name)
    assert entry["id"] == expected,\
        f"Entry '{entry!s}' returned for query name: '{name}' did not match expected term '{expected}'"


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Werner Syndrome", "MONDO:0010196"),
        ("breast carcinoma","MONDO:0004989"),
        ("breast cancer","MONDO:0007254"),
        ("Von Hippel-Lindau","MONDO:0008667")
    ],
)
def test_mondo_lookup(name:str, expected:str):
    entry: dict = lookup_mondo(name)
    assert entry["id"] == expected,\
        f"Entry '{entry!s}' returned for query name: '{name}' did not match expected term '{expected}'"


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
        ("Skeletal muscle", "UBERON:0004857")
    ],
)
def test_uberon_lookup(name:str, expected:str):
    entry: dict = lookup_uberon(name)
    assert entry["id"] == expected,\
        f"Entry '{entry!s}' returned for query name: '{name}' did not match expected term '{expected}'"
