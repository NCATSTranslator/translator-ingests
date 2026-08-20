import pytest

from translator_ingest.util.ontology import lookup, lookup_go, lookup_mondo, lookup_uberon

def check_match(query: str, expected: str, match: dict[str,str] | None):
    assert match is not None, f"No entry returned for query name: '{query}'"
    assert match["id"] == expected, (
        f"Entry '{match!s}' returned for query name: '{query}' did not match expected term '{expected}'"
    )


@pytest.mark.parametrize(
    "query,ontology,expected",
    [
        ("Glucose", "CHEBI","CHEBI:17234"),
    ],
)
def test_generic_exact_match_lookup(query:str, ontology:str, expected:str):
    check_match(query, expected, lookup(query, ontology))


@pytest.mark.parametrize(
    "query,expected",
    [
        ("Cytoplasm", "GO:0005737"),
        ("cell proliferation", "GO:1904898"),
        ("apoptotic process", "GO:0006915"),
        ("apoptosis", "GO:0006915"),
        ("mitochondrion", "GO:0005739"),
        ("DNA binding", "GO:0003677"),
    ],
)
def test_go_exact_match_lookup(query:str, expected:str):
    check_match(query, expected, lookup_go(query))


@pytest.mark.parametrize(
    "query,expected",
    [
        ("Werner Syndrome", "MONDO:0010196"),
        ("breast carcinoma","MONDO:0004989"),
        ("breast cancer","MONDO:0007254"),
        ("Von Hippel-Lindau","MONDO:0008667")
    ],
)
def test_mondo_exact_match_lookup(query:str, expected:str):
    check_match(query, expected, lookup_mondo(query))


@pytest.mark.parametrize(
    "query,expected",
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
def test_uberon_exact_match_lookup(query:str, expected:str):
    check_match(query, expected, lookup_uberon(query))
