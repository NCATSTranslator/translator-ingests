from typing import Any
import pytest

from translator_ingest.util.ontology import lookup, lookup_go, lookup_mondo, lookup_uberon
from translator_ingest.util.ontology.cache import cache_lookup

@pytest.mark.parametrize(
    "query,ontology,only_taxa,expected_term,biolink_type",
    [
        (
                "WRN",
                "NCBIGene",
                "NCBITaxon:9606",
                "NCBIGene:7486",
                "biolink:Gene"
        ),
        (
                "Glucose",
                "CHEBI",
                None,
                "CHEBI:17234",
                "biolink:SmallMolecule"
        ),
        (
                "Cytoplasm",
                "GO",
                None,
                "GO:0005737",
                "biolink:CellularComponent"
        ),
        (
                "Werner Syndrome",
                "MONDO",
                None,
                "MONDO:0010196",
                "biolink:Disease"
        ),
        (
                "Placenta",
                "UBERON",
                None,
                "UBERON:0001987",
                "biolink:GrossAnatomicalStructure"
        )
    ],
)
def test_cache_lookup(
        query: str,
        ontology: str,
        only_taxa:str|None,
        expected_term:str,
        biolink_type:str
):
    """Test that the NRS cache retrieval works."""
    match: dict[str, Any] | None = cache_lookup(query=query, ontology=ontology, only_taxa=only_taxa)
    assert match is not None, "No entry returned for test query"
    assert "curie" in match, "Entry missing 'curie' key for test query"
    assert only_taxa is None or only_taxa in match["taxa"], "Taxon not found in entry for test query"
    assert match["curie"] == expected_term, "Incorrect entry returned for test query"
    assert match["types"][0] == biolink_type, "Incorrect specific Biolink type returned for test query"


def check_match(query: str, expected: str, match: dict[str,str] | None):
    assert match is not None, f"No entry returned for query name: '{query}'"
    assert match["curie"] == expected, (
        f"Entry '{match!s}' returned for query name: '{query}' did not match expected term '{expected}'"
    )


@pytest.mark.parametrize(
    "query,expected",
    [
        ("Cytoplasm", "GO:0005737"),
        ("apoptotic process", "GO:0006915"),

        # apoptosis is a problem child query term, gets some other
        # match even though it is a synonym of apoptotic process
        ("apoptosis", "GO:0097194"),

        ("mitochondrion", "GO:0005739"),
        ("DNA binding", "GO:0003677"),
    ],
)
def test_go_lookup(query:str, expected:str):
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
def test_mondo_lookup(query:str, expected:str):
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
        ("Cerebrospinal Fluid (CSF)", "UBERON:0001359"),

        # Gross anatomical structures
        ("Placenta", "UBERON:0001987"),
        ("Skeletal muscle", "UBERON:0014892")
    ],
)
def test_uberon_lookup(query:str, expected:str):
    check_match(query, expected, lookup_uberon(query))
