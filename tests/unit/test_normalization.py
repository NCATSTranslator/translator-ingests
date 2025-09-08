# Unit tests for normalizations of nodes and edges, based on Pydantic models
"""
TODO - Evan commented this out after implementing normalization through ORION, we need to decide if we want to continue
    to use ORION or to implement specific normalization functionality in translator-ingests. If in ORION these kinds of
    tests should live there, if in translator-ingests we should revive these.

from typing import Optional, Dict

import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    Association,
    Gene
)

from src.translator_ingest.util.normalize import (
    convert_to_preferred,
    normalize_edge,
    normalize_node
)

#################### MOCK Node Normalizer result data ###################

MOCK_NN_GENE_DATA = {
    "equivalent_identifiers": [
      {
        "identifier": "NCBIGene:7486",
        "label": "WRN"
      },
      {
        "identifier": "ENSEMBL:ENSG00000165392",
        "label": "WRN (Hsap)"
      },
      {
        "identifier": "HGNC:12791",
        "label": "WRN"
      },
      {
        "identifier": "OMIM:604611"
      },
      {
        "identifier": "UMLS:C1337007",
        "label": "WRN gene"
      },
      {
        "identifier": "UniProtKB:Q14191",
        "label": "WRN_HUMAN Bifunctional 3'-5' exonuclease/ATP-dependent helicase WRN (sprot)"
      },
      {
        "identifier": "PR:Q14191",
        "label": "bifunctional 3'-5' exonuclease/ATP-dependent helicase WRN (human)"
      },
      {
        "identifier": "ENSEMBL:ENSP00000298139"
      },
      {
        "identifier": "ENSEMBL:ENSP00000298139.5"
      },
      {
        "identifier": "UMLS:C0388246",
        "label": "WRN protein, human"
      }
    ],
    "id": {
      "identifier": "NCBIGene:7486",
      "label": "WRN"
    },
    "information_content": 100,
    "type": [
      "biolink:Gene",
      "biolink:GeneOrGeneProduct",
      "biolink:GenomicEntity",
      "biolink:ChemicalEntityOrGeneOrGeneProduct",
      "biolink:PhysicalEssence",
      "biolink:OntologyClass",
      "biolink:BiologicalEntity",
      "biolink:ThingWithTaxon",
      "biolink:NamedThing",
      "biolink:PhysicalEssenceOrOccurrent",
      "biolink:MacromolecularMachineMixin",
      "biolink:Protein",
      "biolink:GeneProductMixin",
      "biolink:Polypeptide",
      "biolink:ChemicalEntityOrProteinOrPolypeptide"
    ]
  }

MOCK_NN_DISEASE_DATA = {
    "equivalent_identifiers": [
      {
        "identifier": "MONDO:0010196",
        "label": "Werner syndrome"
      },
      {
        "identifier": "DOID:5688",
        "label": "Werner syndrome"
      },
      {
        "identifier": "OMIM:277700"
      },
      {
        "identifier": "orphanet:902"
      },
      {
        "identifier": "UMLS:C0043119",
        "label": "Werner Syndrome"
      },
      {
        "identifier": "MESH:D014898",
        "label": "Werner Syndrome"
      },
      {
        "identifier": "MEDDRA:10049429"
      },
      {
        "identifier": "NCIT:C3447",
        "label": "Werner Syndrome"
      },
      {
        "identifier": "SNOMEDCT:51626007"
      },
      {
        "identifier": "medgen:12147"
      },
      {
        "identifier": "icd11.foundation:1864550134"
      }
    ],
    "id": {
      "identifier": "MONDO:0010196",
      "label": "Werner syndrome"
    },
    "information_content": 100,
    "type": [
      "biolink:Disease",
      "biolink:DiseaseOrPhenotypicFeature",
      "biolink:BiologicalEntity",
      "biolink:ThingWithTaxon",
      "biolink:NamedThing"
    ]
  }
#################### End of MOCK Node Normalizer result data ###################

@pytest.fixture(scope="module")
def nn_query():
    def mock_node_normalizer_query(query: Dict) -> Optional[Dict]:
        # Fixture sanity check for a well-formed query input
        assert query and "curies" in query
        query_id = query["curies"][0]
        if query_id in ["HGNC:12791", "NCBIGene:7486", "UniProtKB:Q14191"]:
            return {query_id: MOCK_NN_GENE_DATA}
        elif query_id == "DOID:5688":
            return {query_id: MOCK_NN_DISEASE_DATA}
        else:
            return { query_id: None }
    return mock_node_normalizer_query


# def convert_to_preferred(curie, allowed_list, nn_query):
def test_convert_to_preferred(nn_query):
    result: str = convert_to_preferred(
        "HGNC:12791", "UniProtKB",
        nn_query=nn_query
    )
    assert result == "UniProtKB:Q14191"


# normalize_node(node: NamedThing, nn_query) -> Optional[NamedThing]
def test_normalize_missing_node(nn_query):
    node = NamedThing(id="foo:bar", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalize_node(
        node,
        nn_query=nn_query
    )
    assert result is None


def test_normalize_node(nn_query):
    node = NamedThing(
        id="HGNC:12791",
        name="Werner Syndrome Locus",
        category=["biolink:NamedThing"],
        **{}
    )
    result: Optional[NamedThing] = normalize_node(
        node,
        nn_query=nn_query
    )
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "NCBIGene:7486"
    assert result.name == "WRN"
    # should include additional expected cross-references
    assert "OMIM:604611" in result.xref
    # ... but not the canonical identifier in xrefs (already used in node id)
    assert "NCBIGene:7486" not in result.xref
    # should include original node name field value as a synonym
    assert "Werner Syndrome Locus" in result.synonym
    # should include additional names as synonyms...
    assert "WRN protein, human" in result.synonym
    # .. but not the canonical name
    assert "WRN" not in result.synonym



def test_normalize_node_already_canonical(nn_query):
    node = NamedThing(id="NCBIGene:7486", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalize_node(
        node,
        nn_query=nn_query
    )
    assert result.id == "NCBIGene:7486"


def test_normalize_non_named_thing_node(nn_query):
    node = Gene(id="HGNC:12791", category=["biolink:Gene"],**{})
    result: Optional[NamedThing] = normalize_node(
        node,
        nn_query=nn_query
    )
    assert result.id == "NCBIGene:7486"


def test_normalize_edge(nn_query):
    edge = Association(
        id="ingest:test-association",
        subject="HGNC:12791",
        predicate="causes",
        object="DOID:5688",
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
        **{})
    result: Optional[Association] = normalize_edge(
        edge,
        nn_query=nn_query
    )
    assert result.subject == "NCBIGene:7486"
    assert result.object == "MONDO:0010196"


def test_normalize_edge_invalid_subject(nn_query):
    # foo:bar is a nonsense 'subject' curie...
    edge = Association(
        id="ingest:test-association",
        subject="foo:bar",
        predicate="causes",
        object="DOID:5688",
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
        **{})
    result: Optional[Association] = normalize_edge(
        edge,
        nn_query=nn_query
    )
    assert result is None
"""