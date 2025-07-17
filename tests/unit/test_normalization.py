# Unit tests for normalizations of nodes and edges, based on Pydantic models
from typing import Optional, Dict

import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    Association,
    Gene
)

from src.translator_ingest.util.normalize import Normalizer

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
def mock_nn_query():
    # return Normalizer().get_normalized_nodes
    def mock_node_normalizer_query(query: Dict) -> Dict[str, Optional[Dict]]:
        # Fixture sanity check for a well-formed query input
        assert query and "curies" in query
        result: Dict[str, Optional[Dict]] = dict()
        for identifier in query["curies"]:
            if identifier in ["HGNC:12791", "NCBIGene:7486", "UniProtKB:Q14191"]:
                result[identifier] = MOCK_NN_GENE_DATA
            elif identifier == "DOID:5688":
                result[identifier] = MOCK_NN_DISEASE_DATA
            else:
                result[identifier] = None
        return result
    return mock_node_normalizer_query



# def convert_to_preferred(curie, allowed_list, mock_nn_query):
def test_convert_to_preferred(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    result: str = normalizer.convert_to_preferred(curie="HGNC:12791", allowed_list=["UniProtKB"])
    assert result == "UniProtKB:Q14191"


# def normalize_identifiers(self, curies: List[str]) -> Dict[str, str]
def test_normalize_identifiers(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    result: Dict[str,str] = normalizer.normalize_identifiers(curies=["HGNC:12791","DOID:5688"])
    assert result
    assert result["HGNC:12791"] == "NCBIGene:7486"
    assert result["DOID:5688"] == "MONDO:0010196"
    with pytest.raises(AssertionError):
        # Empty curies list not permitted
        normalizer.normalize_identifiers(curies=[])

# normalize_node(node: NamedThing, mock_nn_query) -> Optional[NamedThing]
def test_normalize_missing_node(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    node = NamedThing(id="foo:bar", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalizer.normalize_node(node)
    assert result is None


def test_normalize_node(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    node = NamedThing(
        id="HGNC:12791",
        name="Werner Syndrome Locus",
        category=["biolink:NamedThing"],
        **{}
    )
    result: Optional[NamedThing] = normalizer.normalize_node(node)
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



def test_normalize_node_already_canonical(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    node = NamedThing(id="NCBIGene:7486", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalizer.normalize_node(node)
    assert result.id == "NCBIGene:7486"


def test_normalize_non_named_thing_node(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    node = Gene(id="HGNC:12791", category=["biolink:Gene"],**{})
    result: Optional[NamedThing] = normalizer.normalize_node(node)
    assert result.id == "NCBIGene:7486"


def test_normalize_edge(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    edge = Association(
        id="ingest:test-association",
        subject="HGNC:12791",
        predicate="causes",
        object="DOID:5688",
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
        **{})
    result: Optional[Association] = normalizer.normalize_edge(edge)
    assert result.subject == "NCBIGene:7486"
    assert result.object == "MONDO:0010196"


def test_normalize_edge_invalid_subject(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    # foo:bar is a nonsense 'subject' curie...
    edge = Association(
        id="ingest:test-association",
        subject="foo:bar",
        predicate="causes",
        object="DOID:5688",
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
        **{})
    result: Optional[Association] = normalizer.normalize_edge(edge)
    assert result is None
