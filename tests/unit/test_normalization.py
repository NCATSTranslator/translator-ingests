# Unit tests for normalizations of nodes and edges, based on Pydantic models
from typing import Optional, Union, List, Dict
from copy import deepcopy

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

MOCK_NN_GENE_ONLY_DATA: Dict[str, Union[List[Union[str, Dict[str,str]]], Dict[str,str], int]] = {
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
      "biolink:MacromolecularMachineMixin"
    ]
  }

MOCK_NN_GP_CONFLATED_GENE_DATA = deepcopy(MOCK_NN_GENE_ONLY_DATA)
MOCK_NN_GP_CONFLATED_GENE_DATA["equivalent_identifiers"].extend(
    [
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
    ]
)
MOCK_NN_GP_CONFLATED_GENE_DATA["type"].extend(
    [
      "biolink:Protein",
      "biolink:GeneProductMixin",
      "biolink:Polypeptide",
      "biolink:ChemicalEntityOrProteinOrPolypeptide"
    ]
)

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
      "label": "Werner syndrome",
      "description": "A rare inherited syndrome characterized by premature aging with onset in the third decade "+
                     "of life  and with cardinal clinical features including bilateral cataracts, short stature, "+
                     "graying and thinning of scalp hair, characteristic skin disorders and premature onset of "+
                     "additional age-related disorders."
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

MOCK_NN_CHEMICAL_ONLY_DATA = {
    "id": {
      "identifier": "CHEBI:15377",
      "label": "Water"
    },
    "equivalent_identifiers": [
      {
        "identifier": "CHEBI:15377",
        "label": "water"
      },
      {
        "identifier": "UNII:059QF0KO0R",
        "label": "WATER"
      },
      {
        "identifier": "PUBCHEM.COMPOUND:962",
        "label": "Water"
      },
      {
        "identifier": "CHEMBL.COMPOUND:CHEMBL1098659",
        "label": "WATER"
      },
      {
        "identifier": "DRUGBANK:DB09145",
        "label": "Water"
      },
      {
        "identifier": "MESH:D014867",
        "label": "Water"
      },
      {
        "identifier": "CAS:231-791-2"
      },
      {
        "identifier": "CAS:7732-18-5"
      },
      {
        "identifier": "HMDB:HMDB0002111",
        "label": "Water"
      },
      {
        "identifier": "KEGG.COMPOUND:C00001",
        "label": "H2O"
      },
      {
        "identifier": "INCHIKEY:XLYOFNOQVPJJNP-UHFFFAOYSA-N"
      },
      {
        "identifier": "UMLS:C0043047",
        "label": "water"
      },
      {
        "identifier": "RXCUI:11295"
      }
    ],
    "type": [
      "biolink:SmallMolecule",
      "biolink:MolecularEntity",
      "biolink:ChemicalEntity",
      "biolink:PhysicalEssence",
      "biolink:ChemicalOrDrugOrTreatment",
      "biolink:ChemicalEntityOrGeneOrGeneProduct",
      "biolink:ChemicalEntityOrProteinOrPolypeptide",
      "biolink:NamedThing",
      "biolink:PhysicalEssenceOrOccurrent"
    ],
    "information_content": 47.7
  }


MOCK_NN_DC_CONFLATED_CHEMICAL_DATA = deepcopy(MOCK_NN_CHEMICAL_ONLY_DATA)
MOCK_NN_DC_CONFLATED_CHEMICAL_DATA["equivalent_identifiers"].extend(
    [
      {
        "identifier": "CHEBI:33813",
        "label": "((18)O)water"
      },
      {
        "identifier": "UNII:7QV8F8BYNJ",
        "label": "WATER O-18"
      },
      {
        "identifier": "PUBCHEM.COMPOUND:105142",
        "label": "Water-18O"
      },
      {
        "identifier": "MESH:C000615259",
        "label": "Oxygen-18"
      },
      {
        "identifier": "CAS:14314-42-2"
      },
      {
        "identifier": "CAS:14797-71-8"
      },
      {
        "identifier": "INCHIKEY:XLYOFNOQVPJJNP-NJFSPNSNSA-N"
      },
      {
        "identifier": "UMLS:C4546909",
        "label": "Oxygen-18"
      },
      {
        "identifier": "UNII:63M8RYN44N",
        "label": "WATER O-15"
      },
      {
        "identifier": "PUBCHEM.COMPOUND:10129877",
        "label": "Water O-15"
      }
      # The real server returns much more stuff but who cares?
    ]
)
MOCK_NN_DC_CONFLATED_CHEMICAL_DATA["type"].extend(
    [
      "biolink:Drug",
      "biolink:OntologyClass",
      "biolink:MolecularMixture",
      "biolink:ChemicalMixture"
    ]
)


#################### End of MOCK Node Normalizer result data ###################

@pytest.fixture(scope="module")
def mock_nn_query():
    # return Normalizer().get_normalized_nodes
    def mock_node_normalizer_query(query: Dict) -> Dict[str, Optional[Dict]]:
        # Fixture sanity check for a well-formed query input
        # I am also mimicking the default "conflate" and "drug_chemical_conflate" modes here
        assert query
        assert "curies" in query
        result: Dict[str, Optional[Dict]] = dict()
        for identifier in query["curies"]:
            if identifier in ["HGNC:12791", "NCBIGene:7486", "UniProtKB:Q14191"]:
                if "conflate" not in query or query["conflate"] is True:
                    # if the flag not provided, the "conflate" default is "True"
                    result[identifier] = MOCK_NN_GP_CONFLATED_GENE_DATA
                else:
                    result[identifier] = MOCK_NN_GENE_ONLY_DATA
            elif identifier == "DOID:5688":
                result[identifier] = MOCK_NN_DISEASE_DATA
            elif identifier == "MESH:D014867":
                if "drug_chemical_conflate" in query and query["drug_chemical_conflate"] is True:
                    result[identifier] = MOCK_NN_DC_CONFLATED_CHEMICAL_DATA
                else:
                    # if the flag is not provided, the "drug_chemical_conflate" default is "False"
                    result[identifier] = MOCK_NN_CHEMICAL_ONLY_DATA
            else:
                result[identifier] = None
        return result
    return mock_node_normalizer_query



# def convert_to_preferred(curie, allowed_list, mock_nn_query):
def test_convert_to_preferred(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    result: str = normalizer.convert_to_preferred(curie="HGNC:12791", allowed_list=["UniProtKB"])
    # Gene-protein conflation is on by default so...
    assert result == "UniProtKB:Q14191"
    result = normalizer.convert_to_preferred(curie="HGNC:12791", allowed_list=["UniProtKB"], gp_conflate=False)
    # but if I explicitly turn it off now...it will not be found!
    assert result is None


# def normalize_identifiers(
#       self, curies: List[str], gp_conflate: bool = False, dc_conflate: bool = False
# ) -> Dict[str, str]
def test_normalize_identifiers(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    result: Dict[str,str] = normalizer.normalize_identifiers(curies=["HGNC:12791","DOID:5688"])
    assert result
    assert result["HGNC:12791"] == "NCBIGene:7486"
    assert result["DOID:5688"] == "MONDO:0010196"
    with pytest.raises(AssertionError):
        # Empty curies list is not permitted
        normalizer.normalize_identifiers(curies=[])

    # conflate gene and proteins
    result = normalizer.normalize_identifiers(curies=["HGNC:12791"])

    # conflate drugs and chemicals
    assert result["HGNC:12791"] == "NCBIGene:7486"
    result = normalizer.normalize_identifiers(curies=["HGNC:12791", "DOID:5688"])
    assert result["HGNC:12791"] == "NCBIGene:7486"


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
    # Gene-protein conflation is on by default
    assert "UniProtKB:Q14191" in result.xref
    # should include original node name field value as a synonym
    assert "Werner Syndrome Locus" in result.synonym
    # should include additional names as synonyms...
    assert "WRN (Hsap)" in result.synonym
    # Gene-protein conflation is on by default
    assert "WRN protein, human" in result.synonym
    # .. but not the canonical name
    assert "WRN" not in result.synonym

def test_gene_protein_conflated_normalize_node(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    node = NamedThing(
        id="HGNC:12791",
        name="Werner Syndrome Locus",
        category=["biolink:NamedThing"],
        **{}
    )
    result: Optional[NamedThing] = normalizer.normalize_node(deepcopy(node), gp_conflate=False)
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "NCBIGene:7486"
    # Gene-protein conflation is 'off' so protein identifiers...
    assert "UniProtKB:Q14191" not in result.xref
    # ... and names should NOT be visible
    assert "WRN protein, human" not in result.synonym

    result: Optional[NamedThing] = normalizer.normalize_node(deepcopy(node), gp_conflate=False)  # default is gp_conflate=False
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "NCBIGene:7486"
    # Gene-protein conflation is 'off' so protein identifiers...
    assert "UniProtKB:Q14191" not in result.xref
    # ... and names should NOT be visible
    assert "WRN protein, human" not in result.synonym

    result: Optional[NamedThing] = normalizer.normalize_node(deepcopy(node))
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "NCBIGene:7486"
    # Gene-protein conflation is 'on' so protein identifiers...
    assert "UniProtKB:Q14191" in result.xref
    # ... and names should now be visible
    assert "WRN protein, human" in result.synonym

def test_drug_chemical_conflated_normalize_node(mock_nn_query):
    normalizer = Normalizer(endpoint=mock_nn_query)
    node = NamedThing(
        id="MESH:D014867",
        name="Water",
        category=["biolink:NamedThing"],
        **{}
    )

    result: Optional[NamedThing] = normalizer.normalize_node(deepcopy(node), dc_conflate=False)
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "CHEBI:15377"
    # Drug-chemical conflation is 'on' so additional chemical identifiers should be returned
    assert "CHEBI:33813" not in result.xref
    # ... and names should now be visible
    assert "((18)O)water" not in result.synonym

    result = normalizer.normalize_node(deepcopy(node))  # default is dc_conflate=False
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "CHEBI:15377"
    # Drug-chemical conflation is 'on' so additional chemical identifiers should be returned
    assert "CHEBI:33813" not in result.xref
    # ... and names should now be visible
    assert "((18)O)water" not in result.synonym

    result = normalizer.normalize_node(deepcopy(node), dc_conflate=True)
    # Valid input query identifier, so should return a result
    assert result is not None
    # ... should be the canonical identifier and name
    assert result.id == "CHEBI:15377"
    # Drug-chemical conflation is 'on' so additional chemical identifiers should be returned
    assert "CHEBI:33813" in result.xref
    # ... and names should now be visible
    assert "((18)O)water" in result.synonym

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
