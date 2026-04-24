import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter
## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    DiseaseOrPhenotypicFeature,  ## using just in case: NodeNorm does use MESH IDs for some phenos
    Gene,
    Association,
)
from translator_ingest.ingests.pubtator.pubtator import transform_row
from translator_ingest.ingests.pubtator.mappings import RELATION_MODELING


## treat
@pytest.fixture
def treat_output():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    ## current webpage: https://www.ncbi.nlm.nih.gov/research/pubtator3/docsum?text=relations:treat%7C@CHEMICAL_IMT504%7C@DISEASE_Pain
    record = {
        "entity1_id": "MESH:C514285",  ## IMT504
        "relation": "treat",
        "entity2_id": "MESH:D010146",  ## Pain
        "pmid_set": ["PMID:36706889", "PMID:33221983", "PMID:33502706", "PMID:38484854"],
        "entity1_type": "Chemical",
        "entity2_type": "Disease",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items


def test_treat_output(treat_output):
    ## check basic output
    entities = treat_output
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents - Association type should match mapping
    association = [e for e in entities if isinstance(e, RELATION_MODELING["treat"]["association"])][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert association.predicate == RELATION_MODELING["treat"]["predicate"]
    assert association.publications
    assert len(association.publications) == 4

    ## check Node contents - code used to map entity type to biolink category
    chem = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chem
    assert chem.id == "MESH:C514285"
    disease = [e for e in entities if isinstance(e, DiseaseOrPhenotypicFeature)][0]
    assert disease
    assert disease.id == "MESH:D010146"


## negative_correlate
@pytest.fixture
def negcorrel_output():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    ## current webpage: https://www.ncbi.nlm.nih.gov/research/pubtator3/docsum?text=relations:negative_correlate%7C@GENE_TIGIT%7C@GENE_LAMP1
    record = {
        "entity1_id": "201633",  ## TIGIT
        "relation": "negative_correlate",
        "entity2_id": "3916",  ## LAMP1/CD107a
        "pmid_set": ["PMID:32903786", "PMID:33767694"],
        "entity1_type": "Gene",
        "entity2_type": "Gene",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items


def test_negcorrel_output(negcorrel_output):
    ## check basic output
    entities = negcorrel_output
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents - Association type should match mapping
    association = [e for e in entities if isinstance(e, Association)][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert association.predicate == RELATION_MODELING["negative_correlate"]["predicate"]
    assert association.publications
    assert len(association.publications) == 2

    ## check Node contents - code used to map entity type to biolink category
    genes = [e for e in entities if isinstance(e, Gene)]
    assert genes
    assert set([i.id for i in genes]) == set(["NCBIGene:201633", "NCBIGene:3916"])


## drug_interact
@pytest.fixture
def drug_interact_output():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    ## current webpage: https://www.ncbi.nlm.nih.gov/research/pubtator3/docsum?text=relations:drug_interact%7C@CHEMICAL_Everolimus%7C@CHEMICAL_Cyclosporine
    record = {
        "entity1_id": "MESH:D000068338",    ## Everolimus
        "relation": "drug_interact",
        "entity2_id": "MESH:D016572",  ## Cyclosporine
        "pmid_set": ["PMID:14550821", "PMID:17543796"],
        "entity1_type": "Chemical",
        "entity2_type": "Chemical",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items


def test_drug_interact_output(drug_interact_output):
    ## check basic output
    entities = drug_interact_output
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents - Association type should match mapping
    association = [e for e in entities if isinstance(e, RELATION_MODELING["drug_interact"]["association"])][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert association.predicate == RELATION_MODELING["drug_interact"]["predicate"]
    assert association.publications
    assert len(association.publications) == 2

    ## check Node contents - code used to map entity type to biolink category
    chems = [e for e in entities if isinstance(e, ChemicalEntity)]
    assert chems
    assert set([i.id for i in chems]) == set(["MESH:D000068338", "MESH:D016572"])
