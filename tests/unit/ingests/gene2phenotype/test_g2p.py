import pytest

# import koza    ## for mocking on_data_begin
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter

## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Disease,
    ChemicalOrGeneOrGeneProductFormOrVariantEnum,
    GeneToDiseaseAssociation,
    RetrievalSource,
)
import datetime
from translator_ingest.ingests.gene2phenotype.gene2phenotype import on_begin, transform


## trying to mock on_data_begin, create koza.state["allelicreq_mappings"] in hard-coded manner.
## but having trouble getting it to work
# @pytest.fixture
# def mock_on_begin():
#     koza = koza.KozaTransform()
#     koza.state["allelicreq_mappings"] = {
#         "biallelic_autosomal": "HP:0000007",
#         "monoallelic_autosomal": "HP:0000006",
#         "biallelic_PAR": "HP:0034341",
#         "monoallelic_PAR": "HP:0034340",
#         "mitochondrial": "HP:0001427",
#         "monoallelic_Y_hemizygous": "HP:0001450",
#         "monoallelic_X": "HP:0001417",
#         "monoallelic_X_hemizygous": "HP:0001419",
#         "monoallelic_X_heterozygous": "HP:0001423"
#     }
#     return koza.state["allelicreq_mappings"]


@pytest.fixture
def single_record_test():
    writer = MockKozaWriter()
    ## From searching resource file: grep -m 1 "monoallelic_X_heterozygous"
    records = [
        {
            "g2p id": "G2P00016",
            "hgnc id": "18704",
            "disease mim": None,
            "disease MONDO": "MONDO:0100124",
            "allelic requirement": "monoallelic_X_heterozygous",
            "confidence": "definitive",
            "molecular mechanism": "undetermined",
            "publications": "25099252",
            "date of last review": "2015-07-22 16:14:09+00:00",
        },
    ]
    ## running on_begin is problematic because it actually runs requests to retrieve outside data
    runner = KozaRunner(
        data=records, writer=writer, hooks=KozaTransformHooks(on_data_begin=[on_begin], transform_record=[transform])
    )
    runner.run()
    return writer.items


def test_ebi_g2p(single_record_test):
    ## check basic output
    entities = single_record_test
    assert entities
    ## 1 edges/associations, 2 nodes
    assert len(entities) == 3

    ## check first record's transform
    ## Doing because entities includes Nodes as well
    association1 = [e for e in entities if isinstance(e, GeneToDiseaseAssociation)][0]
    assert association1
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert (
        association1.subject_form_or_variant_qualifier
        == ChemicalOrGeneOrGeneProductFormOrVariantEnum.genetic_variant_form
    )
    ## koza turns the dates into this type
    assert association1.update_date == datetime.date(2015, 7, 22)
    assert association1.allelic_requirement == "HP:0001423"
    ## publications stuff
    assert len(association1.publications) == 1
    assert association1.publications[0] == "PMID:25099252"
    ## sources stuff
    assert association1.sources
    assert len(association1.sources) == 1
    ebi_g2p_source = association1.sources[0]
    assert isinstance(ebi_g2p_source, RetrievalSource)
    assert ebi_g2p_source.source_record_urls == ["https://www.ebi.ac.uk/gene2phenotype/lgd/G2P00016"]
    ## nodes
    gene = [e for e in entities if isinstance(e, Gene)][0]
    assert gene.id == "HGNC:18704"
    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MONDO:0100124"
