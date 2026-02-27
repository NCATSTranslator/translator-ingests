import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
)

## import what I'm testing
from translator_ingest.ingests.drugcentral.drugcentral import (
    omop_transform,
)
## import from mapping file
from translator_ingest.ingests.drugcentral.mappings import OMOP_RELATION_MAPPING


## omop_relationship
## testing record for "off-label use": has the extra edge-attribute
@pytest.fixture
def omop_off_label():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "struct_id": 144,
        "relationship_name": "off-label use",
        "umls_cui": "C0022336",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[omop_transform])
    )
    runner.run()
    return writer.items

def test_omop_output(omop_off_label):
    ## check basic output
    entities = omop_off_label
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## predicate + edge-attribute
    assert association.predicate == OMOP_RELATION_MAPPING["off-label use"]["predicate"]
    ## should have edge-attribute for off-label
    assert association.clinical_approval_status == "off_label_use"

    ## sources
    assert association.sources
    assert len(association.sources) == 1
    ## check source_record_url
    assert len(association.sources[0].source_record_urls) == 1
