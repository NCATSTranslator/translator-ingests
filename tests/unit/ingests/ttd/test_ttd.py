import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter

## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    DiseaseOrPhenotypicFeature,
    RetrievalSource,
)
from translator_ingest.ingests.ttd.ttd import (
    P1_05_transform,
)


## P1-05
@pytest.fixture
def P1_05_output():
    writer = MockKozaWriter()
    ## example of df row after parsing with P1_05_prepare
    ## from notebook, row with multiple clinical status values
    record = {
        "subject_pubchem": "PUBCHEM.COMPOUND:136033680",
        "biolink_predicate": "biolink:in_clinical_trials_for",
        "object_nameres_id": "MONDO:0018177",
        "subject_ttd_drug": {"D0V8AG"},
        "object_indication_name": {"Glioblastoma multiforme", "Recurrent glioblastoma"},
        "clinical_status": {"Phase 2", "Phase 1/2"},
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[P1_05_transform])
    )
    runner.run()
    return writer.items


def test_P1_05_output(P1_05_output):
    ## check basic output
    entities = P1_05_output
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents
    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## sources stuff
    assert association.sources
    assert len(association.sources) == 1
    P1_05_source = association.sources[0]
    assert isinstance(P1_05_source, RetrievalSource)
    assert P1_05_source.source_record_urls == [
        "https://ttd.idrblab.cn/data/drug/details/d0v8ag"
    ]

    chem = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chem.id == "PUBCHEM.COMPOUND:136033680"

    dop = [e for e in entities if isinstance(e, DiseaseOrPhenotypicFeature)][0]
    assert dop.id == "MONDO:0018177"
