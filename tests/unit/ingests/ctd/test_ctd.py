import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalEntity,
    Disease,
    RetrievalSource,
)

from koza.runner import KozaRunner, KozaTransformHooks
from translator_ingest.ingests.ctd.ctd import (
    transform_chemical_to_disease as ctd_transform,
    BIOLINK_ASSOCIATED_WITH,
    BIOLINK_CORRELATED_WITH,
    BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
)

from tests.unit.ingests import MockKozaWriter


@pytest.fixture
def therapeutic_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "10,11-dihydro-10-hydroxycarbamazepine",
        "ChemicalID": "C039775",
        "CasRN": "",
        "DiseaseName": "Epilepsy",
        "DiseaseID": "MESH:D004827",
        "DirectEvidence": "therapeutic",
        "InferenceGeneSymbol": "",
        "InferenceScore": "",
        "OmimIDs": "",
        "PubMedIDs": "17516704|123",
    }
    runner = KozaRunner(data=[record], writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_therapeutic_entities(therapeutic_output):
    entities = therapeutic_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT
    assert "PMID:17516704" in association.publications
    assert "PMID:123" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D004827"
    assert disease.name == "Epilepsy"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C039775"
    assert chemical.name == "10,11-dihydro-10-hydroxycarbamazepine"


@pytest.fixture
def marker_mechanism_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "10,10-bis(4-pyridinylmethyl)-9(10H)-anthracenone",
        "ChemicalID": "C112297",
        "CasRN": "",
        "DiseaseName": "Hyperkinesis",
        "DiseaseID": "MESH:D006948",
        "DirectEvidence": "marker/mechanism",
        "InferenceGeneSymbol": "",
        "InferenceScore": "",
        "OmimIDs": "",
        "PubMedIDs": "19098162",
    }
    runner = KozaRunner(data=[record], writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_marker_mechanism(marker_mechanism_output):
    entities = marker_mechanism_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_CORRELATED_WITH
    assert "PMID:19098162" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D006948"
    assert disease.name == "Hyperkinesis"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C112297"
    assert chemical.name == "10,10-bis(4-pyridinylmethyl)-9(10H)-anthracenone"


@pytest.fixture
def genetic_inference_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "10-(2-pyrazolylethoxy)camptothecin",
        "ChemicalID": "C534422",
        "CasRN": "",
        "DiseaseName": "Melanoma",
        "DiseaseID": "MESH:D008545",
        "DirectEvidence": "",
        "InferenceGeneSymbol": "CASP8",
        "InferenceScore": "4.23",
        "OmimIDs": "",
        "PubMedIDs": "18563783|21983787",
    }
    runner = KozaRunner(data=[record], writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_genetic_inference(genetic_inference_output):
    entities = genetic_inference_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_ASSOCIATED_WITH
    assert "PMID:21983787" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    assert association.has_confidence_score == 4.23

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D008545"
    assert disease.name == "Melanoma"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C534422"
    assert chemical.name == "10-(2-pyrazolylethoxy)camptothecin"
