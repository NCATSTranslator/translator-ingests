from typing import Iterable

import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalEntity,
    Disease, RetrievalSource,
)
from koza.io.writer.writer import KozaWriter
from koza.runner import KozaRunner, KozaTransformHooks
from src.translator_ingest.ingests.ctd.ctd import transform_record_chemical_to_disease as ctd_transform


BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"


class MockWriter(KozaWriter):
    def __init__(self):
        self.items = []

    def write(self, entities):
        self.items += entities

    def write_nodes(self, nodes: Iterable):
        self.items += nodes

    def write_edges(self, edges: Iterable):
        self.items += edges

    def finalize(self):
        pass


@pytest.fixture
def therapeutic_output():
    writer = MockWriter()
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
    runner = KozaRunner(data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_therapeutic_entities(therapeutic_output):
    entities = therapeutic_output
    assert entities
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association
    assert association.predicate == BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT
    assert "PMID:17516704" in association.publications
    assert "PMID:123" in association.publications

    assert association.sources and \
           isinstance(association.sources[0], RetrievalSource) and \
           association.sources[0].resource_id == "infores:ctd"

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D004827"
    assert disease.name == "Epilepsy"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C039775"
    assert chemical.name == "10,11-dihydro-10-hydroxycarbamazepine"

