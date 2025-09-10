from typing import Iterable

import pytest
#We need to import this to go from a string to a dictonary (in a safe way). 
from ast import literal_eval
from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToExpressionSiteAssociation,
    Gene,
    Cell,
)
from koza.io.writer.writer import KozaWriter
from koza.runner import KozaRunner, KozaTransformHooks
from src.translator_ingest.ingests.bgee.bgee import transform_ingest_by_record as bgee_transform


BIOLINK_EXPRESSED_IN = "biolink:expressed_in"
INFORES_BGEE = "infores:bgee"

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

bgee_record = {
    'Gene ID': 'ENSG00000000003', 
    'Gene name': 'TSPAN6',
    'Anatomical entity ID': 'CL:0000015',
    'Anatomical entity name':'male germ cell',
    'Expression': 'present',
    'Call quality':'gold quality',
    'FDR': 0.0018249569679985526,
    'Expression score': 99.04, 
    'Expression rank': 449.0
    }

@pytest.fixture
def bgee_output():
    writer = MockWriter()

    runner = KozaRunner(data=iter([bgee_record]), writer=writer, hooks=KozaTransformHooks(transform_record=[bgee_transform]))
    runner.run()
    return writer.items


def test_bgee(bgee_output):
    entities = bgee_output
    assert entities
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, GeneToExpressionSiteAssociation)][0]
    assert association
    assert association.predicate == BIOLINK_EXPRESSED_IN
    assert association.primary_knowledge_source == INFORES_BGEE
    assert association.adjusted_p_value == bgee_record["FDR"]
    attribute_dict = literal_eval(association.has_attribute[0])
    assert attribute_dict
    assert attribute_dict["BGee_CallQuality"]==bgee_record['Call quality']
    assert attribute_dict["BGee_FDR"]==bgee_record["FDR"]
    assert attribute_dict["BGee_ExpressionScore"]==bgee_record["Expression score"]
    assert attribute_dict["BGee_ExpressionRank"]==bgee_record["Expression rank"]


    cell = [e for e in entities if isinstance(e, Cell)][0]
    assert cell.id == bgee_record["Anatomical entity ID"]
    assert cell.name == bgee_record["Anatomical entity name"]

    gene = [e for e in entities if isinstance(e, Gene)][0]
    assert gene.id == "ENSEMBLE:" + bgee_record["Gene ID"]
    assert gene.name == bgee_record["Gene name"]

