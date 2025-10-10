import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import  Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.sider.sider import (
    transform_ingest_all_streaming
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)

# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = [
    "id",
    "name",
    "category"
]

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "predicate",
    "object",
    "sources",
    "knowledge_level",
    "agent_type"
]


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - concept_type = LLT -> returns None
            [{
                "label": "EMA/WC500020092.html",
                "STITCH_compound_id_flat": "CID100216416",
                "STITCH_compound_id_stereo": "CID000216416",
                "UMLS_concept_id_label": "C0000737",
                "MedDRA_concept_type": "LLT",
                "UMLS_concept_id": "C0000737",
                "side_effect_name": "Abdominal pain",
            }],
            None,
            None
        ),
        (  # Query 1 - Another record complete with PubMedIDs
            [{

                "label": "safety/2008_-_May_PI_-_Viread_PI.html",
                "STITCH_compound_id_flat": "CID100119830",
                "STITCH_compound_id_stereo": "CID005481350",
                "UMLS_concept_id_label": "C1608945",
                "MedDRA_concept_type": "PT",
                "UMLS_concept_id": "C1608945",
                "side_effect_name": "Exfoliative rash",
            }],

            # Captured node contents
            [
                {
                    "id": "PUBCHEM.COMPOUND:5481350",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UMLS:C1608945",
                    "name": "Exfoliative rash",
                    "category": ["biolink:DiseaseOrPhenotypicFeature"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation"],
                "subject": "PUBCHEM.COMPOUND:5481350",
                "predicate": "biolink:has_side_effect",
                "object": "UMLS:C1608945",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:sider"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent
            }
        )
    ]
)
def test_ingest_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):

    for result in transform_ingest_all_streaming(mock_koza_transform, test_record):
        validate_transform_result(
            result=result,
            expected_nodes=result_nodes,
            expected_edges=result_edge,
            node_test_slots=NODE_TEST_SLOTS,
            association_test_slots=ASSOCIATION_TEST_SLOTS
        )
