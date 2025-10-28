import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests._ingest_template._ingest_template import (
    on_begin_ingest_by_record,
    transform_ingest_by_record,
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ["id", "name", "category"]

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "publications",
    "sources",
    "knowledge_level",
    "agent_type",
]


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - Missing PubMedIDs - returns None
            {
                "ChemicalName": "10074-G5",
                "ChemicalID": "C534883",
                "CasRN": "",
                "DiseaseName": "Burkitt Lymphoma",
                "DiseaseID": "MESH:D002051",
                "DirectEvidence": "",
                "InferenceGeneSymbol": "MYC",
                "InferenceScore": "",
                "OmimIDs": "113970",
                "PubMedIDs": "",  # empty expected field, hence, parse doesn't return a knowledge graph
            },
            None,
            None,
        ),
        (  # Query 1 - Another record complete with PubMedIDs
            {
                "ChemicalName": "10074-G5",
                "ChemicalID": "C534883",
                "CasRN": "",
                "DiseaseName": "Androgen-Insensitivity Syndrome",
                "DiseaseID": "MESH:D013734",
                "DirectEvidence": "",
                "InferenceGeneSymbol": "AR",
                "InferenceScore": "6.89",
                "OmimIDs": "300068|312300",
                "PubMedIDs": "1303262|8281139",
            },
            # Captured node contents
            [
                {"id": "MESH:C534883", "name": "10074-G5", "category": ["biolink:ChemicalEntity"]},
                {"id": "MESH:D013734", "name": "Androgen-Insensitivity Syndrome", "category": ["biolink:Disease"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation"],
                "subject": "MESH:C534883",
                "predicate": "biolink:related_to",
                "object": "MESH:D013734",
                "publications": ["PMID:1303262", "PMID:8281139"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:ctd"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
    ],
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    on_begin_ingest_by_record(mock_koza_transform)
    validate_transform_result(
        result=transform_ingest_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        association_test_slots=ASSOCIATION_TEST_SLOTS,
    )
