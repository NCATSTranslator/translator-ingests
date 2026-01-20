import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.chembl.chembl import transform_complexes

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = ("category", "subject", "predicate", "object", "sources", "knowledge_level", "agent_type")


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 1 - Another record complete with PubMedIDs
            [
                {
                    "target_type": "PROTEIN COMPLEX",
                    "target_name": "Anti-estrogen binding site (AEBS)",
                    "target_chembl_id": "CHEMBL612409",
                    "organism_tax_id": "9606",
                    "component_type": "PROTEIN",
                    "accession": "Q15125",
                    "description": "3-beta-hydroxysteroid-Delta(8),Delta(7)-isomerase",
                    "organism":"Homo sapiens",
                    "component_tax_id":"9606",
                    "db_source":"SWISS-PROT"
                }
            ],
            # Captured node contents
            [
                {"id": "UniProtKB:Q15125", "category": ["biolink:Protein"]},
                {"id": "CHEMBL.TARGET:CHEMBL612409", "name": "Anti-estrogen binding site (AEBS)", "category": ["biolink:MacromolecularComplex"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityToAnatomicalEntityPartOfAssociation"],
                "subject": "UniProtKB:Q15125",
                "predicate": "biolink:part_of",
                "object": "CHEMBL.TARGET:CHEMBL612409",
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:chembl"}],
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

    mock_koza_transform.state['chembl_proteins'] = {
        "Q15125": {
            "id": "Q15125",
            "name": "3-beta-hydroxysteroid-Delta(8),Delta(7)-isomerase"
        }
    }
    
    for result in transform_complexes(mock_koza_transform, test_record):
        validate_transform_result(
            result=result,
            expected_nodes=result_nodes,
            expected_edges=result_edge,
            node_test_slots=NODE_TEST_SLOTS,
            edge_test_slots=ASSOCIATION_TEST_SLOTS,
        )
