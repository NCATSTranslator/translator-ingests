import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum, ChemicalEntity
import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.drug_rep_hub.drug_rep_hub import transform_drug_rep_hub_annotations

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

        (  # Query 1 - Drug-Repurposing Hub record
            [
                {
                    "pert_iname": "5-fluorouracil",
                    "clinical_phase": "Launched",
                    "moa": "thymidylate synthase inhibitor",
                    "target": "",
                    "disease_area": "oncology",
                    "indication": "breast cancer",
                }
            ],
            # Captured node contents
            [
                {"id": "PUBCHEM.COMPOUND:3385", "category": ["biolink:ChemicalEntity"]},
                {"id": "MONDO:0007254", "name": "breast cancer", "category": ["biolink:DiseaseOrPhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation"],
                "subject": "PUBCHEM.COMPOUND:3385",
                "predicate": "biolink:treats",
                "object": "MONDO:0007254",
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:drug-repurposing-hub"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (  # Query 2 - Drug-Repurposing Hub record with single indication and
            [
                {
                    "pert_iname": "5-fluorouracil",
                    "clinical_phase": "Launched",
                    "moa": "thymidylate synthase inhibitor",
                    "target": "CD44",
                    "disease_area": "oncology",
                    "indication": "",
                }
            ],
            # Captured node contents
            [
                {"id": "PUBCHEM.COMPOUND:3385", "category": ["biolink:ChemicalEntity"]},
                {"id": "HGNC:1681", "name": "CD44", "category": ["biolink:Gene"], "symbol": "CD44"},
            ],
            # Captured edge contents

            {
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "PUBCHEM.COMPOUND:3385",
                "predicate": "biolink:affects",
                "object": "HGNC:1681",
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:drug-repurposing-hub"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            }
        ),
    ],
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):

    mock_koza_transform.state['samples'] = {
        "5-fluorouracil": {
            "PUBCHEM.COMPOUND:3385": ChemicalEntity(
                id="PUBCHEM.COMPOUND:3385",
                name="5-fluorouracil",
                category=["biolink:ChemicalEntity"],
            )
        }
    }
    for result in transform_drug_rep_hub_annotations(mock_koza_transform, test_record):
        validate_transform_result(
            result=result,
            expected_nodes=result_nodes,
            expected_edges=result_edge,
            node_test_slots=NODE_TEST_SLOTS,
            edge_test_slots=ASSOCIATION_TEST_SLOTS,
        )
