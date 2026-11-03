import pytest

from typing import Optional
from pathlib import Path
from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from koza.model.graphs import KnowledgeGraph

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform

from translator_ingest.ingests.ohd_carolina.ohd_carolina_util import process_ohdc_record

@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(
        extra_fields=dict(),
        writer=writer,
        mappings=mappings,
        transform_metadata={},
        # Swap in the following code for temporary debugging using the real data file
        # input_files_dir=INGESTS_DATA_PATH / "ohd_carolina"  # Path(__file__).resolve().parent
        input_files_dir = Path(__file__).resolve().parent
    )


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
    "name",
    "category"
)

# list of slots whose values are
# to be checked in a result edge
CORE_ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "object",
    "has_confidence_score",
    "has_supporting_studies",
    "sources",
    "knowledge_level",
    "agent_type"
)

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - A UMLS disease to phenotype record
            {
                "subject": "UMLS:C0496912",
                "subject_name": "Neoplasm of uncertain or unknown behavior of larynx",
                "object": "HP:0012027",
                "object_name": "Laryngeal edema",
                "predicate": "biolink:positively_correlated_with",
                "chi_squared_p_value": 0.000,
                "log_odds_ratio": 4.982,
                "log_odds_ratio_95_ci": "[4.455178293670431, 5.509214309994943]",
                "score": 3.579,
                "total_sample_size": 2344578,
                "primary_knowledge_source": "infores:openhealthdata-carolina",
            },
            # Captured node contents
            [
                {
                    "id": "UMLS:C0496912",
                    "name": "Neoplasm of uncertain or unknown behavior of larynx",
                    "category": ["biolink:Disease"]
                },
                {
                    "id": "HP:0012027",
                    "name": "Laryngeal edema",
                    "category": ["biolink:PhenotypicFeature"]
                },

            ],
            # Captured edge contents
            {
                "category": ["biolink:Association"],
                "subject": "UMLS:C0496912",
                "predicate": "biolink:positively_correlated_with",
                "object": "HP:0012027",
                "has_confidence_score": 3.579,
                "has_supporting_studies": {
                    "infores:openhealthdata-carolina": {
                        "id": "infores:openhealthdata-carolina",
                        "category": ["biolink:Study"],
                        "has_study_results": [
                            {
                                "category": ["biolink:IceesStudyResult"],
                                "chi_squared_p": 0.000,
                                "total_sample_size": 2344578,
                                "log_odds_ratio": 4.982,
                                "log_odds_ratio_95_ci": [4.455178293670431, 5.509214309994943]
                            }
                        ]
                    }
                },
                "sources": [
                    {
                        "resource_id": "infores:openhealthdata-carolina",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": [],
                    },
                ],
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.data_analysis_pipeline,
            },
        ),
    ],
)
def test_transform_ohdc_edges(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    validate_transform_result(
        result=process_ohdc_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        edge_test_slots=CORE_ASSOCIATION_TEST_SLOTS
    )
