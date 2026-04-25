import pytest

from typing import Optional
from pathlib import Path
from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.hmdb.hmdb import (
    on_begin_hmdb_ingest,
    transform_hmdb_ingest,
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


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
        # input_files_dir=INGESTS_DATA_PATH / "hmdb_metabolites.zip"  # Path(__file__).resolve().parent
        input_files_dir = Path(__file__).resolve().parent
    )


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "publications",
    "sources",
    "knowledge_level",
    "agent_type",
)

# TODO: these are the _template test values -- revise them HMDB, if and as necessary
# @pytest.mark.parametrize(
#     "result_nodes,result_edge",
#     [
#         (  # Query 0 -
#             # Captured node contents
#             [
#                 {"id": "MESH:C534883", "name": "10074-G5", "category": ["biolink:ChemicalEntity"]},
#                 {"id": "MESH:D013734", "name": "Androgen-Insensitivity Syndrome", "category": ["biolink:Disease"]},
#             ],
#             # Captured edge contents
#             {
#                 "category": ["biolink:ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation"],
#                 "subject": "MESH:C534883",
#                 "predicate": "biolink:related_to",
#                 "object": "MESH:D013734",
#                 "publications": ["PMID:1303262", "PMID:8281139"],
#                 "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:ctd"}],
#                 "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
#                 "agent_type": AgentTypeEnum.manual_agent,
#             },
#         ),
#     ],
# )
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    # result_nodes: Optional[list],
    # result_edge: Optional[dict],
):
    on_begin_hmdb_ingest(mock_koza_transform)

    # the data argument is ignored internally (i.e., the HMDB data file is read directly)
    # hence we pass an empty list as data. The mock KozaTransform object points to the
    # sample HMDB data file located in the tests/unit/ingests/hmdb folder itself.
    # An iterable derived list of KnowledgeGraph objects is returned (a bit challenging to test but...)
    knowledge_graphs = list(transform_hmdb_ingest(mock_koza_transform, []))

    for graph in knowledge_graphs:
        print(graph)

        # TODO: Rather tricky to use the usual shared test
        #       method, with a streaming transform datasource...
        # validate_transform_result(
        #     result=graph,
        #     expected_nodes=result_nodes,
        #     expected_edges=result_edge,
        #     node_test_slots=NODE_TEST_SLOTS,
        #     edge_test_slots=ASSOCIATION_TEST_SLOTS,
        # )
