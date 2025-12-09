import pytest

from typing import Optional
from pathlib import Path

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.bindingdb.bindingdb import (
    # prepare_bindingdb_data,
    transform_bindingdb_by_record
)
from tests.unit.ingests import (
    validate_transform_result,
    MockKozaWriter,
    MockKozaTransform
)
from tests.unit.ingests.bindingdb.sample_data import (
    CASPASE3_KI_RECORD,
    CASPASE1_KI_RECORD,
    CASPASE1_WEAK_KI_RECORD,
    CASPASE1_RECORD_WITH_DOI,
    BINDINGDB_RECORD_WITH_A_US_PATENT
)
# from translator_ingest.ingests.bindingdb.bindingdb_util import set_bindingdb_input_file


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(
        extra_fields=dict(),
        writer=writer,
        mappings=mappings,
        input_files_dir=Path(__file__).resolve().parent
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
    "object",
    "publications",
    "sources",
    "knowledge_level",
    "agent_type",
)

#
# We need to test the prepare_bindingdb_data() method separately?
#
# def test_ingest_transform(
#     mock_koza_transform: koza.KozaTransform,
#     test_records: list[dict],
#     result_nodes: Optional[list],
#     result_edge: Optional[dict],
# ):
#     # Special utility function to allow soft resetting
#     # of the input file name for testing purposes
#     set_bindingdb_input_file("test_data_homo_sapiens.tsv")
#
#     # The prepare_bindingdb_data() method returns an iterable of records,
#     # where duplication in the original assay records is removed, merging into a single edge...
#     merged_records_iterable = prepare_bindingdb_data(mock_koza_transform, test_records)
#
#     # ... the resulting record stream is processed
#     # by the transform_bindingdb_by_record() method.
#     # First, we simulate the pipeline streaming of the records...
#     merged_records_iterator = iter(merged_records_iterable)
#     test_record = next(merged_records_iterator, None)
#
#     if result_nodes is None and result_edge is None:
#         assert test_record is None
#     else:
#         # ... one record at a time...
#         validate_transform_result(
#             result=transform_bindingdb_by_record(mock_koza_transform, test_record),
#             expected_nodes=result_nodes,
#             expected_edges=result_edge,
#             node_test_slots=NODE_TEST_SLOTS,
#             edge_test_slots=ASSOCIATION_TEST_SLOTS,
#         )
#
#         # ... but we should only see at most one record per unit test
#         with pytest.raises(StopIteration):
#             next(merged_records_iterator)



@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Test record 0: Caspase-3 inhibitor with Ki = 90 nM
            CASPASE3_KI_RECORD,
            [
                {
                    "id": "CID:5327301",
                    "name": "Thiophene Scaffold 47c",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P42574",
                    "name": "Caspase-3",
                    "category": ["biolink:Protein"]
                },
            ],
            {
                # Since we are not yet reporting the various activity assays in BindingDb,
                # then it may be premature to publish the edges as "biolink:ChemicalAffectsGeneAssociation"
                "category": ["biolink:ChemicalGeneInteractionAssociation"],
                "subject": "CID:5327301",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P42574",
                "publications": ["PMID:12408711"],
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:bindingdb"}
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
                #
                # The initial iteration of BindingDb will ignore study results
                # "has_attribute": [
                #     {
                #         "has_attribute_type": "biolink:ki_inhibition_constant",
                #         "has_quantitative_value": "90"
                #     }
                # ]
            }
        ),
        (   # Test record 1: Caspase-1 inhibitor with Ki = 160 nM
            CASPASE1_KI_RECORD,
            [
                {
                    "id": "CID:5327302",
                    "name": "Inhibitor 3",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P29466",
                    "name": "Caspase-1",
                    "category": ["biolink:Protein"]
                },
            ],
            {
                # Since we are not yet reporting the various activity assays in BindingDb,
                # then it may be premature to publish the edges as "biolink:ChemicalAffectsGeneAssociation"
                "category": ["biolink:ChemicalGeneInteractionAssociation"],
                "subject": "CID:5327302",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P29466",
                "publications": ["PMID:12408711"],
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:bindingdb"}
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
                #
                # The initial iteration of BindingDb will ignore study results
                # "has_attribute": [
                #     {
                #         "has_attribute_type": "biolink:ki_inhibition_constant",
                #         "has_quantitative_value": "160"
                #     }
                # ]
            }
        ),
        (   # Test record 2: Caspase-1 inhibitor with Ki = 3900 nM (weaker binder)
            CASPASE1_WEAK_KI_RECORD,
            [
                {
                    "id": "CID:5327304",
                    "name": "Pyridine Scaffold 4",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P29466",
                    "name": "Caspase-1",
                    "category": ["biolink:Protein"]
                },
            ],
            {
                # Since we are not yet reporting the various activity assays in BindingDb,
                # then it may be premature to publish the edges as "biolink:ChemicalAffectsGeneAssociation"
                "category": ["biolink:ChemicalGeneInteractionAssociation"],
                "subject": "CID:5327304",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P29466",
                "publications": ["PMID:12408711"],
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:bindingdb"}
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
                #
                # The initial iteration of BindingDb will ignore study results
                # "has_attribute": [
                #     {
                #         "has_attribute_type": "biolink:ki_inhibition_constant",
                #         "has_quantitative_value": "3900"
                #     }
                # ]
            }
        ),
        (   # Test record 3: Caspase-1 record with only a DOI publication citation
            CASPASE1_RECORD_WITH_DOI,
            [
                {
                    "id": "CID:5327304",
                    "name": "Pyridine Scaffold 4",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P29466",
                    "name": "Caspase-1",
                    "category": ["biolink:Protein"]
                },
            ],
            {
                # Since we are not yet reporting the various activity assays in BindingDb,
                # then it may be premature to publish the edges as "biolink:ChemicalAffectsGeneAssociation"
                "category": ["biolink:ChemicalGeneInteractionAssociation"],
                "subject": "CID:5327304",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P29466",
                "publications": ["doi:10.1021/jm020230j"],
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:bindingdb"}
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
                #
                # The initial iteration of BindingDb will ignore study results
                # "has_attribute": [
                #     {
                #         "has_attribute_type": "biolink:ki_inhibition_constant",
                #         "has_quantitative_value": "3900"
                #     }
                # ]
            }
        ),
        (  # Test record 4: BindingDb record with a US Patent citation
            BINDINGDB_RECORD_WITH_A_US_PATENT,
            [
                {
                    "id": "CID:71463198",
                    "name": "US9447092, 3",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P08684",
                    "name": "Cytochrome P450 3A4",
                    "category": ["biolink:Protein"]
                },
            ],
            {
                # Since we are not yet reporting the various activity assays in BindingDb,
                # then it may be premature to publish the edges as "biolink:ChemicalAffectsGeneAssociation"
                "category": ["biolink:ChemicalGeneInteractionAssociation"],
                "subject": "CID:71463198",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P08684",
                "publications": ["uspto-patent:9447092"],
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:bindingdb"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:uspto-patent"}
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
                #
                # The initial iteration of BindingDb will ignore study results
                # "has_attribute": [
                #     {
                #         "has_attribute_type": "biolink:ki_inhibition_constant",
                #         "has_quantitative_value": "3900"
                #     }
                # ]
            }
        )
    ]
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_bindingdb_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS
    )
