import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.bindingdb.bindingdb import (
    prepare_bindingdb_data,
    transform_ingest_by_record
)
from tests.unit.ingests import (
    validate_transform_result,
    MockKozaWriter,
    MockKozaTransform
)
from tests.unit.ingests.bindingdb.test_data import (
    NO_PMID_RECORD,
    CASPASE3_KI_RECORD,
    CASPASE1_KI_RECORD,
    CASPASE1_WEAK_KI_RECORD,
    CASPASE3_KI_RECORD_DUPLICATION
)


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


@pytest.mark.parametrize(
    "test_records,result_nodes,result_edge",
    [
        (   # Test record 0: Record with no PMID (should be filtered out)
            [NO_PMID_RECORD],
            None,  # Should be filtered out
            None,
        ),
        (   # Test record 1: Caspase-3 inhibitor with Ki = 90 nM
            [CASPASE3_KI_RECORD],
            [
                {
                    "id": "CID:5327301",  # or appropriate ID format
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
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "CID:5327301",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P42574",
                "publications": ["PMID:12408711"],
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
        (   # Test record 2: Caspase-1 inhibitor with Ki = 160 nM
            [CASPASE1_KI_RECORD],
            [
                {
                    "id": "CID:5327302",  # or appropriate ID format
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
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "CID:5327302",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P29466",
                "publications": ["PMID:12408711"],
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
        (   # Test record 3: Caspase-1 inhibitor with Ki = 3900 nM (weaker binder)
            [CASPASE1_WEAK_KI_RECORD],
            [
                {
                    "id": "CID:5327304",  # or appropriate ID format
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
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "CID:5327304",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P29466",
                "publications": ["PMID:12408711"],
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
        (   # Test record 2: Duplication of Caspase-3 inhibitor assays,
            #                to test merging of edges with identical ligand and target.
            [
                CASPASE3_KI_RECORD,
                CASPASE3_KI_RECORD_DUPLICATION
            ],
            [
                {
                    "id": "CID:5327301",  # or appropriate ID format
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
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "CID:5327301",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P42574",
                "publications": ["PMID:12408711"],
                #
                # The initial iteration of BindingDb will ignore study results
                # "has_attribute": [
                #     {
                #         "has_attribute_type": "biolink:ki_inhibition_constant",
                #         "has quantitative value": "90"
                #     },
                #     {
                #         "has_attribute_type": "biolink:ic50_half_maximal_inhibitory_concentration",
                #         "has_quantitative_value": "6676.9"
                #     }
                # ]
            }
        )

    ]
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_records: list[dict],
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    # The prepare_bindingdb_data() method returns an iterable of records,
    # where duplication in the original assay records is removed, merging into a single edge...
    merged_records_iterable = prepare_bindingdb_data(mock_koza_transform, test_records)

    # ... the resulting record stream is processed
    # by the transform_ingest_by_record() method.
    # First, we simulate the pipeline streaming of the records....
    merged_records_iterator = iter(merged_records_iterable)
    test_record = next(merged_records_iterator)

    # ... one record at a time...
    validate_transform_result(
        result=transform_ingest_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )

    # ... but we should only see at most one record per unit test
    with pytest.raises(StopIteration):
        next(merged_records_iterator)
