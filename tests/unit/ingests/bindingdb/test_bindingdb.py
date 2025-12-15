import pytest
from pathlib import Path
from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from tests.unit.ingests import (
    validate_transform_result,
    MockKozaWriter,
    MockKozaTransform
)

# from translator_ingest import INGESTS_DATA_PATH

from translator_ingest.ingests.bindingdb.bindingdb import (
    on_begin_ingest_by_record,
    prepare_bindingdb_data,
    transform_bindingdb_by_record
)
from translator_ingest.ingests.bindingdb.bindingdb_util import (
    REACTANT_SET_ID,
    LIGAND_SMILES,
    TARGET_NAME,
    SOURCE_ORGANISM,
    PUBLICATION,
    SUPPORTING_DATA_ID
)
from tests.unit.ingests.bindingdb.sample_data import (
    RECORD_MISSING_FIELDS,
    CASPASE3_KI_RECORD,
    CASPASE1_KI_RECORD,
    CASPASE1_WEAK_KI_RECORD,
    CASPASE1_RECORD_WITH_DOI,
    BINDINGDB_RECORD_WITH_A_US_PATENT
)


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(
        extra_fields=dict(),
        writer=writer,
        mappings=mappings,
        # Swap in the following code for temporary debugging using the real data file
        # input_files_dir=INGESTS_DATA_PATH / "bindingdb"  # Path(__file__).resolve().parent
        input_files_dir = Path(__file__).resolve().parent
    )


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
    "name",
    "category",
    "in_taxon",
    "in_taxon_label"
)

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

def test_prepare_bindingdb_data(
    mock_koza_transform: koza.KozaTransform,
    # test_records: list[dict],
    # result_nodes: Optional[list],
    # result_edge: Optional[dict],
):
    # calling this simply to ensure that context
    # dictionary keys are created; not otherwise used
    on_begin_ingest_by_record(mock_koza_transform)

    # The BindingDB implementation of prepare_bindingdb_data() method
    # bypasses Koza to directly read in the input data file to return
    # an iterable sequence of records, where duplication in the
    # original assay records is removed, merging into a single edge...
    merged_records_iterable = prepare_bindingdb_data(mock_koza_transform, data=[])

    for test_record in merged_records_iterable:
        # Record "3" excluded because it duplicates "4" but "4" is the duplicate entry last seen
        # Record "6" excluded because the source organism "Pan troglodytes" is not in the target list of taxa
        assert test_record[REACTANT_SET_ID] not in ["3", "6"]

        # Didn't extract this field (among others...) - column was not needed
        assert LIGAND_SMILES not in test_record

        # Check that the publication and supporting data fields are set correctly
        if test_record[REACTANT_SET_ID] == "1":
            assert test_record[PUBLICATION] == "PMID:12408711"
            assert test_record[SUPPORTING_DATA_ID] is None
        elif test_record[REACTANT_SET_ID] == "2":
            assert test_record[PUBLICATION] == "doi:10.1021/jm020230j"
            assert test_record[SUPPORTING_DATA_ID] == "infores:ki-database"
        elif test_record[REACTANT_SET_ID] == "4":
            assert test_record[TARGET_NAME] == "Caspase-1b"
            assert test_record[PUBLICATION] == "uspto-patent:9447092"
            assert test_record[SUPPORTING_DATA_ID] == "infores:uspto-patent"
        elif test_record[REACTANT_SET_ID] == "5":
            assert test_record[SOURCE_ORGANISM] == "Mus musculus"
            assert test_record[PUBLICATION] == "doi:10.1021/jm020230j"
            assert test_record[SUPPORTING_DATA_ID] == "infores:ki-database"


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (
                RECORD_MISSING_FIELDS,
                None,  # Should be filtered out
                None
        ),
        (   # Test record 0: Caspase-3 inhibitor with Ki = 90 nM
            CASPASE3_KI_RECORD,
            [
                {
                    "id": "CID:5327301",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P42574",
                    "name": "Caspase-3",
                    "category": ["biolink:Protein"],
                    "in_taxon": ["NCBITaxon:9606"],
                    "in_taxon_label": "Homo sapiens"
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
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P29466",
                    "name": "Caspase-1",
                    "category": ["biolink:Protein"],
                    "in_taxon": ["NCBITaxon:9606"],
                    "in_taxon_label": "Homo sapiens"
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
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P29466",
                    "name": "Caspase-1",
                    "category": ["biolink:Protein"],
                    "in_taxon": ["NCBITaxon:9606"],
                    "in_taxon_label": "Homo sapiens"
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
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P29452",
                    "name": "Caspase-1",
                    "category": ["biolink:Protein"],
                    "in_taxon": ["NCBITaxon:10090"],
                    "in_taxon_label": "Mus musculus"
                },
            ],
            {
                # Since we are not yet reporting the various activity assays in BindingDb,
                # then it may be premature to publish the edges as "biolink:ChemicalAffectsGeneAssociation"
                "category": ["biolink:ChemicalGeneInteractionAssociation"],
                "subject": "CID:5327304",
                "predicate": "biolink:directly_physically_interacts_with",
                "object": "UniProtKB:P29452",
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
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P08684",
                    "name": "Cytochrome P450 3A4",
                    "category": ["biolink:Protein"],
                    "in_taxon": ["NCBITaxon:9606"],
                    "in_taxon_label": "Homo sapiens"
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
    # calling this simply to ensure that context
    # dictionary keys are created; not otherwise used
    on_begin_ingest_by_record(mock_koza_transform)

    validate_transform_result(
        result=transform_bindingdb_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS
    )
