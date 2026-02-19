from typing import Optional, Any
import pytest
from pathlib import Path
import polars as pl

from biolink_model.datamodel.pydanticmodel_v2 import (
    AffinityMeasurement,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

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
    on_end_ingest_by_record,
    prepare_bindingdb_data,
    transform_bindingdb_by_record, on_begin_ingest_by_record
)
from translator_ingest.ingests.bindingdb.bindingdb_util import (
    REACTANT_SET_ID,
    LIGAND_SMILES,
    TARGET_NAME,
    SOURCE_ORGANISM,
    PUBLICATION,
    SUPPORTING_DATA_ID,
    ROWS_MISSING_AFFINITY,
    AFFINITY_PARAMETERS,
    get_affinity_measurements,
    filter_affinity_values
)
from tests.unit.ingests.bindingdb.sample_data import (
    RECORD_MISSING_FIELD_1,
    RECORD_MISSING_FIELD_2,
    CASPASE3_KI_RECORD,
    CASPASE1_KD_RECORD,
    CASPASE1_WEAK_KON_RECORD,
    CASPASE1_RECORD_WITH_DOI,
    BINDINGDB_RECORD_WITH_A_US_PATENT
)


# get_affinity_measurements(record: dict[str, Any]) -> Optional[list[AffinityMeasurement]]:
@pytest.mark.parametrize(
    "test_record,expected",
    [
        (   # Query 0
            {},
            None
        ),
        (   # Query 1
            RECORD_MISSING_FIELD_1,
            ("pKi", 7.0, "equal_to")
        ),
        (   # Query 2
            CASPASE1_KD_RECORD,
            ("pKd", 6.7958800173440754, "less_than")
        ),
        (   # Query 3
            CASPASE1_RECORD_WITH_DOI,
            ("pEC50", 5.4089353929735005, "equal_to")
        ),
        (   # Query 4
            BINDINGDB_RECORD_WITH_A_US_PATENT,
            ("pIC50", 4.3010299956639813, "greater_than")
        )
    ]
)
def test_get_affinity_measurements(test_record: dict[str, Any], expected: tuple[str,float,str]):
    result: Optional[list[AffinityMeasurement]] = get_affinity_measurements(test_record)
    if expected is None:
        assert result is None
    else:
        assert result is not None, "Unexpected null result from get_affinity_measurements?"
        affinity_measurement: AffinityMeasurement = result[0]
        assert affinity_measurement.affinity_parameter == expected[0]
        assert affinity_measurement.affinity == expected[1]
        assert affinity_measurement.has_binary_relation == expected[2]


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
    # Initializes ingest stats keys
    on_begin_ingest_by_record(mock_koza_transform)

    # The BindingDB implementation of prepare_bindingdb_data() method
    # bypasses Koza to directly read in the input data file to return
    # an iterable sequence of records, where duplication in the
    # original assay records is removed, merging into a single edge...
    merged_records_iterable = prepare_bindingdb_data(mock_koza_transform, data=[])

    for test_record in merged_records_iterable:
        # Record "3" excluded because it duplicates "4" but "4" is the duplicate entry last seen
        # Record "6" excluded because the source organism "Pan troglodytes" is not in the target list of taxa
        # Record "7" excluded because it has no affinity values at all
        # Record "8" excluded because its only affinity value (IC50=50000) is out of range
        assert test_record[REACTANT_SET_ID] not in [3, 6, 7, 8], \
            f"Unexpected reactant set ID # {test_record[REACTANT_SET_ID]}"

        # ... but expecting all the other records being tested further
        assert test_record[REACTANT_SET_ID] in [1, 2, 4, 5, 9], \
            f"Missing expected reactant set ID # {test_record[REACTANT_SET_ID]}"

        # Didn't extract this field (among others...) - column was not needed
        assert LIGAND_SMILES not in test_record

        # Check that the publication and supporting data fields are set correctly
        if test_record[REACTANT_SET_ID] == 1:
            assert test_record[PUBLICATION] == "PMID:12408711"
            assert test_record[SUPPORTING_DATA_ID] is None

        elif test_record[REACTANT_SET_ID] == 2:
            assert test_record[PUBLICATION] == "doi:10.1021/jm020230j"
            assert test_record[SUPPORTING_DATA_ID] == "infores:ki-database"

        elif test_record[REACTANT_SET_ID] == 4:
            assert test_record[TARGET_NAME] == "Caspase-1b"
            assert test_record[PUBLICATION] == "uspto-patent:9447092"
            assert test_record[SUPPORTING_DATA_ID] == "infores:uspto-patent"

            # testing here that the greater than binary relation
            # operator is kept during filtering
            assert test_record["Ki (nM)"] == ">390"

        elif test_record[REACTANT_SET_ID] == 5:
            assert test_record[SOURCE_ORGANISM] == "Mus musculus"
            assert test_record[PUBLICATION] == "doi:10.1021/jm020230j"
            assert test_record[SUPPORTING_DATA_ID] == "infores:ki-database"

        elif test_record[REACTANT_SET_ID] == 9:
            # Row 9 has Ki=90 (in range) and IC50=50000 (out of range for 1e4 bound);
            # Ki should be retained, IC50 should be nulled out
            assert test_record["Ki (nM)"] == "90"
            assert test_record["IC50 (nM)"] is None


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (
                RECORD_MISSING_FIELD_1,
                None,  # Should be filtered out
                None
        ),
        (
                RECORD_MISSING_FIELD_2,
                None,  # Should be filtered out
                None
        ),
        (   # Test record 0: Caspase-3 inhibitor with Ki = 90 nM
            CASPASE3_KI_RECORD,
            [
                {
                    "id": "PUBCHEM.COMPOUND:5327301",
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
                "subject": "PUBCHEM.COMPOUND:5327301",
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
                CASPASE1_KD_RECORD,
                [
                {
                    "id": "PUBCHEM.COMPOUND:5327302",
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
                "subject": "PUBCHEM.COMPOUND:5327302",
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
                CASPASE1_WEAK_KON_RECORD,
                [
                {
                    "id": "PUBCHEM.COMPOUND:5327304",
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
                "subject": "PUBCHEM.COMPOUND:5327304",
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
                    "id": "PUBCHEM.COMPOUND:5327304",
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
                "subject": "PUBCHEM.COMPOUND:5327304",
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
                    "id": "PUBCHEM.COMPOUND:71463198",
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
                "subject": "PUBCHEM.COMPOUND:71463198",
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
    # sanity check: each test iteration should start without any metadata
    mock_koza_transform.transform_metadata.clear()

    # Initializes ingest stats keys
    on_begin_ingest_by_record(mock_koza_transform)

    validate_transform_result(
        result=transform_bindingdb_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS
    )

    on_end_ingest_by_record(mock_koza_transform)


@pytest.fixture
def affinity_metadata_transform() -> koza.KozaTransform:
    """Separate fixture with mutable metadata for filter_affinity_values tests."""
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(
        extra_fields=dict(),
        writer=writer,
        mappings=mappings,
        transform_metadata={},
        input_files_dir=Path(__file__).resolve().parent
    )


def _affinity_df(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal polars DataFrame with affinity columns from row dicts."""
    columns = {col: [] for col in AFFINITY_PARAMETERS.values()}
    for row in rows:
        for col in columns:
            columns[col].append(row.get(col))
    schema = {col: pl.Utf8 for col in AFFINITY_PARAMETERS.values()}
    return pl.DataFrame(columns, schema=schema)


@pytest.mark.parametrize(
    "rows,expected_count,expected_filtered,description",
    [
        (
            [{"Ki (nM)": "90"}],
            1, 0,
            "single in-range Ki value passes"
        ),
        (
            [{}],
            0, 1,
            "row with no affinity values is filtered"
        ),
        (
            [{"IC50 (nM)": "50000"}],
            0, 1,
            "IC50=50000 exceeds 1000 nanomolar (1 micromolar) concentration"
        ),
        (
            [{"Ki (nM)": "90", "IC50 (nM)": "50000"}],
            1, 0,
            "Ki in range keeps row; IC50 out of range is nulled"
        ),
        (
            [{"Ki (nM)": "<500"}],
            1, 0,
            "relational prefix '<' is stripped before range check"
        ),
        (
            [{"Ki (nM)": ">2000000"}],
            0, 1,
            "Ki=2000000 exceeds 1000 nanomolar (1 micromolar) concentration"
        ),
        (
            [{"Kd (nM)": "0"}],
            0, 1,
            "Kd=0 excluded by exclusive lower bound"
        ),
        (
            [{"Kd (nM)": "1000"}, {"Kd (nM)": "2000"}],
            1, 1,
            "Kd=1000 at upper bound passes; Kd=2000 exceeds 1000 nanomolar (1 micromolar) concentration"
        ),
    ]
)
def test_filter_affinity_values(
    affinity_metadata_transform: koza.KozaTransform,
    rows: list[dict],
    expected_count: int,
    expected_filtered: int,
    description: str
):
    df = _affinity_df(rows)
    result = filter_affinity_values(affinity_metadata_transform, df)
    assert result.height == expected_count, description
    actual_filtered = affinity_metadata_transform.transform_metadata.get(ROWS_MISSING_AFFINITY, 0)
    assert actual_filtered == expected_filtered, description
    # Reset metadata for the next parametrized call
    affinity_metadata_transform.transform_metadata.clear()


def test_filter_affinity_values_nulls_out_of_range_columns(
    affinity_metadata_transform: koza.KozaTransform,
):
    """Verify that out-of-range values are nulled while in-range values are preserved."""
    df = _affinity_df([{"Ki (nM)": "90", "IC50 (nM)": "50000"}])
    result = filter_affinity_values(affinity_metadata_transform, df)
    row = result.to_dicts()[0]
    assert row["Ki (nM)"] == "90"
    assert row["IC50 (nM)"] is None
