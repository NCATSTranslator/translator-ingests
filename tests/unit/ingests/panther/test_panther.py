import pytest
from pathlib import Path
from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.panther.panther import (
    get_latest_version,
    transform_gene_to_gene_orthology
)

from translator_ingest.ingests.panther.panther_orthologs_utils import (
    extract_panther_data_polars,
    parse_gene_info,
    panther_taxon_map,
    db_to_curie_map,
    _resolve_gene_curie,
    GENE_COL,
    GENE_A_ID_COL,
    GENE_B_ID_COL,
    NCBITAXON_A_COL,
    NCBITAXON_B_COL,
    GENE_FAMILY_ID_COL,
)

import polars as pl

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform

TEST_DATA_DIR = Path(__file__).resolve().parent
TEST_ARCHIVE = TEST_DATA_DIR / "sample_panther_data.tar.gz"

# Test normally works 99.9% of the time, except when
# the Panther website is inaccessible, thus, breaking CI
@pytest.mark.skip
def test_get_latest_version():
    assert get_latest_version() != "unknown"


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(
        extra_fields=dict(),
        writer=writer,
        mappings=mappings,
        input_files_dir=TEST_DATA_DIR
    )


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
    "in_taxon",
    "category"
)

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "object",
    "has_evidence",
    "sources",
    "knowledge_level",
    "agent_type"
)


# --- Tests for prepare_data (polars pipeline) ---

def test_prepare_panther_data():
    """Test that extract_panther_data_polars reads the test archive, filters species, and resolves CURIEs."""
    df = extract_panther_data_polars(TEST_ARCHIVE)

    # The test archive has 5 rows total, but 2 have non-target species,
    # so only 3 should survive filtering
    assert len(df) == 3

    # Verify the expected columns exist
    assert set(df.columns) == {GENE_A_ID_COL, GENE_B_ID_COL, NCBITAXON_A_COL, NCBITAXON_B_COL, GENE_FAMILY_ID_COL}

    records = df.to_dicts()

    # Query 3 equivalent: HUMAN HGNC to RAT RGD
    r0 = records[0]
    assert r0[GENE_A_ID_COL] == "HGNC:11477"
    assert r0[GENE_B_ID_COL] == "RGD:1564893"
    assert r0[NCBITAXON_A_COL] == "NCBITaxon:9606"
    assert r0[NCBITAXON_B_COL] == "NCBITaxon:10116"
    assert r0[GENE_FAMILY_ID_COL] == "PANTHER.FAMILY:PTHR12434"

    # Query 4 equivalent: HUMAN Ensembl (version stripped) to MOUSE MGI
    r1 = records[1]
    assert r1[GENE_A_ID_COL] == "ENSEMBL:ENSG00000275949"
    assert r1[GENE_B_ID_COL] == "MGI:99431"
    assert r1[NCBITAXON_A_COL] == "NCBITaxon:9606"
    assert r1[NCBITAXON_B_COL] == "NCBITaxon:10090"
    assert r1[GENE_FAMILY_ID_COL] == "PANTHER.FAMILY:PTHR11711"

    # Query 5 equivalent: HUMAN non-canonical (UniProtKB fallback) to RAT RGD
    r2 = records[2]
    assert r2[GENE_A_ID_COL] == "UniProtKB:A6NNC1"
    assert r2[GENE_B_ID_COL] == "RGD:7561849"
    assert r2[NCBITAXON_A_COL] == "NCBITaxon:9606"
    assert r2[NCBITAXON_B_COL] == "NCBITaxon:10116"
    assert r2[GENE_FAMILY_ID_COL] == "PANTHER.FAMILY:PTHR15566"


def test_prepare_panther_data_filters_excluded_species():
    """Test that rows with non-target species are excluded by the polars pipeline."""
    df = extract_panther_data_polars(TEST_ARCHIVE)

    # No row should have a non-target taxon
    for record in df.to_dicts():
        assert record[NCBITAXON_A_COL] in {"NCBITaxon:9606", "NCBITaxon:10090", "NCBITaxon:10116"}
        assert record[NCBITAXON_B_COL] in {"NCBITaxon:9606", "NCBITaxon:10090", "NCBITaxon:10116"}


# --- Cross-validation: _resolve_gene_curie vs parse_gene_info ---

@pytest.mark.parametrize(
    "gene_info_str,expected_curie",
    [
        ("HUMAN|HGNC=11477|UniProtKB=Q6GZX4", "HGNC:11477"),
        ("RAT|RGD=1564893|UniProtKB=Q6GZX2", "RGD:1564893"),
        ("HUMAN|Ensembl=ENSG00000275949.5|UniProtKB=A0A0G2JMH3", "ENSEMBL:ENSG00000275949"),
        ("MOUSE|MGI=MGI=99431|UniProtKB=P84078", "MGI:99431"),
        ("HUMAN|Gene=P12LL_HUMAN|UniProtKB=A6NNC1", "UniProtKB:A6NNC1"),
        ("RAT|RGD=7561849|UniProtKB=A0A8I6A0K9", "RGD:7561849"),
    ]
)
def test_resolve_gene_curie_matches_parse_gene_info(gene_info_str: str, expected_curie: str):
    """Cross-validate that the polars _resolve_gene_curie expression matches parse_gene_info output."""
    # Reference: parse_gene_info
    _, ref_curie = parse_gene_info(gene_info_str, panther_taxon_map, db_to_curie_map)
    assert ref_curie == expected_curie

    # Polars: _resolve_gene_curie
    test_df = pl.DataFrame({GENE_COL: [gene_info_str]})
    result = test_df.select(_resolve_gene_curie(GENE_COL).alias("curie"))
    polars_curie = result["curie"][0]
    assert polars_curie == expected_curie

    # They should match each other
    assert ref_curie == polars_curie


# --- Tests for transform_record (using pre-processed dict format) ---

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 3 - Regular record, HUMAN (HGNC identified gene) to RAT ortholog
            {
                GENE_A_ID_COL: "HGNC:11477",
                GENE_B_ID_COL: "RGD:1564893",
                NCBITAXON_A_COL: "NCBITaxon:9606",
                NCBITAXON_B_COL: "NCBITaxon:10116",
                GENE_FAMILY_ID_COL: "PANTHER.FAMILY:PTHR12434",
            },

            # Captured node contents
            [
                {
                    "id": "HGNC:11477",
                    "in_taxon": ["NCBITaxon:9606"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "RGD:1564893",
                    "in_taxon": ["NCBITaxon:10116"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "PANTHER.FAMILY:PTHR12434",
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            [
                {
                    "category": ["biolink:GeneToGeneHomologyAssociation"],
                    "subject": "HGNC:11477",
                    "object": "RGD:1564893",
                    "predicate": "biolink:orthologous_to",
                    "has_evidence": ["PANTHER.FAMILY:PTHR12434"],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "HGNC:11477",
                    "object": "PANTHER.FAMILY:PTHR12434",
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "RGD:1564893",
                    "object": "PANTHER.FAMILY:PTHR12434",
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                }
            ]
        ),
        (   # Query 4 - HUMAN Ensembl (version-stripped) to MOUSE MGI
            {
                GENE_A_ID_COL: "ENSEMBL:ENSG00000275949",
                GENE_B_ID_COL: "MGI:99431",
                NCBITAXON_A_COL: "NCBITaxon:9606",
                NCBITAXON_B_COL: "NCBITaxon:10090",
                GENE_FAMILY_ID_COL: "PANTHER.FAMILY:PTHR11711",
            },

            # Captured node contents
            [
                {
                    "id": "ENSEMBL:ENSG00000275949",
                    "in_taxon": ["NCBITaxon:9606"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "MGI:99431",
                    "in_taxon": ["NCBITaxon:10090"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "PANTHER.FAMILY:PTHR11711",
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            [
                {
                    "category": ["biolink:GeneToGeneHomologyAssociation"],
                    "subject": "ENSEMBL:ENSG00000275949",
                    "object": "MGI:99431",
                    "predicate": "biolink:orthologous_to",
                    "has_evidence": ["PANTHER.FAMILY:PTHR11711"],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "ENSEMBL:ENSG00000275949",
                    "object": "PANTHER.FAMILY:PTHR11711",
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "MGI:99431",
                    "object": "PANTHER.FAMILY:PTHR11711",
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                }
            ]
        ),
        (   # Query 5 - HUMAN non-canonical (UniProtKB fallback) to RAT RGD
            {
                GENE_A_ID_COL: "UniProtKB:A6NNC1",
                GENE_B_ID_COL: "RGD:7561849",
                NCBITAXON_A_COL: "NCBITaxon:9606",
                NCBITAXON_B_COL: "NCBITaxon:10116",
                GENE_FAMILY_ID_COL: "PANTHER.FAMILY:PTHR15566",
            },

            # Captured node contents
            [
                {
                    "id": "UniProtKB:A6NNC1",
                    "in_taxon": ["NCBITaxon:9606"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "RGD:7561849",
                    "in_taxon": ["NCBITaxon:10116"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "PANTHER.FAMILY:PTHR15566",
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            [
                {
                    "category": ["biolink:GeneToGeneHomologyAssociation"],
                    "subject": "UniProtKB:A6NNC1",
                    "object": "RGD:7561849",
                    "predicate": "biolink:orthologous_to",
                    "has_evidence": ["PANTHER.FAMILY:PTHR15566"],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "UniProtKB:A6NNC1",
                    "object": "PANTHER.FAMILY:PTHR15566",
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "RGD:7561849",
                    "object": "PANTHER.FAMILY:PTHR15566",
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                }
            ]
        )
    ]
)
def test_ingest_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    validate_transform_result(
        result=transform_gene_to_gene_orthology(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        expected_no_of_edges=3,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS
    )
