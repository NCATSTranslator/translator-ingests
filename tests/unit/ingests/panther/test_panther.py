import pytest
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlsplit, quote as urlencode

from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    AgentTypeEnum,
    GeneToGeneHomologyAssociation,
    GeneToGeneFamilyAssociation,
    RetrievalSource,
    ResourceRoleEnum,
)

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.panther.panther import (
    get_latest_version,
    transform_gene_to_gene_orthology
)

from translator_ingest.ingests.panther.panther_orthologs_utils import (
    extract_panther_data_polars,
    GENE_COL,
    GENE_A_ID_COL,
    GENE_B_ID_COL,
    NCBITAXON_A_COL,
    NCBITAXON_B_COL,
    GENE_FAMILY_ID_COL,
    panther_sources
)

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


# def panther_sources(gene_family_id: str, orthology_edge: bool) -> list[RetrievalSource]:
def test_panther_source():

    
    def _check_source(
            sources: list[RetrievalSource],
            query_field: str,
            validate,  # lambda predicate test
            err_msg: str
    ):
        # Shared code method to 
        # :param sources: list[RetrievalSource] to be validated (assumed to be a list of one RetrievalSource
        # :param query_field: source_record_url query parameter field to be validated
        # :param validate: validation predicate method to validate the value of the specified query_field
        # :param err_msg: str, error message to assert if validate predicate fails 'False'
        # :return: None
        # :raise: AssertionError
        assert len(sources) == 1
        for source in sources:
            assert source.source_record_urls is not None, "The source.source_record_urls cannot be None"
            assert len(source.source_record_urls) == 1, "Unexpected multiple source.source_record_urls"
            parsed = urlsplit(source.source_record_urls[0])
            qs = parse_qs(parsed.query)
            query_field_value = qs[query_field]
            assert validate(query_field_value[0]), err_msg

    # orthology sources
    gene_column_identifier = "HUMAN|HGNC=29675|UniProtKB=Q96S66"
    _check_source(
        sources=panther_sources(identifier=gene_column_identifier, orthology_edge=True),
        query_field="acc",
        validate=lambda x: x == gene_column_identifier,
        err_msg="Panther 'Gene Column Identifier' is not returned in the orthology 'source_record_url'?"
    )
    # gene_family sources
    gene_family_id = "PANTHER.FAMILY:PTHR12434"
    _check_source(
        sources=panther_sources(identifier=gene_family_id, orthology_edge=False),
        query_field="clsAccession",
        validate=lambda x: x == gene_family_id.removeprefix("PANTHER.FAMILY:"),
        err_msg="Panther 'gene_family_id' is not returned in the Gene Family 'source_record_url'?"
    )


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
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
    assert set(df.columns) == {
        GENE_COL, GENE_A_ID_COL, GENE_B_ID_COL, NCBITAXON_A_COL, NCBITAXON_B_COL, GENE_FAMILY_ID_COL
    }

    records = df.to_dicts()

    # Query 3 equivalent: HUMAN HGNC to RAT RGD
    r0 = records[0]
    assert r0[GENE_COL] == "PANTHER.FAMILY:PTHR12434"
    assert r0[GENE_A_ID_COL] == "HGNC:11477"
    assert r0[GENE_B_ID_COL] == "RGD:1564893"
    assert r0[NCBITAXON_A_COL] == "NCBITaxon:9606"
    assert r0[NCBITAXON_B_COL] == "NCBITaxon:10116"
    assert r0[GENE_FAMILY_ID_COL] == "PANTHER.FAMILY:PTHR12434"

    # Query 4 equivalent: HUMAN Ensembl (version stripped) to MOUSE MGI
    r1 = records[1]
    assert r0[GENE_COL] == "PANTHER.FAMILY:PTHR12434"
    assert r1[GENE_A_ID_COL] == "ENSEMBL:ENSG00000275949"
    assert r1[GENE_B_ID_COL] == "MGI:99431"
    assert r1[NCBITAXON_A_COL] == "NCBITaxon:9606"
    assert r1[NCBITAXON_B_COL] == "NCBITaxon:10090"
    assert r1[GENE_FAMILY_ID_COL] == "PANTHER.FAMILY:PTHR11711"

    # Query 5 equivalent: HUMAN non-canonical (UniProtKB fallback) to RAT RGD
    r2 = records[2]
    assert r0[GENE_COL] == "PANTHER.FAMILY:PTHR12434"
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


# --- Tests for transform_record (using pre-processed dict format) ---

ORTHO_SRL_TEMPLATE = "https://pantherdb.org/genes/gene.do?acc={gene_col}#orthologs"

HOMO_GENE_COL_ID = "HUMAN|HGNC=11477|UniProtKB=Q15528"
HOMO_ORTHO_SRL = ORTHO_SRL_TEMPLATE.format(gene_col=urlencode(HOMO_GENE_COL_ID))

MUS_GENE_COL_ID = "MOUSE|MGI=MGI=98446|UniProtKB=Q62276"
MUS_ORTHO_SRL = ORTHO_SRL_TEMPLATE.format(gene_col=urlencode(MUS_GENE_COL_ID))

RAT_GENE_COL_ID = "RAT|RGD=1564893|UniProtKB=A0JPN6"
RAT_ORTHO_SRL = ORTHO_SRL_TEMPLATE.format(gene_col=urlencode(RAT_GENE_COL_ID))

GFAM_SRL_TEMPLATE = "https://pantherdb.org/panther/family.do?clsAccession={gf_id}"
SAMPLE_GFAM_ID = "PTHR12434"
SAMPLE_GFAM_CURIE = f"PANTHER.FAMILY:{SAMPLE_GFAM_ID}"
SAMPLE_GFAM_SRL = GFAM_SRL_TEMPLATE.format(gf_id=SAMPLE_GFAM_ID)

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 0 - Regular record, HUMAN (HGNC identified gene) to RAT ortholog
            {
                GENE_COL: "HUMAN|HGNC=11477|UniProtKB=Q15528",
                GENE_A_ID_COL: "HGNC:11477",
                GENE_B_ID_COL: "RGD:1564893",
                NCBITAXON_A_COL: "NCBITaxon:9606",
                NCBITAXON_B_COL: "NCBITaxon:10116",
                GENE_FAMILY_ID_COL: SAMPLE_GFAM_CURIE,
            },

            # Captured node contents
            [
                {
                    "id": "HGNC:11477",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "RGD:1564893",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": SAMPLE_GFAM_CURIE,
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
                    "has_evidence": [SAMPLE_GFAM_CURIE],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [HOMO_ORTHO_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "HGNC:11477",
                    "object": SAMPLE_GFAM_CURIE,
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [SAMPLE_GFAM_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "RGD:1564893",
                    "object": SAMPLE_GFAM_CURIE,
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [SAMPLE_GFAM_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.automated_agent
                }
            ]
        ),
        (   # Query 1 - Chicken Ensembl (version-stripped) to MOUSE MGI
            {
                GENE_COL: "CHICK|Ensembl=ENSGALG00010028857.1|UniProtKB=A0A8V1AFQ2",
                GENE_A_ID_COL: "ENSEMBL:ENSGALG00010028857",
                GENE_B_ID_COL: "MGI:99431",
                NCBITAXON_A_COL: "NCBITaxon:9031",
                NCBITAXON_B_COL: "NCBITaxon:10090",
                GENE_FAMILY_ID_COL: SAMPLE_GFAM_CURIE,
            },

            # Captured node contents
            [
                {
                    "id": "ENSEMBL:ENSGALG00010028857",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "MGI:99431",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": SAMPLE_GFAM_CURIE,
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            [
                {
                    "category": ["biolink:GeneToGeneHomologyAssociation"],
                    "subject": "ENSEMBL:ENSGALG00010028857",
                    "object": "MGI:99431",
                    "predicate": "biolink:orthologous_to",
                    "has_evidence": [SAMPLE_GFAM_CURIE],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [MUS_ORTHO_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "ENSEMBL:ENSGALG00010028857",
                    "object": SAMPLE_GFAM_CURIE,
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [SAMPLE_GFAM_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "MGI:99431",
                    "object": SAMPLE_GFAM_CURIE,
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [SAMPLE_GFAM_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.automated_agent
                }
            ]
        ),
        (   # Query 2 - HUMAN non-canonical (UniProtKB fallback) to RAT RGD
            {
                GENE_COL: "HUMAN|HGNC=11477|UniProtKB=Q15528",
                GENE_A_ID_COL: "UniProtKB:Q15528",
                GENE_B_ID_COL: "RGD:7561849",
                NCBITAXON_A_COL: "NCBITaxon:9606",
                NCBITAXON_B_COL: "NCBITaxon:10116",
                GENE_FAMILY_ID_COL: SAMPLE_GFAM_CURIE
            },

            # Captured node contents
            [
                {
                    "id": "UniProtKB:Q15528",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "RGD:7561849",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": SAMPLE_GFAM_CURIE,
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            [
                {
                    "category": ["biolink:GeneToGeneHomologyAssociation"],
                    "subject": "UniProtKB:Q15528",
                    "object": "RGD:7561849",
                    "predicate": "biolink:orthologous_to",
                    "has_evidence": [SAMPLE_GFAM_CURIE],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [RAT_ORTHO_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "UniProtKB:Q15528",
                    "object": SAMPLE_GFAM_CURIE,
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [SAMPLE_GFAM_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.automated_agent
                },
                {
                    "category": ["biolink:GeneToGeneFamilyAssociation"],
                    "subject": "RGD:7561849",
                    "object": SAMPLE_GFAM_CURIE,
                    "predicate": "biolink:member_of",
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:panther",
                            "source_record_urls": [SAMPLE_GFAM_SRL]
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.automated_agent
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


# ── Pydantic round-trip fixtures & test ──────────────────────────────

_PANTHER_SOURCES = [
    RetrievalSource(
        id="infores:panther",
        resource_id="infores:panther",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": GeneToGeneHomologyAssociation,
        "params": {
            "id": "uuid:panther-test-1",
            "subject": "HGNC:11477",
            "predicate": "biolink:orthologous_to",
            "object": "RGD:1564893",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _PANTHER_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneFamilyAssociation,
        "params": {
            "id": "uuid:panther-test-2",
            "subject": "HGNC:11477",
            "predicate": "biolink:member_of",
            "object": "PANTHER.FAMILY:PTHR12434",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _PANTHER_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f["association_class"].__name__,
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
