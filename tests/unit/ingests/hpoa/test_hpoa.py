import pytest

from typing import Optional, Iterable, Any
from os.path import join, abspath, dirname
from loguru import logger
from pathlib import Path

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.hpoa.phenotype_ingest_utils import get_hpoa_genetic_predicate

from translator_ingest.ingests.hpoa.hpoa import (
    transform_record_disease_to_phenotype,
    transform_record_gene_to_disease,
    prepare_data_gene_to_phenotype,
    transform_record_gene_to_phenotype,
)

from tests.unit.ingests import MockKozaWriter, MockKozaTransform, validate_transform_result

HPOA_UNIT_TESTS = abspath(dirname(__file__))
HPOA_TEST_DATA_PATH = join(HPOA_UNIT_TESTS, "test_data")
logger.info("HPOA test data path: {}".format(HPOA_TEST_DATA_PATH))


# test mondo_map for the gene_to_phenotype
# disease_id to disease_context_qualifier mappings
mock_mondo_sssom_map: dict[str, dict[str, str]] = {
    "OMIM:231550": {"subject_id": "MONDO:0009279"},
    "Orphanet:442835": {"subject_id": "MONDO:0018614"},
    "OMIM:614129": {"subject_id": "MONDO:0013588"},
    "OMIM:613013": {"subject_id": "MONDO:0700041"},
}


@pytest.fixture(scope="package")
def mock_koza_transform_1() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = {"mondo_map": mock_mondo_sssom_map}
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category", "inheritance")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "publications",
    "has_evidence",
    "sex_qualifier",
    "onset_qualifier",
    "has_percentage",
    "has_quotient",
    "frequency_qualifier",
    "disease_context_qualifier",
    "sources",
    "knowledge_level",
    "agent_type",
)


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        ({}, None, None),  # Query 0 - missing data (empty record, hence, missing fields)
        (  # Query 1 - An 'aspect' == 'C' record processed
            {
                "database_id": "OMIM:614856",
                "disease_name": "Osteogenesis imperfecta, type XIII",
                "qualifier": "NOT",
                "hpo_id": "HP:0000343",
                "reference": "OMIM:614856",
                "evidence": "TAS",
                "onset": "HP:0003593",
                "frequency": "1/1",
                "sex": "FEMALE",
                "modifier": "",
                "aspect": "C",  # assert 'Clinical' test record
                "biocuration": "HPO:skoehler[2012-11-16]",
            },
            # This is not a 'P' nor 'I' record, so it should be skipped
            None,
            None,
        ),
        (  # Query 2 - An 'aspect' == 'P' record processed
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                "qualifier": "",
                "hpo_id": "HP:0001249",
                "reference": "OMIM:117650",
                "evidence": "TAS",
                "onset": "",
                "frequency": "50%",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:probinson[2009-02-17]",
            },
            # Captured node identifiers
            [
                {"id": "OMIM:117650", "name": "Cerebrocostomandibular syndrome", "category": ["biolink:Disease"]},
                {"id": "HP:0001249", "category": ["biolink:PhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                "negated": False,
                "object": "HP:0001249",
                # Although "OMIM:117650" is recorded above as
                # a reference, it is not used as a publication
                "publications": [],
                "has_evidence": ["ECO:0000304"],
                "sex_qualifier": None,
                "onset_qualifier": None,
                "has_percentage": 50.0,
                "has_quotient": 0.5,
                # '50%' above implies HPO term that the phenotype
                # is 'Present in 30% to 79% of the cases'.
                "frequency_qualifier": "HP:0040282",
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (  # Query 3 - Another 'aspect' == 'P' record processed
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                # "qualifier" was actually empty in the original Monarch test data;
                # however, we want to trigger a test of the negation, so we lie!
                "qualifier": "NOT",
                "hpo_id": "HP:0001545",
                "reference": "OMIM:117650",
                "evidence": "TAS",
                "onset": "",
                "frequency": "HP:0040283",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:skoehler[2017-07-13]",
            },
            [
                {"id": "OMIM:117650", "name": "Cerebrocostomandibular syndrome", "category": ["biolink:Disease"]},
                {"id": "HP:0001545", "category": ["biolink:PhenotypicFeature"]},
            ],
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                "negated": True,
                "object": "HP:0001545",
                "publications": [],
                "has_evidence": ["ECO:0000304"],
                "sex_qualifier": None,
                "onset_qualifier": None,
                "has_percentage": None,
                "has_quotient": None,
                "frequency_qualifier": "HP:0040283",
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (  # Query 4 - Same 'aspect' == 'P' record but lacking any frequency qualifier
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                # "qualifier" was actually empty in the original Monarch test data;
                # however, we want to trigger a test of the negation, so we lie!
                "qualifier": "",
                "hpo_id": "HP:0001545",
                "reference": "OMIM:117650",
                "evidence": "TAS",
                "onset": "",
                "frequency": "",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:skoehler[2017-07-13]",
            },
            [
                {"id": "OMIM:117650", "name": "Cerebrocostomandibular syndrome", "category": ["biolink:Disease"]},
                {"id": "HP:0001545", "category": ["biolink:PhenotypicFeature"]},
            ],
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                "negated": False,
                "object": "HP:0001545",
                "publications": [],
                "has_evidence": ["ECO:0000304"],
                "sex_qualifier": None,
                "onset_qualifier": None,
                "has_percentage": None,
                "has_quotient": None,
                "frequency_qualifier": None,
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (  # Query 5 - Disease inheritance 'aspect' == 'I' record processed
            {
                "database_id": "OMIM:300425",
                "disease_name": "Autism susceptibility, X-linked 1",
                "hpo_id": "HP:0001417",
                "reference": "OMIM:300425",
                "evidence": "IEA",
                "onset": "",
                "frequency": "",
                "sex": "",
                "modifier": "",
                "aspect": "I",  # assert 'Inheritance' test record
                "biocuration": "HPO:iea[2009-02-17]",
            },
            [
                {
                    "id": "OMIM:300425",
                    "name": "Autism susceptibility, X-linked 1",
                    "category": ["biolink:Disease"],
                    "inheritance": "X-linked inheritance",
                }
            ],
            None,  # no edge is created for this record
        ),
    ],
)
def test_disease_to_phenotype_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_record_disease_to_phenotype(mock_koza_transform_1, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )


@pytest.mark.parametrize(
    ("association", "expected_predicate"),
    [
        ("MENDELIAN", "biolink:causes"),
        ("POLYGENIC", "biolink:contributes_to"),
        ("UNKNOWN", "biolink:associated_with"),
    ],
)
def test_predicate(association: str, expected_predicate: str):
    predicate = get_hpoa_genetic_predicate(association)

    assert predicate == expected_predicate


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        ({}, None, None),  # Query 0 - missing data (empty record, hence, missing fields)
        (  # Query 1 - Sample Mendelian disease
            {
                "association_type": "MENDELIAN",
                "disease_id": "OMIM:212050",
                "gene_symbol": "CARD9",
                "ncbi_gene_id": "NCBIGene:64170",
                "source": "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen",
            },
            # Captured node contents
            [
                {"id": "NCBIGene:64170", "name": "CARD9", "category": ["biolink:Gene"]},
                {"id": "OMIM:212050", "category": ["biolink:Disease"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:CausalGeneToDiseaseAssociation"],
                "subject": "NCBIGene:64170",
                "predicate": "biolink:causes",
                "object": "OMIM:212050",
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:medgen"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
    ],
)
def test_gene_to_disease_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_record_gene_to_disease(mock_koza_transform_1, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )


@pytest.fixture(scope="package")
def mock_koza_transform_2() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    return MockKozaTransform(
        writer=writer, mappings=dict(), extra_fields=dict(), input_files_dir=Path(HPOA_TEST_DATA_PATH)
    )


def test_transform_record_disease_to_phenotype(mock_koza_transform_2: koza.KozaTransform):
    result: Iterable[dict[str, Any]] | None = prepare_data_gene_to_phenotype(mock_koza_transform_2, [])
    assert result is not None
    expected_entry: dict[str, Any] = {
        "ncbi_gene_id": 22,  # this gotta be an 'int'!
        "gene_symbol": "ABCB7",
        "hpo_id": "HP:0002470",
        "hpo_name": "Nonprogressive cerebellar ataxia",
        "publications": "PMID:26242992;PMID:4045952;PMID:11050011",
        "frequency": "11/11",
        "disease_id": "OMIM:301310",
        "gene_to_disease_association_types": "MENDELIAN",
    }
    # Find any entry with the expected fields in the result list
    assert any(
        [
            # Check that all expected fields are present in the entry
            all([key in expected_entry.keys() and expected_entry[key] == value for key, value in entry.items()])
            for entry in result
        ]
    )


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        ({}, None, None),  # Query 0 - missing data (empty record (hence, missing fields)
        (  # Query 1 - Full record, with the empty ("-") frequency field
            {
                "ncbi_gene_id": 8086,
                "gene_symbol": "AAAS",
                "hpo_id": "HP:0000252",
                "hpo_name": "Microcephaly",
                "publications": "PMID:11062474",
                "frequency": "-",
                "disease_id": "OMIM:231550",
                "gene_to_disease_association_types": "MENDELIAN",
            },
            # Captured node contents
            [
                {"id": "NCBIGene:8086", "name": "AAAS", "category": ["biolink:Gene"]},
                {"id": "HP:0000252", "category": ["biolink:PhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8086",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0000252",
                "frequency_qualifier": None,
                "has_percentage": None,
                "has_quotient": None,
                "has_count": None,
                "has_total": None,
                "disease_context_qualifier": "MONDO:0009279",
                "publications": ["PMID:11062474"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent,
            },
        ),
        (  # Query 2 - Full record, with a HPO term defined frequency field value
            {
                "ncbi_gene_id": 8120,
                "gene_symbol": "AP3B2",
                "hpo_id": "HP:0001298",
                "hpo_name": "Encephalopathy",
                "publications": "",
                "frequency": "HP:0040281",
                "disease_id": "ORPHA:442835",
                "gene_to_disease_association_types": "MENDELIAN",
            },
            # Captured node contents
            [
                {"id": "NCBIGene:8120", "name": "AP3B2", "category": ["biolink:Gene"]},
                {"id": "HP:0001298", "category": ["biolink:PhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8120",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0001298",
                "frequency_qualifier": "HP:0040281",
                "has_percentage": None,
                "has_quotient": None,
                "has_count": None,
                "has_total": None,
                "disease_context_qualifier": "MONDO:0018614",
                "publications": [],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent,
            },
        ),
        (  # Query 3 - Full record, with a ratio ("quotient") frequency field value
            {
                "ncbi_gene_id": 8192,
                "gene_symbol": "CLPP",
                "hpo_id": "HP:0000013",
                "hpo_name": "Hypoplasia of the uterus",
                "publications": "PMID:23541340",
                "frequency": "3/9",
                "disease_id": "OMIM:614129",
                "gene_to_disease_association_types": "MENDELIAN",
            },
            # Captured node contents
            [
                {"id": "NCBIGene:8192", "name": "CLPP", "category": ["biolink:Gene"]},
                {"id": "HP:0000013", "category": ["biolink:PhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8192",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0000013",
                "frequency_qualifier": "HP:0040282",
                "has_percentage": 33.0,
                "has_quotient": 0.33,
                "has_count": 3,
                "has_total": 9,
                "disease_context_qualifier": "MONDO:0013588",
                "publications": ["PMID:23541340"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent,
            },
        ),
        (  # Query 4 - Full record, with a percentage frequency field value
            # 8929	PHOX2B	HP:0003005	Ganglioneuroma	5%	OMIM:613013
            {
                "ncbi_gene_id": 8929,
                "gene_symbol": "PHOX2B",
                "hpo_id": "HP:0003005",
                "hpo_name": "Ganglioneuroma",
                "publications": "PMID:23541340;PMID:12345678",
                "frequency": "5%",
                "disease_id": "OMIM:613013",
                "gene_to_disease_association_types": "MENDELIAN",
            },
            # Captured node contents
            [
                {"id": "NCBIGene:8929", "name": "PHOX2B", "category": ["biolink:Gene"]},
                {"id": "HP:0003005", "category": ["biolink:PhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8929",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0003005",
                "frequency_qualifier": "HP:0040283",
                "has_percentage": 5,
                "has_quotient": 0.05,
                "has_count": None,
                "has_total": None,
                "disease_context_qualifier": "MONDO:0700041",
                "publications": ["PMID:23541340", "PMID:12345678"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent,
            },
        ),
    ],
)
def test_gene_to_phenotype_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_record_gene_to_phenotype(mock_koza_transform_1, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )
