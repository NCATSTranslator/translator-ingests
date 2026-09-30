from collections.abc import Iterable
from os.path import abspath, dirname, join
from pathlib import Path
from typing import Any

import koza
import yaml
from koza import KozaConfig
from koza.io.writer.writer import KozaWriter
from koza.io.yaml_loader import UniqueIncludeLoader
from koza.transform import Mappings

import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    CausalGeneToDiseaseAssociation,
    DiseaseToPhenotypicFeatureAssociation,
    GeneToDiseaseAssociation,
    GeneToPhenotypicFeatureAssociation,
    GeneToPhenotypicFeaturePredicateEnum,
    KnowledgeLevelEnum,
    ResourceRoleEnum,
    RetrievalSource,
)

from tests.unit.ingests import MockKozaTransform, MockKozaWriter, validate_transform_result
from translator_ingest.ingests.hpoa.hpoa import (
    prepare_gene_to_phenotype_data,
    transform_disease_to_phenotype_edge_record,
    transform_disease_to_phenotype_node_record,
    transform_gene_to_disease_record,
    transform_gene_to_phenotype_record,
)
from translator_ingest.ingests.hpoa.phenotype_ingest_utils import get_qualified_predicate
from tests.util import get_ingest_config_yaml_path

HPOA_UNIT_TESTS = abspath(dirname(__file__))
HPOA_TEST_DATA_PATH = join(HPOA_UNIT_TESTS, "sample_data")


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
    return MockKozaTransform(extra_fields={}, writer=writer, mappings=mappings)


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
    "qualified_predicate",
    "subject_form_or_variant_qualifier",
    "publications",
    "has_evidence_of_type",
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

# list of slots whose values are
# to be checked in a result edge
GENE_TO_PHENOTYPE_ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "qualified_predicate",
    "subject_form_or_variant_qualifier",
    "frequency_qualifier",
    "has_percentage",
    "has_quotient",
    "has_count",
    "has_total",
    "disease_context_qualifier",
    "publications",
    "sources",
    "knowledge_level",
    "agent_type",
)


@pytest.mark.parametrize(
    "test_record,result_nodes",
    [
        (  # Query 0 - An 'aspect' == 'C' record processed (edge-specific fields ignored)
            {
                "database_id": "OMIM:614856",
                "disease_name": "Osteogenesis imperfecta, type XIII",
                "hpo_id": "HP:0000343",
                "aspect": "C",  # assert 'Clinical' test record
            },
            # This is not a 'P' nor 'I' record, so it should be skipped
            None
        ),
        (  # Query 1 - An 'aspect' == 'P' record processed (edge-specific fields ignored)
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                "hpo_id": "HP:0001249",
                "aspect": "P",
                "biocuration": "HPO:probinson[2009-02-17]",
            },
            # Captured node identifiers
            [
                {
                    "id": "OMIM:117650",
                    "name": "Cerebrocostomandibular syndrome",
                    "category": ["biolink:Disease"]
                },
                {
                    "id": "HP:0001249",
                    "category": ["biolink:PhenotypicFeature"]
                },
            ]
        ),
        (  # Query 2 - Disease inheritance 'aspect' == 'I' record processed (edge-specific fields ignored)
            {
                "database_id": "OMIM:300425",
                "disease_name": "Autism susceptibility, X-linked 1",
                "hpo_id": "HP:0001417",
                "aspect": "I",  # assert 'Inheritance' test record
            },
            [
                {
                    "id": "OMIM:300425",
                    "name": "Autism susceptibility, X-linked 1",
                    "category": ["biolink:Disease"],
                    "inheritance": "X-linked inheritance",
                }
            ]
        ),
    ],
)
def test_disease_to_phenotype_node_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_nodes: list | None
):
    validate_transform_result(
        result=transform_disease_to_phenotype_node_record(mock_koza_transform_1, test_record),
        expected_nodes=result_nodes,
        expected_edges=None,
        node_test_slots=NODE_TEST_SLOTS
    )


@pytest.mark.parametrize(
    "test_record,result_edge",
    [
        (  # Query 0 - An 'aspect' == 'C' record processed
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
            # This is not a 'P' record, so it should be skipped
            None
        ),
        (  # Query 1 - An 'aspect' == 'P' record processed
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                "qualifier": "",
                "hpo_id": "HP:0001249",
                "reference": "OMIM:117650",
                "evidence": "IEA",
                "onset": "",
                "frequency": "50%",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:probinson[2009-02-17]",
            },
            # Captured edge contents
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                # "negated": False,  # removed, see https://github.com/NCATSTranslator/translator-ingests/issues/474
                "object": "HP:0001249",
                # Although "OMIM:117650" is recorded above as
                # a reference, it is not used as a publication
                "publications": [],
                "has_evidence_of_type": ["ECO:0000501"],
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
                "agent_type": AgentTypeEnum.text_mining_agent,
            }
        ),
        (  # Query 2 - Another 'aspect' == 'P' record processed
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                # "qualifier" was actually empty in the original Monarch test data;
                # however, we want to trigger a test of the negation, so we lie!
                "qualifier": "NOT",
                "hpo_id": "HP:0001545",
                "reference": "OMIM:117650",
                "evidence": "PCS",
                "onset": "",
                "frequency": "HP:0040283",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:skoehler[2017-07-13]",
            },
            # Negated edges now suppressed, see https://github.com/NCATSTranslator/translator-ingests/issues/474
            None
            # {
            #     "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
            #     "subject": "OMIM:117650",
            #     "predicate": "biolink:has_phenotype",
            #     "negated": True,
            #     "object": "HP:0001545",
            #     "publications": [],
            #     "has_evidence_of_type": ["ECO:0000304"],
            #     "sex_qualifier": None,
            #     "onset_qualifier": None,
            #     "has_percentage": None,
            #     "has_quotient": None,
            #     "frequency_qualifier": "HP:0040283",
            #     "sources": [
            #         {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
            #         {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
            #     ],
            #     "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            #     "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            # }
        ),
        (  # Query 3 - Same 'aspect' == 'P' record but lacking any frequency qualifier
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
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                # "negated": False, # removed, see https://github.com/NCATSTranslator/translator-ingests/issues/474
                "object": "HP:0001545",
                "publications": [],
                "has_evidence_of_type": ["ECO:0000304"],
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
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            },
        )
    ],
)
def test_disease_to_phenotype_edge_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_edge: dict | None,
):
    validate_transform_result(
        result=transform_disease_to_phenotype_edge_record(mock_koza_transform_1, test_record),
        expected_nodes=None,
        expected_edges=result_edge,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )


@pytest.mark.parametrize(
    ("association", "expected_predicate"),
    [
        ("MENDELIAN", "biolink:causes"),
        ("POLYGENIC", "biolink:contributes_to"),
        ("UNKNOWN", None),
    ],
)
def test_predicate(association: str, expected_predicate: str | None):
    predicate = get_qualified_predicate(association)

    assert predicate == expected_predicate


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - Sample Mendelian disease
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
                "predicate": "biolink:associated_with",
                "object": "OMIM:212050",
                "qualified_predicate": "biolink:causes",
                "subject_form_or_variant_qualifier": "genetic_variant_form",
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:medgen"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (   # Query 1 - Sample Polygenic disease
            {
                "association_type": "POLYGENIC",
                "disease_id": "OMIM:615232",
                "gene_symbol": "SLC1A1",
                "ncbi_gene_id": "NCBIGene:6505",
                "source": "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen",
            },
            # Captured node contents
            [
                {"id": "NCBIGene:6505", "name": "SLC1A1", "category": ["biolink:Gene"]},
                {"id": "OMIM:615232", "category": ["biolink:Disease"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneToDiseaseAssociation"],
                "subject": "NCBIGene:6505",
                "predicate": "biolink:associated_with",
                "object": "OMIM:615232",
                "qualified_predicate": "biolink:contributes_to",
                "subject_form_or_variant_qualifier": "genetic_variant_form",
                "sources": [
                    {"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:medgen"},
                    {"resource_role": "supporting_data_source", "resource_id": "infores:omim"},
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (   # Query 2 - Sample disease of UNKNOWN association type
            {
                "association_type": "UNKNOWN",
                "disease_id": "ORPHA:79414",
                "gene_symbol": "HRAS",
                "ncbi_gene_id": "NCBIGene:3265",
                "source": "http://www.orphadata.org/data/xml/en_product6.xml",
            },
            # UNKNOWN gene-to-disease associations are actually in the ingest for now (see the RIG)
            # Captured node contents
            None,
            # Captured edge contents
            None
        ),
    ],
)
def test_gene_to_disease_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_nodes: list | None,
    result_edge: dict | None,
):
    validate_transform_result(
        result=transform_gene_to_disease_record(mock_koza_transform_1, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )

@pytest.fixture(scope="package")
def mock_koza_transform_2() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    return MockKozaTransform(writer=writer,
                             mappings={},
                             extra_fields={},
                             input_files_dir=Path(HPOA_TEST_DATA_PATH))


def test_transform_record_disease_to_phenotype(mock_koza_transform_2: koza.KozaTransform):
    result: Iterable[dict[str, Any]] | None = prepare_gene_to_phenotype_data(mock_koza_transform_2, [])
    assert result is not None
    expected_entry: dict[str, Any] = {
        "ncbi_gene_id": 22,  # this gotta be an 'int'!
        "gene_symbol": "ABCB7",
        "hpo_id": "HP:0002470",
        "hpo_name": "Nonprogressive cerebellar ataxia",
        "evidence": "PCS",
        "publications": "PMID:26242992;PMID:4045952;PMID:11050011",
        "frequency": "11/11",
        "disease_id": "OMIM:301310",
        "gene_to_disease_association_types": "MENDELIAN",
    }
    # Find any entry with the expected fields in the result list
    assert any(
        # Check that all expected fields are present in the entry
        all(key in expected_entry and expected_entry[key] == value for key, value in entry.items())
        for entry in result
    )


# The gene_to_phenotype ingest keeps only rows whose underlying gene-disease association
# asserts MENDELIAN inheritance: HPO infers G-P from G-D plus D-P, and that inference only
# holds where a single gene is causal. See hpoa_rig.yaml included_content/filtered_content.
# Regression test for https://github.com/NCATSTranslator/translator-ingests/issues/537, where
# this filter lived in hpoa.yaml as a reader filter and therefore never ran: the reader filter
# was applied to raw rows that prepare_gene_to_phenotype_data discards, so every Orphanet
# (UNKNOWN) and OMIM POLYGENIC row was emitted as a 'causes' edge anyway.
@pytest.mark.parametrize(
    "hpo_id,disease_id,association_types,kept",
    [
        # MENDELIAN - the inference holds, row is kept
        ("HP:0002463", "OMIM:301310", "MENDELIAN", True),
        ("HP:0002470", "OMIM:301310", "MENDELIAN", True),
        ("HP:0001133", "OMIM:601718", "MENDELIAN", True),
        # the gene-disease pair carries both a MENDELIAN and a POLYGENIC assertion; a MENDELIAN
        # assertion exists, so the inference holds and the row is kept
        ("HP:0000548", "OMIM:248200", "MENDELIAN;POLYGENIC", True),
        # POLYGENIC only - the gene may be one of many contributing factors, row is dropped
        ("HP:0000510", "OMIM:153800", "POLYGENIC", False),
        # UNKNOWN (every Orphanet gene-disease row is UNKNOWN), row is dropped
        ("HP:0001105", "ORPHA:791", "UNKNOWN", False),
    ],
)
def test_gene_to_phenotype_mendelian_filter(
    mock_koza_transform_2: koza.KozaTransform,
    hpo_id: str,
    disease_id: str,
    association_types: str,
    kept: bool,
):
    """
    Check that prepare_gene_to_phenotype_data keeps only gene-phenotype rows inferred over a
    MENDELIAN gene-disease association, whatever the disease source.
    """
    result = prepare_gene_to_phenotype_data(mock_koza_transform_2, [])
    assert result is not None
    matches = [row for row in result if row["hpo_id"] == hpo_id and row["disease_id"] == disease_id]
    if kept:
        assert len(matches) == 1, f"expected {hpo_id}/{disease_id} ({association_types}) to be kept"
        assert matches[0]["gene_to_disease_association_types"] == association_types
    else:
        assert not matches, f"expected {hpo_id}/{disease_id} ({association_types}) to be filtered out"


def test_gene_to_phenotype_prepared_data_is_all_mendelian(mock_koza_transform_2: koza.KozaTransform):
    """
    Check that no non-MENDELIAN gene-disease association type survives preparation. Guards the
    whole prepared set rather than the specific sample rows above, so a filter that regresses
    for an association type not represented in the sample data still fails here.
    """
    result = prepare_gene_to_phenotype_data(mock_koza_transform_2, [])
    assert result is not None
    rows = list(result)
    assert rows, "expected at least one prepared gene_to_phenotype row"
    non_mendelian = [row["gene_to_disease_association_types"] for row in rows if "MENDELIAN" not in row["gene_to_disease_association_types"]]
    assert not non_mendelian, f"non-MENDELIAN association types survived preparation: {sorted(set(non_mendelian))}"


# Edge counts measured against the HPOA release downloaded 2026-09-29:
#   disease_to_phenotype_edges (aspect 'P', hpo_id set, not negated)  268,180
#   gene_to_disease            (MENDELIAN 7,094 + POLYGENIC 582)        7,676
#   gene_to_phenotype          (MENDELIAN only)                       158,788
#                                                              total  434,644
# With the MENDELIAN filter broken, gene_to_phenotype emits 333,984 rows instead - the 175,196
# extra being 171,700 Orphanet UNKNOWN plus 3,496 OMIM POLYGENIC, matching issue #537 - for a
# total of 609,840. writer.max_edge_count has to sit between the two so that the regression trips
# the build. koza enforces it in KozaWriter.validate_counts() after the run, over the total edges
# written by all of hpoa.yaml's readers.
HPOA_EXPECTED_EDGE_COUNT = 434_644
HPOA_UNFILTERED_EDGE_COUNT = 609_840


def test_hpoa_max_edge_count_catches_a_broken_mendelian_filter():
    """
    Check that hpoa.yaml configures an edge ceiling that the measured output clears but a
    regressed MENDELIAN filter does not. A tripwire for the class of bug in issue #537, where
    the filter silently stopped applying and 52% of gene_to_phenotype output was wrong.
    """
    config_yaml_file_path = get_ingest_config_yaml_path("hpoa")
    assert config_yaml_file_path is not None
    with config_yaml_file_path.open("r", encoding="utf-8") as fh:
        config = KozaConfig(**yaml.load(fh, Loader=UniqueIncludeLoader.with_file_base(str(config_yaml_file_path))))  # noqa: S506

    max_edge_count = config.writer.max_edge_count
    assert max_edge_count is not None, "hpoa.yaml must set writer.max_edge_count as a filter tripwire"
    assert max_edge_count > HPOA_EXPECTED_EDGE_COUNT, (
        f"writer.max_edge_count of {max_edge_count} is below the {HPOA_EXPECTED_EDGE_COUNT} edges "
        f"the ingest is measured to emit, so a correct build would fail"
    )
    assert max_edge_count < HPOA_UNFILTERED_EDGE_COUNT, (
        f"writer.max_edge_count of {max_edge_count} is at or above the {HPOA_UNFILTERED_EDGE_COUNT} "
        f"edges emitted when the gene_to_phenotype MENDELIAN filter stops applying, so it would "
        f"not catch that regression"
    )


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - Full record, with the empty ("-") frequency field
            {
                "ncbi_gene_id": 8086,
                "gene_symbol": "AAAS",
                "hpo_id": "HP:0000252",
                "hpo_name": "Microcephaly",
                "evidence": "IEA",
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
                "predicate": GeneToPhenotypicFeaturePredicateEnum.biolinkCOLONassociated_with,
                "object": "HP:0000252",
                "qualified_predicate": "biolink:causes",
                "subject_form_or_variant_qualifier": "genetic_variant_form",
                "frequency_qualifier": None,
                "has_percentage": None,
                "has_quotient": None,
                "has_count": None,
                "has_total": None,
                "disease_context_qualifier": "MONDO:0009279",
                "publications": ["PMID:11062474"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.text_mining_agent,
            },
        ),
        (  # Query 1 - Full record, with a HPO term defined frequency field value
            {
                "ncbi_gene_id": 8120,
                "gene_symbol": "AP3B2",
                "hpo_id": "HP:0001298",
                "hpo_name": "Encephalopathy",
                "evidence": "PCS",
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
                "predicate": GeneToPhenotypicFeaturePredicateEnum.biolinkCOLONassociated_with,
                "object": "HP:0001298",
                "qualified_predicate": "biolink:causes",
                "subject_form_or_variant_qualifier": "genetic_variant_form",
                "frequency_qualifier": "HP:0040281",
                "has_percentage": None,
                "has_quotient": None,
                "has_count": None,
                "has_total": None,
                "disease_context_qualifier": "MONDO:0018614",
                "publications": [],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (  # Query 2 - Full record, with a ratio ("quotient") frequency field value
            {
                "ncbi_gene_id": 8192,
                "gene_symbol": "CLPP",
                "hpo_id": "HP:0000013",
                "hpo_name": "Hypoplasia of the uterus",
                "evidence": "TAS",
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
                "predicate": GeneToPhenotypicFeaturePredicateEnum.biolinkCOLONassociated_with,
                "object": "HP:0000013",
                "qualified_predicate": "biolink:causes",
                "subject_form_or_variant_qualifier": "genetic_variant_form",
                "frequency_qualifier": "HP:0040282",
                "has_percentage": 33.0,
                "has_quotient": 0.33,
                "has_count": 3,
                "has_total": 9,
                "disease_context_qualifier": "MONDO:0013588",
                "publications": ["PMID:23541340"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            },
        ),
        (  # Query 3 - Full record, with a percentage frequency field value
            # 8929	PHOX2B	HP:0003005	Ganglioneuroma	5%	OMIM:613013
            {
                "ncbi_gene_id": 8929,
                "gene_symbol": "PHOX2B",
                "hpo_id": "HP:0003005",
                "hpo_name": "Ganglioneuroma",
                "evidence": "PCS",
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
                "predicate": GeneToPhenotypicFeaturePredicateEnum.biolinkCOLONassociated_with,
                "object": "HP:0003005",
                "qualified_predicate": "biolink:causes",
                "subject_form_or_variant_qualifier": "genetic_variant_form",
                "frequency_qualifier": "HP:0040283",
                "has_percentage": 5,
                "has_quotient": 0.05,
                "has_count": None,
                "has_total": None,
                "disease_context_qualifier": "MONDO:0700041",
                "publications": ["PMID:23541340", "PMID:12345678"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hpo-annotations"}],
                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
    ],
)
def test_gene_to_phenotype_transform(
    mock_koza_transform_1: koza.KozaTransform,
    test_record: dict,
    result_nodes: list,
    result_edge: dict
):
    validate_transform_result(
        result=transform_gene_to_phenotype_record(mock_koza_transform_1, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=GENE_TO_PHENOTYPE_ASSOCIATION_TEST_SLOTS,
    )


# ── Pydantic round-trip fixtures & test ──────────────────────────────

_HPOA_SOURCES = [
    RetrievalSource(
        id="infores:hpo-annotations",
        resource_id="infores:hpo-annotations",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": DiseaseToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:hpoa-test-1",
            "subject": "OMIM:117650",
            "predicate": "biolink:has_phenotype",
            "object": "HP:0001249",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _HPOA_SOURCES,
        },
    },
    {
        "association_class": CausalGeneToDiseaseAssociation,
        "params": {
            "id": "uuid:hpoa-test-2",
            "subject": "NCBIGene:64170",
            "predicate": "biolink:associated_with",
            "object": "OMIM:212050",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _HPOA_SOURCES,
        },
    },
    {
        "association_class": GeneToDiseaseAssociation,
        "params": {
            "id": "uuid:hpoa-test-3",
            "subject": "NCBIGene:6505",
            "predicate": "biolink:associated_with",
            "object": "OMIM:615232",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _HPOA_SOURCES,
        },
    },
    {
        "association_class": GeneToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:hpoa-test-4",
            "subject": "NCBIGene:8086",
            "predicate": GeneToPhenotypicFeaturePredicateEnum.biolinkCOLONassociated_with,
            "object": "HP:0000252",
            "knowledge_level": KnowledgeLevelEnum.logical_entailment,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _HPOA_SOURCES,
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
