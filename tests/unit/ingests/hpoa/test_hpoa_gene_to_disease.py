from typing import Optional, List, Dict

import pytest

from biolink_model.datamodel.pydanticmodel_v2 import GeneToDiseaseAssociation, CausalGeneToDiseaseAssociation

from src.translator_ingest.util.monarch.constants import (
    BIOLINK_CAUSES,
    BIOLINK_CONTRIBUTES_TO,
    BIOLINK_GENE_ASSOCIATED_WITH_CONDITION,
    INFORES_MEDGEN,
    INFORES_MONARCHINITIATIVE,
    INFORES_OMIM,
    INFORES_ORPHANET,
)
from src.translator_ingest.ingests.hpoa.phenotype_ingest_utils import get_knowledge_sources, get_predicate
from src.translator_ingest.ingests.hpoa.gene_to_disease_transform import transform_record
from . import transform_test_runner

@pytest.mark.parametrize(
    ("original_source", "expected_primary_knowledge_source", "expected_aggregator_knowledge_source"),
    [
        (
            "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen",
            INFORES_OMIM,
            [INFORES_MEDGEN, INFORES_MONARCHINITIATIVE],
        ),
        ("http://www.orphadata.org/data/xml/en_product6.xml", INFORES_ORPHANET, [INFORES_MONARCHINITIATIVE]),
    ],
)
def test_knowledge_source(
    original_source: str, expected_primary_knowledge_source: str, expected_aggregator_knowledge_source: List[str]
):
    primary_knowledge_source, aggregator_knowledge_source = get_knowledge_sources(
        original_source, INFORES_MONARCHINITIATIVE
    )

    assert primary_knowledge_source == expected_primary_knowledge_source
    assert aggregator_knowledge_source.sort() == expected_aggregator_knowledge_source.sort()


@pytest.mark.parametrize(
    ("association", "expected_predicate"),
    [
        ("MENDELIAN", BIOLINK_CAUSES),
        ("POLYGENIC", BIOLINK_CONTRIBUTES_TO),
        ("UNKNOWN", BIOLINK_GENE_ASSOCIATED_WITH_CONDITION),
    ],
)
def test_predicate(association: str, expected_predicate: str):
    predicate = get_predicate(association)

    assert predicate == expected_predicate


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - missing data (empty 'hpo_id' field)
            {},
            None,
            None
        ),
        (  # Query 1 - Sample Mendelian disease
            {
                "association_type": "MENDELIAN",
                "disease_id": "OMIM:212050",
                "gene_symbol": "CARD9",
                "ncbi_gene_id": "NCBIGene:64170",
                "source": "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen",
            },

            # Captured node identifiers
            ["NCBIGene:64170", "OMIM:212050"],

            # Captured edge contents
            {
                "category": ["biolink:CausalGeneToDiseaseAssociation"],
                "subject": "NCBIGene:64170",
                "predicate": "biolink:causes",
                "object": "OMIM:212050",

                # We still need to fix the 'sources' serialization
                # in Pydantic before somehow testing the following
                # "primary_knowledge_source": "infores:hpo-annotations"
                # "supporting_knowledge_source": "infores:medgen"
                # assert "infores:monarchinitiative" in association.aggregator_knowledge_source
            }
        )
    ]
)
def test_gene_to_disease_transform(
        test_record: Dict,
        result_nodes: Optional[List],
        result_edge: Optional[Dict]
):
    transform_test_runner(transform_record(test_record), result_nodes, result_edge)
