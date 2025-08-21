from typing import Optional

import pytest

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza

from translator_ingest.util.biolink import (
    BIOLINK_CAUSES,
    BIOLINK_CONTRIBUTES_TO,
    BIOLINK_ASSOCIATED_WITH,
)
from translator_ingest.ingests.hpoa.phenotype_ingest_utils import get_hpoa_genetic_predicate

from translator_ingest.ingests.hpoa.gene_to_disease_transform import transform_record

from . import transform_test_runner


@pytest.mark.parametrize(
    ("association", "expected_predicate"),
    [
        ("MENDELIAN", BIOLINK_CAUSES),
        ("POLYGENIC", BIOLINK_CONTRIBUTES_TO),
        ("UNKNOWN", BIOLINK_ASSOCIATED_WITH),
    ],
)
def test_predicate(association: str, expected_predicate: str):
    predicate = get_hpoa_genetic_predicate(association)

    assert predicate == expected_predicate


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - missing data (empty record, hence, missing fields)
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

            # Captured node contents
            [
                {
                    "id": "NCBIGene:64170",
                    "name": "CARD9",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "OMIM:212050",
                    "category": ["biolink:Disease"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:CausalGeneToDiseaseAssociation"],
                "subject": "NCBIGene:64170",
                "predicate": "biolink:causes",
                "object": "OMIM:212050",

                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:hpo-annotations"
                    },
                    {
                        "resource_role": "supporting_data_source",
                        "resource_id": "infores:medgen"
                    },
                    {
                        "resource_role": "supporting_data_source",
                        "resource_id": "infores:omim"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent
            }
        )
    ]
)
def test_gene_to_disease_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    transform_test_runner(transform_record(mock_koza_transform, test_record), result_nodes, result_edge)
