"""
Tests against translator_ingest.ingests.hpoa.gene_to_phenotype_transform.transform_record

Note that the implementation has a 'prepare' file data merging step for which testing is tricky.
Thus, our test data is actually an artificially preprocessed version of the original data.
"""

from typing import Optional

import pytest

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza

from translator_ingest.ingests.hpoa.gene_to_phenotype_transform import transform_record

from . import transform_test_runner


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 0 - missing data (empty record (hence, missing fields)
            {},
            None,
            None
        ),
        (   # Query 1 - Full record, with the empty ("-") frequency field
            {
                "ncbi_gene_id": "8086",
                "gene_symbol": "AAAS",
                "hpo_id": "HP:0000252",
                "hpo_name": "Microcephaly",
                "publications": "PMID:11062474",
                "frequency": "-",
                "disease_id": "OMIM:231550",
                "gene_to_disease_association_types": "MENDELIAN"
            },

            # Captured node contents
            [
                {
                    "id": "NCBIGene:8086",
                    "name": "AAAS",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "HP:0000252",
                    "category": ["biolink:PhenotypicFeature"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8086",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0000252",

                "frequency_qualifier": None,
                "has_percentage":  None,
                "has_quotient":  None,
                "has_count":  None,
                "has_total": None,
                "disease_context_qualifier": "OMIM:231550",
                "publications": ["PMID:11062474"],

                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:hpo-annotations"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent
            }
        ),
        (   # Query 2 - Full record, with a HPO term defined frequency field value
            {
                "ncbi_gene_id": "8120",
                "gene_symbol": "AP3B2",
                "hpo_id": "HP:0001298",
                "hpo_name": "Encephalopathy",
                "publications": "",
                "frequency": "HP:0040281",
                "disease_id": "ORPHA:442835",
                "gene_to_disease_association_types": "MENDELIAN"
            },

            # Captured node contents
            [
                {
                    "id": "NCBIGene:8120",
                    "name": "AP3B2",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "HP:0001298",
                    "category": ["biolink:PhenotypicFeature"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8120",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0001298",

                "frequency_qualifier": "HP:0040281",
                "has_percentage":  None,
                "has_quotient":  None,
                "has_count":  None,
                "has_total": None,
                "disease_context_qualifier": "Orphanet:442835", # this ought to be MONDO in the future
                "publications": [],

                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:hpo-annotations"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent
            }
        ),
        (   # Query 3 - Full record, with a ratio ("quotient") frequency field value
            {
                "ncbi_gene_id": "8192",
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
                {
                    "id": "NCBIGene:8192",
                    "name": "CLPP",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "HP:0000013",
                    "category": ["biolink:PhenotypicFeature"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8192",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0000013",

                "frequency_qualifier": None,
                "has_percentage":  33.33333333333333,
                "has_quotient":  0.3333333333333333,
                "has_count":  3,
                "has_total": 9,
                "disease_context_qualifier": "OMIM:614129", # this ought to be MONDO in the future
                "publications": ["PMID:23541340"],

                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:hpo-annotations"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent
            }
        ),

        (   # Query 4 - Full record, with a percentage frequency field value
            # 8929	PHOX2B	HP:0003005	Ganglioneuroma	5%	OMIM:613013
            {
                "ncbi_gene_id": "8929",
                "gene_symbol": "PHOX2B",
                "hpo_id": "HP:0003005",
                "hpo_name": "Ganglioneuroma",
                "publications": "PMID:23541340;PMID:12345678",
                "frequency": "5%",
                "disease_id": "OMIM:613013",
                "gene_to_disease_association_types": "MENDELIAN"
            },

            # Captured node contents
            [
                {
                    "id": "NCBIGene:8929",
                    "name": "PHOX2B",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "HP:0003005",
                    "category": ["biolink:PhenotypicFeature"]
                }

            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8929",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0003005",

                "frequency_qualifier": None,
                "has_percentage":  5,
                "has_quotient":  0.05,
                "has_count":  None,
                "has_total": None,
                "disease_context_qualifier": "OMIM:613013", # this ought to be MONDO in the future
                "publications": ["PMID:23541340", "PMID:12345678"],

                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:hpo-annotations"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent
            }
        )
    ]
)
def test_gene_to_phenotype_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    transform_test_runner(transform_record(mock_koza_transform, test_record), result_nodes, result_edge)
