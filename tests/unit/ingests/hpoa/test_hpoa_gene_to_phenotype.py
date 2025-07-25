from typing import Optional, List, Dict

import pytest

from typing import Optional, Dict, List

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

from src.translator_ingest.ingests.hpoa.gene_to_phenotype_transform import transform_record

from . import transform_test_runner


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 0 - missing data (empty record (hence, missing fields)
            {},
            None,
            None
        ),
        (   # Query 1 - Full record, with empty ("-") frequency field
            {
                "ncbi_gene_id": "8086",
                "gene_symbol": "AAAS",
                "hpo_id": "HP:0000252",
                "hpo_name": "Microcephaly",
                "frequency": "-",
                "disease_id": "OMIM:231550"
            },

            # Captured node identifiers
            ["NCBIGene:8086", "HP:0000252"],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8086",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0000252",

                # frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
                # has_percentage=frequency.has_percentage,
                # has_quotient=frequency.has_quotient,
                # has_count=frequency.has_count,
                # has_total=frequency.has_total,
                # disease_context_qualifier=dis_id,
                # publications=publications,

                # We still need to fix the 'sources' serialization
                # in Pydantic before somehow testing the following
                # "primary_knowledge_source": "infores:hpo-annotations"
                # "supporting_knowledge_source": "infores:medgen"
                # assert "infores:monarchinitiative" in association.aggregator_knowledge_source

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
                "frequency": "HP:0040281",
                "disease_id": "ORPHA:442835"
            },

            # Captured node identifiers
            ["NCBIGene:8120", "HP:0001298"],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8120",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0001298",

                # frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
                # has_percentage=frequency.has_percentage,
                # has_quotient=frequency.has_quotient,
                # has_count=frequency.has_count,
                # has_total=frequency.has_total,
                # disease_context_qualifier=dis_id,
                # publications=publications,

                # We still need to fix the 'sources' serialization
                # in Pydantic before somehow testing the following
                # "primary_knowledge_source": "infores:hpo-annotations"
                # "supporting_knowledge_source": "infores:medgen"
                # assert "infores:monarchinitiative" in association.aggregator_knowledge_source

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
                "frequency": "3/9",
                "disease_id": "OMIM:614129"
            },

            # Captured node identifiers
            ["NCBIGene:8192", "HP:0000013"],

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:8192",
                "predicate": "biolink:has_phenotype",
                "object": "HP:0000013",

                # frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
                # has_percentage=frequency.has_percentage,
                # has_quotient=frequency.has_quotient,
                # has_count=frequency.has_count,
                # has_total=frequency.has_total,
                # disease_context_qualifier=dis_id,
                # publications=publications,

                # We still need to fix the 'sources' serialization
                # in Pydantic before somehow testing the following
                # "primary_knowledge_source": "infores:hpo-annotations"
                # "supporting_knowledge_source": "infores:medgen"
                # assert "infores:monarchinitiative" in association.aggregator_knowledge_source

                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent
            }
        ),
    ]
)
def test_gene_to_disease_transform(
        test_record: Dict,
        result_nodes: Optional[List],
        result_edge: Optional[Dict]
):
    transform_test_runner(transform_record(test_record), result_nodes, result_edge)
