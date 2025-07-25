from typing import Optional, List, Dict

import pytest

from typing import Optional, Dict, List

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

from src.translator_ingest.ingests.hpoa.gene_to_phenotype_transform import transform_record

from . import transform_test_runner


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 0 - missing data (empty 'hpo_id' field)
            {},
            None,
            None
        ),
        (   # Query 1 - Sample Mendelian disease
            {
                "ncbi_gene_id": "8192",
                "gene_symbol": "CLPP",
                "hpo_id": "HP:0000252",
                "hpo_name": "Microcephaly",
            },

            # Captured node identifiers
            ["NCBIGene:64170", "OMIM:212050"],

            #     assert basic_hpoa
            #     assert len(basic_hpoa) == 1
            #     association = [entity for entity in basic_hpoa if isinstance(entity, GeneToPhenotypicFeatureAssociation)][0]
            #     assert len(basic_hpoa) == 1
            #     assert basic_hpoa[0]
            #     assert basic_hpoa[0].subject == "NCBIGene:8192"
            #     assert basic_hpoa[0].object == "HP:0000252"
            #     assert basic_hpoa[0].predicate == "biolink:has_phenotype"
            #     assert association.primary_knowledge_source == "infores:hpo-annotations"
            #     assert "infores:monarchinitiative" in association.aggregator_knowledge_source
            #
            #   - 'id'
            #   - 'category'
            #   - 'subject'
            #   - 'predicate'
            #   - 'object'
            #   - 'aggregator_knowledge_source'
            #   - 'primary_knowledge_source'
            #   - 'knowledge_level'
            #   - 'agent_type'
            #   - 'frequency_qualifier'
            #   - 'has_count'
            #   - 'has_total'
            #   - 'has_percentage'
            #   - 'has_quotient'
            #   - 'disease_context_qualifier'
            #   - 'publications'

            # id="uuid:" + str(uuid.uuid1()),
            # subject=gene_id,
            # predicate="biolink:has_phenotype",
            # object=hpo_id,
            # frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
            # has_percentage=frequency.has_percentage,
            # has_quotient=frequency.has_quotient,
            # has_count=frequency.has_count,
            # has_total=frequency.has_total,
            # disease_context_qualifier=dis_id,
            # publications=publications,
            # primary_knowledge_source="infores:hpo-annotations",
            # knowledge_level=KnowledgeLevelEnum.logical_entailment,
            # agent_type=AgentTypeEnum.automated_agent,

            # Captured edge contents
            {
                "category": ["biolink:GeneToPhenotypicFeatureAssociation"],
                "subject": "NCBIGene:64170",
                "predicate": "biolink:causes",
                "object": "OMIM:212050",

                # We still need to fix the 'sources' serialization
                # in Pydantic before somehow testing the following
                # "primary_knowledge_source": "infores:hpo-annotations"
                # "supporting_knowledge_source": "infores:medgen"
                # assert "infores:monarchinitiative" in association.aggregator_knowledge_source

                "knowledge_level": KnowledgeLevelEnum.logical_entailment,
                "agent_type": AgentTypeEnum.automated_agent
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
