import pytest

from typing import Optional, Dict, List

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza

from src.translator_ingest.ingests.hpoa.disease_to_phenotype_transform import transform_record

from . import transform_test_runner


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - missing data (empty record, hence, missing fields)
            {},
            None,
            None
        ),
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
            None
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
            ["OMIM:117650", "HP:0001249"],

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
                # see TODO semantic concerns documented in the inline comment
                #          in the phenotype_frequency_to_hpo_term() utility method
                "has_quotient": 0.5,
                # '50%' above implies HPO term that the phenotype
                # is 'Present in 30% to 79% of the cases'.
                "frequency_qualifier": None, # TODO: should perhaps this be set to "HP:0040282"?
                "sources": [
                    {
                       "resource_role": "primary_knowledge_source",
                       "resource_id": "infores:hpo-annotations"
                    },
                    {
                       "resource_role": "supporting_data_source",
                       "resource_id": "infores:omim"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent
            }
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
            ["OMIM:117650", "HP:0001545"],
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
                    {
                       "resource_role": "primary_knowledge_source",
                       "resource_id": "infores:hpo-annotations"
                    },
                    {
                       "resource_role": "supporting_data_source",
                       "resource_id": "infores:omim"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent
            }
        ),
        (  # Query 4 - Disease inheritance 'aspect' == 'I' record processed
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
            ["OMIM:300425", "HP:0001417"],
            {
                "category": ["biolink:DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation"],
                "subject": "OMIM:300425",
                "predicate": "biolink:has_mode_of_inheritance",
                "object": "HP:0001417",
                "publications": [],
                "has_evidence": ["ECO:0000501"],
                "sex_qualifier": None,
                "onset_qualifier": None,
                "has_percentage": None,
                "has_quotient": None,
                "frequency_qualifier": None,

                "sources": [
                    {
                       "resource_role": "primary_knowledge_source",
                       "resource_id": "infores:hpo-annotations"
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
def test_disease_to_phenotype_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: Dict,
        result_nodes: Optional[List],
        result_edge: Optional[Dict]
):
    transform_test_runner(transform_record(mock_koza_transform, test_record), result_nodes, result_edge)
