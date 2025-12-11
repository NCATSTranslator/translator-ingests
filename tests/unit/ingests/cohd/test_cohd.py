import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza

from translator_ingest.ingests.cohd.cohd import (
    transform_cohd_node,
    transform_cohd_edge
)

from tests.unit.ingests import validate_transform_result, mock_koza_transform


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
    "name",
    "category"
)

# list of slots whose values are
# to be checked in a result edge
CORE_ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "object",
    "sources",
    "knowledge_level",
    "agent_type"
)

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - Missing id - returns None
            {
                # "id": "PUBCHEM.COMPOUND:2083",
            }
            ,
            None,
            None,
        ),
        (   # Query 1 - A complete node record
            {
                "id": "SNOMEDCT:60108003",
                "name": "Congenital dislocation of one hip with subluxation of other",
                "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                "attributes": [
                    # The 'attributes' here are strings of embedded JSON data, re-formatted for readability here
                    "{"+
                        "\"attribute_source\": \"infores:cohd\","
                        " \"attribute_type_id\": \"EDAM:data_0954\","
                        " \"attributes\": ["
                            "{"
                               "\"attribute_source\": \"infores:omop-ohdsi\", "
                               "\"attribute_type_id\": \"EDAM:data_1087\", "
                               "\"original_attribute_name\": \"concept_id\", "
                               "\"value\": \"OMOP:77661\", "
                               "\"value_type_id\": \"EDAM:data_1087\", "
                               "\"value_url\": \"https://athena.ohdsi.org/search-terms/terms/77661\""
                            "}, "
                               "{\"attribute_source\": \"infores:omop-ohdsi\", "
                                "\"attribute_type_id\": \"EDAM:data_2339\", "
                                "\"original_attribute_name\": \"concept_name\", "
                                "\"value\": \"Congenital dislocation of one hip with subluxation of other\", "
                                "\"value_type_id\": \"EDAM:data_2339\""
                            "}, "
                                "{\"attribute_source\": \"infores:omop-ohdsi\", "
                                "\"attribute_type_id\": \"EDAM:data_0967\", "
                                "\"original_attribute_name\": \"domain\", "
                                "\"value\": \"Condition\", "
                                "\"value_type_id\": \"EDAM:data_0967\""
                            "}"
                        "], "
                        "\"original_attribute_name\": \"Database cross-mapping\", "
                        "\"value\": \"(OMOP:2313993)-[OMOP Map]-(CPT:93976)\", "
                        "\"value_type_id\": \"EDAM:data_0954\""
                    "}"
                ]
            },
            # Captured node contents
            [
                {
                    "id": "SNOMEDCT:60108003",
                    "name": "Congenital dislocation of one hip with subluxation of other",
                    "category": ["biolink:DiseaseOrPhenotypicFeature"],
                }
            ],
            # Captured edge contents - n/a
            None
        ),
        (   # Query 2- Another complete node record
            {
                "id": "CPT:73540",
                "name": "Radiologic examination, pelvis and hips, infant or child, minimum of 2 views",
                "categories": ["biolink:Procedure"],
                "attributes": [
                    # The 'attributes' here are strings of embedded JSON data, re-formatted for readability here
                    "{"
                        "\"attribute_source\": \"infores:cohd\", "
                        "\"attribute_type_id\": \"EDAM:data_0954\", "
                        "\"attributes\": ["
                            "{"
                            "\"attribute_source\": \"infores:omop-ohdsi\", "
                            "\"attribute_type_id\": \"EDAM:data_1087\", "
                            "\"original_attribute_name\": \"concept_id\", "
                            "\"value\": \"OMOP:2211477\", "
                            "\"value_type_id\": \"EDAM:data_1087\", "
                            "\"value_url\": \"https://athena.ohdsi.org/search-terms/terms/2211477\""
                            "}, "
                            "{"
                            "\"attribute_source\": \"infores:omop-ohdsi\", "
                            "\"attribute_type_id\": \"EDAM:data_2339\", "
                            "\"original_attribute_name\": \"concept_name\", "
                            "\"value\": \"Radiologic examination, pelvis and hips, infant or child, minimum of 2 views\", "
                            "\"value_type_id\": \"EDAM:data_2339\""
                            "}, "
                            "{"
                            "\"attribute_source\": \"infores:omop-ohdsi\", "
                            "\"attribute_type_id\": \"EDAM:data_0967\", "
                            "\"original_attribute_name\": \"domain\", "
                            "\"value\": \"Procedure\", "
                            "\"value_type_id\": \"EDAM:data_0967\""
                            "}"
                        "], "
                        "\"original_attribute_name\": \"Database cross-mapping\", "
                        "\"value\": \"(OMOP:2313993)-[OMOP Map]-(CPT:93976)\", "
                        "\"value_type_id\": \"EDAM:data_0954\""
                    "}"
                ]
            },
            #
            # Captured node contents
            [
                {
                    "id": "CPT:73540",
                    "name": "Radiologic examination, pelvis and hips, infant or child, minimum of 2 views",
                    "category": ["biolink:Procedure"],
                }
            ],
            # Captured edge contents - n/a
            None
        ),
        (   # Query 4- Another complete node record
            {
                "id": "UMLS:C0160047",
                "name": "Sprain, coracoclavicular ligament",
                "categories": [
                    "biolink:Disease",
                    "biolink:DiseaseOrPhenotypicFeature",
                    "biolink:BiologicalEntity",
                    "biolink:ThingWithTaxon",
                    "biolink:NamedThing"
                ],
                "attributes": [
                    # The 'attributes' here are strings of embedded JSON data, re-formatted for readability here
                    "{"
                    "\"attribute_source\": \"infores:cohd\", "
                    "\"attribute_type_id\": \"EDAM:data_0954\", "
                    "\"attributes\": "
                    "["
                    "{"
                    "\"attribute_source\": \"infores:omop-ohdsi\", "
                    "\"attribute_type_id\": \"EDAM:data_1087\", "
                    "\"original_attribute_name\": \"concept_id\", "
                    "\"value\": \"OMOP:77698\", "
                    "\"value_type_id\": \"EDAM:data_1087\", "
                    "\"value_url\": \"https://athena.ohdsi.org/search-terms/terms/77698\""
                    "}, "
                    "{"
                    "\"attribute_source\": \"infores:omop-ohdsi\", "
                    "\"attribute_type_id\": \"EDAM:data_2339\", "
                    "\"original_attribute_name\": \"concept_name\", "
                    "\"value\": \"Sprain, coracoclavicular ligament\", "
                    "\"value_type_id\": \"EDAM:data_2339\"}, "
                    "{"
                    "\"attribute_source\": \"infores:omop-ohdsi\", "
                    "\"attribute_type_id\": \"EDAM:data_0967\", "
                    "\"original_attribute_name\": \"domain\", "
                    "\"value\": \"Condition\", "
                    "\"value_type_id\": \"EDAM:data_0967\""
                    "}"
                    "], "
                    "\"original_attribute_name\": \"Database cross-mapping\", "
                    "\"value\": \"(OMOP:2313993)-[OMOP Map]-(CPT:93976)\", "
                    "\"value_type_id\": \"EDAM:data_0954\""
                    "}"
                ]
             },
            #
            # Captured node contents
            [
                {
                    "id": "UMLS:C0160047",
                    "name": "Sprain, coracoclavicular ligament",
                    "category": ["biolink:Disease"],
                }
            ],
            # Captured edge contents - n/a
            None
        ),
    ],
    #
)
def test_transform_cohd_nodes(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_cohd_node(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS
    )


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - Missing fields (all in fact!)
            {},
            None,
            None
        ),
        (   # Query 1 - A complete record
            {
                "subject": "PUBCHEM.COMPOUND:2083",
                "predicate": "biolink:positively_correlated_with",
                "object": "MONDO:0007079",
                "primary_knowledge_source": "infores:cohd",
            },
            # Captured node contents
            None,
            # Captured edge contents
            {
                "category": ["biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation"],
                "subject": "PUBCHEM.COMPOUND:2083",
                "subject_feature_name": "AlbuterolRx",
                "predicate": "biolink:positively_correlated_with",
                "object": "MONDO:0007079",
                "object_feature_name": "AlcoholDependenceDx",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:cohd"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            }
        ),
        (  # Query 2 - A complete record with different qualifiers
            {
                "subject": "NCBIGene:3105",
                "predicate": "biolink:correlated_with",
                "object": "NCBITaxon:12092",
                "primary_knowledge_source": "infores:cohd",
                "attributes": [
                    "{\"attribute_type_id\": \"biolink:has_supporting_study_result\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/ICEES\"}", "{\"attribute_type_id\": \"terms_and_conditions_of_use\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES-and-ICEES-KG-Terms-and-Conditions-of-Use\"}", "{\"attribute_type_id\": \"subject_feature_name\", \"value\": \"A*02:01\"}", "{\"attribute_type_id\": \"object_feature_name\", \"value\": \"Anti_HAV\"}", "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Augmentin_DILI_patient_v4_binned_deidentified|dili|binned|2024_03_15_17_08_01\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 8.340160221149161e-08}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 2}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 0.9999999582991997}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 228.0}]}"
                ]
            },
            # Captured node contents
            None,
            # Captured edge contents
            {
                "category": ["biolink:VariantToDiseaseAssociation"],
                "subject": "NCBIGene:3105",
                "predicate": "biolink:correlated_with",
                "object": "NCBITaxon:12092",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:cohd"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            }
        ),
        (   # Query 3 - A complete record with different qualifiers
            {
                "subject": "NCBIGene:3105",
                "predicate": "biolink:correlated_with",
                "object": "UMLS:C4049590",
                "primary_knowledge_source": "infores:cohd",
            },
            # Captured node contents
            None,
            # Captured edge contents
            {
                "category": ["biolink:CausalGeneToDiseaseAssociation"],
                "subject": "NCBIGene:3105",
                "predicate": "biolink:correlated_with",
                "object": "UMLS:C4049590",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:cohd"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            }
        )

    ],
)
def test_transform_cohd_edges(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    validate_transform_result(
        result=transform_cohd_edge(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        edge_test_slots=CORE_ASSOCIATION_TEST_SLOTS
    )
