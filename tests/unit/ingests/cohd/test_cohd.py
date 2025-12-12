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
        (   # Query 1 - A SNOMEDCT record
            {
                "subject": "SNOMEDCT:60108003",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73540",
                "score": 5.861265273152199,
                # TODO: The CODH "attributes" data is (thousands of characters)
                #       exceedingly long at the moment, and will thus
                #       require future iterations of review and parsing.
                "attributes": [
                    "{"
                        "\"attribute_source\": \"infores:cohd\", "
                        "\"attribute_type_id\": \"biolink:knowledge_level\", "
                        "\"value\": \"statistical_association\""
                    "}",
                    "{"
                        "\"attribute_source\": \"infores:cohd\", "
                        "\"attribute_type_id\": \"biolink:agent_type\", "
                        "\"value\": \"data_analysis_pipeline\""
                    "}",
                    "{"
                        "\"attribute_source\": \"infores:cohd\", "
                        "\"attribute_type_id\": \"biolink:has_supporting_study_result\", "
                        "\"description\": \"A study result describing the initial count of concepts\", "
                        "\"value\": \"SNOMEDCT:60108003: 11; CPT:73540: 927; pair: 12\", "
                        "\"value_type_id\": \"biolink:ConceptCountAnalysisResult\", "
                        "\"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", "
                        "\"attributes\": ["
                            "{"
                                "\"attribute_type_id\": \"biolink:concept_pair_count\", "
                                "\"original_attribute_name\": \"concept_pair_count\", "
                                "\"value\": 12, "
                                "\"value_type_id\": \"EDAM:data_0006\", "
                                "\"attribute_source\": \"infores:cohd\", "
                                "\"description\": \"Observed concept count between the pair of subject and object nodes\""
                            "}, "
                            "{"
                                "\"attribute_type_id\": \"biolink:concept_count_subject\", "
                                "\"original_attribute_name\": \"concept_count_subject\", "
                                "\"value\": 11, "
                                "\"value_type_id\": \"EDAM:data_0006\", "
                                "\"attribute_source\": \"infores:cohd\", "
                                "\"description\": \"Observed concept count of the subject node (SNOMEDCT:60108003)\""
                            "}, "
                            "{"
                                "\"attribute_type_id\": \"biolink:concept_count_object\", "
                                "\"original_attribute_name\": \"concept_count_object\", "
                                "\"value\": 927, "
                                "\"value_type_id\": \"EDAM:data_0006\", "
                                "\"attribute_source\": \"infores:cohd\", "
                                "\"description\": \"Observed concept count of the object node (CPT:73540)\""
                            "}, "
                            "{"
                                "\"attribute_type_id\": \"biolink:dataset_count\", "
                                "\"original_attribute_name\": \"patient_count\", "
                                "\"value\": 1790431, "
                                "\"value_type_id\": \"EDAM:data_0006\", "
                                "\"attribute_source\": \"infores:cohd\", "
                                "\"description\": \"Number of patients in the COHD dataset\""
                            "}, "
                            "{"
                                "\"attribute_type_id\": \"biolink:supporting_data_set\", "
                                "\"original_attribute_name\": \"dataset_id\", "
                                "\"value\": \"COHD:dataset_1\", "
                                "\"value_type_id\": \"EDAM:data_1048\", "
                                "\"attribute_source\": \"infores:cohd\", "
                                "\"description\": \"Dataset ID within COHD\""
                            "}, "
                            "{"
                                "\"attribute_type_id\": \"biolink:knowledge_level\", "
                                "\"value\": \"statistical_association\", "
                                "\"attribute_source\": \"infores:cohd\""
                            "}, "
                            "{"
                                "\"attribute_type_id\": \"biolink:agent_type\", "
                                "\"value\": \"data_analysis_pipeline\", "
                                "\"attribute_source\": \"infores:cohd\""
                            "}"
                        "]"
                    "}",
                    "{"
                        "\"attribute_source\": \"infores:cohd\", "
                        "\"attribute_type_id\": \"biolink:has_supporting_study_result\", "
                        "\"description\": \"A study result describing a chi-squared analysis on a single pair of concepts\", "
                        "\"value\": \"p-value: 1.00e-12; Bonferonni p-value: 1.00e-12\", "
                        "\"value_type_id\": \"biolink:ChiSquaredAnalysisResult\", "
                        "\"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", "
                        "\"attributes\": ["
                            "{"
                                "\"attribute_type_id\": \"biolink:unadjusted_p_value\", "
                                "\"original_attribute_name\": \"p-value\", \"value\": 1e-12, \"value_type_id\": \"EDAM:data_1669\", \"attribute_source\": \"infores:cohd\", \"value_url\": \"http://edamontology.org/data_1669\", \"description\": \"Chi-square p-value, unadjusted.\"}, {\"attribute_type_id\": \"biolink:bonferonni_adjusted_p_value\", \"original_attribute_name\": \"p-value adjusted\", \"value\": 1e-12, \"value_type_id\": \"EDAM:data_1669\", \"attribute_source\": \"infores:cohd\", \"value_url\": \"http://edamontology.org/data_1669\", \"description\": \"Chi-square p-value, Bonferonni adjusted by number of pairs of concepts.\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing an observed-expected frequency analysis on a single pair of concepts\", \"value\": \"7.653 [5.861, 8.387]\", \"value_type_id\": \"biolink:ObservedExpectedFrequencyAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:expected_count\", \"original_attribute_name\": \"expected_count\", \"value\": 0.005695276723872632, \"value_type_id\": \"EDAM:operation_3438\", \"attribute_source\": \"infores:cohd\", \"description\": \"Calculated expected count of concept pair.\"}, {\"attribute_type_id\": \"biolink:ln_ratio\", \"original_attribute_name\": \"ln_ratio\", \"value\": 7.653024742380254, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed-expected frequency ratio.\"}, {\"attribute_type_id\": \"biolink:ln_ratio_confidence_interval\", \"original_attribute_name\": \"ln_ratio_confidence_interval\", \"value\": [5.861265273152199, 8.386993917460455], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed-expected frequency ratio 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a relative frequency analysis on a single pair of concepts\", \"value\": \"Relative to SNOMEDCT:60108003: 1.091 [0.200, 5.500]; Relative to CPT:73540: 0.013 [0.004, 0.026]\", \"value_type_id\": \"biolink:RelativeFrequencyAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:relative_frequency_subject\", \"original_attribute_name\": \"relative_frequency_subject\", \"value\": 1.0909090909090908, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency, relative to the subject node (SNOMEDCT:60108003).\"}, {\"attribute_type_id\": \"biolink:relative_frequency_subject_confidence_interval\", \"original_attribute_name\": \"relative_freq_subject_confidence_interval\", \"value\": [0.2, 5.5], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency (subject) 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:relative_frequency_object\", \"original_attribute_name\": \"relative_frequency_object\", \"value\": 0.012944983818770227, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency, relative to the object node (CPT:73540).\"}, {\"attribute_type_id\": \"biolink:relative_frequency_object_confidence_interval\", \"original_attribute_name\": \"relative_freq_object_confidence_interval\", \"value\": [0.003976143141153081, 0.02588235294117647], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency (object) 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a log-odds analysis on a single pair of concepts\", \"value\": \"999.000 [999.000, 999.000]\", \"value_type_id\": \"biolink:LogOddsAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:log_odds_ratio\", \"original_attribute_name\": \"log_odds\", \"value\": 999, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Natural logarithm of the odds-ratio\"}, {\"attribute_type_id\": \"biolink:log_odds_ratio_95_ci\", \"original_attribute_name\": \"log_odds_ci\", \"value\": [999, 999], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Log-odds 95% confidence interval\"}, {\"attribute_type_id\": \"biolink:total_sample_size\", \"original_attribute_name\": \"concept_pair_count\", \"value\": 12, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count between the pair of subject and object nodes\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}"
                ],
                "sources": [
                    {
                        "resource_id": "infores:columbia-cdw-ehr-data",
                        "resource_role": "supporting_data_source"
                    },
                    {
                        "resource_id": "infores:cohd",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": [
                            "infores:columbia-cdw-ehr-data"
                        ]
                    }
                ]
             },
            # Captured node contents
            None,
            # Captured edge contents
            {
                # A very general edge category for now: see ingest transform commentary
                "category": ["biolink:Association"],
                "provided_by": ["infores:cohd"],
                "subject": "SNOMEDCT:60108003",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73540",
                "score": 5.861265273152199,
                "sources": [
                    {
                        "resource_id": "infores:columbia-cdw-ehr-data",
                        "resource_role": "supporting_data_source"
                    },
                    {
                        "resource_id": "infores:cohd",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": [
                            "infores:columbia-cdw-ehr-data"
                        ]
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.data_analysis_pipeline
            }
        ),
        (  # Query 2 - A UMLS record
            {
                "subject": "UMLS:C0160047",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73030",
                "score": 3.3254987202521264,
                "attributes": [
                    "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\"}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\"}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing the initial count of concepts\", \"value\": \"UMLS:C0160047: 11; CPT:73030: 29261; pair: 16\", \"value_type_id\": \"biolink:ConceptCountAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:concept_pair_count\", \"original_attribute_name\": \"concept_pair_count\", \"value\": 16, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count between the pair of subject and object nodes\"}, {\"attribute_type_id\": \"biolink:concept_count_subject\", \"original_attribute_name\": \"concept_count_subject\", \"value\": 11, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count of the subject node (UMLS:C0160047)\"}, {\"attribute_type_id\": \"biolink:concept_count_object\", \"original_attribute_name\": \"concept_count_object\", \"value\": 29261, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count of the object node (CPT:73030)\"}, {\"attribute_type_id\": \"biolink:dataset_count\", \"original_attribute_name\": \"patient_count\", \"value\": 1790431, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Number of patients in the COHD dataset\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a chi-squared analysis on a single pair of concepts\", \"value\": \"p-value: 1.00e-12; Bonferonni p-value: 1.00e-12\", \"value_type_id\": \"biolink:ChiSquaredAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:unadjusted_p_value\", \"original_attribute_name\": \"p-value\", \"value\": 1e-12, \"value_type_id\": \"EDAM:data_1669\", \"attribute_source\": \"infores:cohd\", \"value_url\": \"http://edamontology.org/data_1669\", \"description\": \"Chi-square p-value, unadjusted.\"}, {\"attribute_type_id\": \"biolink:bonferonni_adjusted_p_value\", \"original_attribute_name\": \"p-value adjusted\", \"value\": 1e-12, \"value_type_id\": \"EDAM:data_1669\", \"attribute_source\": \"infores:cohd\", \"value_url\": \"http://edamontology.org/data_1669\", \"description\": \"Chi-square p-value, Bonferonni adjusted by number of pairs of concepts.\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing an observed-expected frequency analysis on a single pair of concepts\", \"value\": \"4.489 [3.325, 5.150]\", \"value_type_id\": \"biolink:ObservedExpectedFrequencyAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:expected_count\", \"original_attribute_name\": \"expected_count\", \"value\": 0.17977291501320072, \"value_type_id\": \"EDAM:operation_3438\", \"attribute_source\": \"infores:cohd\", \"description\": \"Calculated expected count of concept pair.\"}, {\"attribute_type_id\": \"biolink:ln_ratio\", \"original_attribute_name\": \"ln_ratio\", \"value\": 4.488649530057807, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed-expected frequency ratio.\"}, {\"attribute_type_id\": \"biolink:ln_ratio_confidence_interval\", \"original_attribute_name\": \"ln_ratio_confidence_interval\", \"value\": [3.3254987202521264, 5.150048012303173], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed-expected frequency ratio 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a relative frequency analysis on a single pair of concepts\", \"value\": \"Relative to UMLS:C0160047: 1.455 [0.350, 6.750]; Relative to CPT:73030: 0.001 [0.000, 0.001]\", \"value_type_id\": \"biolink:RelativeFrequencyAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:relative_frequency_subject\", \"original_attribute_name\": \"relative_frequency_subject\", \"value\": 1.4545454545454546, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency, relative to the subject node (UMLS:C0160047).\"}, {\"attribute_type_id\": \"biolink:relative_frequency_subject_confidence_interval\", \"original_attribute_name\": \"relative_freq_subject_confidence_interval\", \"value\": [0.35, 6.75], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency (subject) 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:relative_frequency_object\", \"original_attribute_name\": \"relative_frequency_object\", \"value\": 0.000546802911725505, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency, relative to the object node (CPT:73030).\"}, {\"attribute_type_id\": \"biolink:relative_frequency_object_confidence_interval\", \"original_attribute_name\": \"relative_freq_object_confidence_interval\", \"value\": [0.00023566643100023565, 0.000936816904340585], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency (object) 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a log-odds analysis on a single pair of concepts\", \"value\": \"999.000 [999.000, 999.000]\", \"value_type_id\": \"biolink:LogOddsAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:log_odds_ratio\", \"original_attribute_name\": \"log_odds\", \"value\": 999, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Natural logarithm of the odds-ratio\"}, {\"attribute_type_id\": \"biolink:log_odds_ratio_95_ci\", \"original_attribute_name\": \"log_odds_ci\", \"value\": [999, 999], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Log-odds 95% confidence interval\"}, {\"attribute_type_id\": \"biolink:total_sample_size\", \"original_attribute_name\": \"concept_pair_count\", \"value\": 16, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count between the pair of subject and object nodes\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}"
                ],
                "sources": [
                    {
                        "resource_id": "infores:columbia-cdw-ehr-data",
                        "resource_role": "supporting_data_source"
                    },
                    {
                        "resource_id": "infores:cohd",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": ["infores:columbia-cdw-ehr-data"]
                    }
                ]
            },
            # Captured node contents
            None,
            # Captured edge contents
            {
                # A very general edge category for now: see ingest transform commentary
                "category": ["biolink:Association"],
                "subject": "UMLS:C0160047",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73030",
                "score": 3.3254987202521264,
                "sources": [
                    {
                        "resource_id": "infores:columbia-cdw-ehr-data",
                        "resource_role": "supporting_data_source"
                    },
                    {
                        "resource_id": "infores:cohd",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": [
                            "infores:columbia-cdw-ehr-data"
                        ]
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.data_analysis_pipeline
            }
        ),
        (   # Query 3 - A MONDO subject record
            {
                "subject": "MONDO:0000888",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:99232",
                "score": 2.5595194408846957,
                "attributes": [
                    "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\"}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\"}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing the initial count of concepts\", \"value\": \"MONDO:0000888: 11; CPT:99232: 75532; pair: 19\", \"value_type_id\": \"biolink:ConceptCountAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:concept_pair_count\", \"original_attribute_name\": \"concept_pair_count\", \"value\": 19, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count between the pair of subject and object nodes\"}, {\"attribute_type_id\": \"biolink:concept_count_subject\", \"original_attribute_name\": \"concept_count_subject\", \"value\": 11, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count of the subject node (MONDO:0000888)\"}, {\"attribute_type_id\": \"biolink:concept_count_object\", \"original_attribute_name\": \"concept_count_object\", \"value\": 75532, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count of the object node (CPT:99232)\"}, {\"attribute_type_id\": \"biolink:dataset_count\", \"original_attribute_name\": \"patient_count\", \"value\": 1790431, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Number of patients in the COHD dataset\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a chi-squared analysis on a single pair of concepts\", \"value\": \"p-value: 1.00e-12; Bonferonni p-value: 1.00e-12\", \"value_type_id\": \"biolink:ChiSquaredAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:unadjusted_p_value\", \"original_attribute_name\": \"p-value\", \"value\": 1e-12, \"value_type_id\": \"EDAM:data_1669\", \"attribute_source\": \"infores:cohd\", \"value_url\": \"http://edamontology.org/data_1669\", \"description\": \"Chi-square p-value, unadjusted.\"}, {\"attribute_type_id\": \"biolink:bonferonni_adjusted_p_value\", \"original_attribute_name\": \"p-value adjusted\", \"value\": 1e-12, \"value_type_id\": \"EDAM:data_1669\", \"attribute_source\": \"infores:cohd\", \"value_url\": \"http://edamontology.org/data_1669\", \"description\": \"Chi-square p-value, Bonferonni adjusted by number of pairs of concepts.\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing an observed-expected frequency analysis on a single pair of concepts\", \"value\": \"3.712 [2.560, 4.323]\", \"value_type_id\": \"biolink:ObservedExpectedFrequencyAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:expected_count\", \"original_attribute_name\": \"expected_count\", \"value\": 0.4640513932120255, \"value_type_id\": \"EDAM:operation_3438\", \"attribute_source\": \"infores:cohd\", \"description\": \"Calculated expected count of concept pair.\"}, {\"attribute_type_id\": \"biolink:ln_ratio\", \"original_attribute_name\": \"ln_ratio\", \"value\": 3.712198950823081, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed-expected frequency ratio.\"}, {\"attribute_type_id\": \"biolink:ln_ratio_confidence_interval\", \"original_attribute_name\": \"ln_ratio_confidence_interval\", \"value\": [2.5595194408846957, 4.323108033146054], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed-expected frequency ratio 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a relative frequency analysis on a single pair of concepts\", \"value\": \"Relative to MONDO:0000888: 1.727 [0.450, 7.750]; Relative to CPT:99232: 0.000 [0.000, 0.000]\", \"value_type_id\": \"biolink:RelativeFrequencyAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:relative_frequency_subject\", \"original_attribute_name\": \"relative_frequency_subject\", \"value\": 1.7272727272727273, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency, relative to the subject node (MONDO:0000888).\"}, {\"attribute_type_id\": \"biolink:relative_frequency_subject_confidence_interval\", \"original_attribute_name\": \"relative_freq_subject_confidence_interval\", \"value\": [0.45, 7.75], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency (subject) 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:relative_frequency_object\", \"original_attribute_name\": \"relative_frequency_object\", \"value\": 0.00025154901233914105, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency, relative to the object node (CPT:99232).\"}, {\"attribute_type_id\": \"biolink:relative_frequency_object_confidence_interval\", \"original_attribute_name\": \"relative_freq_object_confidence_interval\", \"value\": [0.00011804672026862187, 0.00041430003341129303], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Relative frequency (object) 99.0% confidence interval\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}", "{\"attribute_source\": \"infores:cohd\", \"attribute_type_id\": \"biolink:has_supporting_study_result\", \"description\": \"A study result describing a log-odds analysis on a single pair of concepts\", \"value\": \"999.000 [999.000, 999.000]\", \"value_type_id\": \"biolink:LogOddsAnalysisResult\", \"value_url\": \"https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP\", \"attributes\": [{\"attribute_type_id\": \"biolink:log_odds_ratio\", \"original_attribute_name\": \"log_odds\", \"value\": 999, \"value_type_id\": \"EDAM:data_1772\", \"attribute_source\": \"infores:cohd\", \"description\": \"Natural logarithm of the odds-ratio\"}, {\"attribute_type_id\": \"biolink:log_odds_ratio_95_ci\", \"original_attribute_name\": \"log_odds_ci\", \"value\": [999, 999], \"value_type_id\": \"EDAM:data_0951\", \"attribute_source\": \"infores:cohd\", \"description\": \"Log-odds 95% confidence interval\"}, {\"attribute_type_id\": \"biolink:total_sample_size\", \"original_attribute_name\": \"concept_pair_count\", \"value\": 19, \"value_type_id\": \"EDAM:data_0006\", \"attribute_source\": \"infores:cohd\", \"description\": \"Observed concept count between the pair of subject and object nodes\"}, {\"attribute_type_id\": \"biolink:supporting_data_set\", \"original_attribute_name\": \"dataset_id\", \"value\": \"COHD:dataset_1\", \"value_type_id\": \"EDAM:data_1048\", \"attribute_source\": \"infores:cohd\", \"description\": \"Dataset ID within COHD\"}, {\"attribute_type_id\": \"biolink:knowledge_level\", \"value\": \"statistical_association\", \"attribute_source\": \"infores:cohd\"}, {\"attribute_type_id\": \"biolink:agent_type\", \"value\": \"data_analysis_pipeline\", \"attribute_source\": \"infores:cohd\"}]}"
                ],
                "sources": [
                    {
                        "resource_id": "infores:columbia-cdw-ehr-data",
                        "resource_role": "supporting_data_source"
                    },
                    {
                        "resource_id": "infores:cohd",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": [
                            "infores:columbia-cdw-ehr-data"
                        ]
                    }
                ]
            },
            # Captured node contents
            None,
            # Captured edge contents
            {
                # A very general edge category for now: see ingest transform commentary
                "category": ["biolink:Association"],
                "subject": "MONDO:0000888",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:99232",
                "score": 2.5595194408846957,
                "sources": [
                    {
                        "resource_id": "infores:columbia-cdw-ehr-data",
                        "resource_role": "supporting_data_source"
                    },
                    {
                        "resource_id": "infores:cohd",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": [
                            "infores:columbia-cdw-ehr-data"
                        ]
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.data_analysis_pipeline
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
