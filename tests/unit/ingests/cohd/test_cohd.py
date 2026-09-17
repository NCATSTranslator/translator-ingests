from typing import Any

import koza
import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    ConceptCountAnalysisResult,
    KnowledgeLevelEnum,
    ResourceRoleEnum,
    RetrievalSource,
    Study,
)
from koza.io.writer.writer import KozaWriter
from koza.transform import Mappings

from tests.unit.ingests import MockKozaTransform, MockKozaWriter, validate_transform_result
from tests.unit.ingests.cohd.sample_cohd_attribute_data import (
    SAMPLE_COHD_CPT_NODE_ATTRIBUTES,
    SAMPLE_COHD_MONDO_EDGE_ATTRIBUTES,
    SAMPLE_COHD_SNOMEDCT_EDGE_ATTRIBUTES,
    SAMPLE_COHD_SNOMEDCT_NODE_ATTRIBUTES,
    SAMPLE_COHD_UMLS_EDGE_ATTRIBUTES,
    SAMPLE_COHD_UMLS_NODE_ATTRIBUTES,
    SAMPLE_EDGE_ATTRIBUTE_STR,
    SAMPLE_NODE_ATTRIBUTE_STR,
)
from translator_ingest.ingests.cohd.cohd import transform_cohd_edge, transform_cohd_node
from translator_ingest.ingests.cohd.cohd_util import get_cohd_supporting_study, parse_attributes, parse_node_properties


@pytest.mark.parametrize(
    "attribute_list,expected_result",
    [
        (
            [SAMPLE_NODE_ATTRIBUTE_STR],
            [
                {
                    "attribute_source": "infores:cohd",
                    "attribute_type_id": "EDAM:data_0954",
                    "attributes": [
                        {
                            "attribute_source": "infores:omop-ohdsi",
                            "attribute_type_id": "EDAM:data_1087",
                            "original_attribute_name": "concept_id",
                            "value": "OMOP:77661",
                            "value_type_id": "EDAM:data_1087",
                            "value_url": "https://athena.ohdsi.org/search-terms/terms/77661"
                        }
                    ]
                }
            ]
        ),
        (
            [SAMPLE_EDGE_ATTRIBUTE_STR],
            [
                {
                    "attribute_source": "infores:cohd",
                    "attribute_type_id": "biolink:has_supporting_study_result",
                    "description": "A study result describing the initial count of concepts",
                    "value": "SNOMEDCT:60108003: 11; CPT:73540: 927; pair: 12",
                    "value_type_id": "biolink:ConceptCountAnalysisResult",
                    "value_url": "https://github.com/NCATSTranslator/Translator-All/wiki/COHD-KP",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:concept_pair_count",
                            "original_attribute_name": "concept_pair_count",
                            "value": 12, "value_type_id": "EDAM:data_0006",
                            "attribute_source": "infores:cohd",
                            "description": "Observed concept count between the pair of subject and object nodes"
                        },
                        {
                            "attribute_type_id": "biolink:concept_count_subject",
                            "original_attribute_name": "concept_count_subject",
                            "value": 11,
                            "value_type_id": "EDAM:data_0006",
                            "attribute_source": "infores:cohd",
                            "description": "Observed concept count of the subject node (SNOMEDCT:60108003)"
                        },
                        {
                            "attribute_type_id": "biolink:concept_count_object",
                            "original_attribute_name": "concept_count_object",
                            "value": 927,
                            "value_type_id": "EDAM:data_0006",
                            "attribute_source": "infores:cohd",
                            "description": "Observed concept count of the object node (CPT:73540)"
                        },
                        {
                            "attribute_type_id": "biolink:dataset_count",
                            "original_attribute_name": "patient_count",
                            "value": 1790431,
                            "value_type_id": "EDAM:data_0006",
                            "attribute_source": "infores:cohd",
                            "description": "Number of patients in the COHD dataset"
                        },
                        {
                            "attribute_type_id": "biolink:supporting_data_set",
                            "original_attribute_name": "dataset_id",
                            "value": "COHD:dataset_1",
                            "value_type_id": "EDAM:data_1048",
                            "attribute_source": "infores:cohd",
                            "description": "Dataset ID within COHD"
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association",
                            "attribute_source": "infores:cohd"
                        },
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "data_analysis_pipeline",
                            "attribute_source": "infores:cohd"
                        }
                    ]
                }
            ]
        )
    ]
)
def test_parse_attributes(attribute_list: list[str], expected_result:list[dict[str, Any]]):
    result:list[dict[str, Any]] =  parse_attributes(attribute_list)
    assert result == expected_result, "Python attribute parsing result parsing doesn't match expectations"

def test_parse_node_properties():
    result = parse_node_properties([SAMPLE_NODE_ATTRIBUTE_STR])
    assert result == {"xref": ["https://athena.ohdsi.org/search-terms/terms/77661"]}


def test_get_cohd_supporting_study():
    cohd_study_data: dict[str, Study] | None = \
        get_cohd_supporting_study(
            edge_id="fake_test_edge",
            attribute_list=[SAMPLE_EDGE_ATTRIBUTE_STR]
        )
    assert cohd_study_data is not None
    assert "COHD:dataset_1" in cohd_study_data
    cohd_study = cohd_study_data["COHD:dataset_1"]
    assert isinstance(cohd_study, Study)
    assert cohd_study.id == "COHD:dataset_1"
    study_results = cohd_study.has_study_results
    assert study_results
    result = study_results[0]
    assert isinstance(result, ConceptCountAnalysisResult)
    assert result.id == "fake_test_edge"
    assert "biolink:ConceptCountAnalysisResult" in result.category
    assert result.name == "SNOMEDCT:60108003: 11; CPT:73540: 927; pair: 12"


@pytest.fixture(scope="module")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = {}
    return MockKozaTransform(extra_fields={}, writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
    "name",
    "category",
    "xref"
)

# list of slots whose values are
# to be checked in a result edge
CORE_ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "object",
    "has_confidence_score",
    "has_supporting_studies",
    "sources",
    "knowledge_level",
    "agent_type"
)

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 0 - A complete node record
            {
                "id": "SNOMEDCT:60108003",
                "name": "Congenital dislocation of one hip with subluxation of other",
                "categories": [
                    "biolink:DiseaseOrPhenotypicFeature"
                ],
                "attributes": SAMPLE_COHD_SNOMEDCT_NODE_ATTRIBUTES
            },
            # Captured node contents
            [
                {
                    "id": "SNOMEDCT:60108003",
                    "name": "Congenital dislocation of one hip with subluxation of other",
                    "category": ["biolink:DiseaseOrPhenotypicFeature"],
                    "xref": ["https://athena.ohdsi.org/search-terms/terms/77661"]
                }
            ],
            # Captured edge contents - n/a
            None
        ),
        (   # Query 1- Another complete node record
            {
                "id": "CPT:73540",
                "name": "Radiologic examination, pelvis and hips, infant or child, minimum of 2 views",
                "categories": [
                    "biolink:Procedure"
                ],
                "attributes": SAMPLE_COHD_CPT_NODE_ATTRIBUTES
            },
            #
            # Captured node contents
            [
                {
                    "id": "CPT:73540",
                    "name": "Radiologic examination, pelvis and hips, infant or child, minimum of 2 views",
                    "category": ["biolink:Procedure"],
                    "xref": ["https://athena.ohdsi.org/search-terms/terms/2211477"]
                }
            ],
            # Captured edge contents - n/a
            None
        ),
        (   # Query 2- Another complete node record
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
                "attributes": SAMPLE_COHD_UMLS_NODE_ATTRIBUTES
            },
            #
            # Captured node contents
            [
                {
                    "id": "UMLS:C0160047",
                    "name": "Sprain, coracoclavicular ligament",
                    "category": ["biolink:Disease"],
                    "xref": ["https://athena.ohdsi.org/search-terms/terms/77698"]
                }
            ],
            # Captured edge contents - n/a
            None
        ),
    ],
)
def test_transform_cohd_nodes(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: list | None,
        result_edge: dict | None,
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
        (   # Query 0 - A SNOMEDCT record
            {
                "subject": "SNOMEDCT:60108003",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73540",
                "score": 5.861265273152199,
                "attributes": SAMPLE_COHD_SNOMEDCT_EDGE_ATTRIBUTES,
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
                "subject": "SNOMEDCT:60108003",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73540",
                "has_confidence_score": 5.861265273152199,
                "has_supporting_studies": [
                    {
                        "COHD:dataset_1": {
                            "id": "COHD:dataset_1",
                            "category": ["biolink:Study"],
                            "has_study_results": [
                                {
                                    "category": ["biolink:ConceptCountAnalysisResult"],
                                    "name": "SNOMEDCT:60108003: 11; CPT:73540: 927; pair: 12"
                                },
                                {
                                    "category": ["biolink:ChiSquaredAnalysisResult"],
                                    "name": "p-value: 1.00e-12; Bonferonni p-value: 1.00e-12"
                                },
                                {
                                    "category": ["biolink:ObservedExpectedFrequencyAnalysisResult"],
                                    "name": "7.653 [5.861, 8.387]"
                                },
                                {
                                    "category": ["biolink:RelativeFrequencyAnalysisResult"],
                                    "name": "Relative to SNOMEDCT:60108003: 1.091 [0.200, 5.500]; Relative to CPT:73540: 0.013 [0.004, 0.026]"
                                },
                                {
                                    "category": ["biolink:LogOddsAnalysisResult"],
                                    "name": "999.000 [999.000, 999.000]"
                                }
                            ]
                        }
                    }
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
                ],
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.data_analysis_pipeline
            }
        ),
        (  # Query 1 - A UMLS record
            {
                "subject": "UMLS:C0160047",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:73030",
                "score": 3.3254987202521264,
                "attributes": SAMPLE_COHD_UMLS_EDGE_ATTRIBUTES,
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
                "has_confidence_score": 3.3254987202521264,
                "has_supporting_studies": [
                    {
                        "COHD:dataset_1": {
                            "id": "COHD:dataset_1",
                            "category": ["biolink:Study"],
                            "has_study_results": [
                                {
                                    "category": ["biolink:ConceptCountAnalysisResult"],
                                    "name": "UMLS:C0160047: 11; CPT:73030: 29261; pair: 16"
                                },
                                {
                                    "category": ["biolink:ChiSquaredAnalysisResult"],
                                    "name": "p-value: 1.00e-12; Bonferonni p-value: 1.00e-12"
                                },
                                {
                                    "category": ["biolink:ObservedExpectedFrequencyAnalysisResult"],
                                    "name": "4.489 [3.325, 5.150]"
                                },
                                {
                                    "category": ["biolink:RelativeFrequencyAnalysisResult"],
                                    "name": "Relative to UMLS:C0160047: 1.455 [0.350, 6.750]; Relative to CPT:73030: 0.001 [0.000, 0.001]"
                                },
                                {
                                    "category": ["biolink:LogOddsAnalysisResult"],
                                    "name": "999.000 [999.000, 999.000]"
                                }
                            ]
                        }
                    }
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
                ],
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.data_analysis_pipeline
            }
        ),
        (   # Query 2 - A MONDO subject record
            {
                "subject": "MONDO:0000888",
                "predicate": "biolink:positively_correlated_with",
                "object": "CPT:99232",
                "score": 2.5595194408846957,
                "attributes": SAMPLE_COHD_MONDO_EDGE_ATTRIBUTES,
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
                "has_confidence_score": 2.5595194408846957,
                "has_supporting_studies": [
                    {
                        "COHD:dataset_1": {
                            "id": "COHD:dataset_1",
                            "category": ["biolink:Study"],
                            "has_study_results": [
                                {
                                    "category": ["biolink:ConceptCountAnalysisResult"],
                                    "name": "MONDO:0000888: 11; CPT:99232: 75532; pair: 19"
                                },
                                {
                                    "category": ["biolink:ChiSquaredAnalysisResult"],
                                    "name": "p-value: 1.00e-12; Bonferonni p-value: 1.00e-12"
                                },
                                {
                                    "category": ["biolink:ObservedExpectedFrequencyAnalysisResult"],
                                    "name": "3.712 [2.560, 4.323]"
                                },
                                {
                                    "category": ["biolink:RelativeFrequencyAnalysisResult"],
                                    "name": "Relative to MONDO:0000888: 1.727 [0.450, 7.750]; Relative to CPT:99232: 0.000 [0.000, 0.000]"
                                },
                                {
                                    "category": ["biolink:LogOddsAnalysisResult"],
                                    "name": "999.000 [999.000, 999.000]"
                                }
                            ]
                        }
                    }
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
        result_nodes: list | None,
        result_edge: dict | None
):
    validate_transform_result(
        result=transform_cohd_edge(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        edge_test_slots=CORE_ASSOCIATION_TEST_SLOTS
    )


# ===== PYDANTIC ROUNDTRIP TESTS =====

COHD_TEST_SOURCES = [
    RetrievalSource(
        id="infores:columbia-cdw-ehr-data",
        resource_id="infores:columbia-cdw-ehr-data",
        resource_role=ResourceRoleEnum.supporting_data_source,
    ),
    RetrievalSource(
        id="infores:cohd",
        resource_id="infores:cohd",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
        upstream_resource_ids=["infores:columbia-cdw-ehr-data"],
    ),
]

EDGE_FIXTURES = [
    {
        "association_class": Association,
        "params": {
            "id": "01dea5b9-eb36-4fdc-8c94-882fe12b2a15",
            "subject": "DOID:0111252",
            "predicate": "biolink:positively_correlated_with",
            "object": "CHEBI:41879",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.data_analysis_pipeline,
            "sources": COHD_TEST_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "c9cc9b4a-b778-4002-80d8-0c62501a93b2",
            "subject": "CHEBI:9150",
            "predicate": "biolink:negatively_correlated_with",
            "object": "MONDO:0008487",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.data_analysis_pipeline,
            "sources": COHD_TEST_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f"{f['association_class'].__name__}_{f['params']['predicate'].split(':')[-1]}",
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
