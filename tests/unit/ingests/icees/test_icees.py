import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.icees.icees import (
    transform_icees_node,
    transform_icees_edge,
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ["id", "name", "category", "xref"]

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "subject_context_qualifier",
    "predicate",
    "object",
    "object_context_qualifier",
    "has_supporting_studies",
    "sources",
    "knowledge_level",
    "agent_type",
]


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
        (  # Query 1 - A complete node record
            {
                "id": "PUBCHEM.COMPOUND:2083",
                "name": "Salbutamol",
                "category": [
                    "biolink:ChemicalEntityOrGeneOrGeneProduct",
                    "biolink:PhysicalEssence",
                    "biolink:ChemicalEntityOrProteinOrPolypeptide",
                    "biolink:PhysicalEssenceOrOccurrent",
                    "biolink:ChemicalExposure",
                    "biolink:ChemicalEntity",
                    "biolink:SmallMolecule",
                    "biolink:ChemicalOrDrugOrTreatment",
                    "biolink:MolecularEntity",
                    "biolink:Drug",
                    "biolink:NamedThing"
                ],
                "equivalent_identifiers": [
                    "PUBCHEM.COMPOUND:2083",
                    "CHEMBL.COMPOUND:CHEMBL714",
                    "CHEBI:2549",
                    "DRUGBANK:DB01001",
                    "DrugCentral:105"
                ]
            },
            # Captured node contents
            [
                {
                    "id": "PUBCHEM.COMPOUND:2083",
                    "name": "Salbutamol",
                    "category": ["biolink:Drug"],
                    "xref": [
                        "PUBCHEM.COMPOUND:2083",
                        "CHEMBL.COMPOUND:CHEMBL714",
                        "CHEBI:2549",
                        "DRUGBANK:DB01001",
                        "DrugCentral:105"
                    ]
                }
            ],
            # Captured edge contents - n/a
            None
        ),
        (  # Query 2- Another complete node record
            {
                "id": "MONDO:0004979",
                "name": "asthma",
                "category": [
                    "biolink:Disease",
                    "biolink:ThingWithTaxon",
                    "biolink:BiologicalEntity",
                    "biolink:NamedThing",
                    "biolink:DiseaseOrPhenotypicFeature"
                ],
                "equivalent_identifiers": [
                    "MONDO:0004979",
                    "DOID:2841",
                    "EFO:0000270"
                ]
            },
            #
            # Captured node contents
            [
                {
                    "id": "MONDO:0004979",
                    "name": "asthma",
                    "category": ["biolink:Disease"],
                    "xref": [
                        "MONDO:0004979",
                        "DOID:2841",
                        "EFO:0000270"
                    ]
                }
            ],
            # Captured edge contents - n/a
            None
        )
    ],
)
def test_transform_icees_nodes(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_icees_node(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS
    )


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - Missing fields (all of them in fact!)
                {},
                None,
                None,
        ),
        (  # Query 1 - A complete record
                {
                    "subject": "PUBCHEM.COMPOUND:2083",
                    "predicate": "biolink:positively_correlated_with",
                    "object": "MONDO:0007079",
                    "primary_knowledge_source": "infores:icees-kg",
                    "attributes": [
                        # These are the attributes in the original representative source data
                        # are ignored in the data parse since they are DRY documented in the RIG?
                        "{\"attribute_type_id\": \"biolink:has_supporting_study_result\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/ICEES\"}",
                        "{\"attribute_type_id\": \"terms_and_conditions_of_use\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES-and-ICEES-KG-Terms-and-Conditions-of-Use\"}",

                        # These attributes are tentatively captured as subject and
                        # object context qualifiers (until further discussion suggests otherwise)
                        "{\"attribute_type_id\": \"subject_feature_name\", \"value\": \"AlbuterolRx\"}",
                        "{\"attribute_type_id\": \"object_feature_name\", \"value\": \"AlcoholDependenceDx\"}",

                        # There are many additional cohorts in the original data from which
                        # this sample is drawn, but...we are just testing things here!
                        # We also don't yet capture any metadata about these cohorts thus
                        # we do not yet record anything about their statistical relationship to the Association
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 26.38523077566414}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 2.7967079822744063e-07}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 4753.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 3.226188583240579}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 2.8581244515361156e-06}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.1713014352915974}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.6996904681742875, 1.6429124024089072]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2013_v5_binned_deidentified|asthma|2013|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 961.1556369070855}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 4.986523229006498e-211}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 158671.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 17.74280067875692}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 4.494353716947507e-97}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.875979838082306}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [2.6254515014378037, 3.126508174726808]}]}",
                    ]
                },
                # Captured node contents
                None,
                # Captured edge contents
                {
                    "category": ["biolink:NamedThingAssociatedWithLikelihoodOfNamedThingAssociation"],
                    "subject": "PUBCHEM.COMPOUND:2083",
                    "subject_context_qualifier": "AlbuterolRx",
                    "predicate": "biolink:positively_correlated_with",
                    "object": "MONDO:0007079",
                    "object_context_qualifier": "AlcoholDependenceDx",
                    "has_supporting_studies": [
                        "PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22",
                        "Asthma_UNC_EPR_patient_2013_v5_binned_deidentified|asthma|2013|2024_03_19_10_34_15"
                    ],
                    "sources": [
                        {
                            "resource_role": "primary_knowledge_source",
                            "resource_id": "infores:icees-kg"
                        }
                    ],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.not_provided
                }
        ),
    ],
)
def test_transform_icees_edges(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict],
):
    validate_transform_result(
        result=transform_icees_edge(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        association_test_slots=ASSOCIATION_TEST_SLOTS,
    )
