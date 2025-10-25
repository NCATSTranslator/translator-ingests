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
    "predicate",
    "object",
    "sources",
    "attributes",
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
                        "{\"attribute_type_id\": \"biolink:has_supporting_study_result\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/ICEES\"}",
                        "{\"attribute_type_id\": \"terms_and_conditions_of_use\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES-and-ICEES-KG-Terms-and-Conditions-of-Use\"}",
                        "{\"attribute_type_id\": \"subject_feature_name\", \"value\": \"AlbuterolRx\"}",
                        "{\"attribute_type_id\": \"object_feature_name\", \"value\": \"AlcoholDependenceDx\"}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 26.38523077566414}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 2.7967079822744063e-07}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 4753.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 3.226188583240579}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 2.8581244515361156e-06}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.1713014352915974}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.6996904681742875, 1.6429124024089072]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2019_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 36.23159136237876}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 1.752072081567904e-09}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 5450.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 2.9089116233205967}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 1.7025495780952487e-08}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.0677789986186366}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.7045466564671972, 1.431011340770076]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2013_v5_binned_deidentified|asthma|2013|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 961.1556369070855}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 4.986523229006498e-211}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 158671.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 17.74280067875692}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 4.494353716947507e-97}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.875979838082306}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [2.6254515014378037, 3.126508174726808]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2017_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 23.584831597412446}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 1.1952602176759147e-06}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 6078.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 2.714198932585082}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 7.83627627157672e-06}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 0.9984968573945092}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.579379337286243, 1.4176143775027754]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2012_v5_binned_deidentified|asthma|2012|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 851.2248875690603}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 3.9379793139447067e-187}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 158944.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 17.52045627538235}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 3.607307924685663e-83}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.86336912828456}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [2.5989351924556603, 3.1278030641134595]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2010_v5_binned_deidentified|asthma|2010|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 495.13370287908117}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 1.0884001530813013e-109}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 159331.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 9.951107227483488}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 1.5914959673132495e-47}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.2976838241235056}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [2.0481324124884335, 2.5472352357585777]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2016_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 30.101842372277037}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 4.0994157380884985e-08}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 5688.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 3.034598139597136}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 4.878977330976173e-07}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.110079007045195}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.693942946539894, 1.526215067550496]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2015_v5_binned_deidentified|asthma|2015|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 958.402095254854}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 1.978533960214989e-210}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 157920.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 4.704771263291241}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 2.2760579053650436e-148}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.5485771561895145}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [1.4407687874945196, 1.6563855248845094]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2016_v5_binned_deidentified|asthma|2016|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 2062.0761426833574}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 0.0}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 157412.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 6.9444313179957415}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 0.0}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.9379400891957366}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [1.8412165239089704, 2.034663654482503]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2021_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 1.0332827902897441}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 0.30938887987023284}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 3526.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 1.8507042253521127}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 0.2439100532915189}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 0.6155662290091947}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [-0.5900051704906099, 1.8211376285089993]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2015_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 3.5223321117182493}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 0.060547195443532285}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 4390.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 1.7747474747474747}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 0.07201681191162829}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 0.5736581450584617}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [-0.033439447860892035, 1.1807557379778153]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2018_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 34.65369816224936}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 3.938895748311474e-09}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 6146.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 2.8574980574980575}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 5.1401626960803136e-08}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.0499464368958915}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.6851178654776757, 1.4147750083141073]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2014_v5_binned_deidentified|asthma|2014|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 704.9078934236488}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 2.561272352226557e-155}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 158336.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 7.883984638966262}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 3.0941559510503567e-80}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.0648334409078375}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [1.8842458102435968, 2.2454210715720784]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2014_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 29.34867716706477}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 6.045777278268566e-08}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 3312.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 6.896805896805897}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 7.994761218640647e-06}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.9310583909192875}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [1.1222943200136912, 2.739822461824884]}]}",
                        "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2011_v5_binned_deidentified|asthma|2011|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 529.6087134947754}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 3.4361759007593224e-117}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 159173.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 10.908574793467153}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 1.0572696006077861e-49}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.3895491582656496}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [2.134465881344837, 2.644632435186462]}]}"
                    ]
                },
                # Captured node contents
                None,
                # Captured edge contents
                {
                    "category": ["biolink:Association"],
                    "subject": "PUBCHEM.COMPOUND:2083",
                    "predicate": "biolink:positively_correlated_with",
                    "object": "MONDO:0007079",
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:icees-kg"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.not_provided
                },
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
