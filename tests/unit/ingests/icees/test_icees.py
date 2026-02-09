import pytest

from typing import Optional, Any

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.icees.icees import (
    transform_icees_node,
    transform_icees_edge,
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="module")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = (
    "id",
    "name",
    "category",
    "equivalent_identifiers"
)

# list of slots whose values are
# to be checked in a result edge
CORE_ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "subject_feature_name",
    "predicate",
    "object",
    "object_feature_name",
    "has_supporting_studies",
    "sources",
    "knowledge_level",
    "agent_type",
)

################ Shared test data ################

PUBCHEM_NODE = {
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
            }

DISEASE_NODE = {
    "id": "MONDO:0007079",
    "name": "alcohol dependence",
    "category": [
        "biolink:Disease",
        "biolink:ThingWithTaxon",
        "biolink:BiologicalEntity",
        "biolink:NamedThing",
        "biolink:DiseaseOrPhenotypicFeature"
    ],
    "equivalent_identifiers": [
        "MONDO:0007079",
        "DOID:0050741",
        "OMIM:103780",
        "EFO:0003829",
        "UMLS:C0001973",
        "MEDDRA:10001585",
        "NCIT:C93040",
        "SNOMEDCT:284591009",
        "KEGG.DISEASE:05034",
        "HP:0030955"
    ]
}

GENE_NODE = {
    "id": "NCBIGene:3105",
    "name": "HLA-A",
    "category": [
        "biolink:Protein",
        "biolink:PhysicalEssenceOrOccurrent",
        "biolink:BiologicalEntity",
        "biolink:SequenceVariant",
        "biolink:Polypeptide",
        "biolink:GeneProductMixin",
        "biolink:Gene",
        "biolink:GenomicEntity",
        "biolink:ChemicalEntityOrGeneOrGeneProduct",
        "biolink:ThingWithTaxon",
        "biolink:ChemicalEntityOrProteinOrPolypeptide",
        "biolink:GeneOrGeneProduct",
        "biolink:OntologyClass",
        "biolink:NamedThing",
        "biolink:MacromolecularMachineMixin",
        "biolink:PhysicalEssence"
    ],
    "equivalent_identifiers": [
        "NCBIGene:3105",
        "ENSEMBL:ENSG00000206503",
        "HGNC:4931",
        "OMIM:142800",
        "UMLS:C1333899",
        "UniProtKB:B1PKY1",
        "PR:P04439"
    ]
}

TAXONOMIC_NODE = {
    "id": "NCBITaxon:12092",
    "name": "Hepatovirus A",
    "category": [
        "biolink:PhenotypicFeature",
        "biolink:OrganismTaxon",
        "biolink:NamedThing"
    ],
    "equivalent_identifiers":
        [
            "NCBITaxon:12092",
            "MESH:D030041",
            "UMLS:C0376325"
        ]
}

UMLS_NODE = {
    "id": "UMLS:C4049590",
    "name": "Anti-HBc IgM antibody",
    "category": [
        "biolink:PhysicalEssenceOrOccurrent",
        "biolink:ActivityAndBehavior",
        "biolink:Procedure",
        "biolink:Occurrent",
        "biolink:PhenotypicFeature",
        "biolink:NamedThing"
    ],
    "equivalent_identifiers": [
        "UMLS:C4049590"
    ]
}

################ End of shared test data ################

@pytest.mark.parametrize(
    "test_record,result_nodes",
    [
        (  # Query 0 - A complete node record
            PUBCHEM_NODE,
            # Captured node contents
            [
                {
                    "id": "PUBCHEM.COMPOUND:2083",
                    "name": "Salbutamol",
                    "category": ["biolink:Drug"],
                    "equivalent_identifiers": [
                        "PUBCHEM.COMPOUND:2083",
                        "CHEMBL.COMPOUND:CHEMBL714",
                        "CHEBI:2549",
                        "DRUGBANK:DB01001",
                        "DrugCentral:105"
                    ]
                }
            ]
        ),
        (  # Query 1- Another complete node record
                DISEASE_NODE,
                #
                # Captured node contents
                [
                {
                    "id": "MONDO:0007079",
                    "name": "alcohol dependence",
                    "category": [
                        "biolink:Disease"
                    ],
                    "equivalent_identifiers": [
                        "MONDO:0007079",
                        "DOID:0050741",
                        "OMIM:103780",
                        "EFO:0003829",
                        "UMLS:C0001973",
                        "MEDDRA:10001585",
                        "NCIT:C93040",
                        "SNOMEDCT:284591009",
                        "KEGG.DISEASE:05034",
                        "HP:0030955"
                    ]
                },
            ]
        ),
        (  # Query 2- Taxonomic node
            TAXONOMIC_NODE,
            #
            # Captured node contents
            [
                {
                    "id": "NCBITaxon:12092",
                    "name": "Hepatovirus A",
                    "category": [
                        "biolink:PhenotypicFeature"
                    ],
                    "equivalent_identifiers": [
                        "NCBITaxon:12092",
                        "MESH:D030041",
                        "UMLS:C0376325"
                    ]
                }
            ]
        ),
        (  # Query 3- UMLS node parsing
            {
                "id": "UMLS:C3836535",
                "name": "patient education about activity/exercise prescribed",
                "category": [
                    "biolink:Occurrent",
                    "biolink:PhysicalEssenceOrOccurrent",
                    "biolink:Activity",
                    "biolink:EnvironmentalExposure",
                    "biolink:ActivityAndBehavior",
                    "biolink:NamedThing"
                ],
                "equivalent_identifiers": [
                    "UMLS:C3836535"
                ]
            },
            #
            # Captured node contents
            [
                {
                    "id": "UMLS:C3836535",
                    "name": "patient education about activity/exercise prescribed",
                    "category": ["biolink:EnvironmentalExposure"],
                    "equivalent_identifiers": [
                        "UMLS:C3836535"
                    ],
                }
            ]
        )

    ],
    #
)
def test_transform_icees_nodes(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list]
):
    validate_transform_result(
        result=transform_icees_node(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=None,
        node_test_slots=NODE_TEST_SLOTS
    )


@pytest.mark.parametrize(
    "test_nodes,test_edge_record,result_edge,qualifiers",
    [
        (   # Query 0 - A complete edge record
            [
                PUBCHEM_NODE,
                DISEASE_NODE
            ],
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
            # Captured edge contents
            {
                "category": ["biolink:Association"],
                "subject": "PUBCHEM.COMPOUND:2083",
                "subject_feature_name": "AlbuterolRx",
                "predicate": "biolink:positively_correlated_with",
                "object": "MONDO:0007079",
                "object_feature_name": "AlcoholDependenceDx",
                #
                # Testing the expected contents of this slot is a bit too challenging at this point in time,
                # but the study generation is validated somewhat by human inspection.
                #
                # "has_supporting_studies": {
                #     "PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22": {
                #         "id": "PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22",
                #         "category": ["biolink:Study"],
                #         "has_study_results": [
                #             {
                #                 "category": ["biolink:IceesStudyResult"],
                #                 "chi_squared_statistic": 26.38523077566414,
                #                 "chi_squared_dof": 1,
                #                 "chi_squared_p": 2.7967079822744063e-07,
                #                 "total_sample_size": 4753,
                #                 "fisher_exact_odds_ratio": 3.226188583240579,
                #                 "fisher_exact_p": 2.8581244515361156e-06,
                #                 "log_odds_ratio": 1.1713014352915974,
                #                 "log_odds_ratio_95_ci": [0.6996904681742875, 1.6429124024089072]
                #             }
                #         ]
                #     } ,
                #     "Asthma_UNC_EPR_patient_2013_v5_binned_deidentified|asthma|2013|2024_03_19_10_34_15": "etc..."
                # },
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:icees-kg"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            },
            (
                "subject_feature_name",
                "object_feature_name"
            )
        ),
        (  # Query 1 - A complete edge record with different qualifiers
            [
                GENE_NODE,
                TAXONOMIC_NODE
            ],
            {
                "subject": "NCBIGene:3105",
                "predicate": "biolink:correlated_with",
                "object": "NCBITaxon:12092",
                "primary_knowledge_source": "infores:icees-kg",
                "attributes": [
                    "{\"attribute_type_id\": \"biolink:has_supporting_study_result\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/ICEES\"}",
                    "{\"attribute_type_id\": \"terms_and_conditions_of_use\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES-and-ICEES-KG-Terms-and-Conditions-of-Use\"}",

                    "{\"attribute_type_id\": \"subject_feature_name\", \"value\": \"A*02:01\"}",
                    "{\"attribute_type_id\": \"object_feature_name\", \"value\": \"Anti_HAV\"}",

                    "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Augmentin_DILI_patient_v4_binned_deidentified|dili|binned|2024_03_15_17_08_01\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 8.340160221149161e-08}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 2}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 0.9999999582991997}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 228.0}]}"
                ]
            },
            # Captured edge contents
            {
                "category": ["biolink:Association"],
                "subject": "NCBIGene:3105",
                "subject_feature_name": "A*02:01",
                "predicate": "biolink:correlated_with",
                "object": "NCBITaxon:12092",
                "object_feature_name": "Anti_HAV",
                # See Query 1 comments above, regarding "has_supporting_studies"
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:icees-kg"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            },
            (
                "subject_feature_name",
                "object_feature_name"
            )
        ),
        (   # Query 2 - A complete edge record with different qualifiers
            [
                GENE_NODE,
                UMLS_NODE
            ],
            {
                "subject": "NCBIGene:3105",
                "predicate": "biolink:correlated_with",
                "object": "UMLS:C4049590",
                "primary_knowledge_source": "infores:icees-kg",
                "attributes": [
                      "{\"attribute_type_id\": \"biolink:has_supporting_study_result\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/ICEES\"}", "{\"attribute_type_id\": \"terms_and_conditions_of_use\", \"value\": \"https://github.com/NCATSTranslator/Translator-All/wiki/Exposures-Provider-ICEES-and-ICEES-KG-Terms-and-Conditions-of-Use\"}", "{\"attribute_type_id\": \"subject_feature_name\", \"value\": \"A*02:01\"}", "{\"attribute_type_id\": \"object_feature_name\", \"value\": \"Anti_HBc_IgM\"}", "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Augmentin_DILI_patient_v4_binned_deidentified|dili|binned|2024_03_15_17_08_01\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 2.7441416237609344}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 2}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 0.253581296304747}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 190.0}]}"
                ]
            },
            # Captured edge contents
            {
                "category": ["biolink:Association"],
                "subject": "NCBIGene:3105",
                "subject_feature_name": "A*02:01",
                "predicate": "biolink:correlated_with",
                "object": "UMLS:C4049590",
                "object_feature_name": "Anti_HBc_IgM",
                # See Query 1 comments above, regarding "has_supporting_studies"
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:icees-kg"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            },
            (
                "subject_feature_name",
            )
        ),
        (   # Query 3 - Gene to Disease association (actually, a fake edge...
            #           just reused the gene and disease nodes above
            [
                GENE_NODE,
                DISEASE_NODE
            ],
            {
                "subject": "NCBIGene:3105",
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
                    "{\"attribute_type_id\": \"subject_feature_name\", \"value\": \"A*02:01\"}",
                    "{\"attribute_type_id\": \"object_feature_name\", \"value\": \"AlcoholDependenceDx\"}",

                    "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 26.38523077566414}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 2.7967079822744063e-07}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 4753.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 3.226188583240579}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 2.8581244515361156e-06}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 1.1713014352915974}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [0.6996904681742875, 1.6429124024089072]}]}",
                    "{\"attribute_type_id\": \"icees_cohort_identifier\", \"value\": \"Asthma_UNC_EPR_patient_2013_v5_binned_deidentified|asthma|2013|2024_03_19_10_34_15\", \"attributes\": [{\"attribute_type_id\": \"chi_squared_statistic\", \"value\": 961.1556369070855}, {\"attribute_type_id\": \"chi_squared_dof\", \"value\": 1}, {\"attribute_type_id\": \"chi_squared_p\", \"value\": 4.986523229006498e-211}, {\"attribute_type_id\": \"total_sample_size\", \"value\": 158671.0}, {\"attribute_type_id\": \"fisher_exact_odds_ratio\", \"value\": 17.74280067875692}, {\"attribute_type_id\": \"fisher_exact_p\", \"value\": 4.494353716947507e-97}, {\"attribute_type_id\": \"log_odds_ratio\", \"value\": 2.875979838082306}, {\"attribute_type_id\": \"log_odds_ratio_95_ci\", \"value\": [2.6254515014378037, 3.126508174726808]}]}",
                ]
            },
            # Captured edge contents
            {
                "category": ["biolink:CorrelatedGeneToDiseaseAssociation"],
                "subject": "NCBIGene:3105",
                "subject_feature_name": "A*02:01",
                "predicate": "biolink:positively_correlated_with",
                "object": "MONDO:0007079",
                "object_feature_name": "AlcoholDependenceDx",
                # See Query 1 comments above, regarding "has_supporting_studies"
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:icees-kg"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            },
            (
                "subject_feature_name",
            )
        )
    ],
)
def test_transform_icees_edges(
        mock_koza_transform: koza.KozaTransform,
        test_nodes: list[dict[str, Any]],
        test_edge_record: dict,
        result_edge: dict,
        qualifiers: tuple
):
    # The edge ingest needs the node categories cached from the corresponding node ingests
    transform_icees_node(mock_koza_transform, test_nodes[0])
    transform_icees_node(mock_koza_transform, test_nodes[1])

    validate_transform_result(
        result=transform_icees_edge(mock_koza_transform, test_edge_record),
        expected_nodes=None,
        expected_edges=result_edge,
        edge_test_slots=CORE_ASSOCIATION_TEST_SLOTS+qualifiers,
    )
