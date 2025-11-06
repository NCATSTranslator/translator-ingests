"""
This file contains utility functions for ICEES data processing
"""

from biolink_model.datamodel.pydanticmodel_v2 import IceesStudyResult, Association


#
# An example of the TRAPI-like attribute data structure for a Study Result.

# {
#     "attribute_type_id": "icees_cohort_identifier",
#     "value": "PCD_UNC_patient_2020_v6_binned_deidentified|pcd|v6|2024_03_20_21_18_22",
#     "attributes": [
#         {
#             "attribute_type_id": "chi_squared_statistic",
#             "value": 26.38523077566414
#         },
#         {
#             "attribute_type_id": "chi_squared_dof",
#             "value": 1
#         },
#         {
#             "attribute_type_id": "chi_squared_p",
#             "value": 2.7967079822744063e-07
#         },
#         {
#             "attribute_type_id": "total_sample_size",
#             "value": 4753.0
#         },
#         {
#             "attribute_type_id": "fisher_exact_odds_ratio",
#             "value": 3.226188583240579
#         },
#         {
#             "attribute_type_id": "fisher_exact_p",
#             "value": 2.8581244515361156e-06
#         },
#         {
#             "attribute_type_id": "log_odds_ratio",
#             "value": 1.1713014352915974
#         },
#         {
#             "attribute_type_id": "log_odds_ratio_95_ci",
#             "value": [
#                 0.6996904681742875,
#                 1.6429124024089072
#             ]
#         }
#     ]
# }
# The embedded 'attributes' of this ICEES study result data
# are the IceesStudyResult to be captured here
def get_icees_study_result(
        edge_id: str,
        study_name: str,
        metadata: list[dict[str, str]]
):
    data = {attribute['attribute_type_id']: attribute['value'] for attribute in metadata}
    return IceesStudyResult(
        # this id could be atrociously long
        id=f"icees:{study_name}|{edge_id}",
        **data
    )

# _type_to_target_map = {
#     "ChemicalToDiseaseOrPhenotypicFeatureAssociation": {
#         "subject": "subject_specialization_qualifier",
#         "object": "object_specialization_qualifier"
#     }
# }

def map_icees_qualifiers(
        association: type[Association],
        qualifiers: dict[str, str]
) -> dict[str, str]:
    # a_type = association.__name__
    # if a_type in _type_to_target_map:
    #     mapping = _type_to_target_map[a_type]
    #     return {mapping[tag]: value for tag, value in qualifiers.items()}
    #
    # return {}
    # Simplified version of the above, that assumes that all observed
    # type[Association] have the same mapping, which is unlikely true
    mapping = {
        "subject": "subject_specialization_qualifier",
        "object": "object_specialization_qualifier"
    }
    return {mapping[tag]: value for tag, value in qualifiers.items()}
