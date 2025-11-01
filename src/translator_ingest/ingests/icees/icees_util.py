"""
This file contains utility functions for ICEES data processing
"""
from typing import Any

#
# An example of the TRAPI-like attribute data structure for a cohort.
# The embedded 'attributes' of this attribute are cohort metadata to be captured?
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


def get_cohort_metadata(metadata: list[dict[str, str]]):
    # TODO: stub code...implement this
    return None