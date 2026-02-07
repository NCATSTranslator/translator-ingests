"""
This file contains utility functions for ICEES data processing
"""
from typing import Optional
from loguru import logger
from biolink_model.datamodel.pydanticmodel_v2 import Study, IceesStudyResult

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

def get_icees_supporting_study(
        edge_id: str,
        study_id: str,
        result: list[dict[str, str]]
)->Study:
    """
    The embedded 'attributes' of ICEES study result data
    are wrapped as instances of IceesStudyResult, then
    embedded in its Study object, which is returned.

    :param edge_id: String identifier for the edge
    :param study_id: String identifier for the study
    :param result: List of Study Results consisting of slot-indexed values, like result statistics.
    :return:
    """
    result_data = {attribute['attribute_type_id']: attribute['value'] for attribute in result}
    result = IceesStudyResult(id=edge_id, **result_data)
    return Study(
        id=study_id,
        has_study_results=[result]
    )

_META_MAP: dict[str,set[str]] = dict()

def log_meta_edge(
        subject_category: str,
        predicate: str,
        object_category: str

):
    """
    This method reverse engineers the meta_knowledge SPOQ of ICEES data encountered (for the RIG).
    :param subject_category:
    :param predicate:
    :param object_category:
    :return: None (just builds the internal meta-knowledge-map)
    """
    association_predicate_map: Optional[set[str]] = _META_MAP.get(association_type)
    if association_predicate_map is None:
        _META_MAP[association_type] = set()

    # Add the predicate to the Set associated with the given association_type
    _META_MAP[association_type].add(predicate)
