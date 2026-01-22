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

#
# TODO: this is a partial implementation of a more
#       sophisticated qualifier mapping approach,
#       ignored and unused for now
#
# _associations_missing_qualifiers: set[type[Association]] = set()
#
# def get_associations_missing_qualifiers() -> set[type[Association]]:
#     return _associations_missing_qualifiers
#
# def map_icees_qualifiers(
#     koza_transform: koza.KozaTransform,
#     association: type[Association],
#     subject_category: list[str],
#     object_category: list[str],
#     qualifiers: dict[str, str]
# ) -> dict[str, str]:
#     """
#     Map ICEES input qualifiers to Association-specific Biolink Model qualifiers.
#
#     :param koza_transform: koza.KozaTransform, ingest context containing state information.
#     :param association: type[Association], specific subtype of biolink:Association being assessed.
#     :param subject_category: list[str], possible categories resolved for the subject in the association.
#     :param object_category: list[str], possible categories resolved for the object in the association.
#     :param qualifiers: dict[str, str], input target qualifiers being translated,
#                                       indexed by target 'subject' or 'object'.
#     :return: dict[str, str] of mapped qualifiers, currently either
#              "subject_specialization_qualifier" or "object_specialization_qualifier"
#              (returns an empty dictionary if not available in the given Association subtype).
#     """
#     mappings: dict[str, str] = {}
#     if "subject_specialization_qualifier" in association.model_fields:
#         mappings = {
#             "subject": "subject_specialization_qualifier",
#             "object": "object_specialization_qualifier"
#         }
#     elif "subject_aspect_qualifier" in association.model_fields:
#         mappings = {
#             "subject": "subject_aspect_qualifier",
#             "object": "object_aspect_qualifier"
#         }
#     elif "subject_form_or_variant_qualifier" in association.model_fields:
#         mappings = {
#             "subject": "subject_form_or_variant_qualifier",
#             # the corresponding 'object' qualifier may not be specific to this association?
#         }
#     if mappings:
#         return {mappings[tag]: value for tag, value in qualifiers.items() if tag in mappings}
#     else:
#        association_type = type(association)
#        if str(e) not in koza_transform.transform_metadata["icees_edges"]:
#             koza_transform.transform_metadata["icees_edges"]["Association_classes_missing_qualifiers"] = {association_type}
#         else:
#             koza_transform.transform_metadata["icees_edges"][Association_classes_missing_qualifiers].add(association_type)
#         return {}

_PREDICATE_MAPPINGS = {
    "DiseaseAssociatedWithResponseToChemicalEntityAssociation": {
        "biolink:positively_correlated_with": "biolink:associated_with_sensitivity_to",
    },
    "VariantToDiseaseAssociation": {
        "biolink:correlated_with": "biolink:related_condition",
    },
    "CausalGeneToDiseaseAssociation": {
        "biolink:correlated_with": "biolink:contributes_to",
    },
}
def remap_icees_predicate(association_type: str, predicate: str) -> str:
    """
    This method remaps the ICEES predicate originally specified in Translator Phase 2 -
    generally one of "biolink:correlated_with", "biolink:positively_correlated_with"
    "biolink:negatively_correlated_with" - onto a suitable predicate
    drawn from the latest Biolink Model release. The association subtype being
    targeted for generation is considered a useful context for resolving this.

    :param association_type: Target Phase 3 ICEES association type.
    :param predicate: Original Phase 2 ICEES predicate
    :return:
    """
    association_predicate_map: Optional[dict[str, str]] = _PREDICATE_MAPPINGS.get(association_type, None)
    if association_predicate_map is None:
        logger.warning(
            "REMAP MISS: No ICEES predicate remap map found "
            f"for association type '{association_type}'"
        )
        _PREDICATE_MAPPINGS[association_type] = dict()
    remapped_predicate = association_predicate_map.get(predicate, None)
    if remapped_predicate is None:
        logger.warning(
            f"REMAP MISS: No specific remapping found for predicate '{predicate}' "
            f"for association type '{association_type}'"
        )
        _PREDICATE_MAPPINGS[association_type][predicate] = remapped_predicate = predicate
    return remapped_predicate
