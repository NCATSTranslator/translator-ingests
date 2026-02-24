"""
This file contains utility functions for COHD data processing
"""
from typing import Any
from json import loads

from biolink_model.datamodel.pydanticmodel_v2 import Study, IceesStudyResult


def _parse_attributes(attribute_list: list[str]) -> list[dict[str, Any]]:
    return [loads(entry) for entry in attribute_list]


def parse_node_properties(attribute_list: list[str]) -> dict[str, Any]:
    if not attribute_list:
        return {}
    node_properties: dict[str, Any] = {}
    attributes: list[dict[str, Any]] = _parse_attributes(attribute_list)
    for attribute in attributes:
        if attribute["attribute_type_id"] == 'EDAM:data_0954':
            # Convert embedded attributes with EDAM type 'Database cross-mapping' into a node xref;
            # However, for accuracy in access, use the URI not the  CURIE since there may be some confusion
            # on namespaces e.g. for 'https://athena.ohdsi.org/search-terms/terms/77661' and 'OMOP:77661'
            # in COHD, the URI matches the description given, but not the OMOP CURIE looked up on the internet
            if "xref" not in node_properties:
                node_properties["xref"] = []
            for entry in attribute["attributes"]:
                if entry["attribute_type_id"] == "EDAM:data_1087":  # 'concept_id'
                    node_properties["xref"].append(entry["value_url"])

    return node_properties


def parse_association_slots(attribute_list: list[str]) -> dict[str, Any]:
    if not attribute_list:
        return {}
    association_slots: dict[str, Any] = {}
    attributes: list[dict[str, Any]] = _parse_attributes(attribute_list)
    for attribute in attributes:
        # If the knowledge_level and agent_type were not
        # as uniform in COHD, we'd have to do more here
        if attribute["attribute_type_id"] in ["biolink:knowledge_level", "biolink:agent_type"]:
            continue
        if attribute["attribute_type_id"] == "biolink:has_supporting_study_result":
            # TODO: capture study data here
            pass
        else:
            # ignore attribute for now
            pass

    return association_slots


def get_cohd_supporting_study(
        edge_id: str,
        study_id: str,
        result: list[dict[str, str]]
)->Study:
    """
    The embedded 'attributes' of COHD study result data
    are wrapped as instances of COHD StudyResult, then
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
