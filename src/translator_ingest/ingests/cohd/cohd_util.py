"""
This file contains utility functions for COHD data processing
"""
from typing import Optional, Any
from json import loads

from biolink_model.datamodel.pydanticmodel_v2 import Study, StudyResult


def parse_attributes(attribute_list: list[str]) -> list[dict[str, Any]]:
    return [loads(entry) for entry in attribute_list]


def parse_node_properties(attribute_list: list[str]) -> dict[str, Any]:
    if not attribute_list:
        return {}
    node_properties: dict[str, Any] = {}
    attributes: list[dict[str, Any]] = parse_attributes(attribute_list)
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


def get_cohd_supporting_study(
        edge_id: str,
        attribute_list: list[str]
)->Optional[dict[str, Study]]:
    """
    Parsing of the embedded 'attributes' of COHD edge
    into instances of COHD StudyResult, then
    embedded in its Study object, which is returned.

    :param edge_id: String identifier for the edge
    :param attribute_list: List of Study Results consisting of slot-indexed values, like result statistics.
    :return: Optional[dict[str, Study]] is a dictionary fragment with the 'study id' as key and a Study as value.
             The method returns None if no such Study record can be resolved.
    """
    if not attribute_list:
        return None

    all_attributes: list[dict[str, Any]] = parse_attributes(attribute_list)
    entry: dict
    study_attributes: list[dict[str, Any]] = [
        entry for entry in all_attributes
        if entry["attribute_type_id"] == "biolink:has_supporting_study_result"
    ]

    # TODO: extract the study_result
    study_result = {}

    # TODO: parse an actual study_id from the embedded "supporting_data_set" attribute
    study_id = "infores:cohd"

    # TODO: perhaps need to defined a specialized CohdStudyResult class in Biolink
    result = StudyResult(id=edge_id, **study_result)

    return {
        study_id: Study(
            id=study_id,
            has_study_results=[result]
        )
    }


def parse_association_slots(attribute_list: list[str]) -> dict[str, Any]:
    if not attribute_list:
        return {}
    association_slots: dict[str, Any] = {}
    attributes: list[dict[str, Any]] = parse_attributes(attribute_list)
    entry: dict
    study_results: list[dict[str, Any]] = [
        entry for entry in attributes
        if entry["attribute_type_id"] == "biolink:has_supporting_study_result"
    ]

    return association_slots