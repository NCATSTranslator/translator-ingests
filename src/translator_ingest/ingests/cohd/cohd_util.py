"""
This file contains utility functions for COHD data processing
"""
from typing import Optional, Any
from json import loads

from biolink_model.datamodel.pydanticmodel_v2 import Study, StudyResult, NamedThing
from bmt.pydantic import get_node_class
from translator_ingest.util.biolink import get_biolink_model_toolkit

bmt = get_biolink_model_toolkit()

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

    # Current iteration assumes only a single supporting data set identifier to be extracted
    # from within the list of attributes embedded inside the study results list for this edge
    study_id: Optional[str] = None

    study_results: list = []
    sa: dict
    for sa in study_attributes:
        # extract the first instance of the COHD dataset ID seen
        if study_id is None:
            sra: list[dict[str,Any]] = sa.get("attributes",[])
            if sra:
                sds = [
                    attribute["value"] for attribute in sra
                    if attribute.get("attribute_type_id",None) == "biolink:supporting_data_set"
                ]
                study_id = sds[0] if sds else None
        else:
            # fall back study_id is the infores?
            study_id = "infores:cohd"

        node_class: type[NamedThing] = get_node_class(node_id=edge_id, categories=[sa["value_type_id"]], bmt=bmt)
        study_result = node_class(id=edge_id, name=sa["value"], **{})
        study_results.append(study_result)

    return {
        study_id: Study(
            id=study_id,
            has_study_results=study_results
        )
    }
