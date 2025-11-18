"""
Text Mining KP attribute parsing utilities.
"""

import json
from typing import Dict, Any
from loguru import logger
from biolink_model.datamodel.pydanticmodel_v2 import TextMiningStudyResult, Study
from koza import KozaTransform


def parse_attributes_json(
    attributes_str: str, koza: KozaTransform, record: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Parse the _attributes JSON string and create Study and TextMiningStudyResult objects.
    Text mining specific attributes are structured as Study objects with TextMiningStudyResult objects.

    Args:
        attributes_str: JSON string containing attributes
        koza: KozaTransform instance for context
        record: The edge record being processed (for context in logging)

    Returns:
        Dictionary of mapped attributes including has_supporting_studies
    """
    if not attributes_str:
        return {}

    try:
        attributes = json.loads(attributes_str)
        if not isinstance(attributes, list):
            return {}

        mapped_attributes = {}
        study_results = []
        text_mining_metadata = {}
        unmapped_attributes = []

        for attr in attributes:
            if isinstance(attr, dict) and "attribute_type_id" in attr:
                # The attributes already come with biolink: prefix
                attr_type_id = attr["attribute_type_id"]
                value = attr.get("value")


                # Handle confidence score mapping
                if attr_type_id == "biolink:confidence_score":
                    mapped_attributes["biolink:has_confidence_level"] = value
                    continue
                
                # Handle supporting_study_result with nested attributes
                if (attr_type_id == "biolink:supporting_study_result" or attr_type_id == "biolink:has_supporting_study_result") and "attributes" in attr:
                    study_result_data = _parse_study_result(attr, mapped_attributes)
                    
                    try:
                        study_result_obj = TextMiningStudyResult(**study_result_data)
                        study_results.append(study_result_obj)
                        
                        # Also add the study result to the mapped attributes for backward compatibility
                        if "biolink:has_supporting_study_result" not in mapped_attributes:
                            mapped_attributes["biolink:has_supporting_study_result"] = []
                        mapped_attributes["biolink:has_supporting_study_result"].append(study_result_obj)
                    except Exception as e:
                        logger.error(f"Error creating TextMiningStudyResult: {e}", extra={"data": study_result_data})

                    continue

                # Handle other attributes
                _handle_other_attributes(attr_type_id, value, mapped_attributes, text_mining_metadata, unmapped_attributes)

        # Create study object if we have text mining data
        if study_results or text_mining_metadata:
            _create_study_object(study_results, text_mining_metadata, record, mapped_attributes)

        # Log unmapped attributes with context
        _log_unmapped_attributes(unmapped_attributes, record)

        return mapped_attributes

    except json.JSONDecodeError as e:
        logger.error(f"Error parsing attributes JSON: {e}")
        return {}


def _parse_study_result(attr: Dict[str, Any], mapped_attributes: Dict[str, Any]) -> Dict[str, Any]:
    """Parse supporting study result with nested attributes."""
    study_result_data = {
        "id": attr.get("value"),  # The study result ID
        "category": ["biolink:TextMiningStudyResult"],
    }

    # Process nested attributes (also in TRAPI format)
    for nested_attr in attr.get("attributes", []):
        if isinstance(nested_attr, dict) and "attribute_type_id" in nested_attr:
            nested_attr_type_id = nested_attr["attribute_type_id"]
            nested_value = nested_attr.get("value")
            nested_attr_name = (
                nested_attr_type_id.replace("biolink:", "")
                if nested_attr_type_id.startswith("biolink:")
                else nested_attr_type_id
            )

            # Map text mining attributes to TextMiningStudyResult fields
            if nested_attr_name == "supporting_text":
                # Convert to list if it's a string
                if isinstance(nested_value, str):
                    study_result_data["supporting_text"] = [nested_value]
                    # Also add to top-level for backward compatibility
                    mapped_attributes["biolink:supporting_text"] = nested_value
                else:
                    study_result_data["supporting_text"] = nested_value
                    mapped_attributes["biolink:supporting_text"] = nested_value
            elif nested_attr_name == "subject_location_in_text":
                _parse_location_attribute(nested_value, study_result_data, mapped_attributes, "subject_location_in_text")
            elif nested_attr_name == "object_location_in_text":
                _parse_location_attribute(nested_value, study_result_data, mapped_attributes, "object_location_in_text")
            elif nested_attr_name == "extraction_confidence_score":
                # Convert to integer if it's a float
                if isinstance(nested_value, float):
                    nested_value = int(nested_value * 100)  # Convert to percentage
                study_result_data["extraction_confidence_score"] = nested_value
                # Also add to top-level for backward compatibility
                mapped_attributes["biolink:extraction_confidence_score"] = nested_value
            elif nested_attr_name == "supporting_document_year":
                study_result_data["supporting_document_year"] = nested_value
            elif nested_attr_name == "supporting_document_type":
                study_result_data["supporting_document_type"] = nested_value
            elif nested_attr_name == "supporting_text_section_type":
                study_result_data["supporting_text_section_type"] = nested_value
            elif (
                nested_attr_name == "supporting_document" or nested_attr_name == "supporting_documents"
            ):
                # TextMiningStudyResult doesn't have a publications field,
                # so we skip this attribute - it should be handled at the Association level
                continue

    return study_result_data


def _parse_location_attribute(nested_value, study_result_data, mapped_attributes, attr_name):
    """Parse location attributes (subject/object location in text)."""
    if isinstance(nested_value, str) and "|" in nested_value:
        try:
            loc_list = [int(x) for x in nested_value.split("|")]
            study_result_data[attr_name] = loc_list
            # Also add to top-level for backward compatibility
            mapped_attributes[attr_name] = loc_list
        except ValueError:
            study_result_data[attr_name] = nested_value
            mapped_attributes[attr_name] = nested_value
    else:
        study_result_data[attr_name] = nested_value
        mapped_attributes[attr_name] = nested_value


def _handle_other_attributes(attr_type_id, value, mapped_attributes, text_mining_metadata, unmapped_attributes):
    """Handle other attribute types."""
    attr_name = attr_type_id.replace("biolink:", "") if attr_type_id.startswith("biolink:") else attr_type_id
    
    # Handle text mining metadata attributes that should be grouped into Study objects
    if attr_type_id in [
        "biolink:has_evidence_count",
        "biolink:tmkp_confidence_score",
        "biolink:supporting_document",
        "biolink:semmed_agreement_count",
        "biolink:supporting_study_result",
    ]:
        # Store these in text mining metadata to be processed later
        text_mining_metadata[attr_name] = value
        return

    # Handle regular attributes
    if attr_type_id == "biolink:publications" and isinstance(value, str):
        # Handle pipe-separated publications
        mapped_attributes[attr_type_id] = value.split("|") if "|" in value else [value]
    elif attr_type_id in ["biolink:primary_knowledge_source", "biolink:aggregator_knowledge_source"]:
        # Preserve these for creating RetrievalSource objects
        mapped_attributes[attr_type_id] = value
    elif attr_type_id == "biolink:supporting_data_source":
        # This is auxiliary info, could be stored but not critical
        pass
    else:
        # Store the value with the biolink-prefixed name
        mapped_attributes[attr_type_id] = value

        # Check if this attribute exists in the Association model
        from biolink_model.datamodel.pydanticmodel_v2 import Association
        if not hasattr(Association, attr_name):
            unmapped_attributes.append(attr_type_id)


def _create_study_object(study_results, text_mining_metadata, record, mapped_attributes):
    """Create a Study object with TextMiningStudyResult objects."""
    # Create a study ID based on the edge ID
    edge_id = record.get("id", "unknown") if record else "unknown"
    study_id = f"text_mining_study_{edge_id}"

    study_data = {
        "id": study_id,
        "category": ["biolink:Study"],
        "name": f"Text mining study for edge {edge_id}",
        "description": "Text mining analysis supporting this association",
    }

    # Add study results if any
    if study_results:
        study_data["has_study_results"] = study_results

    try:
        study_obj = Study(**study_data)
        mapped_attributes["has_supporting_studies"] = {study_id: study_obj}
    except Exception as e:
        logger.error(f"Error creating Study: {e}", extra={"data": study_data})


def _log_unmapped_attributes(unmapped_attributes, record):
    """Log unmapped attributes with context."""
    if unmapped_attributes and record:
        subject = record.get("subject", "unknown")
        predicate = record.get("predicate", "unknown")
        object_id = record.get("object", "unknown")
        edge_id = record.get("id", "unknown")

        logger.warning(
            f"Unmapped Biolink attributes for association {edge_id} "
            f"({subject} --[{predicate}]--> {object_id}): "
            f"{', '.join(unmapped_attributes)}"
        )