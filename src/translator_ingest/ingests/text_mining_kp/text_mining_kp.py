"""
Text Mining KP KGX Pass-through Ingest

This module processes KGX files from the Text Mining Knowledge Provider
by extracting a tar.gz archive and passing through nodes while processing
edge attributes to map them to Biolink model slots.
"""

import json
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, Iterable, Optional, Any
from datetime import datetime
import koza
from koza import KozaTransform
from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    ChemicalEntity,
    Disease,
    Gene,
    Protein,
    Association,
    RetrievalSource,
    ResourceRoleEnum,
    AgentTypeEnum,
    KnowledgeLevelEnum,
    StudyResult
)
from koza.model.graphs import KnowledgeGraph


# Constants for the Text Mining KP
TMKP_INFORES = "infores:text-mining-provider-targeted"


def get_latest_version() -> str:
    """
    Return the version of the Text Mining KP data being processed.
    Uses the current date when the pipeline runs.
    """
    return datetime.now().strftime("%Y-%m-%d")


def create_biolink_entity(node_id: str, category: str, name: str):
    """
    Create the appropriate biolink entity based on the category.
    
    Args:
        node_id: The node ID
        category: The biolink category
        name: The node name
    
    Returns:
        The appropriate biolink entity instance
    """
    # Map categories to their corresponding classes
    category_mapping = {
        'biolink:ChemicalEntity': ChemicalEntity,
        'biolink:Disease': Disease,
        'biolink:Gene': Gene,
        'biolink:Protein': Protein,
    }
    
    # Get the appropriate class, default to NamedThing
    entity_class = category_mapping.get(category, NamedThing)
    
    # For NamedThing, we need to ensure category is exactly 'biolink:NamedThing'
    if entity_class == NamedThing and category != 'biolink:NamedThing':
        category = 'biolink:NamedThing'
    
    return entity_class(
        id=node_id,
        category=[category],
        name=name
    )


def extract_tar_gz(tar_path: str, koza_instance: KozaTransform) -> str:
    """
    Extract tar.gz file to a temporary directory and return the path.

    Args:
        tar_path: Path to the tar.gz file
        koza_instance: KozaTransform instance for logging

    Returns:
        Path to the extracted directory
    """
    extract_dir = tempfile.mkdtemp(prefix="text_mining_kp_extract_")

    koza_instance.log(f"Extracting {tar_path} to {extract_dir}")
    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(extract_dir)

    return extract_dir


def map_attribute_to_biolink_slot(attribute_name: str, koza_instance: KozaTransform) -> Optional[str]:
    """
    Map Text Mining KP attribute names to Biolink model slots.

    Args:
        attribute_name: The attribute name from the KGX file
        koza_instance: KozaTransform instance for logging

    Returns:
        The corresponding Biolink slot name, or None if no mapping exists
    """
    # Map from Text Mining KP attribute names to Biolink model slot names
    attribute_mappings = {
        # Confidence/evidence mappings
        'confidence_score': 'has_confidence_level',
        'tmkp_confidence_score': 'has_confidence_level',
        'extraction_confidence_score': 'extraction_confidence_score',
        'has_evidence_count': 'evidence_count',

        # Study/publication mappings
        'supporting_study_result': 'has_supporting_study_result',
        'supporting_study_results': 'has_supporting_study_result',
        'supporting_publications': 'publications',
        'supporting_document': 'publications',

        # Text-mining-specific slots
        'supporting_text': 'supporting_text',
        'subject_location_in_text': 'subject_location_in_text',
        'object_location_in_text': 'object_location_in_text',
        'supporting_text_located_in': 'supporting_text_section_type',
        'supporting_document_year': 'supporting_document_year',
        'supporting_document_type': 'supporting_document_type',

        # Knowledge source mappings
        'primary_knowledge_source': 'primary_knowledge_source',
        'aggregator_knowledge_source': 'aggregator_knowledge_source',
        'supporting_data_source': 'supporting_data_source',

        # Study metadata mappings
        'supporting_study_method_type': 'supporting_study_method_type',
        'supporting_study_method_description': 'supporting_study_method_description',
        'supporting_study_size': 'supporting_study_size',
        'supporting_study_cohort': 'supporting_study_cohort',
        'supporting_study_date_range': 'supporting_study_date_range',
        'supporting_study_context': 'supporting_study_context'
    }

    biolink_slot = attribute_mappings.get(attribute_name)
    if not biolink_slot:
        koza_instance.log(f"No Biolink slot mapping found for attribute: {attribute_name}")
        return None

    # Add biolink: prefix to the slot name
    return f"biolink:{biolink_slot}"


def parse_attributes_json(attributes_str: str, koza_instance: KozaTransform) -> Dict[str, Any]:
    """
    Parse the _attributes JSON string and map to Biolink slots.
    Handles TRAPI attribute model structure and simplifies to Biolink slots.

    Args:
        attributes_str: JSON string containing attributes
        koza_instance: KozaTransform instance for logging

    Returns:
        Dictionary of mapped attributes with biolink: prefixed keys
    """
    if not attributes_str:
        return {}

    try:
        attributes = json.loads(attributes_str)
        if not isinstance(attributes, list):
            return {}

        mapped_attributes = {}
        supporting_study_results = []

        for attr in attributes:
            if isinstance(attr, dict) and 'attribute_type_id' in attr:
                # Extract just the attribute name without biolink: prefix
                attr_name = attr['attribute_type_id'].replace('biolink:', '')
                biolink_slot = map_attribute_to_biolink_slot(attr_name, koza_instance)

                if biolink_slot:
                    # Extract just the value field from TRAPI attribute structure
                    value = attr.get('value')

                    # Handle supporting_study_result with nested attributes
                    if biolink_slot == 'biolink:has_supporting_study_result' and 'attributes' in attr:
                        # This is a supporting study result with nested attributes
                        study_result_data = {
                            'id': value,  # The study result ID
                        }

                        # Process nested attributes (also in TRAPI format)
                        for nested_attr in attr.get('attributes', []):
                            if isinstance(nested_attr, dict) and 'attribute_type_id' in nested_attr:
                                nested_attr_name = nested_attr['attribute_type_id'].replace('biolink:', '')
                                nested_value = nested_attr.get('value')
                                nested_slot = map_attribute_to_biolink_slot(nested_attr_name, koza_instance)

                                if nested_slot:
                                    # These text mining attributes should be on the Association, not StudyResult
                                    if nested_attr_name in ['supporting_text', 'subject_location_in_text',
                                                          'object_location_in_text', 'supporting_text_located_in',
                                                          'extraction_confidence_score', 'supporting_document_year',
                                                          'supporting_document_type']:
                                        # Handle character offsets as integers
                                        if nested_attr_name in ['subject_location_in_text', 'object_location_in_text']:
                                            # Convert pipe-separated string to list of integers
                                            if isinstance(nested_value, str) and '|' in nested_value:
                                                try:
                                                    mapped_attributes[nested_slot] = [int(x) for x in nested_value.split('|')]
                                                except ValueError:
                                                    mapped_attributes[nested_slot] = nested_value
                                            else:
                                                mapped_attributes[nested_slot] = nested_value
                                        else:
                                            mapped_attributes[nested_slot] = nested_value
                                    elif nested_attr_name == 'supporting_document':
                                        # Handle publications
                                        if isinstance(nested_value, str):
                                            pubs = nested_value.split('|') if '|' in nested_value else [nested_value]
                                            study_result_data['publications'] = pubs
                                        else:
                                            study_result_data['publications'] = nested_value

                        try:
                            # Create StudyResult instance with simplified data
                            study_result_obj = StudyResult(**study_result_data)
                            supporting_study_results.append(study_result_obj)
                        except Exception as e:
                            koza_instance.log(f"Error creating StudyResult: {e}, data: {study_result_data}")

                        continue

                    # Handle regular attributes
                    if biolink_slot == 'biolink:publications' and isinstance(value, str):
                        # Handle pipe-separated publications
                        mapped_attributes[biolink_slot] = value.split('|') if '|' in value else [value]
                    elif biolink_slot in ['biolink:primary_knowledge_source', 
                                        'biolink:aggregator_knowledge_source']:
                        # Preserve these for creating RetrievalSource objects
                        mapped_attributes[biolink_slot] = value
                    elif biolink_slot == 'biolink:supporting_data_source':
                        # This is auxiliary info, could be stored but not critical
                        continue
                    else:
                        # Store the value with the biolink-prefixed slot name
                        mapped_attributes[biolink_slot] = value

        # Add supporting study results if any
        if supporting_study_results:
            mapped_attributes['biolink:has_supporting_study_result'] = supporting_study_results

        return mapped_attributes

    except json.JSONDecodeError as e:
        koza_instance.log(f"Error parsing attributes JSON: {e}")
        return {}


@koza.prepare_data()
def prepare_text_mining_kp_data(koza_instance: KozaTransform, data: Iterable[Dict]) -> Iterable[Dict]:
    """
    Extract tar.gz and yield nodes and edges from KGX files.
    """
    koza_instance.log("Preparing Text Mining KP data: extracting tar.gz")

    # Path to the downloaded tar.gz file
    tar_path = "data/text_mining_kp/targeted_assertions.tar.gz"

    # Extract the tar.gz file
    extracted_path = extract_tar_gz(tar_path, koza_instance)

    # Find the nodes and edges files
    nodes_file = Path(extracted_path) / "nodes.tsv"
    edges_file = Path(extracted_path) / "edges.tsv"

    if not nodes_file.exists() or not edges_file.exists():
        koza_instance.log(f"ERROR: Could not find nodes.tsv or edges.tsv in {extracted_path}")
        return

    koza_instance.log(f"Found KGX files: {nodes_file} and {edges_file}")

    # First, yield all nodes
    koza_instance.log("Processing nodes...")
    with open(nodes_file, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            fields = line.strip().split('\t')
            if len(fields) >= len(header):
                node_dict = dict(zip(header, fields))
                node_dict['_record_type'] = 'node'
                yield node_dict

    # Then, yield all edges
    koza_instance.log("Processing edges...")
    with open(edges_file, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split('\t')
        for line in f:
            fields = line.strip().split('\t')
            if len(fields) >= len(header):
                edge_dict = dict(zip(header, fields))
                edge_dict['_record_type'] = 'edge'
                yield edge_dict


@koza.transform()
def transform_text_mining_kp(koza_instance: KozaTransform, data: Iterable[Dict]) -> Iterable[KnowledgeGraph]:
    """
    Transform Text Mining KP data with attribute processing.
    """
    nodes = []
    edges = []

    # Statistics tracking
    node_count = 0
    edge_count = 0
    attribute_errors = 0

    for record in data:
        record_type = record.get('_record_type')

        if record_type == 'node':
            # Pass through nodes as-is
            node_id = record.get('id')
            category = record.get('category', 'biolink:NamedThing')
            name = record.get('name')

            if not node_id:
                continue

            # Create the appropriate biolink entity based on category
            node = create_biolink_entity(node_id, category, name)

            nodes.append(node)
            node_count += 1

        elif record_type == 'edge':
            # Process edges with attribute extraction
            edge_id = record.get('id')
            subject = record.get('subject')
            predicate = record.get('predicate')
            object_id = record.get('object')

            if not all([subject, predicate, object_id]):
                continue

            # Parse and map attributes
            attributes_str = record.get('_attributes', '[]')
            mapped_attributes = parse_attributes_json(attributes_str, koza_instance)

            # Extract knowledge source information from attributes if available
            primary_source = mapped_attributes.get('biolink:primary_knowledge_source', TMKP_INFORES)
            aggregator_sources = mapped_attributes.get('biolink:aggregator_knowledge_source', [])
            
            # Create RetrievalSource objects
            sources = []
            
            # Add primary knowledge source
            if primary_source:
                sources.append(RetrievalSource(
                    id=primary_source if isinstance(primary_source, str) else TMKP_INFORES,
                    resource_id=primary_source if isinstance(primary_source, str) else TMKP_INFORES,
                    resource_role=ResourceRoleEnum.primary_knowledge_source
                ))
            
            # Add aggregator knowledge sources if any
            if aggregator_sources:
                if isinstance(aggregator_sources, str):
                    aggregator_sources = [aggregator_sources]
                for agg_source in aggregator_sources:
                    sources.append(RetrievalSource(
                        id=agg_source,
                        resource_id=agg_source,
                        resource_role=ResourceRoleEnum.aggregator_knowledge_source
                    ))
            
            # Build the association
            association_data = {
                'id': edge_id or f"{subject}-{predicate}-{object_id}",
                'subject': subject,
                'predicate': predicate,
                'object': object_id,
                'sources': sources,
                'knowledge_level': KnowledgeLevelEnum.statistical_association,
                'agent_type': AgentTypeEnum.text_mining_agent
            }
            
            # Remove knowledge source attributes from mapped_attributes since they're handled via sources
            mapped_attributes.pop('biolink:primary_knowledge_source', None)
            mapped_attributes.pop('biolink:aggregator_knowledge_source', None)

            # Add mapped attributes
            if mapped_attributes:
                for key, value in mapped_attributes.items():
                    # Remove biolink: prefix for setting attributes
                    attr_name = key.replace('biolink:', '')
                    # Check if Association has this attribute
                    if hasattr(Association, attr_name):
                        association_data[attr_name] = value
                    else:
                        koza_instance.log(f"Association does not have attribute: {attr_name}")

            # Handle qualified predicates if present
            if record.get('qualified_predicate'):
                association_data['qualified_predicate'] = record['qualified_predicate']

            # Handle qualifiers
            qualifier_fields = [
                'subject_aspect_qualifier', 'subject_direction_qualifier',
                'object_aspect_qualifier', 'object_direction_qualifier'
            ]

            qualifiers = {}
            for field in qualifier_fields:
                if record.get(field):
                    qualifiers[field] = record[field]

            if qualifiers:
                association_data['qualifiers'] = qualifiers

            try:
                association = Association(**association_data)
                edges.append(association)
                edge_count += 1
            except Exception as e:
                attribute_errors += 1
                koza_instance.log(f"Error creating association: {e}")

    koza_instance.log(f"Processed {node_count} nodes and {edge_count} edges ({attribute_errors} errors)")

    yield KnowledgeGraph(nodes=nodes, edges=edges)