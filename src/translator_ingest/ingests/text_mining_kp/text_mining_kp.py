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
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalAffectsGeneAssociation,
    GeneToGeneAssociation,
    GeneToDiseaseAssociation,
    RetrievalSource,
    ResourceRoleEnum,
    AgentTypeEnum,
    KnowledgeLevelEnum,
    TextMiningStudyResult
)
from koza.model.graphs import KnowledgeGraph


# Constants for the Text Mining KP
TMKP_INFORES = "infores:text-mining-provider-targeted"

# Mapping from subject/predicate/object patterns to Association classes
# Based on content_metadata.json from the Text Mining KP data
SPO_TO_ASSOCIATION_MAP = {
    # ChemicalAffectsGeneAssociation patterns
    ('biolink:Protein', 'biolink:affects', 'biolink:SmallMolecule'): ChemicalAffectsGeneAssociation,
    ('biolink:Protein', 'biolink:affects', 'biolink:NamedThing'): ChemicalAffectsGeneAssociation,
    ('biolink:Protein', 'biolink:affects', 'biolink:ChemicalEntity'): ChemicalAffectsGeneAssociation,
    ('biolink:Protein', 'biolink:affects', 'biolink:MolecularMixture'): ChemicalAffectsGeneAssociation,
    ('biolink:Protein', 'biolink:affects', 'biolink:ComplexMolecularMixture'): ChemicalAffectsGeneAssociation,
    ('biolink:SmallMolecule', 'biolink:affects', 'biolink:Protein'): ChemicalAffectsGeneAssociation,
    ('biolink:MolecularMixture', 'biolink:affects', 'biolink:Protein'): ChemicalAffectsGeneAssociation,
    ('biolink:ChemicalEntity', 'biolink:affects', 'biolink:Protein'): ChemicalAffectsGeneAssociation,
    ('biolink:NamedThing', 'biolink:affects', 'biolink:Protein'): ChemicalAffectsGeneAssociation,
    ('biolink:NamedThing', 'biolink:affects', 'biolink:SmallMolecule'): ChemicalAffectsGeneAssociation,
    
    # GeneRegulatesGeneAssociation patterns  
    ('biolink:Protein', 'biolink:affects', 'biolink:Protein'): GeneRegulatesGeneAssociation,
    
    # GeneToDiseaseAssociation patterns
    ('biolink:Disease', 'biolink:contributes_to', 'biolink:Protein'): GeneToDiseaseAssociation,
    ('biolink:Disease', 'biolink:affects', 'biolink:Protein'): GeneToDiseaseAssociation,
    ('biolink:PhenotypicFeature', 'biolink:contributes_to', 'biolink:Protein'): GeneToDiseaseAssociation,
    ('biolink:PhenotypicFeature', 'biolink:affects', 'biolink:Protein'): GeneToDiseaseAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:Protein'): GeneToDiseaseAssociation,
    ('biolink:Protein', 'biolink:contributes_to', 'biolink:Disease'): GeneToDiseaseAssociation,
    
    # ChemicalToDiseaseOrPhenotypicFeatureAssociation patterns
    ('biolink:NamedThing', 'biolink:treats', 'biolink:SmallMolecule'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:treats', 'biolink:SmallMolecule'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:treats', 'biolink:ChemicalEntity'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:contributes_to', 'biolink:SmallMolecule'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:contributes_to', 'biolink:MolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:treats', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:contributes_to', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:contributes_to', 'biolink:ChemicalEntity'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:treats', 'biolink:MolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:treats', 'biolink:Protein'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:treats', 'biolink:ComplexMolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:Disease', 'biolink:contributes_to', 'biolink:ComplexMolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:contributes_to', 'biolink:SmallMolecule'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:treats', 'biolink:SmallMolecule'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:contributes_to', 'biolink:ChemicalEntity'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:treats', 'biolink:ChemicalEntity'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:contributes_to', 'biolink:MolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:contributes_to', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:treats', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:treats', 'biolink:Protein'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:treats', 'biolink:MolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:contributes_to', 'biolink:ComplexMolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:PhenotypicFeature', 'biolink:treats', 'biolink:ComplexMolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:SmallMolecule'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:treats', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:treats', 'biolink:MolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:treats', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:ChemicalEntity'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:treats', 'biolink:ChemicalEntity'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:treats', 'biolink:Protein'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:MolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:treats', 'biolink:ComplexMolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:NamedThing', 'biolink:contributes_to', 'biolink:ComplexMolecularMixture'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:SmallMolecule', 'biolink:treats', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:SmallMolecule', 'biolink:treats', 'biolink:PhenotypicFeature'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:SmallMolecule', 'biolink:contributes_to', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:SmallMolecule', 'biolink:contributes_to', 'biolink:NamedThing'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:MolecularMixture', 'biolink:treats', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:MolecularMixture', 'biolink:contributes_to', 'biolink:PhenotypicFeature'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:MolecularMixture', 'biolink:contributes_to', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    ('biolink:ChemicalEntity', 'biolink:treats', 'biolink:Disease'): ChemicalToDiseaseOrPhenotypicFeatureAssociation,
}


def get_association_class(subject_category: str, predicate: str, object_category: str):
    """
    Get the appropriate Association class based on subject/predicate/object pattern.
    
    Args:
        subject_category: Subject category (e.g., 'biolink:Protein')
        predicate: Predicate (e.g., 'biolink:affects')
        object_category: Object category (e.g., 'biolink:SmallMolecule')
        
    Returns:
        The appropriate Association class, defaults to Association if no specific match
    """
    spo_key = (subject_category, predicate, object_category)
    return SPO_TO_ASSOCIATION_MAP.get(spo_key, Association)


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
        'biolink:NamedThing': NamedThing
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



def parse_attributes_json(attributes_str: str, koza_instance: KozaTransform, record: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Parse the _attributes JSON string and pass through biolink-prefixed attributes.
    Logs any attributes that are not found in the Biolink model.

    Args:
        attributes_str: JSON string containing attributes
        koza_instance: KozaTransform instance for logging
        record: The edge record being processed (for context in logging)

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
        unmapped_attributes = []

        for attr in attributes:
            if isinstance(attr, dict) and 'attribute_type_id' in attr:
                # The attributes already come with biolink: prefix
                attr_type_id = attr['attribute_type_id']
                value = attr.get('value')
                
                # Extract the attribute name without biolink: prefix for checking
                attr_name = attr_type_id.replace('biolink:', '') if attr_type_id.startswith('biolink:') else attr_type_id

                # Handle supporting_study_result with nested attributes
                if attr_type_id == 'biolink:has_supporting_study_result' and 'attributes' in attr:
                    # This is a supporting study result with nested attributes
                    study_result_data = {
                        'id': value,  # The study result ID
                    }

                    # Process nested attributes (also in TRAPI format)
                    for nested_attr in attr.get('attributes', []):
                        if isinstance(nested_attr, dict) and 'attribute_type_id' in nested_attr:
                            nested_attr_type_id = nested_attr['attribute_type_id']
                            nested_value = nested_attr.get('value')
                            nested_attr_name = nested_attr_type_id.replace('biolink:', '') if nested_attr_type_id.startswith('biolink:') else nested_attr_type_id

                            # Map text mining attributes to TextMiningStudyResult fields
                            if nested_attr_name == 'supporting_text':
                                # Convert to list if it's a string
                                if isinstance(nested_value, str):
                                    study_result_data['supporting_text'] = [nested_value]
                                else:
                                    study_result_data['supporting_text'] = nested_value
                            elif nested_attr_name == 'subject_location_in_text':
                                # Convert pipe-separated string to list of integers
                                if isinstance(nested_value, str) and '|' in nested_value:
                                    try:
                                        study_result_data['subject_location_in_text'] = [int(x) for x in nested_value.split('|')]
                                    except ValueError:
                                        study_result_data['subject_location_in_text'] = nested_value
                                else:
                                    study_result_data['subject_location_in_text'] = nested_value
                            elif nested_attr_name == 'object_location_in_text':
                                # Convert pipe-separated string to list of integers
                                if isinstance(nested_value, str) and '|' in nested_value:
                                    try:
                                        study_result_data['object_location_in_text'] = [int(x) for x in nested_value.split('|')]
                                    except ValueError:
                                        study_result_data['object_location_in_text'] = nested_value
                                else:
                                    study_result_data['object_location_in_text'] = nested_value
                            elif nested_attr_name == 'extraction_confidence_score':
                                study_result_data['extraction_confidence_score'] = nested_value
                            elif nested_attr_name == 'supporting_document_year':
                                study_result_data['supporting_document_year'] = nested_value
                            elif nested_attr_name == 'supporting_document_type':
                                study_result_data['supporting_document_type'] = nested_value
                            elif nested_attr_name == 'supporting_text_section_type':
                                study_result_data['supporting_text_section_type'] = nested_value
                            elif nested_attr_name == 'supporting_document' or nested_attr_name == 'supporting_documents':
                                # TextMiningStudyResult doesn't have a publications field, 
                                # so we skip this attribute - it should be handled at the Association level
                                continue

                    try:
                        # Create TextMiningStudyResult instance with text mining-specific fields
                        study_result_obj = TextMiningStudyResult(**study_result_data)
                        supporting_study_results.append(study_result_obj)
                    except Exception as e:
                        koza_instance.log(f"Error creating TextMiningStudyResult: {e}, data: {study_result_data}")

                    continue

                # Handle regular attributes
                if attr_type_id == 'biolink:publications' and isinstance(value, str):
                    # Handle pipe-separated publications
                    mapped_attributes[attr_type_id] = value.split('|') if '|' in value else [value]
                elif attr_type_id in ['biolink:primary_knowledge_source',
                                    'biolink:aggregator_knowledge_source']:
                    # Preserve these for creating RetrievalSource objects
                    mapped_attributes[attr_type_id] = value
                elif attr_type_id == 'biolink:supporting_data_source':
                    # This is auxiliary info, could be stored but not critical
                    continue
                elif attr_type_id in ['biolink:has_evidence_count', 'biolink:tmkp_confidence_score', 
                                    'biolink:supporting_document', 'biolink:supporting_study_result']:
                    # These attributes are not valid for Association objects, skip them
                    unmapped_attributes.append(attr_type_id)
                    continue
                else:
                    # Store the value with the biolink-prefixed name
                    mapped_attributes[attr_type_id] = value
                    
                    # Check if this attribute exists in the Association model
                    if not hasattr(Association, attr_name):
                        unmapped_attributes.append(attr_type_id)

        # Add supporting study results if any
        if supporting_study_results:
            mapped_attributes['biolink:has_supporting_study_result'] = supporting_study_results

        # Log unmapped attributes with context
        if unmapped_attributes and record:
            subject = record.get('subject', 'unknown')
            predicate = record.get('predicate', 'unknown')
            object_id = record.get('object', 'unknown')
            edge_id = record.get('id', 'unknown')
            
            koza_instance.log(
                f"Unmapped Biolink attributes for association {edge_id} "
                f"({subject} --[{predicate}]--> {object_id}): "
                f"{', '.join(unmapped_attributes)}"
            )

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

    # Path to the downloaded tar.gz file (use latest version directory)
    version = get_latest_version()
    tar_path = f"data/text_mining_kp/{version}/source_data/targeted_assertions.tar.gz"

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
    
    # Cache for node categories
    node_categories = {}

    for record in data:
        record_type = record.get('_record_type')

        if record_type == 'node':
            # Pass through nodes as-is
            node_id = record.get('id')
            category = record.get('category', 'biolink:NamedThing')
            name = record.get('name')

            if not node_id:
                continue

            # Cache node category for later association type determination
            node_categories[node_id] = category

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
            mapped_attributes = parse_attributes_json(attributes_str, koza_instance, record)

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

            # Determine the appropriate Association class based on subject/predicate/object types
            subject_category = node_categories.get(subject, 'biolink:NamedThing')
            object_category = node_categories.get(object_id, 'biolink:NamedThing')
            
            association_class = get_association_class(subject_category, predicate, object_category)
            
            # Add mapped attributes using correct biolink space case format
            if mapped_attributes:
                for key, value in mapped_attributes.items():
                    # Remove biolink: prefix to get the space case attribute name
                    attr_name = key.replace('biolink:', '')
                    # Check against the specific association class, not just base Association
                    if hasattr(association_class, attr_name):
                        association_data[attr_name] = value
                    else:
                        koza_instance.log(f"{association_class.__name__} does not have attribute: {attr_name}")

            # Handle qualified predicates if the association class supports it
            if record.get('qualified_predicate') and hasattr(association_class, 'qualified_predicate'):
                association_data['qualified_predicate'] = record['qualified_predicate']

            # Handle qualifier fields directly as association properties for specific association classes
            qualifier_fields = [
                'subject_aspect_qualifier', 'subject_direction_qualifier',
                'object_aspect_qualifier', 'object_direction_qualifier'
            ]

            for field in qualifier_fields:
                if record.get(field) and hasattr(association_class, field):
                    association_data[field] = record[field]

            try:
                association = association_class(**association_data)
                edges.append(association)
                edge_count += 1
            except Exception as e:
                attribute_errors += 1
                koza_instance.log(f"Error creating {association_class.__name__}: {e}")

    koza_instance.log(f"Processed {node_count} nodes and {edge_count} edges ({attribute_errors} errors)")

    yield KnowledgeGraph(nodes=nodes, edges=edges)
