"""
Text Mining KP KGX Pass-through Ingest

This module provides functionality to process pre-existing KGX files from the Text Mining Knowledge Provider
through the translator-ingests pipeline. This allows for validation, normalization, and optional
transformations while maintaining the KGX format.
"""

from typing import Dict, Iterable, Optional, Any
import uuid
from datetime import datetime

from koza import KozaTransform
from biolink_model.datamodel.pydanticmodel_v4 import (
    Gene,
    Protein,
    Disease,
    ChemicalEntity,
    NamedThing,
    GeneToGeneAssociation,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    GeneToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalToGeneAssociation,
    Association,
    KnowledgeGraph
)


# Constants for the Text Mining KP
TMKP_INFORES = "infores:text-mining-provider-cooccurrence"
TMKP_VERSION = "2024-12-01"  # Update this based on actual KGX file version


def get_latest_version() -> str:
    """
    Return the version of the Text Mining KP data being processed.
    This should be updated to reflect the actual version of the KGX files.
    """
    return TMKP_VERSION


@koza.transform()
def transform_kgx_passthrough(koza: KozaTransform, data: Iterable[Dict]) -> KnowledgeGraph:
    """
    Pass through KGX data from Text Mining KP with optional processing.
    
    This function reads nodes and edges from pre-existing KGX files and:
    1. Validates the data structure
    2. Ensures Biolink compliance
    3. Optionally filters based on confidence scores or evidence
    4. Maintains provenance information
    
    Args:
        koza: The KozaTransform instance
        data: Iterable of dictionaries representing KGX records
        
    Returns:
        KnowledgeGraph containing processed nodes and edges
    """
    nodes = []
    edges = []
    
    # Statistics tracking
    stats = {
        'nodes_processed': 0,
        'edges_processed': 0,
        'nodes_filtered': 0,
        'edges_filtered': 0,
        'validation_errors': 0
    }
    
    for record in data:
        try:
            # Determine if this is a node or edge based on presence of 'subject' field
            if 'subject' in record:
                # Process edge
                edge = process_edge(record, koza)
                if edge:
                    edges.append(edge)
                    stats['edges_processed'] += 1
                else:
                    stats['edges_filtered'] += 1
            else:
                # Process node
                node = process_node(record, koza)
                if node:
                    nodes.append(node)
                    stats['nodes_processed'] += 1
                else:
                    stats['nodes_filtered'] += 1
                    
        except Exception as e:
            stats['validation_errors'] += 1
            koza.log(f"Error processing record: {e}", record=record)
            continue
    
    # Log final statistics
    koza.log(f"Processing complete. Stats: {stats}")
    
    return KnowledgeGraph(nodes=nodes, edges=edges)


def process_node(record: Dict[str, Any], koza: KozaTransform) -> Optional[NamedThing]:
    """
    Process a node record from KGX format.
    
    Validates and creates appropriate Biolink node instances based on category.
    """
    try:
        # Extract required fields
        node_id = record.get('id')
        categories = record.get('category', [])
        
        if not node_id or not categories:
            koza.log(f"Missing required fields in node: {record}")
            return None
        
        # Get the primary category (first in list)
        primary_category = categories[0] if isinstance(categories, list) else categories
        
        # Common node attributes
        node_attrs = {
            'id': node_id,
            'category': categories if isinstance(categories, list) else [categories],
            'name': record.get('name'),
            'description': record.get('description'),
            'xref': record.get('xref', []),
            'synonym': record.get('synonym', [])
        }
        
        # Create appropriate node type based on category
        if 'biolink:Gene' in categories:
            return Gene(**node_attrs)
        elif 'biolink:Protein' in categories:
            return Protein(**node_attrs)
        elif 'biolink:Disease' in categories:
            return Disease(**node_attrs)
        elif 'biolink:ChemicalEntity' in categories or 'biolink:SmallMolecule' in categories:
            return ChemicalEntity(**node_attrs)
        else:
            # Default to NamedThing for other categories
            return NamedThing(**node_attrs)
            
    except Exception as e:
        koza.log(f"Error creating node: {e}", record=record)
        return None


def process_edge(record: Dict[str, Any], koza: KozaTransform) -> Optional[Association]:
    """
    Process an edge record from KGX format.
    
    Validates and creates appropriate Biolink association instances.
    Optionally filters based on confidence scores or other criteria.
    """
    try:
        # Extract required fields
        edge_id = record.get('id')
        subject = record.get('subject')
        predicate = record.get('predicate')
        object_id = record.get('object')
        
        if not all([edge_id, subject, predicate, object_id]):
            koza.log(f"Missing required fields in edge: {record}")
            return None
        
        # Optional filtering based on confidence scores
        # (Text Mining KP may include confidence scores)
        confidence = record.get('confidence_score', 1.0)
        min_confidence = koza.get_config().get('min_confidence_threshold', 0.0)
        
        if confidence < min_confidence:
            koza.log(f"Filtering edge due to low confidence: {confidence}")
            return None
        
        # Common edge attributes
        edge_attrs = {
            'id': edge_id,
            'subject': subject,
            'predicate': predicate,
            'object': object_id,
            'knowledge_level': record.get('knowledge_level', 'statistical_association'),
            'agent_type': record.get('agent_type', 'text_mining_agent'),
            'primary_knowledge_source': record.get('primary_knowledge_source', TMKP_INFORES),
            'aggregator_knowledge_source': record.get('aggregator_knowledge_source', [])
        }
        
        # Add publications if present
        if 'publications' in record:
            edge_attrs['publications'] = record['publications']
        
        # Add any qualifiers
        if 'qualifiers' in record:
            edge_attrs['qualifiers'] = record['qualifiers']
        
        # Create generic Association
        # In a more sophisticated implementation, you might choose specific
        # association types based on subject/object categories
        return Association(**edge_attrs)
        
    except Exception as e:
        koza.log(f"Error creating edge: {e}", record=record)
        return None


@koza.on_data_begin()
def on_data_begin(koza: KozaTransform):
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza.log(f"Starting Text Mining KP pass-through processing")
    koza.log(f"Version: {get_latest_version()}")
    
    # Could add validation of KGX file structure here
    

@koza.on_data_end()
def on_data_end(koza: KozaTransform):
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza.log("Text Mining KP pass-through processing complete")