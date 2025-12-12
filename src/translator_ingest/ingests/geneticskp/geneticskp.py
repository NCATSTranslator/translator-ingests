"""
GeneticsKP ingest.

This ingest processes KGX data from GeneticsKP MAGMA analysis.
The data comes as a tar.gz file containing:
- edges_geneticsKP_magma.jsonl: Edge information 
- nodes_geneticsKP_magma.jsonl: Node information

This ingest validates and transforms the KGX data through Pydantic classes.
"""

import json
import gzip
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Iterable
import uuid
from datetime import datetime
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Disease,
    PhenotypicFeature,
    NamedThing,
    Pathway,
    BiologicalProcess,
    MolecularActivity,
    GeneToDiseaseAssociation,
    GeneToPhenotypicFeatureAssociation,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import build_association_knowledge_sources

from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.biolink import get_current_biolink_version

INFORES_GENETICSKP = "infores:geneticskp"

logger = get_logger(__name__)


def get_latest_version() -> str:
    """Return the latest version of GeneticsKP data.
    
    Uses the current date as the version since we're using a static file.
    """
    return datetime.now().strftime("%Y-%m-%d")


def create_node(node_data: dict) -> Any:
    """Create appropriate Biolink node from node data."""
    node_id = node_data.get("id")
    name = node_data.get("name")
    categories = node_data.get("category", [])
    
    if not categories:
        logger.warning(f"No category found for node {node_id}, using NamedThing")
        return NamedThing(
            id=node_id,
            name=name,
            category=["biolink:NamedThing"]
        )
    
    # Get the primary category
    category = categories[0] if isinstance(categories, list) else categories
    
    # Map category to appropriate Pydantic class
    if "Gene" in category:
        return Gene(
            id=node_id,
            name=name,
            category=categories if isinstance(categories, list) else [categories]
        )
    elif "Disease" in category:
        return Disease(
            id=node_id,
            name=name,
            category=categories if isinstance(categories, list) else [categories]
        )
    elif "PhenotypicFeature" in category:
        return PhenotypicFeature(
            id=node_id,
            name=name,
            category=categories if isinstance(categories, list) else [categories]
        )
    elif "Pathway" in category:
        return Pathway(
            id=node_id,
            name=name,
            category=categories if isinstance(categories, list) else [categories]
        )
    elif "BiologicalProcess" in category:
        return BiologicalProcess(
            id=node_id,
            name=name,
            category=categories if isinstance(categories, list) else [categories]
        )
    elif "MolecularActivity" in category:
        return MolecularActivity(
            id=node_id,
            name=name,
            category=categories if isinstance(categories, list) else [categories]
        )
    else:
        # For unknown categories, try to use NamedThing but with valid category
        logger.debug(f"Unknown category {category} for node {node_id}, using NamedThing")
        return NamedThing(
            id=node_id,
            name=name,
            category=["biolink:NamedThing"]  # Always use valid NamedThing category
        )


@koza.on_data_begin(tag="edges")
def on_data_begin_edges(koza: koza.KozaTransform) -> None:
    """Extract tar.gz and load all nodes into memory before processing edges."""
    
    # First extract the tar.gz if it exists
    tar_path = Path(koza.input_files_dir) / "genetics_magma.tar.gz"
    if tar_path.exists():
        logger.info(f"Extracting {tar_path} to {koza.input_files_dir}")
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(koza.input_files_dir)
        logger.info("Extraction complete")
    
    nodes_file_path = Path(koza.input_files_dir) / "nodes_geneticsKP_magma.jsonl"
    
    # Check if file exists - if not, it might be gzipped
    if not nodes_file_path.exists():
        nodes_file_path = Path(koza.input_files_dir) / "nodes_geneticsKP_magma.jsonl.gz"
    
    logger.info("Loading all nodes into memory...")
    nodes_lookup = {}
    node_count = 0
    
    if nodes_file_path.suffix == '.gz':
        opener = gzip.open
        mode = 'rt'
    else:
        opener = open
        mode = 'r'
    
    with opener(nodes_file_path, mode) as f:
        for line in f:
            if line.strip():
                node = json.loads(line)
                node_id = node.get("id")
                if node_id:
                    nodes_lookup[node_id] = node
                    node_count += 1
    
    logger.info(f"Loaded {node_count} nodes into memory")
    koza.state["nodes_lookup"] = nodes_lookup


@koza.transform_record(tag="edges")
def transform(koza: koza.KozaTransform, record: Dict[str, Any]) -> Optional[KnowledgeGraph]:
    """Transform edge records into KnowledgeGraph objects with validated nodes and edges."""
    
    # Get edge properties
    subject_id = record.get("subject")
    object_id = record.get("object")
    predicate = record.get("predicate")
    
    if not all([subject_id, object_id, predicate]):
        logger.warning(f"Skipping edge missing required fields: {record}")
        return None
    
    # Get nodes lookup from state
    nodes_lookup = koza.state.get("nodes_lookup", {})
    
    # Look up subject and object nodes
    subject_node_data = nodes_lookup.get(subject_id)
    object_node_data = nodes_lookup.get(object_id)
    
    if not subject_node_data or not object_node_data:
        logger.warning(f"Skipping edge - missing node data for {subject_id} or {object_id}")
        return None
    
    # Create subject and object nodes
    subject_node = create_node(subject_node_data)
    object_node = create_node(object_node_data)
    
    # Build edge properties
    edge_props = {
        "id": record.get("id", str(uuid.uuid4())),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "knowledge_level": record.get("knowledge_level", KnowledgeLevelEnum.statistical_association),
        "agent_type": record.get("agent_type", AgentTypeEnum.computational_model),
    }
    
    # Add optional properties if present
    if "publications" in record:
        edge_props["publications"] = record["publications"]
    
    # Add attributes like p-value, z-score, etc if present
    if "has_attribute" in record:
        edge_props["has_attribute"] = record["has_attribute"]
    
    # Handle qualifiers if present
    for qualifier_key in ["subject_aspect_qualifier", "subject_direction_qualifier", 
                          "object_aspect_qualifier", "object_direction_qualifier",
                          "qualified_predicate"]:
        if qualifier_key in record:
            edge_props[qualifier_key] = record[qualifier_key]
    
    # Handle sources - if not provided, use default
    if "sources" in record:
        edge_props["sources"] = record["sources"]
    else:
        edge_props["sources"] = build_association_knowledge_sources(primary=INFORES_GENETICSKP)
    
    # Determine association type based on predicate and node types
    categories = record.get("category", ["biolink:Association"])
    category = categories[0] if isinstance(categories, list) else categories
    
    # Create appropriate association based on category or predicate
    if "genetically_associated_with" in predicate:
        if isinstance(object_node, Disease):
            association = GeneToDiseaseAssociation(**edge_props)
        elif isinstance(object_node, PhenotypicFeature):
            association = GeneToPhenotypicFeatureAssociation(**edge_props)
        else:
            association = Association(**edge_props)
    elif category == "biolink:GeneToDiseaseAssociation":
        association = GeneToDiseaseAssociation(**edge_props)
    elif category == "biolink:GeneToPhenotypicFeatureAssociation":
        association = GeneToPhenotypicFeatureAssociation(**edge_props)
    else:
        # Default to generic Association
        association = Association(**edge_props)
    
    # Return KnowledgeGraph with both nodes and the edge
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=[association])