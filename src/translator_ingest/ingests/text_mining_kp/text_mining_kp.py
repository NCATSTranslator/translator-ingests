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
from typing import Dict, Iterable, Any
from datetime import datetime
import os
import koza
from koza import KozaTransform
from loguru import logger
from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    ChemicalEntity,
    Disease,
    Gene,
    Protein,
    Association,
    ChemicalAffectsGeneAssociation,
    GeneToPhenotypicFeatureAssociation,
    GeneToGeneAssociation,
    GeneToDiseaseAssociation,
    RetrievalSource,
    ResourceRoleEnum,
    AgentTypeEnum,
    KnowledgeLevelEnum,
    TextMiningStudyResult,
    Study,
)
from koza.model.graphs import KnowledgeGraph
from .parsers import parse_attributes_json


# Global state for statistics tracking
processing_stats = {
    "nodes_processed": 0,
    "edges_processed": 0,
    "attribute_errors": 0,
    "extraction_start_time": None,
}


@koza.on_data_begin()
def initialize_processing(koza: KozaTransform) -> None:
    """
    Initialize processing state and log startup information.
    """
    global processing_stats
    processing_stats["extraction_start_time"] = datetime.now()
    logger.info(f"Starting Text Mining KP data processing with TMKP_INFORES: {TMKP_INFORES}")
    logger.info(f"Input files directory: {koza.input_files_dir}")


@koza.on_data_end()
def cleanup_and_report(koza: KozaTransform) -> None:
    """
    Report final statistics and cleanup resources.
    """
    global processing_stats
    if processing_stats["extraction_start_time"]:
        duration = datetime.now() - processing_stats["extraction_start_time"]
        logger.info(f"Text Mining KP processing completed in {duration.total_seconds():.2f} seconds")
    
    logger.info(
        f"Final statistics - Nodes: {processing_stats['nodes_processed']:,}, "
        f"Edges: {processing_stats['edges_processed']:,}, "
        f"Errors: {processing_stats['attribute_errors']:,}"
    )


# Constants for the Text Mining KP
TMKP_INFORES = "infores:text-mining-provider-targeted"

# Mapping from subject/predicate/object patterns to Association classes
# Based on content_metadata.json from the Text Mining KP data
SPO_TO_ASSOCIATION_MAP = {
    # ChemicalAffectsGeneAssociation patterns
    ("biolink:Protein", "biolink:affects", "biolink:SmallMolecule"): ChemicalAffectsGeneAssociation,
    ("biolink:Protein", "biolink:affects", "biolink:NamedThing"): Association,
    ("biolink:Protein", "biolink:affects", "biolink:ChemicalEntity"): ChemicalAffectsGeneAssociation,
    ("biolink:Protein", "biolink:affects", "biolink:MolecularMixture"): ChemicalAffectsGeneAssociation,
    ("biolink:Protein", "biolink:affects", "biolink:ComplexMolecularMixture"): ChemicalAffectsGeneAssociation,
    ("biolink:SmallMolecule", "biolink:affects", "biolink:Protein"): ChemicalAffectsGeneAssociation,
    ("biolink:MolecularMixture", "biolink:affects", "biolink:Protein"): ChemicalAffectsGeneAssociation,
    ("biolink:ChemicalEntity", "biolink:affects", "biolink:Protein"): ChemicalAffectsGeneAssociation,
    ("biolink:NamedThing", "biolink:affects", "biolink:Protein"): Association,
    ("biolink:NamedThing", "biolink:affects", "biolink:SmallMolecule"): ChemicalAffectsGeneAssociation,
    # GeneToGeneAssociation patterns
    ("biolink:Protein", "biolink:affects", "biolink:Protein"): GeneToGeneAssociation,
    # GeneToDiseaseAssociation patterns
    ("biolink:Disease", "biolink:contributes_to", "biolink:Protein"): GeneToDiseaseAssociation,
    ("biolink:Disease", "biolink:affects", "biolink:Protein"): GeneToDiseaseAssociation,
    ("biolink:PhenotypicFeature", "biolink:contributes_to", "biolink:Protein"): GeneToPhenotypicFeatureAssociation,
    ("biolink:PhenotypicFeature", "biolink:affects", "biolink:Protein"): GeneToPhenotypicFeatureAssociation,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:Protein"): Association,
    ("biolink:Protein", "biolink:contributes_to", "biolink:Disease"): GeneToDiseaseAssociation,
    # ChemicalToDiseaseOrPhenotypicFeatureAssociation patterns
    ("biolink:NamedThing", "biolink:treats", "biolink:SmallMolecule"): Association,
    ("biolink:Disease", "biolink:treats", "biolink:SmallMolecule"): Association,
    ("biolink:Disease", "biolink:treats", "biolink:ChemicalEntity"): Association,
    ("biolink:Disease", "biolink:contributes_to", "biolink:SmallMolecule"): Association,
    ("biolink:Disease", "biolink:contributes_to", "biolink:MolecularMixture"): Association,
    ("biolink:Disease", "biolink:treats", "biolink:NamedThing"): Association,
    ("biolink:Disease", "biolink:contributes_to", "biolink:NamedThing"): Association,
    ("biolink:Disease", "biolink:contributes_to", "biolink:ChemicalEntity"): Association,
    ("biolink:Disease", "biolink:treats", "biolink:MolecularMixture"): Association,
    ("biolink:Disease", "biolink:treats", "biolink:Protein"): Association,
    ("biolink:Disease", "biolink:treats", "biolink:ComplexMolecularMixture"): Association,
    ("biolink:Disease", "biolink:contributes_to", "biolink:ComplexMolecularMixture"): Association,
    ("biolink:PhenotypicFeature", "biolink:contributes_to", "biolink:SmallMolecule"): Association,
    ("biolink:PhenotypicFeature", "biolink:treats", "biolink:SmallMolecule"): Association,
    ("biolink:PhenotypicFeature", "biolink:contributes_to", "biolink:ChemicalEntity"): Association,
    ("biolink:PhenotypicFeature", "biolink:treats", "biolink:ChemicalEntity"): Association,
    ("biolink:PhenotypicFeature", "biolink:contributes_to", "biolink:MolecularMixture"): Association,
    ("biolink:PhenotypicFeature", "biolink:contributes_to", "biolink:NamedThing"): Association,
    ("biolink:PhenotypicFeature", "biolink:treats", "biolink:NamedThing"): Association,
    ("biolink:PhenotypicFeature", "biolink:treats", "biolink:Protein"): Association,
    ("biolink:PhenotypicFeature", "biolink:treats", "biolink:MolecularMixture"): Association,
    ("biolink:PhenotypicFeature", "biolink:contributes_to", "biolink:ComplexMolecularMixture"): Association,
    ("biolink:PhenotypicFeature", "biolink:treats", "biolink:ComplexMolecularMixture"): Association,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:SmallMolecule"): Association,
    ("biolink:NamedThing", "biolink:treats", "biolink:Disease"): Association,
    ("biolink:NamedThing", "biolink:treats", "biolink:MolecularMixture"): Association,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:NamedThing"): Association,
    ("biolink:NamedThing", "biolink:treats", "biolink:NamedThing"): Association,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:ChemicalEntity"): Association,
    ("biolink:NamedThing", "biolink:treats", "biolink:ChemicalEntity"): Association,
    ("biolink:NamedThing", "biolink:treats", "biolink:Protein"): Association,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:MolecularMixture"): Association,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:Disease"): Association,
    ("biolink:NamedThing", "biolink:treats", "biolink:ComplexMolecularMixture"): Association,
    ("biolink:NamedThing", "biolink:contributes_to", "biolink:ComplexMolecularMixture"): Association,
    ("biolink:SmallMolecule", "biolink:treats", "biolink:Disease"): Association,
    ("biolink:SmallMolecule", "biolink:treats", "biolink:PhenotypicFeature"): Association,
    ("biolink:SmallMolecule", "biolink:contributes_to", "biolink:Disease"): Association,
    ("biolink:SmallMolecule", "biolink:contributes_to", "biolink:NamedThing"): Association,
    ("biolink:MolecularMixture", "biolink:treats", "biolink:Disease"): Association,
    ("biolink:MolecularMixture", "biolink:contributes_to", "biolink:PhenotypicFeature"): Association,
    ("biolink:MolecularMixture", "biolink:contributes_to", "biolink:Disease"): Association,
    ("biolink:ChemicalEntity", "biolink:treats", "biolink:Disease"): Association,
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
    Uses environment variable if set, otherwise current date.
    """
    version = os.environ.get("TMKP_VERSION", "latest")
    if version == "latest":
        return datetime.now().strftime("%Y-%m-%d")
    return version


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
        "biolink:ChemicalEntity": ChemicalEntity,
        "biolink:Disease": Disease,
        "biolink:Gene": Gene,
        "biolink:Protein": Protein,
        "biolink:NamedThing": NamedThing,
    }

    # Get the appropriate class, default to NamedThing
    entity_class = category_mapping.get(category, NamedThing)

    # For NamedThing, we need to ensure category is exactly 'biolink:NamedThing'
    if entity_class == NamedThing and category != "biolink:NamedThing":
        category = "biolink:NamedThing"

    return entity_class(id=node_id, category=[category], name=name)


def extract_tar_gz(tar_path: str, koza: KozaTransform) -> str:
    """
    Extract tar.gz file to a temporary directory and return the path.

    Args:
        tar_path: Path to the tar.gz file
        koza: KozaTransform instance for context

    Returns:
        Path to the extracted directory
    """
    extract_dir = tempfile.mkdtemp(prefix="text_mining_kp_extract_")

    logger.info(f"Extracting {tar_path} to {extract_dir}")
    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(extract_dir)
    except Exception as e:
        logger.error(f"Failed to extract {tar_path}: {e}")
        raise

    return extract_dir



@koza.prepare_data()
def prepare_text_mining_kp_data(koza: KozaTransform, data: Iterable[Dict]) -> Iterable[Dict]:
    """
    Extract tar.gz and yield nodes and edges from KGX files.
    """
    logger.info("Preparing Text Mining KP data: extracting tar.gz")

    # Use koza's input files directory instead of hardcoded paths
    tar_path = f"{koza.input_files_dir}/targeted_assertions.tar.gz"
    
    # Check if file exists
    if not os.path.exists(tar_path):
        logger.error(f"Archive not found: {tar_path}")
        return

    # Extract the tar.gz file
    extracted_path = extract_tar_gz(tar_path, koza)

    # Find the nodes and edges files
    nodes_file = Path(extracted_path) / "nodes.tsv"
    edges_file = Path(extracted_path) / "edges.tsv"

    if not nodes_file.exists() or not edges_file.exists():
        logger.error(f"Could not find nodes.tsv or edges.tsv in {extracted_path}")
        return

    logger.info(f"Found KGX files: {nodes_file} and {edges_file}")

    # First, yield all nodes
    logger.info("Processing nodes...")
    with open(nodes_file, "r", encoding="utf-8") as f:
        header = f.readline().strip().split("\t")
        for line in f:
            fields = line.strip().split("\t")
            if len(fields) >= len(header):
                node_dict = dict(zip(header, fields))
                node_dict["_record_type"] = "node"
                yield node_dict

    # Then, yield all edges
    logger.info("Processing edges...")
    with open(edges_file, "r", encoding="utf-8") as f:
        header = f.readline().strip().split("\t")
        for line in f:
            fields = line.strip().split("\t")
            if len(fields) >= len(header):
                edge_dict = dict(zip(header, fields))
                edge_dict["_record_type"] = "edge"
                yield edge_dict


@koza.transform()
def transform_text_mining_kp(koza: KozaTransform, data: Iterable[Dict]) -> KnowledgeGraph:
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
        record_type = record.get("_record_type")

        if record_type == "node":
            # Pass through nodes as-is
            node_id = record.get("id")
            category = record.get("category", "biolink:NamedThing")
            name = record.get("name")

            if not node_id:
                continue

            # Cache node category for later association type determination
            node_categories[node_id] = category

            # Create the appropriate biolink entity based on category
            node = create_biolink_entity(node_id, category, name)

            nodes.append(node)
            node_count += 1
            processing_stats["nodes_processed"] += 1

        elif record_type == "edge":
            # Process edges with attribute extraction
            edge_id = record.get("id")
            subject = record.get("subject")
            predicate = record.get("predicate")
            object_id = record.get("object")

            if not all([subject, predicate, object_id]):
                continue

            # Parse and map attributes
            attributes_str = record.get("_attributes", "[]")
            mapped_attributes = parse_attributes_json(attributes_str, koza, record)

            # Extract knowledge source information from attributes if available
            primary_source = mapped_attributes.get("biolink:primary_knowledge_source", TMKP_INFORES)
            aggregator_sources = mapped_attributes.get("biolink:aggregator_knowledge_source", [])

            # Create RetrievalSource objects
            sources = []

            # Add primary knowledge source
            if primary_source:
                sources.append(
                    RetrievalSource(
                        id=primary_source if isinstance(primary_source, str) else TMKP_INFORES,
                        resource_id=primary_source if isinstance(primary_source, str) else TMKP_INFORES,
                        resource_role=ResourceRoleEnum.primary_knowledge_source,
                    )
                )

            # Add aggregator knowledge sources if any
            if aggregator_sources:
                if isinstance(aggregator_sources, str):
                    aggregator_sources = [aggregator_sources]
                for agg_source in aggregator_sources:
                    sources.append(
                        RetrievalSource(
                            id=agg_source,
                            resource_id=agg_source,
                            resource_role=ResourceRoleEnum.aggregator_knowledge_source,
                        )
                    )

            # Build the association
            association_data = {
                "id": edge_id or f"{subject}-{predicate}-{object_id}",
                "subject": subject,
                "predicate": predicate,
                "object": object_id,
                "sources": sources,
                "knowledge_level": KnowledgeLevelEnum.statistical_association,
                "agent_type": AgentTypeEnum.text_mining_agent,
            }

            # Remove knowledge source attributes from mapped_attributes since they're handled via sources
            mapped_attributes.pop("biolink:primary_knowledge_source", None)
            mapped_attributes.pop("biolink:aggregator_knowledge_source", None)

            # Determine the appropriate Association class based on subject/predicate/object types
            subject_category = node_categories.get(subject, "biolink:NamedThing")
            object_category = node_categories.get(object_id, "biolink:NamedThing")

            association_class = get_association_class(subject_category, predicate, object_category)

            # Add mapped attributes using correct biolink space case format
            if mapped_attributes:
                for key, value in mapped_attributes.items():
                    # Handle has_supporting_studies directly since it doesn't have biolink prefix
                    if key == "has_supporting_studies":
                        association_data[key] = value
                    else:
                        # Remove biolink: prefix to get the space case attribute name
                        attr_name = key.replace("biolink:", "")
                        # Check against the specific association class, not just base Association
                        if hasattr(association_class, attr_name):
                            association_data[attr_name] = value
                        else:
                            logger.debug(f"{association_class.__name__} does not have attribute: {attr_name}")

            # Handle qualified predicates if the association class supports it
            if record.get("qualified_predicate") and hasattr(association_class, "qualified_predicate"):
                association_data["qualified_predicate"] = record["qualified_predicate"]

            # Handle qualifier fields directly as association properties for specific association classes
            qualifier_fields = [
                "subject_aspect_qualifier",
                "subject_direction_qualifier",
                "object_aspect_qualifier",
                "object_direction_qualifier",
            ]

            for field in qualifier_fields:
                if record.get(field) and hasattr(association_class, field):
                    association_data[field] = record[field]

            try:
                association = association_class(**association_data)
                edges.append(association)
                edge_count += 1
                processing_stats["edges_processed"] += 1
            except Exception as e:
                attribute_errors += 1
                processing_stats["attribute_errors"] += 1
                logger.error(f"Error creating {association_class.__name__}: {e}")

    logger.info(f"Processed {node_count} nodes and {edge_count} edges ({attribute_errors} errors)")

    return KnowledgeGraph(nodes=nodes, edges=edges)
