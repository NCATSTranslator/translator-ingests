import json
import gzip
import urllib.request
from pathlib import Path
from typing import Any
import uuid
import math
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    MolecularMixture,
    Drug,
    DiseaseOrPhenotypicFeature,
    Disease,
    PhenotypicFeature,
    SmallMolecule,
    ComplexMolecularMixture,
    NamedThing,
    Association,
    EntityToDiseaseAssociation,
    EntityToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
)
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import build_association_knowledge_sources

from translator_ingest.util.logging_utils import get_logger

INFORES_DAKP = "infores:multiomics-drugapprovals"

logger = get_logger(__name__)


# Helper function to create node from data
def create_node(node_data: dict) -> Any:
    node_id = node_data.get("id")
    name = node_data.get("name")
    category = node_data.get("category")

    if not category:
        return NamedThing(
            id=node_id,
            name=name,
            category=["biolink:NamedThing"]
        )

    # Map category to appropriate Pydantic class
    category_to_class = {
        "SmallMolecule": SmallMolecule,
        "MolecularMixture": MolecularMixture,
        "ChemicalEntity": ChemicalEntity,
        "ComplexMolecularMixture": ComplexMolecularMixture,
        "Drug": Drug,
        "Disease": Disease,
        "PhenotypicFeature": PhenotypicFeature,
        "DiseaseOrPhenotypicFeature": DiseaseOrPhenotypicFeature,
    }

    node_class = category_to_class.get(category)

    if node_class:
        return node_class(
            id=node_id,
            name=name,
            category=[f"biolink:{category}"]
        )
    else:
        # For unknown categories, use NamedThing as default
        logger.debug(f"Unknown category {category} for node {node_id}, using NamedThing")
        return NamedThing(
            id=node_id,
            name=name,
            category=[f"biolink:{category}"] if category else ["biolink:NamedThing"]
        )


def get_latest_version() -> str:
    """Get version from the manifest file"""
    manifest_url = "https://raw.githubusercontent.com/multiomicsKP/drug_approvals_kp/main/manifest.json"
    with urllib.request.urlopen(manifest_url) as response:
        manifest = json.load(response)

    try:
        version = manifest['version']
    except KeyError:
        raise RuntimeError('Version field could not be found in manifest file.')
    return version


@koza.on_data_begin(tag="edges")
def on_data_begin_edges(koza: koza.KozaTransform) -> None:

    # Download files
    nodes_file_path = Path(koza.input_files_dir) / "drug_approvals_kg_nodes.jsonl.gz"

    # Load all nodes into memory
    logger.info("Loading all nodes into memory...")
    nodes_lookup = {}
    node_count = 0

    with gzip.open(nodes_file_path, 'rt') as f:
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
def transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform edge records into KnowledgeGraph objects with both nodes and edges."""

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
    publications = record.get("publications", [])

    edge_props = {
        "id": record.get("id", str(uuid.uuid4())),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "publications": publications if publications else None,
        "knowledge_level": record.get("knowledge_level", KnowledgeLevelEnum.knowledge_assertion),
        "agent_type": record.get("agent_type", AgentTypeEnum.manual_agent),
    }

    # Add optional edge properties if present
    if "N_cases" in record:
        if(math.isnan(record["N_cases"])):
            edge_props["number_of_cases"] = -1
#        if(not type(record["N_cases"]) in [int]): print(f'Type: {type(record["N_cases"])} - {record["N_cases"]}, {math.isnan(record["N_cases"])}')
        else:
            edge_props["number_of_cases"] = record["N_cases"]

    # Add clinical_approval_status directly to edge properties
    # This is available on EntityToDiseaseAssociation and EntityToPhenotypicFeatureAssociation
    if "clinical_approval_status" in record:
        if(record["clinical_approval_status"]=="?"):
            edge_props["clinical_approval_status"] = "not_provided"
        else:
            edge_props["clinical_approval_status"] = record["clinical_approval_status"]
    
    # Add FDA regulatory approvals
    if "approvals" in record and record["approvals"]:
        edge_props["FDA_regulatory_approvals"] = record["approvals"]

    # Handle has_evidence field - store as publications for now
    if "has_evidence" in record and record["has_evidence"]:
        if not edge_props["publications"]:
            edge_props["publications"] = []
        edge_props["publications"].extend(record["has_evidence"])

    # Handle sources field - convert from DAKP format to proper RetrievalSource
    if "sources" in record:
        sources = []
        for source in record["sources"]:
            retrieval_source = RetrievalSource(
                id=str(uuid.uuid4()),  # Generate unique ID
                resource_id=source.get("resource_id"),
                resource_role=source.get("resource_role", "primary_knowledge_source"),
                upstream_resource_ids=source.get("upstream_resource_ids"),
                source_record_urls=source.get("source_record_urls"),
            )
            sources.append(retrieval_source)
        edge_props["sources"] = sources
    else:
        # Default to standard source if not provided
        edge_props["sources"] = build_association_knowledge_sources(primary=INFORES_DAKP)

    # Determine which association class to use based on category
    categories = record.get("category", ["biolink:Association"])
    category = categories[0] if categories else "biolink:Association"

    # Map category to the appropriate association class
    if category == "biolink:EntityToDiseaseAssociation":
        association = EntityToDiseaseAssociation(**edge_props)
    elif category == "biolink:EntityToPhenotypicFeatureAssociation":
        association = EntityToPhenotypicFeatureAssociation(**edge_props)
    else:
        # Default to generic Association for any unexpected categories
        logger.warning(f"Unexpected association category: {category}, using generic Association")
        association = Association(**edge_props)

    # Return KnowledgeGraph with both nodes and the edge
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=[association])