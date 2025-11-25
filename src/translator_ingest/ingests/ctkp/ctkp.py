import json
import gzip
import logging
import urllib.request
from pathlib import Path
from typing import Any, Iterable
import uuid

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    MolecularMixture,
    DiseaseOrPhenotypicFeature,
    Disease,
    PhenotypicFeature,
    ClinicalTrial,
    SmallMolecule,
    Protein,
    Drug,
    ComplexMolecularMixture,
    OrganismTaxon,
    NamedThing,
    Association,
    EntityToDiseaseAssociation,
    EntityToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
)
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import build_association_knowledge_sources

INFORES_CTKP = "infores:multiomics-clinicaltrials"

logger = logging.getLogger(__name__)


def get_latest_version() -> str:
    """Get version from the manifest file."""
    # This function is called by the pipeline before Koza context is available.
    # The actual version will be extracted from the manifest during prepare_data.
    # Return a placeholder that will be replaced with the actual version.
    return "pending"



@koza.prepare_data(tag="nodes")
def prepare_nodes_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Download and prepare nodes data based on manifest version."""

    # Read manifest to get version
    manifest_path = Path(koza.input_files_dir) / "manifest.json"
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    # Get version directly from the version property
    version = manifest.get("version", "unknown")
    
    # Store version in koza.state
    koza.state["ctkp_version"] = version
    
    logger.info(f"Using CTKP version: {version}")

    # Construct JSONL.gz URLs
    nodes_jsonl_url = f"https://db.systemsbiology.net/gestalt/KG/clinical_trials_kg_nodes_v{version}.jsonl.gz"
    edges_jsonl_url = f"https://db.systemsbiology.net/gestalt/KG/clinical_trials_kg_edges_v{version}.jsonl.gz"

    # Download nodes file
    nodes_file_path = Path(koza.input_files_dir) / "clinical_trials_kg_nodes.jsonl.gz"
    logger.info(f"Downloading nodes from {nodes_jsonl_url}")
    urllib.request.urlretrieve(nodes_jsonl_url, nodes_file_path)

    # Also download edges file for later use
    edges_file_path = Path(koza.input_files_dir) / "clinical_trials_kg_edges.jsonl.gz"
    logger.info(f"Downloading edges from {edges_jsonl_url}")
    urllib.request.urlretrieve(edges_jsonl_url, edges_file_path)

    # Store node information in state for edge processing
    koza.state["nodes_lookup"] = {}

    # Read and yield nodes data
    with gzip.open(nodes_file_path, 'rt') as f:
        for line in f:
            if line.strip():
                node = json.loads(line)
                # Store node info for edge processing
                koza.state["nodes_lookup"][node.get("id")] = node
                yield node


@koza.prepare_data(tag="edges")
def prepare_edges_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Read edges data from downloaded file."""

    edges_file_path = Path(koza.input_files_dir) / "clinical_trials_kg_edges.jsonl.gz"

    # Read and yield edges data
    with gzip.open(edges_file_path, 'rt') as f:
        for line in f:
            if line.strip():
                edge = json.loads(line)
                yield edge


@koza.transform_record(tag="nodes")
def transform_nodes(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform node records, especially handling clinical trial nodes."""

    node_id = record.get("id")
    if not node_id:
        return None

    # Get node categories
    categories = record.get("category", [])
    if not categories:
        # If no category, create a NamedThing node
        logger.debug(f"Node {node_id} has no category, creating NamedThing")
        node = NamedThing(
            id=node_id,
            name=record.get("name"),
            category=["biolink:NamedThing"]
        )
        return KnowledgeGraph(nodes=[node], edges=[])
    
    category = categories[0]  # Use first category for determining node type
    
    # Check if this is a clinical trial node
    if node_id.startswith("CLINICALTRIALS:"):
        # Convert age boolean properties to multivalued age_stage
        age_stages = []
        if record.get("clinical_trial_child", False):
            age_stages.append("child")
        if record.get("clinical_trial_adult", False):
            age_stages.append("adult")
        if record.get("clinical_trial_older_adult", False):
            age_stages.append("older_adult")
        
        # Create ClinicalTrial node
        node = ClinicalTrial(
            id=node_id,
            name=record.get("name"),
            category=["biolink:ClinicalTrial"],  # Add missing category
            clinical_trial_phase=record.get("clinical_trial_phase"),
            clinical_trial_tested_intervention=record.get("clinical_trial_tested_intervention"),
            clinical_trial_overall_status=record.get("clinical_trial_overall_status"),
            clinical_trial_start_date=record.get("clinical_trial_start_date"),
            clinical_trial_enrollment=record.get("clinical_trial_enrollment"),
            clinical_trial_enrollment_type=record.get("clinical_trial_enrollment_type"),
            clinical_trial_age_range=record.get("clinical_trial_age_range"),
            clinical_trial_age_stage=age_stages if age_stages else None,
            clinical_trial_primary_purpose=record.get("clinical_trial_primary_purpose"),
            clinical_trial_intervention_model=record.get("clinical_trial_intervention_model"),
        )
        
        # Store the transformed node data back in the lookup with ALL properties
        transformed_node_data = {
            "id": node_id,
            "name": record.get("name"),
            "category": ["biolink:ClinicalTrial"],
            "clinical_trial_phase": record.get("clinical_trial_phase"),
            "clinical_trial_tested_intervention": record.get("clinical_trial_tested_intervention"),
            "clinical_trial_overall_status": record.get("clinical_trial_overall_status"),
            "clinical_trial_start_date": record.get("clinical_trial_start_date"),
            "clinical_trial_enrollment": record.get("clinical_trial_enrollment"),
            "clinical_trial_enrollment_type": record.get("clinical_trial_enrollment_type"),
            "clinical_trial_age_range": record.get("clinical_trial_age_range"),
            "clinical_trial_age_stage": age_stages if age_stages else None,
            "clinical_trial_primary_purpose": record.get("clinical_trial_primary_purpose"),
            "clinical_trial_intervention_model": record.get("clinical_trial_intervention_model"),
            "interventions": record.get("interventions"),
            "conditions": record.get("conditions"),
        }
        koza.state["nodes_lookup"][node_id] = transformed_node_data
    
    else:
        # Map category to appropriate Pydantic class
        category_to_class = {
            "biolink:SmallMolecule": SmallMolecule,
            "biolink:MolecularMixture": MolecularMixture,
            "biolink:ChemicalEntity": ChemicalEntity,
            "biolink:Protein": Protein,
            "biolink:Drug": Drug,
            "biolink:ComplexMolecularMixture": ComplexMolecularMixture,
            "biolink:Disease": Disease,
            "biolink:PhenotypicFeature": PhenotypicFeature,
            "biolink:DiseaseOrPhenotypicFeature": DiseaseOrPhenotypicFeature,
            "biolink:OrganismTaxon": OrganismTaxon,
            "biolink:ChemicalMixture": ChemicalEntity,  # ChemicalMixture -> ChemicalEntity
        }
        
        node_class = category_to_class.get(category)
        
        if node_class:
            node = node_class(
                id=node_id,
                name=record.get("name"),
                category=categories
            )
        else:
            # For unknown categories, use ChemicalEntity as default
            logger.debug(f"Unknown category {category} for node {node_id}, using ChemicalEntity")
            node = ChemicalEntity(
                id=node_id,
                name=record.get("name"),
                category=categories
            )
    
    return KnowledgeGraph(nodes=[node], edges=[])


@koza.transform_record(tag="edges")
def transform_edges(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform edge records into KnowledgeGraph objects."""

    # Get edge properties
    subject_id = record.get("subject")
    object_id = record.get("object")
    predicate = record.get("predicate")

    if not all([subject_id, object_id, predicate]):
        logger.warning(f"Skipping edge missing required fields: {record}")
        return None

    # Look up nodes from state
    nodes_lookup = koza.state.get("nodes_lookup", {})
    subject_node_data = nodes_lookup.get(subject_id)
    object_node_data = nodes_lookup.get(object_id)

    if not subject_node_data or not object_node_data:
        logger.warning(f"Skipping edge - missing node data for {subject_id} or {object_id}")
        return None

    # Create node objects based on categories
    nodes = []

    # Create subject node (intervention - chemical or molecular mixture)
    subject_categories = subject_node_data.get("category", [])
    if "biolink:MolecularMixture" in subject_categories:
        subject_node = MolecularMixture(
            id=subject_id,
            name=subject_node_data.get("name"),
            category=subject_categories
        )
    else:
        # Default to ChemicalEntity
        subject_node = ChemicalEntity(
            id=subject_id,
            name=subject_node_data.get("name"),
            category=subject_categories or ["biolink:ChemicalEntity"]
        )
    nodes.append(subject_node)

    # Create object node (disease/phenotype)
    object_node = DiseaseOrPhenotypicFeature(
        id=object_id,
        name=object_node_data.get("name"),
        category=object_node_data.get("category", ["biolink:DiseaseOrPhenotypicFeature"])
    )
    nodes.append(object_node)

    # Get association properties from the edge record
    publications = record.get("publications", [])
    qualifiers = record.get("qualifiers", [])
    
    # Map CTKP edge properties to Biolink properties
    # Note: elevate_to_prediction is not in Biolink and will be excluded
    edge_props = {
        "id": record.get("id", str(uuid.uuid4())),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "publications": publications if publications else None,
        "qualifiers": qualifiers if qualifiers else None,
        "knowledge_level": record.get("knowledge_level", KnowledgeLevelEnum.knowledge_assertion),
        "agent_type": record.get("agent_type", AgentTypeEnum.manual_agent),
    }
    
    # Add optional edge properties if present
    if "max_research_phase" in record:
        edge_props["max_research_phase"] = record["max_research_phase"]
    if "has_supporting_studies" in record:
        edge_props["has_supporting_studies"] = record["has_supporting_studies"]
    if "tested_intervention" in record:
        edge_props["clinical_trial_tested_intervention"] = record["tested_intervention"]
    
    # Handle sources field - convert from CTKP format to proper RetrievalSource
    if "sources" in record:
        sources = []
        for source in record["sources"]:
            retrieval_source = RetrievalSource(
                resource_id=source.get("resource_id"),
                resource_role=source.get("resource_role", "primary_knowledge_source"),
                upstream_resource_ids=source.get("upstream_resource_ids"),
                source_record_urls=source.get("source_record_urls"),
            )
            sources.append(retrieval_source)
        edge_props["sources"] = sources
    else:
        # Default to standard source if not provided
        edge_props["sources"] = build_association_knowledge_sources(primary=INFORES_CTKP)

    # Determine which association class to use based on category
    categories = record.get("category", ["biolink:Association"])
    category = categories[0] if categories else "biolink:Association"
    
    # Map category to the appropriate association class
    # Based on analysis of CTKP data, there are only two categories:
    # - biolink:EntityToDiseaseAssociation (368,424 occurrences)
    # - biolink:EntityToPhenotypicFeatureAssociation (53,813 occurrences)
    if category == "biolink:EntityToDiseaseAssociation":
        association = EntityToDiseaseAssociation(**edge_props)
    elif category == "biolink:EntityToPhenotypicFeatureAssociation":
        association = EntityToPhenotypicFeatureAssociation(**edge_props)
    else:
        # Default to generic Association for any unexpected categories
        logger.warning(f"Unexpected association category: {category}, using generic Association")
        association = Association(**edge_props)

    return KnowledgeGraph(nodes=nodes, edges=[association])
