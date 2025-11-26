import json
import gzip
import logging
import urllib.request
from datetime import datetime
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
    ChemicalMixture,
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
from bmt.pydantic import build_association_knowledge_sources


INFORES_CTKP = "infores:multiomics-clinicaltrials"

logger = logging.getLogger(__name__)


# Helper function to create node from data
def create_node(node_data: dict) -> Any:
    node_id = node_data.get("id")
    name = node_data.get("name")
    categories = node_data.get("category", [])

    if not categories:
        return NamedThing(
            id=node_id,
            name=name,
            category=["biolink:NamedThing"]
        )

    category = categories[0]

    # Special handling for clinical trial nodes
    if node_id.startswith("CLINICALTRIALS:"):
        # Convert age boolean properties to multivalued age_stage
        age_stages = []
        if node_data.get("clinical_trial_child", False):
            age_stages.append("child")
        if node_data.get("clinical_trial_adult", False):
            age_stages.append("adult")
        if node_data.get("clinical_trial_older_adult", False):
            age_stages.append("older_adult")

        return ClinicalTrial(
            id=node_id,
            name=name,
            category=["biolink:ClinicalTrial"],
            clinical_trial_phase=node_data.get("clinical_trial_phase"),
            clinical_trial_tested_intervention=node_data.get("clinical_trial_tested_intervention"),
            clinical_trial_overall_status=node_data.get("clinical_trial_overall_status"),
            # clinical_trial_start_date=node_data.get("clinical_trial_start_date"),  # Commented out due to incomplete date formats
            clinical_trial_enrollment=node_data.get("clinical_trial_enrollment"),
            clinical_trial_enrollment_type=node_data.get("clinical_trial_enrollment_type"),
            clinical_trial_age_range=node_data.get("clinical_trial_age_range"),
            clinical_trial_age_stage=age_stages if age_stages else None,
            clinical_trial_primary_purpose=node_data.get("clinical_trial_primary_purpose"),
            clinical_trial_intervention_model=node_data.get("clinical_trial_intervention_model"),
        )

    # Map category to appropriate Pydantic class
    category_to_class = {
        "biolink:SmallMolecule": SmallMolecule,
        "biolink:MolecularMixture": MolecularMixture,
        "biolink:ChemicalEntity": ChemicalEntity,
        "biolink:Protein": Protein,
        "biolink:Drug": Drug,
        "biolink:ComplexMolecularMixture": ComplexMolecularMixture,
        "biolink:ChemicalMixture": ChemicalMixture,
        "biolink:Disease": Disease,
        "biolink:PhenotypicFeature": PhenotypicFeature,
        "biolink:DiseaseOrPhenotypicFeature": DiseaseOrPhenotypicFeature,
        "biolink:OrganismTaxon": OrganismTaxon,
    }

    node_class = category_to_class.get(category)

    if node_class:
        return node_class(
            id=node_id,
            name=name,
            category=categories
        )
    else:
        # For unknown categories, use NamedThing as default
        logger.debug(f"Unknown category {category} for node {node_id}, using NamedThing")
        return NamedThing(
            id=node_id,
            name=name,
            category=categories
        )


def get_latest_version() -> str:
    """Get version from the manifest file."""
    # This function is called by the pipeline before Koza context is available.
    # The actual version will be extracted from the manifest during prepare_data.
    # Return a placeholder that will be replaced with the actual version.
    return "pending"


@koza.prepare_data(tag="edges")
def prepare_edges_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Download files and prepare data for processing."""

    # Read manifest to get version
    manifest_path = Path(koza.input_files_dir) / "manifest.json"
    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    # Get version directly from the version property
    version = manifest.get("version", "unknown")

    logger.info(f"Using CTKP version: {version}")

    # Store the actual version in Koza state so it can be used by the pipeline
    koza.state["actual_version"] = version

    # Also store in transform metadata so it's accessible after transform completes
    koza.transform_metadata["actual_version"] = version

    # Construct JSONL.gz URLs
    nodes_jsonl_url = f"https://db.systemsbiology.net/gestalt/KG/clinical_trials_kg_nodes_v{version}.jsonl.gz"
    edges_jsonl_url = f"https://db.systemsbiology.net/gestalt/KG/clinical_trials_kg_edges_v{version}.jsonl.gz"

    # Download files
    nodes_file_path = Path(koza.input_files_dir) / "clinical_trials_kg_nodes.jsonl.gz"
    edges_file_path = Path(koza.input_files_dir) / "clinical_trials_kg_edges.jsonl.gz"

    logger.info(f"Downloading nodes from {nodes_jsonl_url}")
    urllib.request.urlretrieve(nodes_jsonl_url, nodes_file_path)

    logger.info(f"Downloading edges from {edges_jsonl_url}")
    urllib.request.urlretrieve(edges_jsonl_url, edges_file_path)

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

    # Yield edges for processing
    with gzip.open(edges_file_path, 'rt') as f:
        for line in f:
            if line.strip():
                edge = json.loads(line)
                yield edge


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
    qualifiers = record.get("qualifiers", [])

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

    # Handle has_supporting_studies - retrieve actual node objects
    if "has_supporting_studies" in record:
        supporting_studies_ids = record["has_supporting_studies"]
        if isinstance(supporting_studies_ids, list):
            # Convert list of IDs to dict of ClinicalTrial objects
            supporting_studies = {}
            for study_id in supporting_studies_ids:
                if study_id in nodes_lookup:
                    node_data = nodes_lookup[study_id]
                    # Create ClinicalTrial object directly since these are clinical trial IDs
                    if study_id.startswith("CLINICALTRIALS:"):
                        # Convert age boolean properties to multivalued age_stage
                        age_stages = []
                        if node_data.get("clinical_trial_child", False):
                            age_stages.append("child")
                        if node_data.get("clinical_trial_adult", False):
                            age_stages.append("adult")
                        if node_data.get("clinical_trial_older_adult", False):
                            age_stages.append("older_adult")

                        clinical_trial = ClinicalTrial(
                            id=study_id,
                            name=node_data.get("name"),
                            category=["biolink:ClinicalTrial"],
                            clinical_trial_phase=node_data.get("clinical_trial_phase"),
                            clinical_trial_tested_intervention=node_data.get("clinical_trial_tested_intervention"),
                            clinical_trial_overall_status=node_data.get("clinical_trial_overall_status"),
                            # clinical_trial_start_date=node_data.get("clinical_trial_start_date"),  # Commented out due to incomplete date formats
                            clinical_trial_enrollment=node_data.get("clinical_trial_enrollment"),
                            clinical_trial_enrollment_type=node_data.get("clinical_trial_enrollment_type"),
                            clinical_trial_age_range=node_data.get("clinical_trial_age_range"),
                            clinical_trial_age_stage=age_stages if age_stages else None,
                            clinical_trial_primary_purpose=node_data.get("clinical_trial_primary_purpose"),
                            clinical_trial_intervention_model=node_data.get("clinical_trial_intervention_model"),
                        )
                        supporting_studies[study_id] = clinical_trial
                    else:
                        logger.warning(f"Unexpected non-clinical trial ID in has_supporting_studies: {study_id}")
                else:
                    logger.warning(f"Clinical trial {study_id} not found in nodes lookup")
            if supporting_studies:
                edge_props["has_supporting_studies"] = supporting_studies
        else:
            # If it's already a dict, pass it through
            edge_props["has_supporting_studies"] = record["has_supporting_studies"]

    # Handle sources field - convert from CTKP format to proper RetrievalSource
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
        edge_props["sources"] = build_association_knowledge_sources(primary=INFORES_CTKP)

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
