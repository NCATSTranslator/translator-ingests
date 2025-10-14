import json
import logging
import tarfile
import tempfile
import uuid
from pathlib import Path
from typing import Any, Iterable

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    GeneToGeneAssociation,
    RetrievalSource,
    ResourceRoleEnum,
)
from koza.model.graphs import KnowledgeGraph

INFORES_GO_CAM = "infores:go-cam"
INFORES_REACTOME = "infores:reactome"

logger = logging.getLogger(__name__)


def get_latest_version() -> str:
    return "v1"


def extract_value(value):
    """Extract a single value from either a string or a list containing one string."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def normalize_id(node_id: str) -> str:
    """Remove duplicate prefixes from node IDs (e.g., MGI:MGI:1921700 -> MGI:1921700)."""
    if not node_id or ":" not in node_id:
        return node_id
    
    # Split on the first colon to get prefix and remainder
    parts = node_id.split(":", 1)
    if len(parts) != 2:
        return node_id
    
    prefix, remainder = parts
    
    # Check if remainder starts with the same prefix followed by colon
    duplicate_prefix = f"{prefix}:"
    if remainder.startswith(duplicate_prefix):
        # Remove the duplicate prefix
        return f"{prefix}:{remainder[len(duplicate_prefix):]}"
    
    return node_id


def map_causal_predicate_to_biolink(causal_predicate: str) -> str:
    """Map RO causal predicates to Biolink predicates."""
    predicate_mapping = {
        "RO:0002629": "biolink:directly_positively_regulates",  # directly positively regulates
        "RO:0002630": "biolink:directly_negatively_regulates",  # directly negatively regulates
        "RO:0002213": "biolink:positively_regulates",  # positively regulates
        "RO:0002212": "biolink:negatively_regulates",  # negatively regulates
        "RO:0002211": "biolink:regulates",  # regulates
        "RO:0002233": "biolink:has_input",  # has input
        "RO:0002234": "biolink:has_output",  # has output
    }
    return predicate_mapping.get(causal_predicate, "biolink:related_to")


def extract_tar_gz(tar_path: str) -> str:
    """Extract tar.gz file to a temporary directory and return the path."""
    extract_dir = tempfile.mkdtemp(prefix="go_cam_extract_")

    logger.info(f"Extracting {tar_path} to {extract_dir}")
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(extract_dir)

    return extract_dir


@koza.prepare_data()
def prepare_go_cam_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Extract tar.gz and yield JSON model data, filtering by taxon from configuration."""
    logger.info("Preparing GO-CAM data: extracting tar.gz and finding all JSON files...")

    # Path to the downloaded tar.gz file (from kghub-downloader)
    tar_path = "data/go_cam/go-cam-networkx.tar.gz"

    # Extract the tar.gz file
    extracted_path = extract_tar_gz(str(tar_path))

    # Find all JSON files
    json_files = list(Path(extracted_path).glob("**/*_networkx.json"))
    logger.info(f"Found {len(json_files)} networkx JSON files to process")

    # Get filter configuration from Koza's extra_fields (from YAML transform.filters)
    filters = koza.extra_fields.get("filters", [])
    target_taxa = set()

    # Extract target taxa from filter configuration
    for filter_config in filters:
        if (
            filter_config.get("column") == "taxon"
            and filter_config.get("filter_code") == "in"
            and filter_config.get("inclusion") == "include"
        ):
            target_taxa = set(filter_config.get("value", []))
            break

    logger.info(f"Filtering for taxa: {target_taxa}")

    models_processed = 0
    models_filtered = 0

    # Yield the content of each JSON file, filtering by species from config
    for json_file in json_files:
        try:
            with open(json_file, "r") as f:
                model_data = json.load(f)

            models_processed += 1

            # Extract taxon from nested structure
            taxon = model_data.get("graph", {}).get("model_info", {}).get("taxon", "")

            # Apply filtering based on configuration
            if target_taxa and taxon in target_taxa:
                model_data["taxon"] = taxon  # Expose for consistency
                model_data["_file_path"] = str(json_file)
                yield model_data
                models_filtered += 1
            elif not target_taxa:
                # No filter configured, include all
                model_data["taxon"] = taxon
                model_data["_file_path"] = str(json_file)
                yield model_data
                models_filtered += 1
            else:
                # Skip models that don't match filter
                logger.debug(f"Skipping model {Path(json_file).name} with taxon: {taxon}")

        except Exception as e:
            logger.error(f"Error reading JSON file {json_file}: {e}")

    logger.info(f"Filtered {models_filtered} models out of {models_processed} total models")


@koza.transform()
def transform_go_cam_models(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Process all GO-CAM model data with linked node/edge validation."""
    for model_data in data:
        file_path = model_data.get("_file_path", "unknown")
        model_name = Path(file_path).name

        logger.info(f"Processing model: {model_name}")

        # Get model info (filtering is now handled by Koza filters in YAML)
        model_id = model_data.get("graph", {}).get("model_info", {}).get("id", "")
        taxon = model_data.get("graph", {}).get("model_info", {}).get("taxon", "")

        # Build lookup of nodes for label/name resolution
        node_lookup = {}
        for node in model_data.get("nodes", []):
            node_id = node.get("id")
            if node_id:
                normalized_id = normalize_id(node_id)
                # Store both original and normalized for edge lookup
                node_lookup[node_id] = {"id": normalized_id, "name": node.get("label"), "taxon": taxon}
                if normalized_id != node_id:
                    node_lookup[normalized_id] = {"id": normalized_id, "name": node.get("label"), "taxon": taxon}

        # Determine knowledge sources based on model_id
        sources = []
        if model_id and "R-HSA-" in model_id:
            # Reactome model (identified by R-HSA pattern in model_id)
            primary_source = RetrievalSource(
                id=INFORES_REACTOME,
                resource_id=INFORES_REACTOME,
                resource_role=ResourceRoleEnum.primary_knowledge_source,
            )
            sources.append(primary_source)

            # Aggregator source is GO-CAM
            aggregator_source = RetrievalSource(
                id=INFORES_GO_CAM,
                resource_id=INFORES_GO_CAM,
                resource_role=ResourceRoleEnum.aggregator_knowledge_source,
            )
            sources.append(aggregator_source)
        else:
            # GO-CAM model - only primary source
            primary_source = RetrievalSource(
                id=INFORES_GO_CAM, resource_id=INFORES_GO_CAM, resource_role=ResourceRoleEnum.primary_knowledge_source
            )
            sources.append(primary_source)

        # Track nodes and edges for this model
        nodes = []
        edges = []

        # Process edges with linked validation
        for edge in model_data.get("edges", []):
            # Extract values that might be strings or lists
            source_id = extract_value(edge.get("source"))
            target_id = extract_value(edge.get("target"))
            causal_predicate = extract_value(edge.get("causal_predicate"))

            # Skip edge if missing required data
            if not all([source_id, target_id, causal_predicate]):
                continue

            # Skip edge if either node is not in our node lookup
            if source_id not in node_lookup or target_id not in node_lookup:
                logger.debug(f"Skipping edge {source_id}->{target_id}: node(s) not found in model")
                continue

            # Create gene nodes for this edge
            edge_failed = False
            for gene_id in [source_id, target_id]:
                try:
                    gene_info = node_lookup[gene_id]
                    gene_node = Gene(
                        id=gene_info["id"],
                        name=gene_info["name"],
                        category=["biolink:Gene"],
                        in_taxon=[gene_info["taxon"]] if gene_info["taxon"] else None,
                    )
                    nodes.append(gene_node)
                except Exception as e:
                    logger.error(f"Failed to create gene node {gene_id}: {e}")
                    edge_failed = True
                    break

            # Skip creating the association if any node failed
            if edge_failed:
                continue

            # Map causal predicate to biolink predicate
            biolink_predicate = map_causal_predicate_to_biolink(causal_predicate)

            # Extract publications from references
            publications = edge.get("causal_predicate_has_reference", [])
            if isinstance(publications, str):
                publications = [publications]
            if publications and isinstance(publications, list):
                publications = [pub for pub in publications if isinstance(pub, str) and pub.startswith("PMID:")]

            # Get normalized IDs for the association
            normalized_source_id = node_lookup[source_id]["id"]
            normalized_target_id = node_lookup[target_id]["id"]
            
            # Create the gene-to-gene association
            association = GeneToGeneAssociation(
                id=str(uuid.uuid4()),
                subject=normalized_source_id,
                predicate=biolink_predicate,
                object=normalized_target_id,
                original_subject=source_id,
                original_predicate=causal_predicate,
                original_object=target_id,
                publications=publications if publications else None,
                sources=sources,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )

            edges.append(association)

        # Yield a KnowledgeGraph for this model if there are any edges
        if edges:
            yield KnowledgeGraph(nodes=nodes, edges=edges)
