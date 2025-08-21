import json
import logging
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Iterable

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    NamedThing,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    GeneToGeneAssociation,
    RetrievalSource,
    ResourceRoleEnum
)

INFORES_GO_CAM = "infores:go-cam"
INFORES_REACTOME = "infores:reactome"

logger = logging.getLogger(__name__)


def extract_value(value):
    """Extract a single value from either a string or a list containing one string."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def map_causal_predicate_to_biolink(causal_predicate: str) -> str:
    """Map RO causal predicates to Biolink predicates."""
    predicate_mapping = {
        "RO:0002629": "biolink:directly_positively_regulates",  # directly positively regulates
        "RO:0002630": "biolink:directly_negatively_regulates",  # directly negatively regulates
        "RO:0002213": "biolink:positively_regulates",           # positively regulates
        "RO:0002212": "biolink:negatively_regulates",           # negatively regulates
        "RO:0002211": "biolink:regulates",                      # regulates
    }
    return predicate_mapping.get(causal_predicate, "biolink:related_to")


def extract_tar_gz(tar_path: str) -> str:
    """Extract tar.gz file to a temporary directory and return the path."""
    extract_dir = tempfile.mkdtemp(prefix="go_cam_extract_")

    logger.info(f"Extracting {tar_path} to {extract_dir}")
    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(extract_dir)

    return extract_dir



@koza.prepare_data()
def prepare_go_cam_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Extract tar.gz and yield JSON model data for processing, filtering for human and mouse only."""
    logger.info("Preparing GO-CAM data: extracting tar.gz and finding all JSON files...")

    # Path to the downloaded tar.gz file (from kghub-downloader)
    tar_path = "data/go_cam/go-cam-networkx.tar.gz"

    # Extract the tar.gz file
    extracted_path = extract_tar_gz(tar_path)

    # Find all JSON files
    json_files = list(Path(extracted_path).glob("**/*_networkx.json"))
    logger.info(f"Found {len(json_files)} networkx JSON files to process")

    # Target species for filtering
    target_taxa = {'NCBITaxon:9606', 'NCBITaxon:10090'}  # Human and Mouse
    
    models_processed = 0
    models_filtered = 0

    # Yield the content of each JSON file as a record, filtering by species
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                model_data = json.load(f)
            
            models_processed += 1
            
            # Get taxon from model_info
            taxon = model_data.get('graph', {}).get('model_info', {}).get('taxon', '')
            
            # Only process human and mouse models
            if taxon in target_taxa:
                # Add file path for reference
                model_data['_file_path'] = str(json_file)
                yield model_data
                models_filtered += 1
            else:
                # Skip non-human/mouse models
                logger.debug(f"Skipping model {Path(json_file).name} with taxon: {taxon}")
                
        except Exception as e:
            logger.error(f"Error reading JSON file {json_file}: {e}")
    
    logger.info(f"Filtered {models_filtered} human/mouse models out of {models_processed} total models")


@koza.transform()
def transform_go_cam_models(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[Any]:
    """Process all GO-CAM model data."""
    for model_data in data:
        file_path = model_data.get('_file_path', 'unknown')
        model_name = Path(file_path).name
        
        logger.info(f"Processing model: {model_name}")

        # Process edges directly from the model data
        processed_genes = set()
        
        # Get model info
        model_id = model_data.get('graph', {}).get('model_info', {}).get('id', '')

        # Determine knowledge sources based on model_id
        sources = []
        if model_id and 'R-HSA-' in model_id:
            # Reactome model (identified by R-HSA pattern in model_id)
            # Primary source is Reactome
            primary_source = RetrievalSource(
                id=INFORES_REACTOME,
                resource_id=INFORES_REACTOME,
                resource_role=ResourceRoleEnum.primary_knowledge_source
            )
            sources.append(primary_source)
            
            # Aggregator source is GO-CAM
            aggregator_source = RetrievalSource(
                id=INFORES_GO_CAM,
                resource_id=INFORES_GO_CAM,
                resource_role=ResourceRoleEnum.aggregator_knowledge_source
            )
            sources.append(aggregator_source)
        else:
            # GO-CAM model - only primary source
            primary_source = RetrievalSource(
                id=INFORES_GO_CAM,
                resource_id=INFORES_GO_CAM,
                resource_role=ResourceRoleEnum.primary_knowledge_source
            )
            sources.append(primary_source)

        # Process edges directly from the networkx JSON
        for edge in model_data.get('edges', []):
            # Extract values that might be strings or lists
            source_id = extract_value(edge.get('source'))
            target_id = extract_value(edge.get('target'))
            causal_predicate = extract_value(edge.get('causal_predicate'))

            if not all([source_id, target_id, causal_predicate]):
                continue

            # Create Gene nodes if not already processed
            for gene_id in [source_id, target_id]:
                if gene_id not in processed_genes:
                    # Find the corresponding node data for the label
                    node_data = next((n for n in model_data.get('nodes', []) if n.get('id') == gene_id), {})
                    gene_node = Gene(
                        id=gene_id,
                        name=node_data.get('label'),
                        category=["biolink:Gene"]
                    )
                    yield gene_node
                    processed_genes.add(gene_id)

            # Map causal predicate to biolink predicate
            biolink_predicate = map_causal_predicate_to_biolink(causal_predicate)

            # Extract publications from references - this field is always expected to be a list
            publications = edge.get('causal_predicate_has_reference', [])
            # Ensure publications is a list, handle case where it might be a single string
            if isinstance(publications, str):
                publications = [publications]
            if publications and isinstance(publications, list):
                publications = [pub for pub in publications if isinstance(pub, str) and pub.startswith('PMID:')]

            # Create the gene-to-gene association
            association = GeneToGeneAssociation(
                id=f"gocam:{model_id}:{source_id}-{biolink_predicate.replace('biolink:', '')}-{target_id}",
                subject=source_id,
                predicate=biolink_predicate,
                object=target_id,
                original_subject=source_id,
                original_predicate=causal_predicate,
                original_object=target_id,
                publications=publications if publications else None,
                sources=sources,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )

            yield association
