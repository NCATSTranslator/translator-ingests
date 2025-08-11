import uuid
import os
import json
from pathlib import Path
from typing import Iterable, Any, Tuple

import requests
import ijson
import koza
from kghub_downloader import index_based_download
from gocam.translation.networkx import ModelNetworkTranslator

from biolink_model.datamodel.pydanticmodel_v2 import (
    Entity,
    Gene,
    BiologicalProcessOrActivity,
    MolecularActivity,
    Association,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    GeneToGeneAssociation
)

INFORES_GO_CAM = "infores:go-cam"

# Create a persistent session for connection pooling
session = requests.Session()
session.headers.update({'User-Agent': 'GO-CAM-Ingest/1.0'})

def extract_curie_prefix(curie: str) -> str:
    """Extract the prefix from a CURIE (e.g., 'GO' from 'GO:0003674')."""
    return curie.split(':')[0] if ':' in curie else ''

def determine_node_category(entity_id: str, entity_type: str = None) -> list:
    """Determine the biolink category for an entity based on its ID prefix."""
    prefix = extract_curie_prefix(entity_id)
    return ["biolink:Gene"]

def get_entity_class_and_category(entity_id: str, entity_type: str = None):
    """Get the appropriate biolink class and category for an entity."""
    return Gene, ["biolink:Gene"]


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


def download_all_gocam_models(output_dir: str = "data/go_cam") -> list[str]:
    """Download all GO-CAM models using kghub-downloader index_based_download."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Define the index configuration for GO-CAM models
    index_config = {
        "index_url": "https://go-data-product-live-go-cam.s3.us-east-1.amazonaws.com/product/json/provider-to-model.json",
        "url_pattern": "https://go-data-product-live-go-cam.s3.us-east-1.amazonaws.com/product/json/{ID}.json",
        "id_path": "",
        "local_name_pattern": "{ID}.json"
    }
    
    # Download all models
    downloaded_files = index_based_download(
        index_url=index_config["index_url"],
        url_pattern=index_config["url_pattern"],
        output_dir=output_dir,
        local_name_pattern=index_config["local_name_pattern"]
    )
    
    return downloaded_files


def process_single_model(model_file_path: str) -> Tuple[list[NamedThing], list[Association]]:
    """Process a single GO-CAM model file and extract gene-gene relationships."""
    nodes = []
    associations = []
    
    try:
        with open(model_file_path, 'r') as f:
            model_data = json.load(f)
        
        # Use ModelNetworkTranslator to extract gene2gene relationships
        translator = ModelNetworkTranslator()
        gene_edges = translator.translate_model_to_gene_gene_edges(model_data)
        
        # Track processed genes to avoid duplicates
        processed_genes = set()
        
        for edge in gene_edges:
            source_id = edge.get('source')
            target_id = edge.get('target')
            causal_predicate = edge.get('causal_predicate')
            model_id = edge.get('model_id')
            
            if not all([source_id, target_id, causal_predicate]):
                continue
                
            # Create Gene nodes if not already processed
            for gene_id in [source_id, target_id]:
                if gene_id not in processed_genes:
                    gene_node = Gene(
                        id=gene_id,
                        category=["biolink:Gene"]
                    )
                    nodes.append(gene_node)
                    processed_genes.add(gene_id)
            
            # Map causal predicate to biolink predicate
            biolink_predicate = map_causal_predicate_to_biolink(causal_predicate)
            
            # Create the gene-to-gene association
            association = GeneToGeneAssociation(
                id=f"gocam:{uuid.uuid4()}",
                subject=source_id,
                predicate=biolink_predicate,
                object=target_id,

                primary_knowledge_source=INFORES_GO_CAM,
                aggregator_knowledge_source=["infores:translator-gocam-kgx"],
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                # Add edge properties from the original edge
                **{k: v for k, v in edge.items() if k not in ['source', 'target', 'causal_predicate']}
            )
            
            associations.append(association)
            
    except Exception as e:
        print(f"Error processing model {model_file_path}: {e}")
        
    return nodes, associations


def process_all_gocam_models(model_dir: str = "data/go_cam") -> Tuple[list[NamedThing], list[Association]]:
    """Process all downloaded GO-CAM models."""
    all_nodes = []
    all_associations = []
    processed_genes = set()
    
    model_files = list(Path(model_dir).glob("*.json"))
    
    for model_file in model_files:
        print(f"Processing {model_file}")
        nodes, associations = process_single_model(str(model_file))
        
        # Add nodes, avoiding duplicates
        for node in nodes:
            if node.id not in processed_genes:
                all_nodes.append(node)
                processed_genes.add(node.id)
        
        all_associations.extend(associations)
    
    return all_nodes, all_associations


@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> Tuple[Iterable[NamedThing], Iterable[Association]]:
    """Main transform function for Koza framework."""
    # Download all GO-CAM models
    downloaded_files = download_all_gocam_models()
    
    # Process all models
    nodes, associations = process_all_gocam_models()
    
    return nodes, associations