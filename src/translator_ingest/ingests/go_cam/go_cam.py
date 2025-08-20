import json
import tarfile
import tempfile
from pathlib import Path
from typing import Any, Tuple

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    NamedThing,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    GeneToGeneAssociation
)

INFORES_GO_CAM = "infores:go-cam"


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
    
    print(f"Extracting {tar_path} to {extract_dir}")
    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(extract_dir)
    
    return extract_dir




def process_single_model(model_file_path: str) -> Tuple[list[NamedThing], list[Association]]:
    """Process a single GO-CAM model file and extract gene-gene relationships."""
    nodes = []
    associations = []

    try:
        with open(model_file_path, 'r') as f:
            model_data = json.load(f)

        # Track processed genes to avoid duplicates
        processed_genes = set()
        
        # Get model info
        model_id = model_data.get('graph', {}).get('model_info', {}).get('id', '')

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
                    nodes.append(gene_node)
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
                publications=publications if publications else None,
                primary_knowledge_source=INFORES_GO_CAM,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )

            associations.append(association)

    except Exception as e:
        print(f"Error processing model {model_file_path}: {e}")

    return nodes, associations




@koza.on_data_begin()
def prepare_go_cam_data(koza: koza.KozaTransform) -> None:
    """Extract tar.gz and prepare list of JSON files for processing."""
    print("Preparing GO-CAM data: extracting tar.gz and finding all JSON files...")
    
    # Path to the downloaded tar.gz file (from kghub-downloader)
    tar_path = "data/go_cam/go-cam-networkx.tar.gz"
    
    # Extract the tar.gz file
    extracted_path = extract_tar_gz(tar_path)
    
    # Find all JSON files
    json_files = list(Path(extracted_path).glob("**/*_networkx.json"))
    print(f"Found {len(json_files)} networkx JSON files to process")
    
    # Store the list of JSON files in koza.state for processing
    koza.state['json_files'] = [str(f) for f in json_files]
    koza.state['current_file_index'] = 0


@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> None:
    """Process a single GO-CAM model file - ignoring input record, using prepared file list."""
    # Get the list of files from state (prepared by on_data_begin)
    json_files = koza.state.get('json_files', [])
    current_index = koza.state.get('current_file_index', 0)
    
    # Only process if we have files and haven't processed them all
    if current_index < len(json_files):
        model_file_path = json_files[current_index]
        model_name = Path(model_file_path).name
        
        print(f"Processing model {current_index + 1}/{len(json_files)}: {model_name}")
        
        # Process this single model file
        nodes, associations = process_single_model(model_file_path)
        
        # Write all nodes and associations from this model to koza
        for node in nodes:
            koza.write(node)
        for association in associations:
            koza.write(association)
        
        # Increment the file index for next call
        koza.state['current_file_index'] = current_index + 1
    else:
        # We've processed all files, skip this record
        pass
