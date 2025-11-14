import logging
import tarfile
import tempfile
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Iterable

import curies
import koza
import requests

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)
from koza.model.graphs import KnowledgeGraph

INFORES_UBERGRAPH = "infores:ubergraph"

logger = logging.getLogger(__name__)

OBO_MISSING_MAPPINGS = {
    'NCBIGene': 'http://purl.obolibrary.org/obo/NCBIGene_',
    'HGNC': 'http://purl.obolibrary.org/obo/HGNC_',
    'SGD': 'http://purl.obolibrary.org/obo/SGD_'
}


def get_latest_version() -> str:
    base_url = 'https://ubergraph.apps.renci.org'
    sparql_url = f'{base_url}/sparql'
    sparql_query = 'PREFIX dcterms: <http://purl.org/dc/terms/> SELECT DISTINCT * WHERE { <http://reasoner.renci.org/ontology> dcterms:created ?date . }'
    headers = {'Accept': 'text/tab-separated-values'}
    payload = {'query': sparql_query}
    response = requests.get(sparql_url, headers=headers, params=payload)
    if response.status_code != 200:
        response.raise_for_status()

    for response_line in response.content.decode('utf-8').splitlines():
        if 'dateTime' in response_line:
            date_time = response_line.split("^^")[0]
            date_time = date_time.strip('"')
            date_time = date_time.split('T')[0]
            return date_time

    raise Exception('Could not establish version from sparql query')


def init_curie_converter() -> curies.Converter:
    from Common.biolink_utils import get_biolink_prefix_map
    biolink_prefix_map = get_biolink_prefix_map()
    iri_to_biolink_curie_converter = curies.Converter.from_prefix_map(biolink_prefix_map)
    iri_to_obo_curie_converter = curies.get_obo_converter()
    custom_converter = curies.Converter.from_prefix_map(OBO_MISSING_MAPPINGS)

    chain_converter = curies.chain([
        iri_to_biolink_curie_converter,
        iri_to_obo_curie_converter,
        custom_converter,
    ])
    return chain_converter


def extract_tar_gz(tar_path: str) -> str:
    extract_dir = tempfile.mkdtemp(prefix="ontology_ingest_extract_")
    logger.info(f"Extracting {tar_path} to {extract_dir}")
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(extract_dir)
    return extract_dir


@koza.on_data_begin(tag="redundant_graph")
def on_begin_redundant_graph(koza: koza.KozaTransform) -> None:
    koza.state["record_counter"] = 0
    koza.state["skipped_record_counter"] = 0
    koza.state["node_curies"] = {}
    koza.state["edge_curies"] = {}
    koza.transform_metadata["redundant_graph"] = {
        "num_source_lines": 0,
        "unusable_source_lines": 0
    }


@koza.on_data_end(tag="redundant_graph")
def on_end_redundant_graph(koza: koza.KozaTransform) -> None:
    logger.info(f"Processed {koza.state['record_counter']} records")
    logger.info(f"Skipped {koza.state['skipped_record_counter']} records")
    koza.transform_metadata["redundant_graph"]["num_source_lines"] = koza.state["record_counter"]
    koza.transform_metadata["redundant_graph"]["unusable_source_lines"] = koza.state["skipped_record_counter"]


@koza.prepare_data(tag="redundant_graph")
def prepare_ontology_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    logger.info("Preparing ontology data: extracting tar.gz and converting IRIs to CURIEs...")
    
    tar_path = f"{koza.input_files_dir}/redundant-graph-table.tgz"
    extracted_path = extract_tar_gz(str(tar_path))
    graph_base_path = "redundant-graph-table"
    
    curie_converter = init_curie_converter()
    
    logger.info("Converting node IRIs to CURIEs...")
    node_curies = {}
    node_mapping_failures = []
    node_labels_path = Path(extracted_path) / graph_base_path / "node-labels.tsv"
    
    with open(node_labels_path, 'r') as node_labels_file:
        for line in node_labels_file:
            node_id, node_iri = tuple(line.rstrip().split('\t'))
            node_curie = curie_converter.compress(node_iri)
            if node_curie is None:
                node_mapping_failures.append(node_iri)
            node_curies[node_id] = node_curie
    
    logger.info(f"Nodes: {len(node_curies)} successfully converted, {len(node_mapping_failures)} failures.")
    if node_mapping_failures:
        logger.info(f"Node conversion failure examples: {node_mapping_failures[:10]}")
    
    logger.info("Converting edge IRIs to CURIEs...")
    edge_curies = {}
    edge_mapping_failures = []
    edge_labels_path = Path(extracted_path) / graph_base_path / "edge-labels.tsv"
    
    with open(edge_labels_path, 'r') as edge_labels_file:
        for line in edge_labels_file:
            edge_id, edge_iri = tuple(line.rstrip().split('\t'))
            edge_curie = curie_converter.compress(edge_iri)
            if edge_curie is None:
                edge_mapping_failures.append(edge_iri)
            edge_curies[edge_id] = edge_curie
    
    logger.info(f"Edges: {len(edge_curies)} successfully converted, {len(edge_mapping_failures)} failures.")
    if edge_mapping_failures:
        logger.info(f"Edge conversion failure examples: {edge_mapping_failures[:10]}")
    
    koza.state["node_curies"] = node_curies
    koza.state["edge_curies"] = edge_curies
    koza.state["extracted_path"] = extracted_path
    koza.state["graph_base_path"] = graph_base_path
    
    logger.info("Reading edges from tar archive...")
    edges_path = Path(extracted_path) / graph_base_path / "edges.tsv"
    with open(edges_path, 'r') as edges_file:
        for line in edges_file:
            subject_id, predicate_id, object_id = tuple(line.rstrip().split('\t'))
            yield {
                "subject_id": subject_id,
                "predicate_id": predicate_id,
                "object_id": object_id
            }


@koza.transform_record(tag="redundant_graph")
def transform_redundant_graph(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    koza.state["record_counter"] += 1
    
    subject_id = record["subject_id"]
    predicate_id = record["predicate_id"]
    object_id = record["object_id"]
    
    subject_curie = koza.state["node_curies"].get(subject_id)
    if not subject_curie:
        koza.state["skipped_record_counter"] += 1
        return None
    
    object_curie = koza.state["node_curies"].get(object_id)
    if not object_curie:
        koza.state["skipped_record_counter"] += 1
        return None
    
    predicate_curie = koza.state["edge_curies"].get(predicate_id)
    if not predicate_curie:
        koza.state["skipped_record_counter"] += 1
        return None
    
    subject_node = NamedThing(id=subject_curie)
    object_node = NamedThing(id=object_curie)
    
    sources = [
        RetrievalSource(
            resource_id=INFORES_UBERGRAPH,
            resource_role=ResourceRoleEnum.primary_knowledge_source
        )
    ]
    
    association = Association(
        id=f"urn:uuid:{koza.state['record_counter']}",
        subject=subject_curie,
        predicate=predicate_curie,
        object=object_curie,
        sources=sources,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=[association])