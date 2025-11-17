import importlib.resources
import json
import logging
import tarfile
from typing import Any, Iterable

import curies
import koza
import requests

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import entity_id, build_association_knowledge_sources

INFORES_UBERGRAPH = "infores:ubergraph"

logger = logging.getLogger(__name__)

EXTRACTED_ONTOLOGY_PREFIXES = [
    "UBERON",
    "CL",
    "GO",
    "CHEBI",
    "PR",
    "NCIT",
    "HPO",
    "MONDO",
    "RO",
    "SO",
    "MP",
    "PATO",
    "ECTO",
    "ENVO",
    "OBI",
    "MAXO",
    "ECO",
    "NCBITAXON",
    "FOODON",
    "MI",
    "UO"
]

OBO_MISSING_MAPPINGS = {
    'NCBIGene': 'http://purl.obolibrary.org/obo/NCBIGene_',
    'HGNC': 'http://purl.obolibrary.org/obo/HGNC_',
    'SGD': 'http://purl.obolibrary.org/obo/SGD_'
}

BIOLINK_MAPPING_CHANGES = {
    'KEGG': 'http://identifiers.org/kegg/',
    'NCBIGene': 'https://identifiers.org/ncbigene/'
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


def get_biolink_prefix_map() -> dict:
    try:
        with importlib.resources.open_text("biolink_model.prefixmaps",
                                          "biolink_model_prefix_map.json") as f:
            biolink_prefix_map = json.load(f)
            logger.debug("Successfully loaded Biolink prefix map from package")
    except Exception as e:
        logger.warning(f"Failed to load Biolink prefix map from package: {e}")
        url = "https://w3id.org/biolink/biolink-model/biolink_model_prefix_map.json"
        response = requests.get(url)
        if response.status_code != 200:
            response.raise_for_status()
        biolink_prefix_map = response.json()
        logger.debug("Successfully loaded Biolink prefix map from URL")

    biolink_prefix_map.update(BIOLINK_MAPPING_CHANGES)
    return biolink_prefix_map


def init_curie_converter() -> curies.Converter:
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
    logger.info("Preparing ontology data: streaming from tar.gz and converting IRIs to CURIEs...")

    tar_path = f"{koza.input_files_dir}/redundant-graph-table.tgz"
    graph_base_path = "redundant-graph-table"

    curie_converter = init_curie_converter()

    logger.info("Building CURIE mapping tables from tar archive (streaming)...")
    node_curies = {}
    node_mapping_failures = []
    edge_curies = {}
    edge_mapping_failures = []

    with tarfile.open(tar_path, 'r:gz') as tar:
        logger.info("Converting node IRIs to CURIEs...")
        node_count = 0
        with tar.extractfile(f'{graph_base_path}/node-labels.tsv') as node_labels_file:
            for line in node_labels_file:
                node_id, node_iri = line.decode('utf-8').rstrip().split('\t')
                node_curie = curie_converter.compress(node_iri)
                if node_curie is None:
                    node_mapping_failures.append(node_iri)
                    continue
                if node_curie.split(":")[0] in EXTRACTED_ONTOLOGY_PREFIXES:
                    node_curies[node_id] = node_curie
                    node_count += 1
                    if node_count % 100000 == 0:
                        print(f"  Processed {node_count:,} node labels...")

        logger.info(f"Nodes: {len(node_curies):,} successfully converted, {len(node_mapping_failures):,} failures.")
        if node_mapping_failures:
            logger.info(f"Node conversion failure examples: {node_mapping_failures[:10]}")

        logger.info("Converting edge IRIs to CURIEs...")
        with tar.extractfile(f'{graph_base_path}/edge-labels.tsv') as edge_labels_file:
            for line in edge_labels_file:
                edge_id, edge_iri = line.decode('utf-8').rstrip().split('\t')
                edge_curie = curie_converter.compress(edge_iri)
                if edge_iri == "http://www.w3.org/2000/01/rdf-schema#subClassOf":
                    edge_curies[edge_id] = edge_curie

        logger.info(f"Edges: {len(edge_curies):,} successfully converted, {len(edge_mapping_failures):,} failures.")
        if edge_mapping_failures:
            logger.info(f"Edge conversion failure examples: {edge_mapping_failures[:10]}")

        koza.state["node_curies"] = node_curies
        koza.state["edge_curies"] = edge_curies

        logger.info("Streaming edges from tar archive...")
        edge_stream_count = 0
        with tar.extractfile(f'{graph_base_path}/edges.tsv') as edges_file:
            for line in edges_file:
                subject_id, predicate_id, object_id = line.decode('utf-8').rstrip().split('\t')
                # Only include edges where predicate is in our edge_curies dict (subClassOf only)
                if predicate_id not in koza.state.get("edge_curies", edge_curies):
                    continue
                # Only include edges where both subject and object are in filtered ontology prefixes
                subject_curie = node_curies.get(subject_id)
                object_curie = node_curies.get(object_id)
                if not subject_curie or not object_curie:
                    continue
                if subject_curie.split(":")[0] not in EXTRACTED_ONTOLOGY_PREFIXES:
                    continue
                if object_curie.split(":")[0] not in EXTRACTED_ONTOLOGY_PREFIXES:
                    continue
                
                edge_stream_count += 1
                if edge_stream_count % 100000 == 0:
                    print(f"  Streaming edge {edge_stream_count:,}...")
                yield {
                    "subject_id": subject_id,
                    "predicate_id": predicate_id,
                    "object_id": object_id
                }
        print(f"  Finished streaming {edge_stream_count:,} edges from tar archive.")


@koza.transform(tag="redundant_graph")
def transform_redundant_graph(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    BATCH_SIZE = 500000
    
    nodes_seen = set()
    nodes_batch = []
    edges_batch = []
    sources = build_association_knowledge_sources(primary=INFORES_UBERGRAPH)
    
    batch_count = 0
    
    for record in data:
        koza.state["record_counter"] += 1
        
        if koza.state["record_counter"] % 500000 == 0:
            print(f"  Transformed {koza.state['record_counter']:,} edges, skipped {koza.state['skipped_record_counter']:,}...")
        
        subject_id = record["subject_id"]
        predicate_id = record["predicate_id"]
        object_id = record["object_id"]
        
        subject_curie = koza.state["node_curies"].get(subject_id)
        if not subject_curie:
            koza.state["skipped_record_counter"] += 1
            continue
        
        object_curie = koza.state["node_curies"].get(object_id)
        if not object_curie:
            koza.state["skipped_record_counter"] += 1
            continue
        
        predicate_curie = koza.state["edge_curies"].get(predicate_id)
        if not predicate_curie:
            koza.state["skipped_record_counter"] += 1
            continue
        
        if subject_curie not in nodes_seen:
            nodes_batch.append(NamedThing(id=subject_curie))
            nodes_seen.add(subject_curie)
        
        if object_curie not in nodes_seen:
            nodes_batch.append(NamedThing(id=object_curie))
            nodes_seen.add(object_curie)
        
        edges_batch.append(Association(
            id=entity_id(),
            subject=subject_curie,
            predicate=predicate_curie,
            object=object_curie,
            sources=sources,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        ))
        
        if len(edges_batch) >= BATCH_SIZE:
            batch_count += 1
            print(f"  Yielding batch {batch_count} with {len(edges_batch):,} edges and {len(nodes_batch):,} nodes...")
            yield KnowledgeGraph(nodes=nodes_batch, edges=edges_batch)
            nodes_batch = []
            edges_batch = []
    
    if edges_batch:
        batch_count += 1
        print(f"  Yielding final batch {batch_count} with {len(edges_batch):,} edges and {len(nodes_batch):,} nodes...")
        yield KnowledgeGraph(nodes=nodes_batch, edges=edges_batch)
