import importlib.resources
import json
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
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id

INFORES_UBERGRAPH = "infores:ubergraph"
UBERGRAPH_SOURCES = build_association_knowledge_sources(primary=INFORES_UBERGRAPH)

EXTRACTED_ONTOLOGY_PREFIXES = [
    "UBERON",
    "CL",
    "GO",
    "CHEBI",
    "PR",
    "NCIT",
    "HP",
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
    "FOODON",
    "MI",
    "UO"
]

EXTRACTED_ONTOLOGY_PREFIXES_SET = set(EXTRACTED_ONTOLOGY_PREFIXES)

# Source predicate IRIs mapped directly to their Biolink predicate. Only edges whose
# predicate IRI appears here are ingested; add entries to support more predicates.
PREDICATE_IRI_TO_BIOLINK = {
    "http://www.w3.org/2000/01/rdf-schema#subClassOf": "biolink:subclass_of",
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
    except Exception:
        url = "https://w3id.org/biolink/biolink-model/biolink_model_prefix_map.json"
        response = requests.get(url)
        if response.status_code != 200:
            response.raise_for_status()
        biolink_prefix_map = response.json()

    return biolink_prefix_map


def init_curie_converter() -> curies.Converter:
    biolink_prefix_map = get_biolink_prefix_map()
    iri_to_biolink_curie_converter = curies.Converter.from_prefix_map(biolink_prefix_map)
    iri_to_obo_curie_converter = curies.get_obo_converter()

    chain_converter = curies.chain([
        iri_to_biolink_curie_converter,
        iri_to_obo_curie_converter,
    ])
    return chain_converter


@koza.on_data_begin(tag="redundant_graph")
def on_begin_redundant_graph(koza: koza.KozaTransform) -> None:
    koza.state["record_counter"] = 0
    koza.state["skipped_record_counter"] = 0
    koza.transform_metadata["redundant_graph"] = {
        "num_source_lines": 0,
        "unusable_source_lines": 0
    }


@koza.on_data_end(tag="redundant_graph")
def on_end_redundant_graph(koza: koza.KozaTransform) -> None:
    koza.log(f"Processed {koza.state['record_counter']:,} records", level="INFO")
    koza.log(f"Skipped {koza.state['skipped_record_counter']:,} records", level="INFO")
    koza.transform_metadata["redundant_graph"]["num_source_lines"] = koza.state["record_counter"]
    koza.transform_metadata["redundant_graph"]["unusable_source_lines"] = koza.state["skipped_record_counter"]


@koza.prepare_data(tag="redundant_graph")
def prepare_ontology_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    koza.log("Preparing ontology data: streaming from tar.gz and converting IRIs to CURIEs...", level="INFO")

    tar_path = f"{koza.input_files_dir}/redundant-graph-table.tgz"
    graph_base_path = "redundant-graph-table"

    curie_converter = init_curie_converter()

    koza.log("Building CURIE mapping tables from tar archive (streaming)...", level="INFO")
    node_curies = {}
    node_mapping_failures = []
    edge_curies = {}

    with tarfile.open(tar_path, 'r:gz') as tar:
        koza.log("Converting node IRIs to CURIEs...", level="INFO")
        with tar.extractfile(f'{graph_base_path}/node-labels.tsv') as node_labels_file:
            for line in node_labels_file:
                node_id, node_iri = line.decode('utf-8').rstrip().split('\t')
                node_curie = curie_converter.compress(node_iri)
                if node_curie is None:
                    node_mapping_failures.append(node_iri)
                    continue
                node_prefix = node_curie.split(":", 1)[0]
                if node_prefix in EXTRACTED_ONTOLOGY_PREFIXES_SET:
                    node_curies[node_id] = node_curie

        koza.log(f"Nodes: {len(node_curies):,} successfully converted, {len(node_mapping_failures):,} failures.", level="INFO")
        if node_mapping_failures:
            koza.log(f"Node conversion failure examples: {node_mapping_failures[:10]}", level="WARNING")

        koza.log("Mapping edge IRIs to Biolink predicates...", level="INFO")
        with tar.extractfile(f'{graph_base_path}/edge-labels.tsv') as edge_labels_file:
            for line in edge_labels_file:
                edge_id, edge_iri = line.decode('utf-8').rstrip().split('\t')
                biolink_predicate = PREDICATE_IRI_TO_BIOLINK.get(edge_iri)
                if biolink_predicate is not None:
                    edge_curies[edge_id] = biolink_predicate
        koza.log(f"Edges: {len(edge_curies):,} predicate IRIs mapped to Biolink predicates.", level="INFO")

        koza.log("Streaming edges from tar archive...", level="INFO")
        source_line_count = 0
        skipped_count = 0
        with tar.extractfile(f'{graph_base_path}/edges.tsv') as edges_file:
            for line in edges_file:
                source_line_count += 1
                subject_id, predicate_id, object_id = line.decode('utf-8').rstrip().split('\t')
                # Only include edges where the predicate and nodes were mapped successfully,
                # this excludes edges with predicates not in the PREDICATE_IRI_TO_BIOLINK lookup.
                predicate = edge_curies.get(predicate_id)
                subject_curie = node_curies.get(subject_id)
                object_curie = node_curies.get(object_id)
                if predicate is None or subject_curie is None or object_curie is None:
                    skipped_count += 1
                    continue
                yield {
                    "subject": subject_curie,
                    "predicate": predicate,
                    "object": object_curie,
                }
        koza.state["record_counter"] = source_line_count
        koza.state["skipped_record_counter"] = skipped_count
        koza.log(
            f"Finished streaming {source_line_count - skipped_count:,} edges "
            f"from {source_line_count:,} source lines ({skipped_count:,} skipped).",
            level="INFO",
        )


@koza.transform(tag="redundant_graph")
def transform_redundant_graph(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    BATCH_SIZE = 2000000

    nodes_seen = set()
    nodes_batch = []
    edges_batch = []

    batch_count = 0

    for record in data:
        # Records are already filtered and converted to CURIEs/predicates in prepare_data.
        subject_curie = record["subject"]
        object_curie = record["object"]

        if subject_curie not in nodes_seen:
            nodes_batch.append(NamedThing(id=subject_curie))
            nodes_seen.add(subject_curie)

        if object_curie not in nodes_seen:
            nodes_batch.append(NamedThing(id=object_curie))
            nodes_seen.add(object_curie)

        edges_batch.append(Association(
            id=entity_id(),
            subject=subject_curie,
            predicate=record["predicate"],
            object=object_curie,
            sources=UBERGRAPH_SOURCES,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        ))

        if len(edges_batch) >= BATCH_SIZE:
            batch_count += 1
            koza.log(f"Yielding batch {batch_count} with {len(edges_batch):,} edges and {len(nodes_batch):,} nodes...", level="INFO")
            yield KnowledgeGraph(nodes=nodes_batch, edges=edges_batch)
            nodes_batch = []
            edges_batch = []

    if edges_batch:
        batch_count += 1
        koza.log(f"Yielding final batch {batch_count} with {len(edges_batch):,} edges and {len(nodes_batch):,} nodes...", level="INFO")
        yield KnowledgeGraph(nodes=nodes_batch, edges=edges_batch)
