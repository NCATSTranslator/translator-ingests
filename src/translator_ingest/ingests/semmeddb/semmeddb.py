from typing import Any
import koza
from koza.model.graphs import KnowledgeGraph

from biolink_model.datamodel.pydanticmodel_v2 import (
    AnatomicalEntity,
    ChemicalEntity,
    Disease,
    Gene,
    NamedThing,
    PhenotypicFeature,
    Protein,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from translator_ingest.util.biolink import INFORES_SEMMEDDB

# map prefixes to node types for proper classification
PREFIX_TO_CLASS = {
    "NCBIGene": Gene,
    "HGNC": Gene,
    "ENSEMBL": Gene,
    "PR": Protein,
    "UniProtKB": Protein,
    "CHEBI": ChemicalEntity,
    "DRUGBANK": ChemicalEntity,
    "MONDO": Disease,
    "DOID": Disease,
    "HP": PhenotypicFeature,
    "UBERON": AnatomicalEntity,
}

def get_latest_version() -> str:
    return "semmeddb-2023-kg2.10.3"

def _make_node(curie: str, koza: koza.KozaTransform = None) -> NamedThing | None:
    # create a node from an identifier
    if ":" not in curie:
        # bad id format, count it for later reporting
        if koza and "bad_id_format" in koza.state:
            koza.state["bad_id_format"] += 1
        return None

    prefix = curie.split(":", 1)[0]
    cls = PREFIX_TO_CLASS.get(prefix, NamedThing)
    return cls(id=curie, category=cls.model_fields["category"].default)

@koza.on_data_begin(tag="filter_edges")
def on_begin_filter_edges(koza: koza.KozaTransform) -> None:
    # initialize counters for processing statistics
    koza.state["seen_node_ids"] = set()
    koza.state["total_edges_processed"] = 0
    koza.state["edges_with_publications"] = 0
    koza.state["edges_without_publications"] = 0
    koza.state["bad_id_format"] = 0
    koza.state["invalid_edges"] = 0
    koza.state["invalid_nodes"] = 0
    koza.state["negated_edges_skipped"] = 0
    koza.state["domain_range_exclusion_skipped"] = 0
    koza.state["low_publication_count_skipped"] = 0
    koza.state["zero_novelty_skipped"] = 0

@koza.on_data_end(tag="filter_edges")
def on_end_filter_edges(koza: koza.KozaTransform) -> None:
    # print processing summary with key metrics
    koza.log("semmeddb processing complete:", level="INFO")
    koza.log(f"  Total edges processed: {koza.state['total_edges_processed']}", level="INFO")
    koza.log(f"  Edges with publications: {koza.state['edges_with_publications']}", level="INFO")
    koza.log(f"  Edges without publications: {koza.state['edges_without_publications']}", level="INFO")
    koza.log(f"  Unique nodes extracted: {len(koza.state['seen_node_ids'])}", level="INFO")
    
    # only log warnings if there were issues
    if koza.state["bad_id_format"] > 0:
        koza.log(f"  Bad ID format skipped: {koza.state['bad_id_format']}", level="WARNING")
    if koza.state["invalid_edges"] > 0:
        koza.log(f"  Invalid edges skipped: {koza.state['invalid_edges']}", level="WARNING")
    if koza.state["invalid_nodes"] > 0:
        koza.log(f"  Invalid nodes skipped: {koza.state['invalid_nodes']}", level="WARNING")
    if koza.state["negated_edges_skipped"] > 0:
        koza.log(f"  Negated edges skipped: {koza.state['negated_edges_skipped']}", level="INFO")
    if koza.state["domain_range_exclusion_skipped"] > 0:
        koza.log(f"  Domain/range exclusion skipped: {koza.state['domain_range_exclusion_skipped']}", level="INFO")
    if koza.state["low_publication_count_skipped"] > 0:
        koza.log(f"  Low publication count skipped: {koza.state['low_publication_count_skipped']}", level="INFO")
    if koza.state["zero_novelty_skipped"] > 0:
        koza.log(f"  Zero novelty skipped: {koza.state['zero_novelty_skipped']}", level="INFO")

@koza.transform_record(tag="filter_edges")
def transform_semmeddb_edge(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # convert one edge record into nodes and connections
    # initialize state if not already done (needed for tests)
    if "total_edges_processed" not in koza.state:
        koza.state["seen_node_ids"] = set()
        koza.state["total_edges_processed"] = 0
        koza.state["edges_with_publications"] = 0
        koza.state["edges_without_publications"] = 0
        koza.state["bad_id_format"] = 0
        koza.state["invalid_edges"] = 0
        koza.state["invalid_nodes"] = 0
        koza.state["negated_edges_skipped"] = 0
        koza.state["domain_range_exclusion_skipped"] = 0
        koza.state["low_publication_count_skipped"] = 0
        koza.state["zero_novelty_skipped"] = 0
    
    koza.state["total_edges_processed"] += 1
    
    # 1. Filter: Negation
    # remove all edges that represent negations
    if record.get("negated"):
        koza.state["negated_edges_skipped"] += 1
        return None

    # 2. Filter: Domain/Range Exclusion
    # domain_range_exclusion == true
    if record.get("domain_range_exclusion"):
        koza.state["domain_range_exclusion_skipped"] += 1
        return None

    # 3. Filter: Publication Count
    # number of publications > 3
    publications = record.get("publications", [])
    if len(publications) <= 3:
        koza.state["low_publication_count_skipped"] += 1
        return None

    # 4. Filter: Novelty
    # subject novelty or object novelty == 0
    # Note: Checking if these fields exist in the record, as they are not always standard
    if record.get("subject_novelty") == 0 or record.get("object_novelty") == 0:
        koza.state["zero_novelty_skipped"] += 1
        return None

    # extract required fields
    subject_id = record.get("subject")
    object_id = record.get("object")
    predicate = record.get("predicate")
    
    if not all([subject_id, object_id, predicate]):
        koza.state["invalid_edges"] += 1
        return None
    
    # create nodes for subject and object, deduplicating as we go
    seen_node_ids = koza.state["seen_node_ids"]
    nodes = []
    
    # process subject node
    if subject_id not in seen_node_ids:
        subject_node = _make_node(subject_id, koza)
        if subject_node is not None:
            nodes.append(subject_node)
            seen_node_ids.add(subject_id)
        else:
            koza.state["invalid_nodes"] += 1
            return None
    
    # process object node
    if object_id not in seen_node_ids:
        object_node = _make_node(object_id, koza)
        if object_node is not None:
            nodes.append(object_node)
            seen_node_ids.add(object_id)
        else:
            koza.state["invalid_nodes"] += 1
            return None
    
    # track publication statistics
    if publications:
        koza.state["edges_with_publications"] += 1
    else:
        koza.state["edges_without_publications"] += 1
    
    # create association between nodes
    association = Association(
        id=entity_id(),
        subject=subject_id,
        predicate=predicate,
        object=object_id,
        publications=publications,
        sources=build_association_knowledge_sources(primary=INFORES_SEMMEDDB),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.automated_agent,
    )
    
    return KnowledgeGraph(nodes=nodes, edges=[association])