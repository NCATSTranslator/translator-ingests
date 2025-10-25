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
from translator_ingest.util.biolink import (
    INFORES_SEMMEDDB,
    entity_id,
    build_association_knowledge_sources
)

# Prefix to Biolink class mapping for proper node typing
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
    """Return version string for SemMedDB data from RTX-KG2."""
    return "kg2.10.3"

def _make_node(curie: str) -> NamedThing:
    """Create a Biolink node instance from a CURIE.
    
    Args:
        curie: The CURIE identifier
        
    Returns:
        NamedThing instance with appropriate Biolink class based on prefix
    """
    if ":" not in curie:
        # Malformed ID, still emit as NamedThing to retain referential integrity
        return NamedThing(id=curie, category=NamedThing.model_fields["category"].default)

    prefix = curie.split(":", 1)[0]
    cls = PREFIX_TO_CLASS.get(prefix, NamedThing)
    return cls(id=curie, category=cls.model_fields["category"].default)

@koza.on_data_begin(tag="filter_edges")
def on_begin_filter_edges(koza: koza.KozaTransform) -> None:
    """Initialize state for edge filtering and node extraction."""
    koza.state["seen_node_ids"] = set()
    koza.state["total_edges_processed"] = 0
    koza.state["edges_with_publications"] = 0
    koza.state["edges_without_publications"] = 0

@koza.on_data_end(tag="filter_edges")
def on_end_filter_edges(koza: koza.KozaTransform) -> None:
    """Log summary statistics."""
    koza.log("SemMedDB processing complete:", level="INFO")
    koza.log(f"  Total edges processed: {koza.state['total_edges_processed']}", level="INFO")
    koza.log(f"  Edges with publications: {koza.state['edges_with_publications']}", level="INFO")
    koza.log(f"  Edges without publications: {koza.state['edges_without_publications']}", level="INFO")
    koza.log(f"  Unique nodes extracted: {len(koza.state['seen_node_ids'])}", level="INFO")

@koza.transform_record(tag="filter_edges")
def transform_semmeddb_edge(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform a SemMedDB edge record into Biolink entities.
    
    Args:
        koza: Koza transform instance
        record: Edge record from RTX-KG2 SemMedDB data
        
    Returns:
        KnowledgeGraph with nodes and edges, or None if record should be skipped
    """
    # Initialize state if not present (for testing)
    if "total_edges_processed" not in koza.state:
        koza.state["total_edges_processed"] = 0
        koza.state["seen_node_ids"] = set()
        koza.state["edges_with_publications"] = 0
        koza.state["edges_without_publications"] = 0
    
    koza.state["total_edges_processed"] += 1
    
    # Extract basic edge information
    subject_id = record.get("subject")
    object_id = record.get("object")
    predicate = record.get("predicate")
    
    if not all([subject_id, object_id, predicate]):
        return None
    
    # Create nodes for subject and object
    seen_node_ids = koza.state["seen_node_ids"]
    nodes = []
    
    if subject_id not in seen_node_ids:
        nodes.append(_make_node(subject_id))
        seen_node_ids.add(subject_id)
    
    if object_id not in seen_node_ids:
        nodes.append(_make_node(object_id))
        seen_node_ids.add(object_id)
    
    # Process publications
    publications = record.get("publications", [])
    if not publications:
        koza.state["edges_without_publications"] += 1
    else:
        koza.state["edges_with_publications"] += 1
    
    # Create association
    association = Association(
        id=entity_id(),
        subject=subject_id,
        predicate=predicate,
        object=object_id,
        publications=publications,
        sources=build_association_knowledge_sources(primary=INFORES_SEMMEDDB),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.automated_agent,  # SemMedDB is automated extraction
    )
    
    # Add additional fields if present
    if record.get("negated"):
        association.negated = record["negated"]
    
    # Note: domain_range_exclusion is not a field in Biolink Association model
    # This information is available in the source data but not modeled in Biolink
    
    return KnowledgeGraph(nodes=nodes, edges=[association])