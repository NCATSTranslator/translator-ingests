"""
Columbia Open Health Data ("COHD") ingest parser
"""
from typing import Optional, Any

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Study,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from bmt.pydantic import get_node_class
from translator_ingest.util.transform_utils import entity_id

import koza
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    get_biolink_model_toolkit,
    build_association_knowledge_sources
)

from translator_ingest.ingests.cohd.cohd_util import (
    to_curie,
    omop_to_biolink_category,
    parse_node_properties,
    get_cohd_supporting_study
)

bmt = get_biolink_model_toolkit()

COHD_SOURCES = build_association_knowledge_sources(primary="infores:panther")

def get_latest_version() -> str:
    # TODO: not sure how the pipeline will use this, but this is the dataset release date on FigShare
    return "2018-11-22"  # https://doi.org/10.6084/m9.figshare.c.4151252.v1


_cohd_nodes: dict[str, NamedThing] = {}


@koza.transform_record(tag="cohd_nodes")
def transform_cohd_node(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Parse a COHD concept entry into a 'node' concept.
    
    Record Fields:
        concept id - Unique numeric code identifying each concept. 
                     This is the key concept id referenced in the other files;
        concept name - The descriptive name of each concept;
        domain - The OMOP domain of each concept;
        vocabulary id - The source vocabulary which originally defined the concept (e.g., SNOMED-CT, RxNorm, etc.);
        concept class id - The OMOP concept class;
        concept code - The identifier from the source vocabulary for this concept.
        
    :param koza_transform: Koza context of the ingest task
    :param record: original Phase 2 COHD 'node' data record
    :return: KnowledgeGraph[nodes=list[NamedThing]]
    """
    # Concept identifier internal to COHD that uniquely indexes
    # clinical concepts defined by the vocabulary id and concept code fields
    cohd_id = record["concept id"]
    
    concept_name = record["concept name"]

    # The source vocabulary that originally defined the concept (e.g., SNOMED-CT, RxNorm, etc.);
    vocabulary_id = record["vocabulary id"]
    
    # The identifier - within the source vocabulary - assigned this concept.
    concept_code = record["concept code"]
    
    node_id = to_curie(vocabulary_id, concept_code)
    
    # The OMOP domain of each concept
    omop_domain: str = record["domain"]
    # The OMOP concept class
    omop_concept_class: int = record["concept class id"]
    category: str = omop_to_biolink_category(omop_domain, omop_concept_class)

    node_class: type[NamedThing] = get_node_class(node_id, [category], bmt=bmt)

    node = node_class(id=node_id, name=concept_name, **{})

    _cohd_nodes[str(cohd_id)] = node

    return KnowledgeGraph(nodes=[node])

# To minimize duplicate logging, keep track of which missing subject and object concept ids we've seen'
subject_node_seen: set[str] = set()
object_node_seen: set[str] = set()

@koza.transform_record(tag="cohd_edges")
def transform_cohd_edge(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Parse COHD 5-year paired concepts data into an edge association.
    
    Record Fields:
    concept id 1 - Unique numeric code identifying the first of the paired concepts;
    concept id 2 - Unique numeric code identifying the second of the paired concepts;
    count - The number of patients with both concepts in this data set;
    prevalence - The number of patients with both concepts divided by the total number of
                 patients in this data set (1.0 is 100%). Each unique pair of concepts has at most one row,
                 i.e., the same two concepts do not appear in two separate rows. The rows are arranged in
                 ascending order by concept id 1 and concept id 2. Concept id 1 is always the smaller numeric value.
                 
    :param koza_transform: Koza context of the ingest task
    :param record: original Phase 2 COHD 'node' data record
    :return: KnowledgeGraph[nodes=list[NamedThing]]
    """
    edge_id = entity_id()

    cohd_subject_id: str = record["concept id 1"]
    subject_node: Optional[NamedThing] = _cohd_nodes.get(str(cohd_subject_id))
    if subject_node is None:
        if cohd_subject_id not in subject_node_seen:
            koza_transform.log(f"Unknown concept id 1: {cohd_subject_id}")
            subject_node_seen.add(cohd_subject_id)
        return None
    
    cohd_object_id: str = record["concept id 2"]
    object_node: Optional[NamedThing] = _cohd_nodes[str(cohd_object_id)]
    if object_node is None:
        if cohd_object_id not in object_node_seen:
            koza_transform.log(f"Unknown concept id 2: {cohd_object_id}")
            object_node_seen.add(cohd_object_id)
        return None

    # confidence_score: Optional[float] = record.get("score", None)

    count: int = record["count"]
    prevalence: float = record["prevalence"]

    association = Association(
        id=edge_id,
        subject=subject_node.id,
        
        # TODO: how can COHD correlations be distinguished as positive versus negative?
        predicate="biolink:correlated_with",
        
        object=object_node.id,
        # has_confidence_score=confidence_score,
        sources=COHD_SOURCES,
        knowledge_level=KnowledgeLevelEnum.statistical_association,
        agent_type=AgentTypeEnum.data_analysis_pipeline,
        ** {}
    )
    return KnowledgeGraph(edges=[association])
