"""
Columbia Open Health Data ("COHD") ingest parser
"""
from typing import Optional, Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Study,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    KnowledgeGraph
)
from bmt.pydantic import get_node_class

import koza

from translator_ingest.util.transform_utils import entity_id
from translator_ingest.util.biolink import (get_biolink_model_toolkit,build_association_knowledge_sources)

from translator_ingest.ingests.cohd.cohd_util import (to_curie, omop_to_biolink_category)

bmt = get_biolink_model_toolkit()

COHD_SOURCES = build_association_knowledge_sources(primary="infores:panther")

def get_latest_version() -> str:
    # TODO: not sure how the pipeline will use this, but this is the dataset release date on FigShare
    return "2018-11-22"  # https://doi.org/10.6084/m9.figshare.c.4151252.v1


_cohd_nodes: dict[str, NamedThing] = {}

@koza.prepare_data(tag="concepts")
def prepare_cohd_concepts(koza_transform: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    """
    Parse a COHD concept entry into an appropriate Biolink Model 'node' category instance,
    but cache the node to defer publication of the node to the later phase of reading single concept counts.
    
    Record Fields:
        concept id: int, Unique numeric code identifying each concept.
                     This is the key concept id referenced in the other files;
        concept name: str, The descriptive name of each concept;
        domain: str, The OMOP domain of each concept;
        vocabulary id: str, The source vocabulary which originally defined the concept (e.g., SNOMED-CT, RxNorm, etc.);
        concept class id, str The OMOP concept class;
        concept code: int, The identifier from the source vocabulary for this concept.
        
    :param koza_transform: Koza context of the ingest task
    :param data: Iterable[dict[str, Any]] of original concepts.txt file entries
    :return: None
    """
    for record in data:
        # Concept identifier internal to COHD that uniquely indexes
        # clinical concepts defined by the vocabulary id and concept code fields
        concept_id = record["concept id"]

        concept_name = record["concept name"]

        # The source vocabulary that originally defined the concept (e.g., SNOMED-CT, RxNorm, etc.);
        vocabulary_id = record["vocabulary id"]

        # The identifier - within the source vocabulary - assigned this concept.
        concept_code = record["concept code"]

        node_id = to_curie(vocabulary_id, concept_code)

        # The OMOP domain of each concept
        omop_domain: str = record["domain"]
        # The OMOP concept class
        omop_concept_class: str = record["concept class id"]
        category: str = omop_to_biolink_category(omop_domain, omop_concept_class)

        node_class: type[NamedThing] = get_node_class(node_id, [category], bmt=bmt)

        node = node_class(id=node_id, name=concept_name, **{})

        _cohd_nodes[str(concept_id)] = node

    return None  # we don't care about publishing any nodes yet

# To minimize duplicate logging, keep track of which
# missing subject and object concept ids we've seen
missing_subject_nodes: set[str] = set()
missing_object_nodes: set[str] = set()

@koza.transform_record(tag="single_concept_counts")
def transform_cohd_single_concept_counts(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Parse COHD 5-year single concept counts
    into their corresponding Biolink node instance.

    Record Fields:
       concept id - Unique numeric code identifying the concept;
       count - The number of patients with this concept in this data set;
       prevalence - The number of patients with this concept divided by the
                    total number of patients in this data set (1.0 is 100%).

    :param koza_transform: Koza context of the ingest task
    :param record: COHD single concept count entry
    :return: KnowledgeGraph[edges=list[Association]]
    """
    concept_id: str = record["concept id"]
    concept_node: Optional[NamedThing] = _cohd_nodes.get(str(concept_id))
    if concept_node is None:
        if concept_id not in missing_subject_nodes:
            koza_transform.log(f"Unknown concept id: {concept_id}")
            missing_subject_nodes.add(concept_id)
        return None

    count: int = record["count"]
    prevalence: float = record["prevalence"]

    concept_node

    return KnowledgeGraph(nodes=[concept_node])


@koza.transform_record(tag="paired_concept_counts")
def transform_cohd_paired_concepts_count(
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
    :param record: COHD paird concepts count entry
    :return: KnowledgeGraph[edges=list[Association]]
    """
    edge_id = entity_id()

    cohd_subject_id: str = record["concept id 1"]
    subject_node: Optional[NamedThing] = _cohd_nodes.get(str(cohd_subject_id))
    if subject_node is None:
        if cohd_subject_id not in missing_subject_nodes:
            koza_transform.log(f"Unknown concept id 1: {cohd_subject_id}")
            missing_subject_nodes.add(cohd_subject_id)
        return None

    cohd_object_id: str = record["concept id 2"]
    object_node: Optional[NamedThing] = _cohd_nodes.get(str(cohd_object_id))
    if object_node is None:
        if cohd_object_id not in missing_object_nodes:
            koza_transform.log(f"Unknown concept id 2: {cohd_object_id}")
            missing_object_nodes.add(cohd_object_id)
        return None

    count: int = record["count"]
    prevalence: float = record["prevalence"]

    association = Association(
        id=edge_id,
        subject=subject_node.id,
        predicate="biolink:correlated_with",
        object=object_node.id,
        sources=COHD_SOURCES,
        knowledge_level=KnowledgeLevelEnum.statistical_association,
        agent_type=AgentTypeEnum.data_analysis_pipeline,
        **{}
    )
    return KnowledgeGraph(edges=[association])
