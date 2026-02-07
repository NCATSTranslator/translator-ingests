from typing import Optional, Any
import json

from loguru import logger
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Study,
    Association,
    CorrelatedGeneToDiseaseAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from bmt.pydantic import (
    entity_id,
    get_node_class,
    build_association_knowledge_sources
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import get_biolink_model_toolkit

from translator_ingest.ingests.icees.icees_util import get_icees_supporting_study

bmt = get_biolink_model_toolkit()

def get_latest_version() -> str:
    return "2024-08-20"  # last Phase 2 release of ICEES


_icees_nodes: dict[str, NamedThing] = {}

@koza.transform_record(tag="nodes")
def transform_icees_node(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Ingest ICEES phase 2 JSONL node entry into a Phase 3 compliant Pydantic node.
    :param koza_transform: Koza context of ingest
    :param record: original Phase 2 ICEES 'node' data record
    :return: KnowledgeGraph[nodes=list[NamedThing]]
    """
    global _icees_nodes

    node_id = record["id"]

    # the node 'category' may be a list of ancestor types
    # along with the most specific type, but the Pydantic
    # class returned is only of the most specific type.
    category = record.get("category", [])
    node_class: Optional[type[NamedThing]] = get_node_class(node_id, category, bmt=bmt)
    if node_class is None:
        logger.warning(f"Pydantic class for node '{node_id}' could not be inferred from categories '{category}'")
        return None

    equivalent_identifiers: Optional[list[str]] = record.get("equivalent_identifiers", None)

    node = node_class(
        id=node_id,
        name=record["name"],
        equivalent_identifiers=equivalent_identifiers,
        **{}
    )

    # Cache the node for dereferencing during edge file ingest
    _icees_nodes[node_id] = node

    return KnowledgeGraph(nodes=[node])


@koza.transform_record(tag="edges")
def transform_icees_edge(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """
    Ingest ICEES phase 2 JSONL 'edge' entry into a Phase 3 compliant Pydantic node.
    :param koza_transform: Koza context of ingest
    :param record: original Phase 2 ICEES 'edge'' data record
    :return: KnowledgeGraph[edges=list[Association]]
    """
    global _icees_nodes

    edge_id = entity_id()

    icees_subject: str = record["subject"]
    subject_node: Optional[NamedThing] = _icees_nodes.get(icees_subject)
    if subject_node is None:
        koza_transform.log(
            msg=f"ICEES Edge 'subject' concept with ID '{icees_subject}' missing in the set of ICEES nodes ingested?",
            level="WARNING"
        )
        return None
    subject_categories: list[str] = subject_node.category

    icees_predicate: str = record["predicate"]

    icees_object: str = record["object"]
    object_node: Optional[NamedThing] = _icees_nodes.get(icees_object)
    if object_node is None:
        koza_transform.log(
            msg=f"ICEES Edge 'object' concept with ID '{icees_object}' missing in the set of ICEES nodes ingested?",
            level="WARNING"
        )
        return None
    object_categories: list[str] = object_node.category

    # Specialized case of G2D Association
    if subject_categories[0] == "biolink:Gene" and object_categories[0] == "biolink:Disease":
        association = CorrelatedGeneToDiseaseAssociation
    else:
        association = Association

    # Convert many of the ICEES edge attributes into specific edge properties
    supporting_studies: dict[str, Study] = {}
    icees_qualifiers: dict[str,str] = {}
    attributes = record["attributes"]
    for attribute_string in attributes:
        # is 'attribute' a dict, or string serialized version of a dict?
        attribute_data = json.loads(attribute_string)
        if attribute_data["attribute_type_id"] == "icees_cohort_identifier":
            study_id = attribute_data["value"]
            supporting_studies[study_id] = get_icees_supporting_study(
                                                edge_id=edge_id,
                                                study_id=study_id,
                                                result=attribute_data["attributes"]
                                            )
        elif attribute_data["attribute_type_id"] in ["subject_feature_name","object_feature_name"]:
            icees_qualifiers[attribute_data["attribute_type_id"]] = attribute_data["value"]
        else:
            pass # all other attributes ignored at this time

    association = association(
        id=entity_id(),
        subject=icees_subject,
        predicate=icees_predicate,
        object=icees_object,
        has_supporting_studies=supporting_studies,
        sources=build_association_knowledge_sources(primary=record["primary_knowledge_source"]),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.not_provided,
        **icees_qualifiers
    )

    return KnowledgeGraph(edges=[association])
