"""
Columbia Open Health Data ("COHD") ingest parser
"""
from typing import Optional, Any

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from bmt.pydantic import (
    entity_id,
    get_node_class
)

import koza
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    get_biolink_model_toolkit,
    knowledge_sources_from_trapi
)

from .cohd_util import (
    parse_node_properties,
    parse_association_slots
)

bmt = get_biolink_model_toolkit()


def get_latest_version() -> str:
    return "2024-11-25"  # last Phase 2 release of COHD


_cohd_nodes: dict[str, NamedThing] = {}


@koza.transform_record(tag="cohd_nodes")
def transform_cohd_node(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Ingest COHD phase 2 JSONL 'node' entry into a Phase 3 compliant Pydantic node.
    :param koza_transform: Koza context of ingest
    :param record: original Phase 2 COHD 'node' data record
    :return: KnowledgeGraph[nodes=list[NamedThing]]
    """
    node_id = record["id"]
    category = record.get("categories", [])
    node_class: type[NamedThing] = get_node_class(node_id, category, bmt=bmt)

    # It currently seems that the COHD attributes block wraps a
    # complex representation of a simple database xref of the node
    node_properties = parse_node_properties(record.get("attributes", []))

    node = node_class(id=node_id, name=record["name"], **node_properties, **{})

    _cohd_nodes[node_id] = node

    return KnowledgeGraph(nodes=[node])


@koza.transform_record(tag="cohd_edges")
def transform_cohd_edge(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Ingest COHD phase 2 JSONL 'edge' entry into a Phase 3 compliant Pydantic association.
    :param koza_transform: Koza context of ingest
    :param record: original Phase 2 COHD 'node' data record
    :return: KnowledgeGraph[nodes=list[NamedThing]]
    """
    edge_id = entity_id()

    cohd_subject: str = record["subject"]

    cohd_predicate: str = record["predicate"]

    cohd_object: str = record["object"]

    confidence_score: Optional[float] = record.get("score", None)

    association_slots = parse_association_slots(record.get("attributes", []))

    association = Association(
        id=edge_id,
        subject=cohd_subject,
        predicate=cohd_predicate,
        object=cohd_object,
        has_confidence_score=confidence_score,
        sources=knowledge_sources_from_trapi(record["sources"]),
        knowledge_level=KnowledgeLevelEnum.statistical_association,
        agent_type=AgentTypeEnum.data_analysis_pipeline,
        **association_slots,
        ** {}
    )
    return KnowledgeGraph(edges=[association])
