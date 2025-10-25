from loguru import logger
import koza

from typing import Any

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from translator_ingest.util.biolink import (
    entity_id,
    get_node_class,
    build_association_knowledge_sources
)
from koza.model.graphs import KnowledgeGraph


def get_latest_version() -> str:
    return "2024-08-20"  # last Phase 2 release of ICEES


@koza.transform_record(tag="icees_nodes")
def transform_icees_node(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:

    try:
        node_id = record["id"]
        # The node record 'category' is actually a list of
        # categories, so we need to get the most specific one
        node_class = get_node_class(node_id, record.get("category", []))
        if node_class is None:
            return None

        # TODO: need to figure out how to handle (certain additional?) ICEES attributes
        # attributes = record["attributes"]
        # # dct:description, biolink:same_as (equivalent_identifiers), etc.
        # for attribute in attributes:
        #     node.attributes.append(
        #         Attribute(
        #             attribute_type_id=attribute["attribute_type_id"],
        #             value=attribute["value"]
        #         )
        #     )

        node = node_class(id=node_id, name=record["name"], **{})
        return KnowledgeGraph(nodes=[node])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_icees_node():  - record: '{str(record)}' with {type(e)} exception: "+ str(e)
        )
        return None


@koza.transform_record(tag="icees_edges")
def transform_icees_edge(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:

    try:
        association = Association(
            id=entity_id(),
            subject=record["subject"],
            predicate=record["predicate"],
            object=record["object"],
            sources=build_association_knowledge_sources(primary=record["primary_knowledge_source"]),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.data_analysis_pipeline,
        )
        return KnowledgeGraph(edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_icees_edge():  - record: '{str(record)}' with {type(e)} exception: "+ str(e)
        )
        return None
