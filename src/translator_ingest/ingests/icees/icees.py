from typing import Optional, Any
from loguru import logger
import koza

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

        xref: Optional[list[str]] = record.get("equivalent_identifiers", None)

        node = node_class(id=node_id, name=record["name"], xref=xref, **{})
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
        # TODO: need to figure out how to select the 'best'
        #       association type, perhaps somewhat like so:
        #
        # association_list = def get_associations(
        #             self,
        #             subject_categories: Optional[List[str]] = None,
        #             predicates: Optional[List[str]] = None,
        #             object_categories: Optional[List[str]] = None,
        #             match_inverses: bool = True,
        #             formatted: bool = False
        #     ) -> List[str]
        #
        # PLUS
        # specific_association = get_most_specific_association(association_list=association_list)
        #
        # THEN
        #
        # edge_class = get_edge_class(specific_association)
        #
        edge_class = Association  # stub implementation
        #
        #
        # TODO: need to figure out how to handle (certain additional?) ICEES edge attributes
        # attributes = record["attributes"]
        # # dct:description, biolink:same_as (equivalent_identifiers), etc.
        # for attribute in attributes:
        #     node.attributes.append(
        #         Attribute(
        #             attribute_type_id=attribute["attribute_type_id"],
        #             value=attribute["value"]
        #         )
        #
        association = edge_class(
            id=entity_id(),
            subject=record["subject"],
            predicate=record["predicate"],
            object=record["object"],
            sources=build_association_knowledge_sources(primary=record["primary_knowledge_source"]),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.not_provided
        )


        return KnowledgeGraph(edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_icees_edge():  - record: '{str(record)}' with {type(e)} exception: "+ str(e)
        )
        return None
