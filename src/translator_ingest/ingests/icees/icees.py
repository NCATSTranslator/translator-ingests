from typing import Optional, Any
import json
from loguru import logger
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from translator_ingest.ingests.icees.icees_util import get_cohort_metadata
from translator_ingest.util.biolink import (
    entity_id,
    get_node_class,
    get_edge_class,
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

        # the node 'category' may be a list of ancestor types
        # along with the most specific type, but the Pydantic
        # class returned is only of the most specific type.
        category = record.get("category", [])
        node_class = get_node_class(node_id, category)
        if node_class is None:
            logger.warning(f"Pydantic class for node '{node_id}' could not be created for category '{category}'")
            return None

        # TODO: need to directly record the 'equivalent_identifiers',
        #       not as 'xref' slot values but as 'equivalent_identifiers
        equivalent_identifiers: Optional[list[str]] = record.get("equivalent_identifiers", None)
        node = node_class(id=node_id, name=record["name"], xref=equivalent_identifiers, **{})
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
        edge_id = entity_id()

        # TODO: need to figure out how to select the 'best'
        #       association type, perhaps somewhat like so:
        #
        # association_list = toolkit.get_associations(
        #             subject_categories: Optional[List[str]] = None,
        #             predicates: Optional[List[str]] = None,
        #             object_categories: Optional[List[str]] = None,
        #             match_inverses: bool = True,
        #             formatted: bool = False
        #     ) -> List[str]
        #
        # PLUS
        # TODO: fix stub implementation
        association_list = ["biolink:NamedThingAssociatedWithLikelihoodOfNamedThingAssociation"]
        #
        # THEN
        #
        edge_class = get_edge_class(edge_id, associations=association_list)

        # Convert many of the ICEES edge attributes into specific edge properties
        supporting_studies: list[str] = []
        subject_context_qualifier = None
        object_context_qualifier = None
        attributes = record["attributes"]
        for attribute_string in attributes:
            # is 'attribute' a dict, or string serialized version of a dict?
            attribute_data = json.loads(attribute_string)
            if attribute_data["attribute_type_id"] == "icees_cohort_identifier":
                supporting_studies.append(attribute_data["value"])
                # TODO: figure out what to do with this metadata...
                get_cohort_metadata(attribute_data["attributes"])
            elif attribute_data["attribute_type_id"] == "subject_feature_name":
                subject_context_qualifier = attribute_data["value"]
            elif attribute_data["attribute_type_id"] == "object_feature_name":
                object_context_qualifier = attribute_data["value"]
            else:
                pass # other attributes ignored at this time

        association = edge_class(
            id=entity_id(),
            subject=record["subject"],
            subject_context_qualifier=subject_context_qualifier,
            predicate=record["predicate"],
            object=record["object"],
            object_context_qualifier=object_context_qualifier,
            has_supporting_studies=supporting_studies,
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
