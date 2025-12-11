"""
Columbia Open Health Data ("COHD") ingest parser
"""

from loguru import logger
import koza

from typing import Any

from biolink_model.datamodel.pydanticmodel_v2 import (
    # ChemicalToDiseaseOrPhenotypicFeatureAssociation,

    KnowledgeLevelEnum,
    AgentTypeEnum
)
from bmt.pydantic import (
    entity_id,
    get_node_class,
    get_edge_class
)

# TODO: Does this method perhaps belong in bmt.pydantic?
from translator_ingest.util.biolink import knowledge_sources_from_trapi, get_biolink_model_toolkit

from koza.model.graphs import KnowledgeGraph

from bmt import Toolkit
bmt: Toolkit = get_biolink_model_toolkit()

def get_latest_version() -> str:
    return "2024-11-25"  # last Phase 2 release of COHD


@koza.transform_record(tag="cohd_nodes")
def transform_cohd_node(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:

    try:
        # COHD uses the value of a "categories" field to indicate the type of node
        # We use it here to specify the correct Pydantic node class model
        node_id = record["id"]
        node_class = get_node_class(node_id, record.get("categories", ["biolink:NamedThing"]))

        # TODO: need to figure out how to handle (certain?) attributes
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
            f"transform_cohd_node():  - record: '{str(record)}' with {type(e)} exception: "+ str(e)
        )
        return None


@koza.transform_record(tag="cohd_edges")
def transform_cohd_edge(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:

    try:
        edge_id = entity_id()

        cohd_subject: str = record["subject"]
        subject_category: list[str] = bmt.get_element_by_prefix(cohd_subject)

        cohd_predicate: str = record["predicate"]

        cohd_object: str = record["object"]
        object_category: list[str] = bmt.get_element_by_prefix(cohd_object)
        association_list = bmt.get_associations(
                    subject_categories=subject_category,
                    predicates= [cohd_predicate],
                    object_categories=object_category,
                    formatted=True
            )

        edge_class = get_edge_class(edge_id, associations=association_list)

        association = edge_class(
            id=edge_id,
            subject=cohd_subject,
            predicate=cohd_predicate,
            object=cohd_object,
            has_confidence_score=record.get("score", None),

            # TODO: need to add additional supporting_data_source?
            sources=knowledge_sources_from_trapi(record["sources"]),

            knowledge_level=KnowledgeLevelEnum.statistical_association,
            agent_type=AgentTypeEnum.data_analysis_pipeline,
        )
        return KnowledgeGraph(edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_cohd_edge():  - record: '{str(record)}' with {type(e)} exception: "+ str(e)
        )
        return None