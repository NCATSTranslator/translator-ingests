from typing import Optional, Any
import json
from loguru import logger
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    StudyResult,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from bmt.pydantic import (
    entity_id,
    get_node_class,
    get_edge_class,
    build_association_knowledge_sources
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.ingests.icees.icees_util import (
    get_icees_study_result,
    map_icees_qualifiers
)

# Use the default Biolink Model release
# for now, unless otherwise indicated
from bmt import Toolkit
bmt: Toolkit = Toolkit()

def get_latest_version() -> str:
    return "2024-08-20"  # last Phase 2 release of ICEES


@koza.on_data_begin(tag="icees_edges")
def on_icees_edge_data_begin(koza_transform: koza.KozaTransform) -> None:
    koza_transform.state["association_classes_missing_qualifiers"] = set()


@koza.on_data_end(tag="icees_edges")
def on_icees_edge_data_end(koza_transform: koza.KozaTransform) -> None:
    if len(koza_transform.state["association_classes_missing_qualifiers"])>0:
        logger.warning("Association classes missing qualifiers: " +
                    f"{'\n'.join(list(koza_transform.state['association_classes_missing_qualifiers']))}"+"\n")
    else:
        logger.info("No association classes missing qualifiers")

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

        equivalent_identifiers: Optional[list[str]] = record.get("equivalent_identifiers", None)
        node = node_class(
            id=node_id,
            name=record["name"],
            equivalent_identifiers=equivalent_identifiers,
            **{}
        )
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

        icees_subject: str = record["subject"]
        subject_category: list[str] = bmt.get_element_by_prefix(icees_subject)

        icees_predicate: str = record["predicate"]

        icees_object: str = record["object"]
        object_category: list[str] = bmt.get_element_by_prefix(icees_object)

        association_list = bmt.get_associations(
                    subject_categories=subject_category,
                    predicates= [icees_predicate],
                    object_categories=object_category,
                    formatted=True
            )

        edge_class = get_edge_class(edge_id, associations=association_list)

        logger.debug(f"edge_class: {edge_class.__name__}")

        # Convert many of the ICEES edge attributes into specific edge properties
        supporting_study_results: list[StudyResult] = []
        icees_qualifiers: dict[str,str] = {}
        attributes = record["attributes"]
        for attribute_string in attributes:
            # is 'attribute' a dict, or string serialized version of a dict?
            attribute_data = json.loads(attribute_string)
            if attribute_data["attribute_type_id"] == "icees_cohort_identifier":
                supporting_study_results.append(
                    get_icees_study_result(
                        edge_id=edge_id,
                        study_name=attribute_data["value"],
                        metadata=attribute_data["attributes"]
                    )
                )
            elif attribute_data["attribute_type_id"] in ["subject_feature_name","object_feature_name"]:
                attribute_type_id = attribute_data["attribute_type_id"]
                target = attribute_type_id.replace("_feature_name","")
                icees_qualifiers[target] = attribute_data["value"]
            else:
                pass # all other attributes ignored at this time

        # TODO: temporary workaround for non-inlined study results,
        #       which should later be list[StudyResult]
        has_supporting_study_results = [str(entry) for entry in supporting_study_results]

        qualifiers: dict[str,str] = map_icees_qualifiers(
            koza_transform,
            association=edge_class,
            subject_category=subject_category,
            object_category=object_category,
            qualifiers=icees_qualifiers
        )

        association = edge_class(
            id=entity_id(),
            subject=icees_subject,
            predicate=icees_predicate,
            object=icees_object,
            has_supporting_study_results=has_supporting_study_results,
            sources=build_association_knowledge_sources(primary=record["primary_knowledge_source"]),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.not_provided,
            **qualifiers
        )

        return KnowledgeGraph(edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_icees_edge():  - record: '{str(record)}' with {type(e)} exception: "+ str(e)
        )
        return None
