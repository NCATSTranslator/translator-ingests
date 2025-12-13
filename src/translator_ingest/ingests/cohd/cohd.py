"""
Columbia Open Health Data ("COHD") ingest parser
"""
import koza

from typing import Any

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from bmt.pydantic import (
    entity_id,
    get_node_class,
    # get_edge_class
)

from translator_ingest.util.biolink import (
    get_biolink_model_toolkit,
    # parse_attributes,
    knowledge_sources_from_trapi
)

from koza.model.graphs import KnowledgeGraph

from bmt import Toolkit
bmt: Toolkit = get_biolink_model_toolkit()


def get_latest_version() -> str:
    return "2024-11-25"  # last Phase 2 release of COHD


@koza.on_data_begin(tag="cohd_nodes")
def on_begin_node_ingest(koza_transform: koza.KozaTransform) -> None:
    koza_transform.log("Starting COHD nodes transformation")
    koza_transform.log(f"Version: {get_latest_version()}")
    koza_transform.transform_metadata["cohd_nodes"] = {}


@koza.on_data_end(tag="cohd_nodes")
def on_end_node_ingest(koza_transform: koza.KozaTransform) -> None:
    if koza_transform.transform_metadata["cohd_nodes"]:
        for tag, value in koza_transform.transform_metadata["cohd_nodes"].items():
            koza_transform.log(
                msg=f"Exception {str(tag)} encountered for records: {',\n'.join(value)}.",
                level="WARNING"
            )
    koza_transform.log("End of COHD nodes transformation")


@koza.transform_record(tag="cohd_nodes")
def transform_cohd_node(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:

    try:
        # COHD uses the value of a "categories" field to indicate the type of node
        # We use it here to specify the correct Pydantic node class model
        node_id = record["id"]
        node_class = get_node_class(node_id, record.get("categories", ["biolink:NamedThing"]), bmt=bmt)

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
        # Tally errors here
        exception_tag = f"{str(type(e))}: {str(e)}"
        rec_id = record.get("id", "Unknown")
        if str(e) not in koza_transform.transform_metadata["cohd_nodes"]:
            koza_transform.transform_metadata["cohd_nodes"][exception_tag] = [rec_id]
        else:
            koza_transform.transform_metadata["cohd_nodes"][exception_tag].append(rec_id)

        return None


@koza.on_data_begin(tag="cohd_edges")
def on_begin_edge_ingest(koza_transform: koza.KozaTransform) -> None:
    koza_transform.log("Starting COHD edges transformation")
    koza_transform.log(f"Version: {get_latest_version()}")
    koza_transform.transform_metadata["cohd_edges"] = {}


@koza.on_data_end(tag="cohd_edges")
def on_end_edge_ingest(koza_transform: koza.KozaTransform) -> None:
    if koza_transform.transform_metadata["cohd_edges"]:
        for tag, value in koza_transform.transform_metadata["cohd_edges"].items():
            koza_transform.log(
                msg=f"Exception {str(tag)} encountered for records: {',\n'.join(value)}.",
                level="WARNING"
            )
    koza_transform.log("End of COHD edges transformation")


@koza.transform_record(tag="cohd_edges")
def transform_cohd_edge(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:

    try:
        edge_id = entity_id()

        cohd_subject: str = record["subject"]
        # subject_category: list[str] = bmt.get_element_by_prefix(cohd_subject)

        cohd_predicate: str = record["predicate"]

        cohd_object: str = record["object"]
        # object_category: list[str] = bmt.get_element_by_prefix(cohd_object)

        # TODO: need to figure out how to handle (certain?) attributes
        # attributes = parse_attributes(record.get("attributes", None))
        #
        # TODO: it would also be nice to have dynamic mapping of edge classes for COHD data;
        #       However, for now, existing observations suggest that the Biolink model
        #       requires a bit of review and revision to better support such dynamic mapping.
        #
        # association_list = bmt.get_associations(
        #             subject_categories=subject_category,
        #             predicates= [cohd_predicate],
        #             object_categories=object_category,
        #             formatted=True
        #     )
        #
        # edge_class = get_edge_class(edge_id, associations=association_list, bmt=bmt)
        #
        # association = edge_class(
        association = Association(
            id=edge_id,
            subject=cohd_subject,
            predicate=cohd_predicate,
            object=cohd_object,
            has_confidence_score=record.get("score", None),
            sources=knowledge_sources_from_trapi(record["sources"]),
            knowledge_level=KnowledgeLevelEnum.statistical_association,
            agent_type=AgentTypeEnum.data_analysis_pipeline,
        )
        return KnowledgeGraph(edges=[association])

    except Exception as e:
        # Tally errors here
        exception_tag = f"{str(type(e))}: {str(e)}"
        rec_id = record.get("id", "Unknown")
        if str(e) not in koza_transform.transform_metadata["cohd_edges"]:
            koza_transform.transform_metadata["cohd_edges"][exception_tag] = [rec_id]
        else:
            koza_transform.transform_metadata["cohd_edges"][exception_tag].append(rec_id)

        return None