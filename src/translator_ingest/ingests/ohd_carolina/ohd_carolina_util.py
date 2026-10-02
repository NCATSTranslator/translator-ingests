"""
This file contains utility functions for OHD Carolina data processing
"""
from typing import Optional, Any
import ast

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    CorrelatedGeneToDiseaseAssociation,
    Study,
    IceesStudyResult,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id

import koza
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import get_biolink_model_toolkit

bmt = get_biolink_model_toolkit()

# Subject and object nodes will be duplicated so we cache them
_ohdc_nodes: dict[str, NamedThing] = {}

def resolve_node(identifier: str, name: str) -> NamedThing:
    if identifier in _ohdc_nodes:
        return _ohdc_nodes[identifier]
    else:
        # TODO: need to be more clever here to perhaps use NodeNormalization
        #       to resolve the node category to one more specific than NamedThing
        node_found = NamedThing(id=identifier, name=name)
        _ohdc_nodes[identifier] = node_found
        return node_found

def wrap_study(edge_id: str, record: dict[str, Any]) -> dict[str, Study]:
    # convert the CI probabilities if given
    lor95_ci_value: str | None = record.get("log_odds_ratio_95_ci")
    if lor95_ci_value is not None:
        log_odds_ratio_95_ci = ast.literal_eval(str(lor95_ci_value))
    else:
        log_odds_ratio_95_ci = None

    study_result = IceesStudyResult(
        id=edge_id,
        chi_squared_p = record.get("chi_squared_p_value"),
        log_odds_ratio = record.get("log_odds_ratio"),
        log_odds_ratio_95_ci = log_odds_ratio_95_ci,
        total_sample_size = record.get("total_sample_size")
    )
    return {
        "infores:openhealthdata-carolina": 
            Study(
                id="infores:openhealthdata-carolina",
                has_study_results=[study_result]
            )
    }

def process_ohdc_record(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> Optional[KnowledgeGraph]:
    edge_id = entity_id()
    subject_node = resolve_node(record["subject"], record["subject_name"])
    predicate = record["predicate"]
    object_node = resolve_node(record["object"], record["object_name"])

    score: Optional[float] = None
    try:
        score = float(record["score"])
    except Exception as e:
        koza_transform.log(msg=f"Could not parse score from {str(record['subject'])}: {str(e)}", level="WARNING")

    if (
        "gene or gene product" in bmt.get_ancestors(subject_node.category[0])
        and object_node.category[0] == "biolink:Disease"
    ):
        association = CorrelatedGeneToDiseaseAssociation
    else:
        association = Association

    association = association(
        id=edge_id,
        subject=subject_node.id,
        predicate=predicate,
        object=object_node.id,
        has_confidence_score=score,
        has_supporting_studies=wrap_study(edge_id, record),
        sources=build_association_knowledge_sources(primary="infores:openhealthdata-carolina"),
        knowledge_level=KnowledgeLevelEnum.statistical_association,
        agent_type=AgentTypeEnum.data_analysis_pipeline,
        **{},
    )

    return KnowledgeGraph(nodes=[subject_node,object_node], edges=[association])
