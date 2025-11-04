from os.path import abspath
import uuid
import koza
from typing import Any, Iterable

import json
import re

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    DiseaseOrPhenotypicFeature,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

from translator_ingest import INGESTS_PARSER_PATH
from bmt.pydantic import build_association_knowledge_sources

from koza.model.graphs import KnowledgeGraph

SIDER_INGEST_PATH = INGESTS_PARSER_PATH / "sider"
SIDER_INGEST_CONFIG_PATH = SIDER_INGEST_PATH / "sider.config.json"

# load ingest declarations from the config file
# using a simple object class to allow attribute access to dictionary keys


class object:

    def __init__(self, parsed_json=None):
        if parsed_json:
            for k, v in parsed_json.items():
                setattr(self, k, to_object(v))


def to_object(parsed_json) -> Any:
    if isinstance(parsed_json, dict):
        return object(parsed_json)
    elif isinstance(parsed_json, list):
        return [to_object(i) for i in parsed_json]
    else:
        return parsed_json


def load_config() -> tuple[str, str]:
    config = json.load(open(abspath(SIDER_INGEST_CONFIG_PATH), "r"))
    obj = to_object(config)
    return (obj.infores, obj.latest_version, obj.column, obj.curie_prefix, obj.predicate, obj.transformations)


(infores, latest_version, column, curie_prefix, predicate, transformations) = load_config()
for t in transformations:
    t.re = re.compile(t.regex_pattern)


# A function that returns a string representing the latest version of the source data.
def get_latest_version() -> str:
    return latest_version


# Implement a function that returns an iterable of dicts, each dict representing a row of the source data.
# The ingest framework will call this function once per transform.
@koza.transform(tag="sider_se_reader")
def transform_ingest_all_streaming(
    koza: koza.KozaTransform, data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    all_triples = set()
    for record in data:
        # apply transformations
        for t in transformations:
            column_name = getattr(column, t.column)
            m = t.re.match(record[column_name])
            if m:
                record[column_name] = t.replacement.format(*m.groups())
        # create nodes and edges
        chemical = ChemicalEntity(id=curie_prefix.CID + record[column.CID_stereo])
        disease = DiseaseOrPhenotypicFeature(
            id=curie_prefix.UMLS + record[column.UMLS_id], name=record[column.side_effect_name]
        )
        # prevent duplicate edges
        if (chemical.id, predicate, disease.id) in all_triples:
            continue
        all_triples.add((chemical.id, predicate, disease.id))
        association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
            id=str(uuid.uuid4()),
            subject=chemical.id,
            predicate=predicate,
            object=disease.id,
            sources=build_association_knowledge_sources(infores),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        knowledgeGraph = KnowledgeGraph(nodes=[chemical, disease], edges=[association])
        # filter to only PT terms
        if record[column.MedDRA_concept_type] != "PT":
            knowledgeGraph = None
        # return results
        yield knowledgeGraph
