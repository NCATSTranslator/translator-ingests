import uuid
import koza
import pandas as pd
from typing import Any, Iterable

import json
import os
import re

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association
)
from translator_ingest.util.biolink import (
    INFORES_CTD,
    entity_id,
    build_association_knowledge_sources
)
from koza.model.graphs import KnowledgeGraph

# load insgest delacations from config file
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
    config = json.load(open("src/translator_ingest/ingests/sider/sider.config.json"))
    obj = to_object(config)
    return (obj.infores, obj.latest_version, obj.column, obj.curie_prefix, obj.predicate, obj.transformations)


(infores, latest_version, column, curie_prefix, predicate, transformations) = load_config()
for t in transformations:
    t.re = re.compile(t.regex_pattern)



# !!! README First !!!
#
# This module provides a template with example code and instructions for implementing an ingest. Replace the body
# of function examples below with ingest specific code and delete all template comments or unused functions.
#
# Note about ingest tags: for the ingests with multiple different input files and/or different transformation processes,
# ingests can be divided into multiple sections using tags. Examples from this template are "ingest_by_record",
# "ingest_all", and "transform_ingest_all_streaming". Tags should be declared as keys in the readers section of ingest
# yaml files, then included with the (tag="tag_id") syntax as parameters in corresponding koza decorators.


# Always implement a function that returns a string representing the latest version of the source data.
# Ideally, this is the version provided by the knowledge source, directly associated with a specific data download.
# If a source does not implement versioning, we need to do it. For static datasets, assign a version string
# corresponding to the current version. For sources that are updated regularly, use file modification dates if
# possible, or the current date. Versions should (ideally) be sortable (ie YYYY-MM-DD) and should contain no spaces.
def get_latest_version() -> str:
    return latest_version



@koza.transform(tag="sider_se_reader")
def transform_ingest_all_streaming(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    for record in data:
        for t in transformations:
            column_name = getattr(column, t.column)
            m = t.re.match(record[column_name])
            if m:
                record[column_name] = t.replacement.format(*m.groups())
        chemical = ChemicalEntity(id=curie_prefix.CID + record[column.CID_stereo])
        disease = Disease(id=curie_prefix.UMLS+record[column.UMLS_id], name=record[column.side_effect_name])
        association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
            id=str(uuid.uuid4()),
            subject=chemical.id,
            predicate=predicate,
            object=disease.id,
            primary_knowledge_source=infores,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        yield KnowledgeGraph(nodes=[chemical, disease], edges=[association])