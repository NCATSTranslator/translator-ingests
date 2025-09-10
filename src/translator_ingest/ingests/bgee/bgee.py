import uuid
import koza
import pandas as pd
from typing import Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToExpressionSiteAssociation,
    Gene,
    Cell,
#    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
#    Disease,
#    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Attribute
#    Association
)
from koza.model.graphs import KnowledgeGraph


# !!! README First !!!
#
# This module provides a template with example code and instructions for implementing an ingest. Replace the body
# of function examples below with ingest specific code and delete all template comments or unused functions.
#
# Note about ingest tags: for ingests with multiple different input files and/or different transformation processes,
# ingests can be divided into multiple sections using tags. Examples from this template are "ingest_by_record",
# "ingest_all", and "transform_ingest_all_streaming". Tags should be declared as keys in the readers section of ingest
# yaml files, then included with the (tag="tag_id") syntax as parameters in corresponding koza decorators.


# Biolink predicates and infores identifiers will eventually be imported from automatically updated modules,
# but for now use hardcoded constants.
BIOLINK_EXPRESSED_IN = "biolink:expressed_in"
INFORES_BGEE = "infores:bgee"

# Always implement a function that returns a string representing the latest version of the source data.
# Ideally, this is the version provided by the knowledge source, directly associated with a specific data download.
# If a source does not implement versioning, we need to do it. For static datasets, assign a version string
# corresponding to the current version. For sources that are updated regularly, use file modification dates if
# possible, or the current date. Versions should (ideally) be sortable (ie YYYY-MM-DD) and should contain no spaces.
def get_latest_version() -> str:
    return "v1"

# Functions decorated with @koza.on_data_begin() or @koza.on_data_end() are optional.
# If implemented they will be called at the beginning and/or end of the transform process.
@koza.on_data_begin(tag="ingest_by_record")
def on_begin_ingest_by_record(koza: koza.KozaTransform) -> None:
    # koza.state is a dictionary that can be used for arbitrary data storage, persisting across an individual transform.
    koza.state['example_counter'] = 0

@koza.on_data_end(tag="ingest_by_record")
def on_end_ingest_by_record(koza: koza.KozaTransform) -> None:
    # for example koza.state could be used for logging
    if koza.state['example_counter'] > 0:
        koza.log(f'{koza.state['example_counter']} rows were discarded for having no publications.', level="INFO")

# Ingests must implement a function decorated with @koza.transform() OR @koza.transform_record() (not both).
# These functions should contain the core data transformation logic generating and returning KnowledgeGraph objects
# with NamedThing (nodes) and Association (edges) from source data.
#
# The transform_record function takes the KozaTransform and a single record, a dictionary typically corresponding to a
# row in a source data file, and returns a KnowledgeGraph with any number of nodes and/or edges.
@koza.transform_record(tag="ingest_by_record")
def transform_ingest_by_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
#    attribute_list = [
#        Attribute(id=str(uuid.uuid4()),has_attribute_type="NCIT:C64214",name="BGee_CallQuality",has_qualitative_value=record["Call quality"]),
#        Attribute(id=str(uuid.uuid4()),name="BGee_FDR",has_quantitative_value=record["FDR"]),
#        Attribute(id=str(uuid.uuid4()),name="BGee_ExpressionScore",has_quantitative_value=record["Expression score"]),
#        Attribute(id=str(uuid.uuid4()),name="BGee_ExpressionRank",has_quantitative_value=record["Expression rank"])
#    ]
    attribute_dict = {
        "BGee_CallQuality":record["Call quality"],
        "BGee_FDR":record["FDR"],
        "BGee_ExpressionScore":record["Expression score"],
        "BGee_ExpressionRank":record["Expression rank"],
    }
    #The attribute for each record must be formatted as a list of string, not sure
    # if this is expected behavior.
    attribute_list = [str(attribute_dict)]
    gene = Gene(id="ENSEMBLE:" + record["Gene ID"], name=record["Gene name"])
    cell = Cell(id=record["Anatomical entity ID"], name=record["Anatomical entity name"])
    association = GeneToExpressionSiteAssociation(
        id=str(uuid.uuid4()),
        subject=gene.id,
        predicate=BIOLINK_EXPRESSED_IN,
        object=cell.id,
        primary_knowledge_source=INFORES_BGEE,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,        
        agent_type=AgentTypeEnum.not_provided,
        adjusted_p_value=record["FDR"],
        has_attribute=attribute_list,
    )
    return KnowledgeGraph(nodes=[gene, cell], edges=[association])