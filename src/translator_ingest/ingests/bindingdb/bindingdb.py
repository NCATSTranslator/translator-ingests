import uuid
import koza
import pandas as pd
from typing import Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association,
)
from translator_ingest.util.biolink import INFORES_CTD, entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph

BASE_LINK_TO_MONOMER: str = "http://www.bindingdb.org/bind/chemsearch/marvin/MolStructure.jsp?monomerid={monomerid}"
BASE_LINK_TO_TARGET: str = ("http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
                            "?energyterm=kJ/mole"
                            "&tag=com"
                            "&complexid=56"
                            "&target={target}"
                            "&column=ki&startPg=0&Increment=50&submit=Search")

LINK_TO_LIGAND_TARGET_PAIR: str = (
    "http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
    "?energyterm=kJ/mole&tag=r21&monomerid={monomerid}"
    "&enzyme={enzyme}"
    "&column=ki&startPg=0&Increment=50&submit=Search"
)

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
    return "202511"  # asking the BindingDB team how to get their latest versions


# Functions decorated with @koza.on_data_begin() or @koza.on_data_end() are optional.
# If implemented they will be called at the beginning and/or end of the transform process.
# @koza.on_data_begin(tag="ingest_by_record")
# def on_begin_ingest_by_record(koza: koza.KozaTransform) -> None:
#     # koza.state is a dictionary that can be used for arbitrary data storage, persisting across an individual transform.
#     koza.state["example_counter"] = 0
#
#     # koza.transform_metadata is a dictionary that can be used to save arbitrary metadata, the contents of  which will
#     # be copied to metadata output files. transform_metadata persists across all tagged transforms for a source.
#     koza.transform_metadata["ingest_by_record"] = {"rows_missing_publications": 0}
#
#
# @koza.on_data_end(tag="ingest_by_record")
# def on_end_ingest_by_record(koza: koza.KozaTransform) -> None:
#     # for example koza.state could be used for logging
#     if koza.state["example_counter"] > 0:
#         koza.log(f"{koza.state['example_counter']} rows were discarded for having no publications.", level="INFO")
#         koza.transform_metadata["ingest_by_record"]["rows_missing_publications"] = koza.state["example_counter"]


# Functions decorated with @koza.prepare_data() are optional. They are called after on_data_begin but before transform.
# They take an Iterable of dictionaries, typically representing the rows of a source data file, and return an Iterable
# of dictionaries which will be the data passed to subsequent transform functions. This allows for operations like
# nontrivial merging or transforming of complex source data on a source wide level, even if the transform will occur
# with a per record transform function.
# @koza.prepare_data(tag="ingest_by_record")
# def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
#
#     # do pandas stuff
#     df = pd.DataFrame(data)
#     return df.dropna().drop_duplicates().to_dict(orient="records")
#
#     # do database stuff:
#     # import sqlite3
#     # con = sqlite3.connect("example.db")
#     # con.row_factory = sqlite3.Row
#     # cur = con.cursor()
#     # cur.execute("SELECT * FROM example_table")
#     # records = cursor.fetchall()
#     # for record in records:
#     #     yield record
#     # con.close()
#
#     # merge stuff in a custom way
#     # koza.state['nodes'] = defaultdict(dict)
#     # for record in data:
#     #    koza.state['nodes'][record['node_id']] # store some merged node properties or something like that


# Ingests must implement a function decorated with @koza.transform() OR @koza.transform_record() (not both).
# These functions should contain the core data transformation logic generating and returning KnowledgeGraph objects
# with NamedThing (nodes) and Association (edges) from source data.
#
# The transform_record function takes the KozaTransform and a single record, a dictionary typically corresponding to a
# row in a source data file, and returns a KnowledgeGraph with any number of nodes and/or edges.
@koza.transform_record(tag="ingest_by_record")
def transform_ingest_by_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:

    # here is an example of skipping a record based off of some condition
    publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    if not publications:
        koza.log(f"No pubmed IDs found for {record['PubMedIDs']}")
        koza.state["example_counter"] += 1
        return None
    else:
        koza.log(f" pubmed IDs found for {record['PubMedIDs']}")

    chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate="biolink:related_to",
        object=disease.id,
        publications=publications,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    return KnowledgeGraph(nodes=[chemical, disease], edges=[association])
