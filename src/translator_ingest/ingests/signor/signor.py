import uuid
import koza
import pandas as pd
from typing import Any, Iterable

## the definition of biolink class can be found here: https://github.com/monarch-initiative/biolink-model-pydantic/blob/main/biolink_model_pydantic/model.py
# * existing biolink category mapping:
#     * 'Gene': 'biolink:Gene',
#     * 'Chemical': 'biolink:ChemicalEntity',
#     * 'Smallmolecule': 'biolink:SmallMolecule',
#     * 'Phenotype': 'biolink:PhenotypicFeature', -> BiologicalProcess
#     * 'Protein': 'biolink:Protein',
# * went through valid check cause there is potential issue:
#     * 'Antibody': 'biolink:Drug', ## check if all are indeed drug
#     * 'Complex': 'biolink:MacromolecularComplex',
#     * 'Mirna': 'biolink:MicroRNA',
#     * 'Ncrna': 'biolink:Noncoding_RNAProduct',

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    ChemicalEntity,
    SmallMolecule,
    PhenotypicFeature,
    Protein,
    Drug,
    MicroRNA,
    NoncodingRNAProduct,
    MacromolecularComplex,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association,
    PredicateMapping,
)
from translator_ingest.util.biolink import (
    INFORES_SIGNOR
)
from koza.model.graphs import KnowledgeGraph

## adding additional needed resources
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_entity_positively_regulated_by_entity = "biolink:entity_positively_regulated_by_entity"

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
    from datetime import date
    today = date.today()
    formatted_date = today.strftime("%Y%m%d")

    return formatted_date

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

# Functions decorated with @koza.prepare_data() are optional. They are called after on_data_begin but before transform.
# They take an Iterable of dictionaries, typically representing the rows of a source data file, and return an Iterable
# of dictionaries which will be the data passed to subsequent transform functions. This allows for operations like
# nontrivial merging or transforming of complex source data on a source wide level, even if the transform will occur
# with a per record transform function.
@koza.prepare_data(tag="ingest_by_record")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## include some basic quality control steps here
    ## Drop nan values
    source_df = source_df.dropna(subset=['ENTITYA', 'ENTITYB'])

    ## rename those columns into desired format
    source_df.rename(columns={'ENTITYA': 'subject_name', 'TYPEA': 'subject_category', 'ENTITYB': 'object_name', 'TYPEB': 'object_category'}, inplace=True)

    ## replace phenotype labeling into biologicalProcess
    source_df["subject_category"] = source_df["subject_category"].replace("phenotype", "BiologicalProcess")
    source_df ["object_category"] = source_df["object_category"].replace("phenotype", "BiologicalProcess")

    ## replace all 'miR-34' to 'miR-34a' in two columns subject_category and object_category in the pandas dataframe
    source_df['subject_name'] = source_df['subject_name'].replace('miR-34', 'miR-34a')
    source_df['object_name'] = source_df['object_name'].replace('miR-34', 'miR-34a')

    ## remove those rows with category in fusion protein or stimulus from source_df for now, and expecting biolink team to add those new categories
    source_df = source_df[(source_df['subject_category'] != 'fusion Protein') & (source_df['object_category'] != 'fusion Protein')]
    source_df = source_df[(source_df['subject_category'] != 'stimulus') & (source_df['object_category'] != 'stimulus')]

    ## for first pass ingestion, limited to the largest portion combo
    ## subject_category: protein, object_category:protein, effect: up-regulates activity
    filtered_df = source_df[
        (source_df['subject_category'] == 'protein') &
        (source_df['object_category'] == 'protein') &
        (source_df['EFFECT'] == 'up-regulates activity')
        ]

    return filtered_df.dropna().drop_duplicates().to_dict(orient="records")


# Ingests must implement a function decorated with @koza.transform() OR @koza.transform_record() (not both).
# These functions should contain the core data transformation logic generating and returning KnowledgeGraph objects
# with NamedThing (nodes) and Association (edges) from source data.
#
# The transform_record function takes the KozaTransform and a single record, a dictionary typically corresponding to a
# row in a source data file, and returns a KnowledgeGraph with any number of nodes and/or edges.
# @koza.transform_record(tag="ingest_by_record")
# def transform_ingest_by_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
#
#     # here is an example of skipping a record based off of some condition
#     publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
#     if not publications:
#         koza.state['example_counter'] += 1
#         return None
#
#     chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
#     disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
#     association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
#         id=entity_id(),
#         subject=chemical.id,
#         predicate="biolink:related_to",
#         object=disease.id,
#         publications=publications,
#         sources=build_association_knowledge_sources(primary=INFORES_CTD),
#         knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
#         agent_type=AgentTypeEnum.manual_agent
#     )
#     return KnowledgeGraph(nodes=[chemical, disease], edges=[association])

# As an alternative to transform_record, functions decorated with @koza.transform() take a KozaTransform and an Iterable
# of dictionaries, typically corresponding to all the rows in a source data file, and return an iterable of
# KnowledgeGraph, each containing any number of nodes and/or edges. Any number of KnowledgeGraphs can be returned:
# all at once, in batches, or using a generator for streaming.
@koza.transform(tag="ingest_all")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []
    for record in data:
        Protein_subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
        Protein_object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])
        association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
            id=str(uuid.uuid4()),
            subject=Protein_subject.id,
            predicate=BIOLINK_entity_positively_regulated_by_entity,
            object=Protein_object.id,
            primary_knowledge_source=INFORES_SIGNOR,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        nodes.append(Protein_subject)
        nodes.append(Protein_object)
        edges.append(association)
    return [KnowledgeGraph(nodes=nodes, edges=edges)]

## Functions decorated with @koza.on_data_begin() run before transform or transform_record

## koza.state is a dictionary that can be used to store arbitrary variables
## Now create specific transform ingest function for each pair of edges in SIGNOR
