import uuid
import koza
import pandas as pd
from typing import Any, Iterable
import csv

csv.field_size_limit(10_000_000)   # allow fields up to 10MB

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
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    GeneRegulatesGeneAssociation,
    AnatomicalEntityToAnatomicalEntityPartOfAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    RetrievalSource,
    ResourceRoleEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from translator_ingest.util.biolink import (
    INFORES_SIGNOR
)
from koza.model.graphs import KnowledgeGraph

## adding additional needed resources
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_entity_positively_regulated_by_entity = "biolink:entity_positively_regulated_by_entity"
BIOLINK_entity_negatively_regulated_by_entity = "biolink:entity_negatively_regulated_by_entity"


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


# Functions decorated with @koza.prepare_data() are optional. They are called after on_data_begin but before transform.
# They take an Iterable of dictionaries, typically representing the rows of a source data file, and return an Iterable
# of dictionaries which will be the data passed to subsequent transform functions. This allows for operations like
# nontrivial merging or transforming of complex source data on a source wide level, even if the transform will occur
# with a per record transform function.
@koza.prepare_data(tag="signor_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## debugging usage
    print(source_df.columns)

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
    # filtered_df = source_df[
    #     (source_df['subject_category'] == 'protein') &
    #     (source_df['object_category'] == 'protein') &
    #     (source_df['EFFECT'] == 'up-regulates activity')
    #     ]

    return source_df.dropna().drop_duplicates().to_dict(orient="records")

# As an alternative to transform_record, functions decorated with @koza.transform() take a KozaTransform and an Iterable
# of dictionaries, typically corresponding to all the rows in a source data file, and return an iterable of
# KnowledgeGraph, each containing any number of nodes and/or edges. Any number of KnowledgeGraphs can be returned:
# all at once, in batches, or using a generator for streaming.
@koza.transform(tag="signor_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:
        object_direction_qualifier = None
        object_aspect_qualifier = None
        predicate = None
        association = None

        list_ppi_accept_effects = ['up-regulates activity', 'up-regulates', 'down-regulates activity', 'down-regulates', 'down-regulates quantity by expression', 'down-regulates quantity by destabilization']
        list_pci_accept_effects = ['form complex']

        if record["subject_category"] == "protein" and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            ## not working as intended
            # if record["EFFECT"] == 'unknown':
            #     predicate = 'biolink:affects'
            #     qualified_predicate = None
            # else:
            #     qualified_predicate = BIOLINK_CAUSES

            if record["EFFECT"] == 'up-regulates activity':
                predicate = "biolink:entity_positively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            if record["EFFECT"] == 'up-regulates':
                predicate = "biolink:entity_positively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            if record["EFFECT"] == 'up-regulates quantity by expression':
                predicate = "biolink:entity_positively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated

            if record["EFFECT"] == 'down-regulates activity':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            if record["EFFECT"] == 'down-regulates':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            if record["EFFECT"] == 'down-regulates quantity by expression':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            if record["EFFECT"] == 'down-regulates quantity by destabilization':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
                object_direction_qualifier = DirectionQualifierEnum.downregulated

            association = GeneRegulatesGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                primary_knowledge_source=INFORES_SIGNOR,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = BIOLINK_CAUSES,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        if record["subject_category"] == "protein" and record["object_category"] == "complex" and record["EFFECT"] in list_pci_accept_effects:
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = MacromolecularComplex(id="SIGNOR:" + record["IDB"], name=record["object_name"])

            if record["EFFECT"] == 'form complex':
                predicate = "biolink:partof"
                association_1 = AnatomicalEntityToAnatomicalEntityPartOfAssociation(
                    id=str(uuid.uuid4()),
                    subject=subject.id,
                    object=object.id,
                    predicate = predicate,
                    primary_knowledge_source=INFORES_SIGNOR,
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
                predicate = "physically_interacts_with"
                association_2 = AnatomicalEntityToAnatomicalEntityPartOfAssociation(
                    id=str(uuid.uuid4()),
                    subject=subject.id,
                    object=object.id,
                    predicate = predicate,
                    primary_knowledge_source=INFORES_SIGNOR,
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)

        if record["subject_category"] == "ChemicalEntity" and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = ChemicalEntity(id="CHEBI:" + record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            if record["EFFECT"] == 'up-regulates activity':
                predicate = "biolink:entity_positively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            if record["EFFECT"] == 'up-regulates':
                predicate = "biolink:entity_positively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            if record["EFFECT"] == 'up-regulates quantity by expression':
                predicate = "biolink:entity_positively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated

            if record["EFFECT"] == 'down-regulates activity':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            if record["EFFECT"] == 'down-regulates':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            if record["EFFECT"] == 'down-regulates quantity by expression':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            if record["EFFECT"] == 'down-regulates quantity by destabilization':
                predicate = "biolink:entity_negatively_regulated_by_entity"
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
                object_direction_qualifier = DirectionQualifierEnum.downregulated

            association = GeneRegulatesGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                primary_knowledge_source=INFORES_SIGNOR,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = BIOLINK_CAUSES,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]

## Functions decorated with @koza.on_data_begin() run before transform or transform_record

## koza.state is a dictionary that can be used to store arbitrary variables
## Now create specific transform ingest function for each pair of edges in SIGNOR
