import uuid
import koza
import pandas as pd
from typing import Any, Iterable
# import csv

# csv.field_size_limit(10_000_000)   # allow fields up to 10MB

# * existing biolink category mapping:
# ChemicalEntity	biolink:ChemicalEntity
# MolecularMixture	biolink:MolecularMixture
# SmallMolecule	biolink:SmallMolecule
# Gene	biolink:Gene

from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import build_association_knowledge_sources

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    ChemicalEntity,
    # SmallMolecule,
    # MolecularMixture,
    NamedThing,
    Association,
    # PredicateMapping,
    # ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    # DrugToGeneAssociation,
    # DrugToGeneInteractionExposure,
    ChemicalAffectsGeneAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    CausalMechanismQualifierEnum,
    DirectionQualifierEnum,
    # RetrievalSource,
    # ResourceRoleEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from translator_ingest.util.biolink import (
    INFORES_GTOPDB
)

## adding additional needed resources
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
# BIOLINK_entity_positively_regulated_by_entity = "biolink:entity_positively_regulated_by_entity"
# BIOLINK_entity_negatively_regulated_by_entity = "biolink:entity_negatively_regulated_by_entity"

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
@koza.prepare_data(tag="gtopdb_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## debugging usage
    print(source_df.columns)

    ## include some basic quality control steps here
    ## Drop nan values
    source_df = source_df.dropna(subset=['Ligand PubChem SID', 'Target UniProt ID'])

    ## rename those columns into desired format
    source_df.rename(columns={'Ligand': 'subject_name', 'Target': 'object_name', 'Ligand ID': 'subject_id', 'Target UniProt ID': 'object_id'}, inplace=True)

    return source_df.dropna().drop_duplicates().to_dict(orient="records")

# As an alternative to transform_record, functions decorated with @koza.transform() take a KozaTransform and an Iterable
# of dictionaries, typically corresponding to all the rows in a source data file, and return an iterable of
# KnowledgeGraph, each containing any number of nodes and/or edges. Any number of KnowledgeGraphs can be returned:
# all at once, in batches, or using a generator for streaming.
@koza.transform(tag="gtopdb_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:
        object_direction_qualifier = None
        object_aspect_qualifier = None
        predicate = None
        qualified_predicate = None
        association = None
        causal_mechanism_qualifier = None

        ## seems all subjects are chemical entity, and all objects are genes
        subject = ChemicalEntity(id="GTOPDB:" + record["subject_id"], name=record["subject_name"])
        object = Gene(id="UniProtKB:" + record["object_id"], name=record["object_name"])

        ## subject: Activator
        if record["Type"] == 'Activator':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            if record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
            if record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            if record["Action"] is None:
                causal_mechanism_qualifier = None
            if record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
            if record["Action"] == "Positive":
                causal_mechanism_qualifier is None
            if record["Action"] == "Potentiation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Agonist
        if record["Type"] == 'Agonist':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES

            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Biased agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biased_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
            ## cannot find in pydanticmodel_v2?
            # if record["Action"] == "Irreversible agonist":
            #     causal_mechanism_qualifier = CausalMechanismQualifierEnum.irreversible_agonism
            #     object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Mixed":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] is None or record["Action"] == "Unknown":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Allosteric modulator
        if record["Type"] == 'Allosteric modulator':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Biased agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biased_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None
            if record["Action"] == "Biphasic":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biphasic_allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None
            ## cannot find full_agonism?
            # if record["Action"] == "Full agonist":
            #     causal_mechanism_qualifier = CausalMechanismQualifierEnum.full_agonism
            #     object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Mixed":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None
            if record["Action"] == "Negative":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_allosteric_modulation
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            # if record["Action"] == None or "Neutral":
            #     causal_mechanism_qualifier = None
            #     object_direction_qualifier = None
            if record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Positive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.positive_allosteric_modulation
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Potentiation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Antagonist
        if record["Type"] == 'Antagonist':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = DirectionQualifierEnum.decreased

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            if record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            if record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
            ## cannot find in pydanticmodel_v2?
            # if record["Action"] == "Irreversible agonist":
            #     causal_mechanism_qualifier = CausalMechanismQualifierEnum.irreversible_agonism
            #     object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Mixed":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_agonism
            if record["Action"] == "Non-competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.non_competitive_antagonism

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Antibody
        if record["Type"] == 'Antibody':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_agonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_agonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
                object_direction_qualifier = None
                qualified_predicate = None
            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] is None:
                causal_mechanism_qualifier = None
                object_direction_qualifier = None
                qualified_predicate = None

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Channel blocker
        if record["Type"] == 'Channel blocker':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] is None:
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = None
                qualified_predicate = None
            if record["Action"] == "Pore blocker":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = None
                qualified_predicate = None

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Fusion protein
        if record["Type"] == 'Fusion protein' and record["Action"] == "Inhibition":
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
            object_direction_qualifier = DirectionQualifierEnum.decreased
            qualified_predicate = BIOLINK_CAUSES

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Gating inhibitor
        if record["Type"] == 'Gating inhibitor':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] is None:
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = None
                qualified_predicate = None
            if record["Action"] == "Pore blocker":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Slows inactivation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            ## not find voltage_dependent_gating_inhibition
            # if record["Action"] == "Slows inactivation":
            #     causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
            #     object_direction_qualifier = DirectionQualifierEnum.decreased
            #     qualified_predicate = BIOLINK_CAUSES

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Inhibitor
        if record["Type"] == 'Inhibitor':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = DirectionQualifierEnum.decreased

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            if record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            if record["Action"] == "Competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.competitive_inhibition
            if record["Action"] == "Feedback inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.feedback_inhibition
            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
            if record["Action"] == "Irreversible inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.irreversible_inhibition
            if record["Action"] == "Non-competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.non_competitive_antagonism
            if record["Action"] is None or record["Action"] == "Unknown":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: None
        if record["Type"] is None:
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES

            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
            if record["Action"] == "Potentiation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = DirectionQualifierEnum.increased

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Subunit-specific
        if record["Type"] == "Subunit-specific":
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES

            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
            if record["Action"] == "Potentiation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = DirectionQualifierEnum.increased

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                predicate = predicate,
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]

## Functions decorated with @koza.on_data_begin() run before transform or transform_record

## koza.state is a dictionary that can be used to store arbitrary variables
## Now create specific transform ingest function for each pair of edges in SIGNOR
