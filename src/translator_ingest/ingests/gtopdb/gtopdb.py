import uuid
import koza
import pandas as pd
from typing import Any, Iterable

from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import build_association_knowledge_sources

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    ChemicalEntity,
    NamedThing,
    Association,
    ChemicalAffectsGeneAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    CausalMechanismQualifierEnum,
    DirectionQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from translator_ingest.util.biolink import (
    INFORES_GTOPDB
)

# adding additional needed resources
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"

def get_latest_version() -> str:
    from datetime import date
    today = date.today()
    formatted_date = today.strftime("%Y%m%d")

    return formatted_date

@koza.prepare_data(tag="gtopdb_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    # convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    # debugging usage
    koza.log.debug(f"DataFrame columns: {source_df.columns.tolist()}")

    # include some basic quality control steps here
    # Drop nan values
    source_df = source_df.dropna(subset=['Ligand PubChem SID', 'Target UniProt ID'])

    # rename those columns into desired format
    source_df.rename(columns={'Ligand': 'subject_name', 'Target': 'object_name', 'Ligand ID': 'subject_id', 'Target UniProt ID': 'object_id'}, inplace=True)

    return source_df.dropna().drop_duplicates().to_dict(orient="records")

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

        # seems all subjects are chemical entity, and all objects are genes
        subject = ChemicalEntity(id="GTOPDB:" + record["subject_id"], name=record["subject_name"])
        object = Gene(id="UniProtKB:" + record["object_id"], name=record["object_name"])

        # subject: Activator
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
                causal_mechanism_qualifier = None
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

        # subject: Agonist
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

        # subject: Allosteric modulator
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

        # subject: Antagonist
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

        # subject: Antibody
        if record["Type"] == 'Antibody':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_inhibition
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

        # subject: Channel blocker
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

        # subject: Fusion protein
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

        # subject: Gating inhibitor
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

        # subject: Inhibitor
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

        # subject: None
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

        # subject: Subunit-specific
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

# Functions decorated with @koza.on_data_begin() run before transform or transform_record

# koza.state is a dictionary that can be used to store arbitrary variables
# Now create specific transform ingest function for each pair of edges in SIGNOR
