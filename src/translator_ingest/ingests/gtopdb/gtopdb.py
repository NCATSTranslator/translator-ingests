import uuid
import koza
import pandas as pd
from typing import Any, Iterable
from collections import defaultdict

from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id, build_association_knowledge_sources


from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    ChemicalEntity,
    NamedThing,
    Association,
    ChemicalAffectsGeneAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    PairwiseMolecularInteraction,
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

## define a global dictionary to store the mapping dictionary between ligand id and pubmed id
pubchem_id_mapping_dict = defaultdict(dict)

def get_latest_version() -> str:
    from datetime import date
    today = date.today()
    formatted_date = today.strftime("%Y%m%d")

    return formatted_date

@koza.prepare_data(tag="gtopdb_ligand_id_mapping")
def prepare_pubchemID_mapping(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    ## define a global dictionary to store the mapping dictionary between ligand id and pubmed id
    global pubchem_id_mapping_dict

    # initialize the global dictionary
    pubchem_id_mapping_dict = {}

    # convert the input iterable into a DataFrame
    source_df = pd.DataFrame(data)

    # sanity check (optional but recommended)
    required_cols = {"Ligand ID", "PubChem CID"}
    missing = required_cols - set(source_df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # build the mapping
    for _, row in source_df.iterrows():
        ligand_id = row["Ligand ID"]
        pubchem_id = row["PubChem CID"]

        # skip nulls if needed
        if pd.notna(ligand_id) and pd.notna(pubchem_id):
            pubchem_id_mapping_dict[ligand_id] = pubchem_id

    # this function is only preparing data, not yielding rows
    return None

## Koza requires a transform function corresponding to each prepare_data.
## Thus we already built the global dictionary in the prepare_data, and still need this empty transform function
@koza.transform(tag="gtopdb_ligand_id_mapping")
def transform_nothing(koza: koza.KozaTransform, record: dict[str, Any]) -> None:
    # This tag only prepares global state; no records emitted
    return None

@koza.prepare_data(tag="gtopdb_interaction_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## debugging usage
    koza.log(f"DataFrame columns: {source_df.columns.tolist()}")

    ## Drop nan values
    source_df = source_df.dropna(subset=["Target UniProt ID", "Ligand ID"])

    ## rename those columns into desired format, note we need to obtain "pubchem CID" as subject id from "Ligand ID"
    source_df.rename(
        columns={
            "Ligand": "subject_name",
            "Target": "object_name",
            "Target UniProt ID": "object_id",
        },
        inplace=True,
    )

    ## apply mapping use the global dictionary
    source_df["subject_id"] = source_df["Ligand ID"].map(pubchem_id_mapping_dict)

    ## drop NA of those dont find a mapping
    source_df = source_df.dropna(subset=["subject_id"])

    return source_df.drop_duplicates().to_dict(orient="records")


@koza.transform(tag="gtopdb_interaction_parsing")
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

        ## Obtain the publications information
        publications = [f"PMID:{p}" for p in record["PubMed ID"].split("|")] if record["PubMed ID"] else None

        # subject: Activator
        if record["Type"] == 'Activator' and record["Action"] == "Binding":
            causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = BIOLINK_AFFECTS,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_direction_qualifier = DirectionQualifierEnum.increased,
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:physically_interacts_with",
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                ## Qi review comment, seems that PairwiseMolecularInteraction don't accept causal_mechanism_qualifier
                # causal_mechanism_qualifier = causal_mechanism_qualifier,
            )

            if publications and association_1 is not None and association_2 is not None:
                association_1.publications = publications
                association_2.publications = publications

            if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association_1)
                edges.append(association_2)

        if record["Type"] == 'Activator' and record["Action"] != "Binding":
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = DirectionQualifierEnum.increased
            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            elif record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            elif record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            elif record["Action"] is None:
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            elif record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
            elif record["Action"] == "Positive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            elif record["Action"] == "Potentiation":
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

            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        # subject: Agonist
        if record["Type"] == 'Agonist' and record["Action"] == "Binding":
            causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = BIOLINK_AFFECTS,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_direction_qualifier = DirectionQualifierEnum.increased,
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:physically_interacts_with",
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                ## Qi review comment, seems that PairwiseMolecularInteraction don't accept causal_mechanism_qualifier
                # causal_mechanism_qualifier = causal_mechanism_qualifier,
            )

            if publications and association_1 is not None and association_2 is not None:
                association_1.publications = publications
                association_2.publications = publications

            if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association_1)
                edges.append(association_2)

        if record["Type"] == 'Agonist' and record["Action"] != "Binding":
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            elif record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            elif record["Action"] == "Biased agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biased_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            elif record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            elif record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
            elif record["Action"] == "Mixed":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            elif record["Action"] is None or record["Action"] == "Unknown":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
            elif record["Action"] == "Partial agonist":
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

            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        # subject: Allosteric modulator
        if record["Type"] == 'Allosteric modulator':
            ## reset a branch_num to 0
            branch_num = 0
            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Biased agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biased_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Negative":
                causal_mechanism_qualifier = None
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Positive":
                causal_mechanism_qualifier = None
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Potentiation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
                ## control to switch to use branch_num == 1 logic
                branch_num = 1

            elif record["Action"] == "Biphasic":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biphasic_allosteric_modulation
                predicate = "biolink:physically_interacts_with"
                object_aspect_qualifier = None
                object_direction_qualifier = None
                qualified_predicate = None
                ## control to switch to use branch_num == 2 logic
                branch_num = 2

            elif record["Action"] == "Mixed" or record["Action"] == "Neutral" or record["Action"] is None:
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.allosteric_modulation
                predicate = "biolink:physically_interacts_with"
                object_direction_qualifier = None
                qualified_predicate = None
                ## control to switch to use branch_num == 2 logic
                branch_num = 2


            if branch_num == 1:
                association_1 = ChemicalAffectsGeneAssociation(
                        id=entity_id(),
                        subject=subject.id,
                        object=object.id,
                        predicate = BIOLINK_AFFECTS,
                        object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                        qualified_predicate = qualified_predicate,
                        object_direction_qualifier = object_direction_qualifier,
                        sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                        agent_type=AgentTypeEnum.manual_agent,
                        causal_mechanism_qualifier = causal_mechanism_qualifier,
                    )

                association_2 = PairwiseMolecularInteraction(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:physically_interacts_with",
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## Qi review comment, seems that PairwiseMolecularInteraction don't accept causal_mechanism_qualifier
                    # causal_mechanism_qualifier = CausalMechanismQualifierEnum.allosteric_modulation,
                )

                if publications and association_1 is not None and association_2 is not None:
                    association_1.publications = publications
                    association_2.publications = publications

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)

            elif branch_num == 2:

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

                if publications:
                    association.publications = publications

                if subject is not None and object is not None and association is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association)

        # subject: Antagonist
        if record["Type"] == 'Antagonist' and record["Action"] == "Binding":
            causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = BIOLINK_AFFECTS,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_direction_qualifier = DirectionQualifierEnum.decreased,
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:physically_interacts_with",
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                ## Qi review comment, seems that PairwiseMolecularInteraction don't accept causal_mechanism_qualifier
            )

            if publications and association_1 is not None and association_2 is not None:
                association_1.publications = publications
                association_2.publications = publications

            if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association_1)
                edges.append(association_2)

        if record["Type"] == 'Antagonist' and record["Action"] != "Binding":
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = DirectionQualifierEnum.decreased

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            elif record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
            elif record["Action"] == "Mixed":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_agonism
            elif record["Action"] == "Non-competitive":
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
            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        # subject: Antibody
        if record["Type"] == 'Antibody' and record["Action"] == "Binding":
            causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = BIOLINK_AFFECTS,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:physically_interacts_with",
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                ## Qi review comment, seems that PairwiseMolecularInteraction don't accept causal_mechanism_qualifier
            )

            if publications and association_1 is not None and association_2 is not None:
                association_1.publications = publications
                association_2.publications = publications

            if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association_1)
                edges.append(association_2)

        if record["Type"] == 'Antibody':
            predicate = BIOLINK_AFFECTS
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_agonism
                object_direction_qualifier = DirectionQualifierEnum.increased
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
                object_direction_qualifier = None
                qualified_predicate = None
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] is None:
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
            if publications:
                association.publications = publications

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
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] is None:
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = None
                qualified_predicate = None
            elif record["Action"] == "Pore blocker":
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
            if publications:
                association.publications = publications

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
            if publications:
                association.publications = publications

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
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] is None:
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = None
                qualified_predicate = None
            elif record["Action"] == "Pore blocker":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = DirectionQualifierEnum.decreased
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Slows inactivation":
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
            if publications:
                association.publications = publications

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
            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            elif record["Action"] == "Competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.competitive_inhibition
            elif record["Action"] == "Feedback inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.feedback_inhibition
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
            elif record["Action"] == "Irreversible inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.irreversible_inhibition
            elif record["Action"] == "Non-competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.non_competitive_antagonism
            elif record["Action"] is None or record["Action"] == "Unknown":
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
            if publications:
                association.publications = publications

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
            elif record["Action"] == "Potentiation":
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
            if publications:
                association.publications = publications

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
            elif record["Action"] == "Potentiation":
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
            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]

# Functions decorated with @koza.on_data_begin() run before transform or transform_record

# koza.state is a dictionary that can be used to store arbitrary variables
# Now create specific transform ingest function for each pair of edges in SIGNOR
