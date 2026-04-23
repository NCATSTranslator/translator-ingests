import uuid
import koza
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Any, Iterable

from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id, build_association_knowledge_sources

from biolink_model.datamodel.pydanticmodel_v2 import (
    # Gene,
    Protein,
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
BIOLINK_REGULATES = "biolink:regulates"
BIOLINK_RELATED = "biolink:related_to"

def get_latest_version() -> str:
    # lacking a better programmatic approach, derive the version from the gtopdb html
    html_page: requests.Response = requests.get('https://www.guidetopharmacology.org/download.jsp')
    resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

    # we expect the html to contain version text like 'Downloads are from the 2025.4 version.'
    # the following should extract the version from it (2025.4)
    search_text = 'Downloads are from the *'
    b_tag: BeautifulSoup.Tag = resp.find('b', string=re.compile(search_text))
    if len(b_tag) > 0:
        html_value = b_tag.text
        html_value = html_value[len(search_text) - 1:]  # remove the 'Downloads are from the' part
        source_version = html_value.split(' version')[0]  # remove the ' version.' part
        return source_version

    raise RuntimeError('Could not find the "Downloads are from the" text in the html to find the latest version.')

@koza.prepare_data(tag="gtopdb_interaction_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## used for debugging only
    ## check whether the mapping tag is in the same execution context
    # print("STATE KEYS:", koza.state.keys())
    # print("MAPPING SIZE:", len(koza.state.get("pubchem_id_mapping_dict", {})))

    ## Load ligands mapping CSV directly
    ## skip the metadata row
    ## Specify that 'Ligand ID' and "PubChem CID" should be read as a string
    ligands_file_path = Path(koza.input_files_dir) / "ligands.csv"
    mapping_df = pd.read_csv(ligands_file_path, skiprows = 1, dtype={'Ligand ID': str, 'PubChem CID': str})
    ## used for debugging only
    # print("Mapping CSV columns:", mapping_df.columns.tolist())

    mapping_dict = dict(zip(
        mapping_df["Ligand ID"].astype(str).str.strip(),
        mapping_df["PubChem CID"].astype(str).str.strip()
    ))

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## Only select needed columns
    sele_cols = ['Target', 'Target UniProt ID', 'Ligand ID', 'Ligand', 'Type', 'Action',
    'Endogenous', 'Ligand Context', 'PubMed ID']
    source_subset_df = source_df[sele_cols].drop_duplicates()

    ## Specify that 'Ligand ID' and "Target UniProt ID" should be read as a string ('object' dtype) to avoid pandas changing identifier from 1102 -> 1102.0
    source_subset_df = source_subset_df.astype({
        "Ligand ID": "string",
        "Target UniProt ID": "string"
    })

    ## debugging usage
    # koza.log(f"DataFrame columns: {source_df.columns.tolist()}")

    ## Drop nan values
    source_subset_df = source_subset_df.dropna(subset=["Target UniProt ID", "Ligand ID"])

    ## Implement logic to aggregate source records into a single edge based on SPO + qualifier pair (subject_name, subject_category, object_name, object_category, MECHANISM, EFFECT, DIRECT)
    group_cols = ['Target', 'Target UniProt ID', 'Ligand ID', 'Ligand', 'Type', 'Action', 'Endogenous']

    source_agg_df = (
        ## In pandas, groupby() drops rows with NA in any grouping key by default, which can silently discard interaction rows (and makes downstream Type/Action is None handling unreachable).
        ## use groupby(..., dropna=False) if intend to keep records with missing qualifiers
        source_subset_df.groupby(group_cols, as_index=False, dropna=False)
        .agg({
            "PubMed ID": lambda x: "|".join(pd.unique(x.dropna().astype(str)))
            })
    )

    ## rename those columns into desired format, note we need to obtain "pubchem CID" as subject id from "Ligand ID"
    source_agg_df.rename(
        columns={
            "Ligand": "subject_name",
            "Target": "object_name",
            "Target UniProt ID": "object_id",
        },
        inplace=True,
    )

    ## avoid mismatching by converting string ids into integer IDs
    source_agg_df["subject_id"] = (
        source_agg_df["Ligand ID"]
        .astype(str)
        .str.strip()
        .map(mapping_dict)
    )

    ## drop NA of those dont find a mapping
    source_agg_df = source_agg_df.dropna(subset=["subject_id"])

    return source_agg_df.drop_duplicates().to_dict(orient="records")


@koza.transform(tag="gtopdb_interaction_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    ## create one-time action list checkers:
    activator_list_with_separate_directly_physically_interacts_with_edge = ['Agonist', 'Binding', 'Full agonist', 'Partial agonist']
    ## all action == agonist edges need a separate directly_physically_interacts_with edge
    agonist_list_with_separate_directly_physically_interacts_with_edge = ['Activation', 'Agonist', 'Biased agonist', 'Binding', 'Full agonist', 'Inverse agonist', 'Irreversible agonist', 'Mixed', 'None', 'Partial agonist', 'Unknown']
    ## all action == Allosteric modulator need a seaparate directly_physically_interacts_with edge, thus no need of the branch switch code
    allosteric_modulator_list_with_separate_directly_physically_interacts_with_edge = ['Activation', 'Agonist', 'Antagonist', 'Biased agonist', 'Binding', 'Biphasic', 'Full agonist', 'Inhibition', 'Inverse agonist', 'Mixed', 'Negative', 'Neutral', None, 'Partial agonist', 'Positive', 'Potentiation']
    ## all action == Antagonist needs a separate directly_physically_interacts_with
    antagonist_list_with_separate_directly_physically_interacts_with_edge = ['Antagonist', 'Binding', 'Inhibition', 'Inverse agonist', 'Irreversible inhibition', 'Mixed', 'Non-competitive', 'Partial agonist']
    ## all action == Antibody needs a separate directly_physically_interacts_with
    antibody_list_with_separate_directly_physically_interacts_with_edge = ['Agonist', 'Antagonist', 'Binding', 'Inhibition', 'None']
    ## all action == Channel blocker needs a separate directly_physically_interacts_with
    channel_blocker_list_with_separate_directly_physically_interacts_with_edge = ['Antagonist', 'Inhibition', 'None', 'Pore blocker']
    ## all action == Fusion protein needs a separate directly_physically_interacts_with
    fusion_protein_list_with_separate_directly_physically_interacts_with_edge = ['Binding', 'Inhibition']
    ## all action == Gating inhibitor needs a separate directly_physically_interacts_with
    gating_inhibitor_list_with_separate_directly_physically_interacts_with_edge = ['Antagonist', 'Inhibition', 'None', 'Pore blocker', 'Slows inactivation', 'Voltage-dependent inhibition']
    ## following action == inhibitor needs a separate directly_physically_interacts_with
    inhibitor_list_with_separate_directly_physically_interacts_with_edge = ['Antagonist', 'Binding', 'Competitive', 'Inhibition', 'Irreversible inhibition', 'Non-competitive', 'None', 'Unknown']
    ## following action == None needs a separate directly_physically_interacts_with
    none_list_with_separate_directly_physically_interacts_with_edge = ['Binding', 'Competitive', 'Inhibition']
    ## following action ==  Subunit-specific needs a separate directly_physically_interacts_with
    subunit_specific_list_with_separate_directly_physically_interacts_with_edge = ['Inhibition']

    for record in data:
        object_direction_qualifier = None
        object_aspect_qualifier = None
        predicate = "None"
        qualified_predicate = None
        association = None
        causal_mechanism_qualifier = None

        # seems all subjects are chemical entity, and all objects are proteins
        subject = ChemicalEntity(id="PUBCHEM.COMPOUND:" + record["subject_id"], name=record["subject_name"])
        object = Protein(id="UniProtKB:" + record["object_id"], name=record["object_name"])

        ## Obtain the publications information
        publications = [f"PMID:{p}" for p in record["PubMed ID"].split("|")] if record["PubMed ID"] else None

        ## Now check whether the column (ENDOGENOUS) = TRUE, in source data records as a flag to indicate that the regulates predicate should be used instead of `affects',
        ## and "upregulates" and "downregulates" should be used as object directions instead of "increased" and decreased".

        ## initialize variables to hold information
        ## on whether an edge should use BIOLINK_AFFECTS and increased/decreased (if Endogenous == False)
        ## or should use BIOLINK_REGULATES and upregulated/downregulated (if Endogenous == True)
        current_predicate_mapping = BIOLINK_AFFECTS
        ## will be assigned as a tuple later, since we need to store two values
        current_direction_mapping = None
        if record["Endogenous"] == "TRUE":
            current_predicate_mapping = BIOLINK_REGULATES
            current_direction_mapping = (DirectionQualifierEnum.upregulated, DirectionQualifierEnum.downregulated)
        else:
            # current_predicate_mapping = BIOLINK_AFFECTS
            current_direction_mapping = (DirectionQualifierEnum.increased, DirectionQualifierEnum.decreased)

        # subject: Activator
        if record["Type"] == 'Activator' and record["Action"] in activator_list_with_separate_directly_physically_interacts_with_edge:
            ## define CausalMechanismQualifierEnum for each unique action values
            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
            elif record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
            elif record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    object_direction_qualifier = current_direction_mapping[0],
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        if record["Type"] == 'Activator' and record["Action"] not in activator_list_with_separate_directly_physically_interacts_with_edge:
            predicate = current_predicate_mapping
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = current_direction_mapping[0]
            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            ## Recorded in source file as a string "None" instead of a none type
            elif record["Action"] == "None":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            elif record["Action"] == "Positive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
            elif record["Action"] == "Potentiation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                ## Five edge attributes in order
                predicate = predicate,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
                ## other edge attributes
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )

            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        ## subject: Agonist
        if record["Type"] == 'Agonist' and record["Action"] in agonist_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Activation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Biased agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biased_agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Full agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Inverse agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = current_direction_mapping[1]
            elif record["Action"] == "Irreversible agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Mixed":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "None" or record["Action"] == "Unknown":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
            elif record["Action"] == "Partial agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
                object_direction_qualifier = current_direction_mapping[0]

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Allosteric modulator
        if record["Type"] == 'Allosteric modulator' and record["Action"] in allosteric_modulator_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Activation":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.activation
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Agonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Antagonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Biased agonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biased_agonism
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Binding":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None

            elif record["Action"] == "Biphasic":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.biphasic_allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None

            elif record["Action"] == "Full agonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.agonism
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Inhibition":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Inverse agonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Mixed":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.mixed_allosteric_modulation
                object_direction_qualifier = None
                qualified_predicate = None

            elif record["Action"] == "Negative":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.negative_allosteric_modulation
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Neutral" or record["Action"] == "None":
                ## print("not applicable")
                ## only jump off the parsing of current records, not the whole dataframe
                continue

            elif record["Action"] == "Partial agonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.partial_agonism
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Positive":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.positive_allosteric_modulation
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Potentiation":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = predicate,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Antagonist
        if record["Type"] == 'Antagonist' and record["Action"] in antagonist_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Antagonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Binding":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Inhibition":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Inverse agonist":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inverse_agonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Irreversible inhibition":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.irreversible_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Mixed":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Non-competitive":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.non_competitive_antagonism
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Partial agonist":
                # print("not applicable")
                ## only jump off the parsing of current records, not the whole dataframe
                continue

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = predicate,
                    object_aspect_qualifier = object_aspect_qualifier,
                    qualified_predicate = qualified_predicate,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Antibody
        if record["Type"] == 'Antibody' and record["Action"] in antibody_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Agonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_agonism
                object_direction_qualifier = current_direction_mapping[0]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.binding
                object_direction_qualifier = None
                qualified_predicate = None

            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antibody_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            elif record["Action"] == "None":
                causal_mechanism_qualifier = None
                object_direction_qualifier = None
                qualified_predicate = None

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier= causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Channel blocker
        if record["Type"] == 'Channel blocker' and record["Action"] in channel_blocker_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "None":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = None
                qualified_predicate = None
            elif record["Action"] == "Pore blocker":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = None
                qualified_predicate = None

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Fusion protein
        if record["Type"] == 'Fusion protein' and record["Action"] in fusion_protein_list_with_separate_directly_physically_interacts_with_edge:
            predicate = current_predicate_mapping
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
            object_direction_qualifier = current_direction_mapping[1]
            qualified_predicate = BIOLINK_CAUSES

            if record["Action"] == "Binding":
                # print("not applicable")
                ## only jump off the parsing of current records, not the whole dataframe
                continue
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.molecular_channel_blockage
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = predicate,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier= object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Gating inhibitor
        if record["Type"] == 'Gating inhibitor' and record["Action"] in gating_inhibitor_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "None":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = None
                qualified_predicate = None
            elif record["Action"] == "Pore blocker":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Slows inactivation":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES
            elif record["Action"] == "Voltage-dependent inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.gating_inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        # subject: Inhibitor
        if record["Type"] == 'Inhibitor' and record["Action"] in inhibitor_list_with_separate_directly_physically_interacts_with_edge:
            predicate = current_predicate_mapping
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = current_direction_mapping[1]

            if record["Action"] == "Antagonist":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            elif record["Action"] == "Binding":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.antagonism
            elif record["Action"] == "Competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.competitive_inhibition
            elif record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
            elif record["Action"] == "Irreversible inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.irreversible_inhibition
            elif record["Action"] == "Non-competitive":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.non_competitive_antagonism
            elif record["Action"] == "None" or record["Action"] == "Unknown":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = predicate,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        if record["Type"] == 'Inhibitor' and record["Action"] not in inhibitor_list_with_separate_directly_physically_interacts_with_edge:
            predicate = current_predicate_mapping
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES
            object_direction_qualifier = current_direction_mapping[1]

            if record["Action"] == "Feedback inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.feedback_inhibition

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                ## Five edge attributes in order
                predicate = predicate,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
                ## other edge attributes
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )

            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        # subject: None
        if record["Type"] == "None" and record["Action"] in none_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Binding" or record["Action"] == "Competitive":
                # print("not applicable")
                ## only jump off the parsing of current records, not the whole dataframe
                continue
            elif record["Action"] == "Inhibition":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = current_direction_mapping[1]
                qualified_predicate = BIOLINK_CAUSES

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = predicate,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        if record["Type"] == "None" and record["Action"] not in none_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "None":
                predicate = BIOLINK_RELATED
                object_aspect_qualifier = None
                causal_mechanism_qualifier = None
                object_direction_qualifier = None
                qualified_predicate = None
            elif record["Action"] == "Potentiation":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                qualified_predicate = BIOLINK_CAUSES
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = current_direction_mapping[0]

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                ## Five edge attributes in order
                predicate = predicate,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
                ## other edge attributes
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,

            )
            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        # subject: Subunit-specific
        if record["Type"] == "Subunit-specific" and record["Action"] in subunit_specific_list_with_separate_directly_physically_interacts_with_edge:
            predicate = current_predicate_mapping
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            qualified_predicate = BIOLINK_CAUSES

            if record["Action"] == "Inhibition":
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.inhibition
                object_direction_qualifier = current_direction_mapping[1]

            association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    ## Five edge attributes in order
                    predicate = predicate,
                    qualified_predicate = qualified_predicate,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = causal_mechanism_qualifier,
                    ## other edge attributes
                    sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

            association_2 = PairwiseMolecularInteraction(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:directly_physically_interacts_with",
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

        if record["Type"] == "Subunit-specific" and record["Action"] not in subunit_specific_list_with_separate_directly_physically_interacts_with_edge:

            if record["Action"] == "Mixed":
                # print("not applicable")
                ## only jump off the parsing of current records, not the whole dataframe
                continue
            elif record["Action"] == "Potentiation":
                predicate = current_predicate_mapping
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                qualified_predicate = BIOLINK_CAUSES
                causal_mechanism_qualifier = CausalMechanismQualifierEnum.potentiation
                object_direction_qualifier = current_direction_mapping[0]

            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=subject.id,
                object=object.id,
                ## Five edge attributes in order
                predicate = predicate,
                qualified_predicate = qualified_predicate,
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier,
                causal_mechanism_qualifier = causal_mechanism_qualifier,
                ## other edge attributes
                sources=build_association_knowledge_sources(primary=INFORES_GTOPDB),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            if publications:
                association.publications = publications

            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
