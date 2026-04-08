import koza
import pandas as pd

from typing import Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    Protein,
    MacromolecularComplex,
    NamedThing,
    ## necessary associations and interactions
    Association,
    GeneRegulatesGeneAssociation,
    #PairwiseMolecularInteraction,
    GeneAffectsChemicalAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    ChemicalAffectsGeneAssociation,
    #PairwiseGeneToGeneInteraction,
    ## necessary enums
    CausalMechanismQualifierEnum,
    DirectionQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import (
    INFORES_SIGNOR
)

# adding additional needed resources
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_REGULATES = "biolink:regulates"

# Qi had used this to avoid an issue with long 'description' fields,
# but I am not seeing any issue without it, so removing it for now.
# csv.field_size_limit(10_000_000)   # allow fields up to 10MB


def get_latest_version() -> str:
    # SIGNOR has some issues with downloading the latest data programmatically.
    # In the short term we implemented downloading it from our own server,
    # so the data version is static. We would like to do something like following when that is fixed.
    #
    # SIGNOR doesn't provide a great way to get the version,
    # but this link serves a file named something like "Oct2025_release.txt"
    # signor_latest_release_url = "https://signor.uniroma2.it/releases/getLatestRelease.php"
    # signor_latest_response = requests.post(signor_latest_release_url)
    # signor_latest_response.raise_for_status()
    # extract the version from the file name
    # file_name = signor_latest_response.headers['Content-Disposition']
    # file_name = file_name.replace("attachment; filename=", "").replace("_release.txt",
    #
    #
    # also note that currently the file we have on the RENCI server corresponds to a date but that's the download date
    # the actual version is
    return "2026_March"


@koza.prepare_data(tag="signor_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## Only select needed columns
    sele_cols = ['ENTITYA', 'ENTITYB', 'TYPEA', 'TYPEB', 'IDA', 'IDB', 'EFFECT', 'MECHANISM', 'TAX_ID', 'CELL_DATA', 'TISSUE_DATA', 'DIRECT', 'SCORE', 'SENTENCE', 'PMID']
    source_subset_df = source_df[sele_cols].drop_duplicates()

    ## include some basic quality control steps here
    ## Drop nan values
    source_subset_df = source_subset_df.dropna(subset=['ENTITYA', 'ENTITYB'])

    ## Implement logic to aggregate source records into a single edge based on SPO + qualifier pair (subject_name, subject_category, object_name, object_category, MECHANISM, EFFECT, DIRECT)
    group_cols = ['ENTITYA', 'ENTITYB', 'TYPEA', 'TYPEB', 'IDA', 'IDB', 'EFFECT', 'MECHANISM', 'TAX_ID', 'CELL_DATA', 'TISSUE_DATA', 'DIRECT', 'SCORE']

    source_agg_df = (
        source_subset_df.groupby(group_cols, as_index=False, dropna=False)
          .agg({
            "PMID": lambda x: "|".join(x.dropna().astype(str)),
            "SENTENCE": lambda x: "|".join(x.dropna().astype(str))
          })
    )

    ## rename those columns into desired format
    source_agg_df.rename(columns={'ENTITYA': 'subject_name', 'TYPEA': 'subject_category', 'ENTITYB': 'object_name', 'TYPEB': 'object_category'}, inplace=True)

    ## replace all 'miR-34' to 'miR-34a' in two columns subject_category and object_category in the pandas dataframe
    source_agg_df['subject_name'] = source_agg_df['subject_name'].replace('miR-34', 'miR-34a')
    source_agg_df['object_name'] = source_agg_df['object_name'].replace('miR-34', 'miR-34a')

    ## remove those rows with category in fusion protein or stimulus from source_df for now, and expecting biolink team to add those new categories
    source_agg_df = source_agg_df[
        (source_agg_df['subject_category'].str.lower() != 'fusion protein')
        & (source_agg_df['object_category'].str.lower() != 'fusion protein')
    ]
    source_agg_df = source_agg_df[
        (source_agg_df['subject_category'].str.lower() != 'stimulus')
        & (source_agg_df['object_category'].str.lower() != 'stimulus')
    ]

    ## only drop rows missing fields required to build a valid record
    required_cols = ['subject_name', 'object_name', 'IDA', 'IDB']

    return source_agg_df.dropna(subset=required_cols).drop_duplicates().to_dict(orient="records")


@koza.transform(tag="signor_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:

        ## reset to default value for each input row from the source file
        ## add those reset to make sure no leftover values from last records got leaked into the current record, but for some reason the make test keep raise errors on them
        #predicate = "None"
        # qualified_predicate = None
        # object_direction_qualifier = None
        # object_aspect_qualifier = None
        # association = None
        #causal_mechanism_qualifier = None

        ## Obtain the publications information
        publications = [f"PMID:{p}" for p in record["PMID"].split("|")] if record["PMID"] else None

        ## Obtain the species context information
        ## notice both "-1" and none values indicating an unknown or unspecified species
        if record["TAX_ID"] == '-1' or record["TAX_ID"] is None:
            species_context_qualifier = None
        else:
            species_context_qualifier = "NCBITaxon:" + record["TAX_ID"]

        ## Obtain the anatomical_context_qualifiers information from either column('CELL_DATA') or column('TISSUE_DATA')
        if record["CELL_DATA"] is not None:
            ## Note, need to split based on ";"
            anatomical_context_qualifier = [f"{p}" for p in record["CELL_DATA"].split(";")]
        elif record["TISSUE_DATA"] is not None:
            ## Note, need to split based on ";"
            anatomical_context_qualifier = [f"{p}" for p in record["TISSUE_DATA"].split(";")]
        else:
            anatomical_context_qualifier = None

        ## Obtain the supporting_text information from record["SENTENCE"]
        ## Note: the expected type for supporting_text is list type, so we do the conversion from a string to list here.
        ## Split by "|" and strip whitespace from each sentence
        supporting_text = [s.strip() for s in record["SENTENCE"].split("|")] if record.get("SENTENCE") else []

        ## Obtain the confidence_score information from record["SCORE"]
        confidence_score = record["SCORE"]

        list_ppi_accept_effects = ['up-regulates', 'up-regulates activity', 'up-regulates quantity', 'up-regulates quantity by expression', 'up-regulates quantity by stabilization', 'down-regulates', 'down-regulates activity', 'down-regulates quantity', 'down-regulates quantity by destabilization', 'down-regulates quantity by repression']
        list_pci_accept_effects = ['form complex']

        ## initialize variables to hold information
        ## on whether an edge should use BIOLINK_AFFECTS and increased/decreased (if Endogenous == False)
        ## or should use BIOLINK_REGULATES and upregulated/downregulated (if Endogenous == True)
        current_predicate_mapping = BIOLINK_AFFECTS
        ## will be assigned as a tuple later, since we need to store two values
        current_direction_mapping = None
        ## checked via HMDB, none of the "smallmolecule" subject that paired with a "protein" object is endogenous, thus should use affect, increased/decreased
        gene_product_list = ["protein", "complex"]
        if record["subject_category"] in gene_product_list and record["object_category"] in gene_product_list:
            current_predicate_mapping = BIOLINK_REGULATES
            current_direction_mapping = (DirectionQualifierEnum.upregulated, DirectionQualifierEnum.downregulated)
        else:
            # current_predicate_mapping = BIOLINK_AFFECTS
            current_direction_mapping = (DirectionQualifierEnum.increased, DirectionQualifierEnum.decreased)

        ## initialize variable to hold information
        ## on which biolink mechanism mapping enum to use based on the column('MECHANISM') from the source filename
        current_causal_mechanism_mapping = None
        if record['MECHANISM'] == 'transcriptional regulation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.transcriptional_regulation
        elif record['MECHANISM'] == 'translation regulation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.translational_regulation
        elif record['MECHANISM'] == 'precursor of':
            ## Note for QA: change to biochemical conversion / precursor once implemented in the biolink CausalMechanismQualifierEnum class
            ## assign None for now to avoid providing false information in the tier 0 graph
            current_causal_mechanism_mapping = None
        elif record['MECHANISM'] == 'binding':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.binding
        elif record['MECHANISM'] == 'stabilization':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.stabilization
        elif record['MECHANISM'] == 'destabilization':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.destabilization
        elif record['MECHANISM'] == 'cleavage':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.cleavage
        elif record['MECHANISM'] == 'isomerization':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.isomerization
        elif record['MECHANISM'] == 'chemical inhibition':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.inhibition
        elif record['MECHANISM'] == 'chemical activation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.activation
        elif record['MECHANISM'] == 'catalytic activity':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.catalytic_activity
        elif record['MECHANISM'] == 'small molecule catalysis':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.catalytic_activity
        elif record['MECHANISM'] == 'gtpase-activating protein':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.gtpase_activation
        elif record['MECHANISM'] == 'guanine nucleotide exchange factor':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.guanyl_nucleotide_exchange
        elif record['MECHANISM'] == 'relocalization':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.relocalization
        elif record['MECHANISM'] == 'chemical modification':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.chemical_modification
        elif record['MECHANISM'] == 'post transcriptional regulation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.post_transcriptional_regulation
        elif record['MECHANISM'] == 'post translational modification':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.molecular_modification
        elif record['MECHANISM'] == 'phosphorylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.phosphorylation
        elif record['MECHANISM'] == 'dephosphorylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.dephosphorylation
        elif record['MECHANISM'] == 'neddylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.neddylation
        elif record['MECHANISM'] == 'lipidation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.lipidation
        elif record['MECHANISM'] == 'tyrosination':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.tyrosination
        elif record['MECHANISM'] == 'carboxylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.carboxylation
        elif record['MECHANISM'] == 'ubiquitination':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.ubiquitination
        elif record['MECHANISM'] == 'monoubiquitination':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.monoubiquitination
        elif record['MECHANISM'] == 'polyubiquitination':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.polyubiquitination
        elif record['MECHANISM'] == 'deubiquitination':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.deubiquitination
        elif record['MECHANISM'] == 'acetylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.acetylation
        elif record['MECHANISM'] == 'oxidation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.oxidation
        elif record['MECHANISM'] == 'deacetylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.deacetylation
        elif record['MECHANISM'] == 'glycosylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.glycosylation
        elif record['MECHANISM'] == 'deglycosylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.deglycosylation
        elif record['MECHANISM'] == 'methylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.methylation
        elif record['MECHANISM'] == 'demethylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.demethylation
        elif record['MECHANISM'] == 'trimethylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.trimethylation
        elif record['MECHANISM'] == 'sumoylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.sumoylation
        elif record['MECHANISM'] == 'desumoylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.desumoylation
        elif record['MECHANISM'] == 'ADP-ribosylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.ADP_ribosylation
        elif record['MECHANISM'] == 'palmitoylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.palmitoylation
        elif record['MECHANISM'] == 'hydroxylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.hydroxylation
        elif record['MECHANISM'] == 's-nitrosylation':
            current_causal_mechanism_mapping = CausalMechanismQualifierEnum.s_nitrosylation
        elif not record.get('MECHANISM'):  # catches None, "", or missing key
            current_causal_mechanism_mapping = None
        else:
            raise NotImplementedError(f'Effect {record["MECHANISM"]} could not be mapped to required qualifiers.')

        ## initialize variables to hold information
        ## on which object_aspect_qualifier (GeneOrGeneProductOrChemicalEntityAspectEnum) and object_direction_qualifier to use
        if record["EFFECT"] == 'up-regulates':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
            object_direction_qualifier = current_direction_mapping[0]
        elif record["EFFECT"] == 'up-regulates activity':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            object_direction_qualifier = current_direction_mapping[0]
        elif record["EFFECT"] == 'up-regulates quantity':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
            object_direction_qualifier = current_direction_mapping[0]
        elif record["EFFECT"] == 'up-regulates quantity by expression':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
            object_direction_qualifier = current_direction_mapping[0]
        elif record["EFFECT"] == 'up-regulates quantity by stabilization':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
            object_direction_qualifier = current_direction_mapping[0]
        elif record["EFFECT"] == 'down-regulates':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
            object_direction_qualifier = current_direction_mapping[1]
        elif record["EFFECT"] == 'down-regulates activity':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
            object_direction_qualifier = current_direction_mapping[1]
        elif record["EFFECT"] == 'down-regulates quantity':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
            object_direction_qualifier = current_direction_mapping[1]
        elif record["EFFECT"] == 'down-regulates quantity by destabilization':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
            object_direction_qualifier = current_direction_mapping[1]
        elif record["EFFECT"] == 'down-regulates quantity by repression':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
            object_direction_qualifier = current_direction_mapping[1]
        elif record["EFFECT"] == 'form complex':
            object_aspect_qualifier = None
            object_direction_qualifier = None
        elif record["EFFECT"] == 'unknown':
            object_aspect_qualifier = None
            object_direction_qualifier = None
        elif not record.get('EFFECT'):  # catches None, "", or missing key
            object_aspect_qualifier = None
            object_direction_qualifier = None
        else:
            raise NotImplementedError(f'Effect {record["EFFECT"]} could not be mapped to required qualifiers.')

        if record["subject_category"] == "protein" and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            ## now use the column("DIRECT") to decide whether a separate biolink:directly_physically_interacts_with needs to be added
            ## record["DIRECT"] == "YES", then add a separate biolink:directly_physically_interacts_with edge
            ## otherwise, don't add a separate edge
            if record["DIRECT"] == "YES":
                ### two associations created
                association_1 = GeneRegulatesGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    ## QW: strange the GeneRegulatesGeneAssociation class doesn't support causal_mechanism_qualifier
                    # causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                )

                association_2 = GeneRegulatesGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:directly_physically_interacts_with",
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    ## QW: strange the GeneRegulatesGeneAssociation class doesn't support causal_mechanism_qualifier
                    # causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                )

                if publications and association_1 is not None and association_2 is not None:
                    association_1.publications = publications
                    association_2.publications = publications

                if supporting_text and association_1 is not None and association_2 is not None:
                    association_1.supporting_text = supporting_text
                    association_2.supporting_text = supporting_text

                if confidence_score and association_1 is not None and association_2 is not None:
                    association_1.has_confidence_score = confidence_score
                    association_2.has_confidence_score = confidence_score

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)
            else:
                association = GeneRegulatesGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    ## QW: strange the GeneRegulatesGeneAssociation class doesn't support causal_mechanism_qualifier
                    # causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                )

                if publications:
                    association.publications = publications
                if supporting_text:
                    association.supporting_text = supporting_text
                if confidence_score:
                    association.has_confidence_score = confidence_score

                if subject is not None and object is not None and association is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association)

        elif record["subject_category"] == "protein" and record["object_category"] == "complex" and record["EFFECT"] in list_pci_accept_effects:
            ## should be protein -> is part_of -> a complex, so no need to reverse the order of subject and object
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = MacromolecularComplex(id="SIGNOR:" + record["IDB"], name=record["object_name"])

            if record["EFFECT"] == 'form complex':
                association = Association(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes
                    predicate = "biolink:part_of",
                    ## should be missing values for qualified predicate for this combo
                    ##qualified_predicate = None,
                    # object_aspect_qualifier = object_aspect_qualifier,
                    # object_direction_qualifier = object_direction_qualifier,
                    # causal_mechanism_qualifier = current_causal_mechanism_mapping,
                )

                if publications:
                    association.publications = publications
                if supporting_text:
                    association.supporting_text = supporting_text
                if confidence_score:
                    association.has_confidence_score = confidence_score

                if subject is not None and object is not None and association is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association)

        elif record["subject_category"] == "protein" and record["object_category"] == "chemical" and record["EFFECT"] in list_ppi_accept_effects:
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = ChemicalEntity(id=record["IDB"], name=record["object_name"])

            ## now use the column("DIRECT") to decide whether a separate biolink:directly_physically_interacts_with needs to be added
            ## record["DIRECT"] == "YES", then add a separate biolink:directly_physically_interacts_with edge
            ## otherwise, don't add a separate edge
            if record["DIRECT"] == "YES":
                ### two associations created
                association_1 = GeneAffectsChemicalAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                    anatomical_context_qualifier = anatomical_context_qualifier,
                )

                association_2 = GeneAffectsChemicalAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:directly_physically_interacts_with",
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    ## QW: strange the GeneRegulatesGeneAssociation class doesn't support causal_mechanism_qualifier
                    # causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                )

                if publications and association_1 is not None and association_2 is not None:
                    association_1.publications = publications
                    association_2.publications = publications

                if supporting_text and association_1 is not None and association_2 is not None:
                    association_1.supporting_text = supporting_text
                    association_2.supporting_text = supporting_text

                if confidence_score and association_1 is not None and association_2 is not None:
                    association_1.has_confidence_score = confidence_score
                    association_2.has_confidence_score = confidence_score

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)
            else:
                association = GeneAffectsChemicalAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                    anatomical_context_qualifier = anatomical_context_qualifier,
                )

                if publications:
                    association.publications = publications
                if supporting_text:
                    association.supporting_text = supporting_text
                if confidence_score:
                    association.has_confidence_score = confidence_score

                if subject is not None and object is not None and association is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association)

        elif (record["subject_category"] == "chemical" or record["subject_category"] == "smallmolecule") and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            ## now use the column("DIRECT") to decide whether a separate biolink:directly_physically_interacts_with needs to be added
            ## record["DIRECT"] == "YES", then add a separate biolink:directly_physically_interacts_with edge
            ## otherwise, don't add a separate edge
            if record["DIRECT"] == "YES":
                ### two associations created
                association_1 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_AFFECTS,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                    anatomical_context_qualifier = anatomical_context_qualifier,
                )

                association_2 = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:directly_physically_interacts_with",
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    ## QW: strange the GeneRegulatesGeneAssociation class doesn't support causal_mechanism_qualifier
                    # causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                )

                if publications and association_1 is not None and association_2 is not None:
                    association_1.publications = publications
                    association_2.publications = publications

                if supporting_text and association_1 is not None and association_2 is not None:
                    association_1.supporting_text = supporting_text
                    association_2.supporting_text = supporting_text

                if confidence_score and association_1 is not None and association_2 is not None:
                    association_1.has_confidence_score = confidence_score
                    association_2.has_confidence_score = confidence_score

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)
            else:
                association = ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## five edge attributes in order
                    predicate = current_predicate_mapping,
                    qualified_predicate = BIOLINK_CAUSES,
                    object_aspect_qualifier = object_aspect_qualifier,
                    object_direction_qualifier = object_direction_qualifier,
                    causal_mechanism_qualifier = current_causal_mechanism_mapping,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                    anatomical_context_qualifier = anatomical_context_qualifier,
                )

                if publications:
                    association.publications = publications
                if supporting_text:
                    association.supporting_text = supporting_text
                if confidence_score:
                    association.has_confidence_score = confidence_score

                if subject is not None and object is not None and association is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association)

        elif record["subject_category"] == "smallmolecule" and (record["object_category"] == "chemical" or record["object_category"] == "smallmolecule") and record["EFFECT"] in list_ppi_accept_effects:
            ## chemical entity already have CHEBI prefix
            subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])
            object = ChemicalEntity(id=record["IDB"], name=record["object_name"])

            ## now use the column("DIRECT") to decide whether a separate biolink:directly_physically_interacts_with needs to be added
            ## record["DIRECT"] == "YES", then add a separate biolink:directly_physically_interacts_with edge
            ## otherwise, don't add a separate edge
            if record["DIRECT"] == "YES":
                ### two associations created
                association_1 = ChemicalEntityToChemicalEntityAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = current_predicate_mapping,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                    ## no following inputs
                    # qualified_predicate = "biolink:causes",
                    # object_aspect_qualifier = object_aspect_qualifier,
                    # object_direction_qualifier = object_direction_qualifier
                )

                association_2 = ChemicalEntityToChemicalEntityAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:directly_physically_interacts_with",
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

                if publications and association_1 is not None and association_2 is not None:
                    association_1.publications = publications
                    association_2.publications = publications

                if supporting_text and association_1 is not None and association_2 is not None:
                    association_1.supporting_text = supporting_text
                    association_2.supporting_text = supporting_text

                if confidence_score and association_1 is not None and association_2 is not None:
                    association_1.has_confidence_score = confidence_score
                    association_2.has_confidence_score = confidence_score

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)
            else:
                association = ChemicalEntityToChemicalEntityAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = current_predicate_mapping,
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    ## additional species and anatomical_context qualifiers if existing in the current association type
                    species_context_qualifier = species_context_qualifier,
                    ## no following inputs
                    # qualified_predicate = "biolink:causes",
                    # object_aspect_qualifier = object_aspect_qualifier,
                    # object_direction_qualifier = object_direction_qualifier
                )

                if publications:
                    association.publications = publications
                if supporting_text:
                    association.supporting_text = supporting_text
                if confidence_score:
                    association.has_confidence_score = confidence_score

                if subject is not None and object is not None and association is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
