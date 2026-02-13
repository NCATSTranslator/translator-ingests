import koza
import pandas as pd

from typing import Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    Protein,
    MacromolecularComplex,
    NamedThing,
    Association,
    GeneRegulatesGeneAssociation,
    PairwiseMolecularInteraction,
    AnatomicalEntityHasPartAnatomicalEntityAssociation,
    GeneAffectsChemicalAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import (
    INFORES_SIGNOR
)

# TODO - was this mapping intended to be applied in some way?
#  for example "smallmolecule" is a type in the source data but is not used, is that right?
# the definition of biolink class can be found here: https://github.com/monarch-initiative/biolink-model-pydantic/blob/main/biolink_model_pydantic/model.py
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
    #                                                                   "").replace('"', '')
    return "2025_Oct"


@koza.prepare_data(tag="signor_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## debugging usage
    # print(source_df.columns)

    ## include some basic quality control steps here
    ## Drop nan values
    source_df = source_df.dropna(subset=['ENTITYA', 'ENTITYB'])

    ## rename those columns into desired format
    source_df.rename(columns={'ENTITYA': 'subject_name', 'TYPEA': 'subject_category', 'ENTITYB': 'object_name', 'TYPEB': 'object_category'}, inplace=True)

    # TODO - it doesn't look like BiologicalProcess is ever used, is this right/necessary?
    # replace phenotype labeling into biologicalProcess
    # source_df["subject_category"] = source_df["subject_category"].replace("phenotype", "BiologicalProcess")
    # source_df["object_category"] = source_df["object_category"].replace("phenotype", "BiologicalProcess")

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


@koza.transform(tag="signor_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:

        list_ppi_accept_effects = ['up-regulates activity', 'up-regulates', 'down-regulates activity', 'down-regulates', 'down-regulates quantity by expression', 'down-regulates quantity by destabilization', 'up-regulates quantity', 'down-regulates quantity']
        list_pci_accept_effects = ['form complex']

        if record["subject_category"] == "protein" and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            if record["EFFECT"] == 'up-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity by expression':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'down-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity by expression':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity by destabilization':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            else:
                raise NotImplementedError(f'Effect {record["EFFECT"]} could not be mapped to required qualifiers.')

            association = GeneRegulatesGeneAssociation(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:regulates",
                sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = "biolink:causes",
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        elif record["subject_category"] == "protein" and record["object_category"] == "complex" and record["EFFECT"] in list_pci_accept_effects:
            subject = MacromolecularComplex(id="SIGNOR:" + record["IDB"], name=record["object_name"])
            object = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])

            if record["EFFECT"] == 'form complex':
                association_1 = AnatomicalEntityHasPartAnatomicalEntityAssociation(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:has_part",
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

                association_2 = PairwiseMolecularInteraction(
                    id=entity_id(),
                    subject=subject.id,
                    object=object.id,
                    predicate = "biolink:physically_interacts_with",
                    sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )

                if subject is not None and object is not None and association_1 is not None and association_2 is not None:
                    nodes.append(subject)
                    nodes.append(object)
                    edges.append(association_1)
                    edges.append(association_2)

        elif record["subject_category"] == "protein" and record["object_category"] == "chemical" and record["EFFECT"] in list_ppi_accept_effects:
            subject = Protein(id="UniProtKB:" + record["IDA"], name=record["subject_name"])
            object = ChemicalEntity(id=record["IDA"], name=record["subject_name"])

            if record["EFFECT"] == 'up-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'down-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity by destabilization':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            else:
                raise NotImplementedError(f'Effect {record["EFFECT"]} could not be mapped to required qualifiers.')

            association = GeneAffectsChemicalAssociation(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:affects",
                sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = "biolink:causes",
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        elif record["subject_category"] == "chemical" and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            if record["EFFECT"] == 'up-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity by expression':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'down-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity by expression':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity by destabilization':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            else:
                raise NotImplementedError(f'Effect {record["EFFECT"]} could not be mapped to required qualifiers.')

            association = GeneRegulatesGeneAssociation(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:affects",
                sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = "biolink:causes",
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        elif record["subject_category"] == "smallmolecule" and record["object_category"] == "protein" and record["EFFECT"] in list_ppi_accept_effects:
            subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])
            object = Protein(id="UniProtKB:" + record["IDB"], name=record["object_name"])

            if record["EFFECT"] == 'up-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'down-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'up-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'down-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            else:
                raise NotImplementedError(f'Effect {record["EFFECT"]} could not be mapped to required qualifiers.')

            association = GeneRegulatesGeneAssociation(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:affects",
                sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                qualified_predicate = "biolink:causes",
                object_aspect_qualifier = object_aspect_qualifier,
                object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        elif record["subject_category"] == "smallmolecule" and record["object_category"] == "chemical" and record["EFFECT"] in list_ppi_accept_effects:
            ## chemical entity already have CHEBI prefix
            subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])
            object = ChemicalEntity(id=record["IDA"], name=record["object_name"])

            if record["EFFECT"] == 'up-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            else:
                raise NotImplementedError(f'Effect {record["EFFECT"]} could not be mapped to required qualifiers.')

            association = ChemicalEntityToChemicalEntityAssociation(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:affects",
                sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                ## no following inputs
                # qualified_predicate = "biolink:causes",
                # object_aspect_qualifier = object_aspect_qualifier,
                # object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

        elif record["subject_category"] == "smallmolecule" and record["object_category"] == "smallmolecule" and record["EFFECT"] in list_ppi_accept_effects:
            subject = ChemicalEntity(id=record["IDA"], name=record["subject_name"])
            object = ChemicalEntity(id=record["IDA"], name=record["object_name"])

            if record["EFFECT"] == 'up-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'up-regulates quantity by expression':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.upregulated
            elif record["EFFECT"] == 'down-regulates activity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated
            elif record["EFFECT"] == 'down-regulates quantity by expression':
                object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
                object_direction_qualifier = DirectionQualifierEnum.downregulated

            association = ChemicalEntityToChemicalEntityAssociation(
                id=entity_id(),
                subject=subject.id,
                object=object.id,
                predicate = "biolink:affects",
                sources=build_association_knowledge_sources(primary=INFORES_SIGNOR),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                ## no following inputs
                # qualified_predicate = "biolink:causes",
                # object_aspect_qualifier = object_aspect_qualifier,
                # object_direction_qualifier = object_direction_qualifier
            )
            if subject is not None and object is not None and association is not None:
                nodes.append(subject)
                nodes.append(object)
                edges.append(association)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
