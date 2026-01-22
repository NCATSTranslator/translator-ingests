from typing import Any

import requests
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalAffectsGeneAssociation,
    ChemicalEntityToBiologicalProcessAssociation,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalEntityToPathwayAssociation,
    Disease,
    DirectionQualifierEnum,
    Gene,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    Pathway,
    PhenotypicFeature,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from translator_ingest.util.biolink import INFORES_CTD

from bs4 import BeautifulSoup
from koza.model.graphs import KnowledgeGraph


BIOLINK_AFFECTS = "biolink:affects"

# not sure if this is a Biolink Model Pydantic code generation bug,
# but a Biolink CURIE is not expected in a 'qualified_predicate' field
BIOLINK_CAUSES = "causes"

BIOLINK_ASSOCIATED_WITH = "biolink:associated_with"
BIOLINK_CORRELATED_WITH = "biolink:correlated_with"
BIOLINK_POSITIVELY_CORRELATED = "biolink:positively_correlated_with"
BIOLINK_NEGATIVELY_CORRELATED = "biolink:negatively_correlated_with"

BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"

CHEM_TO_DISEASE_PREDICATES = {
    "therapeutic": BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
    "marker/mechanism": BIOLINK_CORRELATED_WITH,
    "inference": BIOLINK_ASSOCIATED_WITH,  # the files don't have "inference" but we use it in the transform
}

EXPOSURE_EVENTS_PREDICATES = {
    "positive correlation": BIOLINK_POSITIVELY_CORRELATED,
    "negative correlation": BIOLINK_NEGATIVELY_CORRELATED
}


# !!! !!! README !!! !!!
# CTD implemented a CAPTCHA which unfortunately breaks dependable programmatic access for determining the version
# and downloading data. If possible, opening a browser and passing the CAPTCHA at ctdbase.org should allow everything
# to run. If not, when determining the latest version, the translator-ingests pipeline will fall back to a previously
# successful build, so if running in an environment where passing the CAPTCHA is not possible, copy a previous CTD
# directory from /data/ including the latest-build.json file and the pipeline will utilize the last successful version.

def get_latest_version():
    # CTD doesn't provide a great programmatic way to determine the latest version, but it does have a Data Status page
    # with a version on it. Fetch the html and parse it to determine the current version.
    html_page: requests.Response = requests.get("http://ctdbase.org/about/dataStatus.go")
    resp: BeautifulSoup = BeautifulSoup(html_page.content, "html.parser")
    version_header: BeautifulSoup.Tag = resp.find(id="pgheading")
    if version_header is not None:
        # pgheading looks like "Data Status: July 2025", convert it to "July_2025"
        return version_header.text.split(":")[1].strip().replace(" ", "_")
    else:
        raise RuntimeError('Could not determine latest version for CTD, "pgheading" header was missing...')

@koza.transform_record(tag="chemicals_diseases")
def transform_chemical_to_disease(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    chemical = ChemicalEntity(id=f"MESH:{record["ChemicalID"]}", name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])

    # Check the evidence type and assign a predicate based on that
    # DirectEvidence should be "therapeutic", "marker/mechanism", or blank (in which case we assign "inference")
    evidence_type = record["DirectEvidence"] if record["DirectEvidence"] else "inference"
    predicate = CHEM_TO_DISEASE_PREDICATES[evidence_type]
    publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate=predicate,
        object=disease.id,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent
    )
    if publications:
        association.publications = publications
    if evidence_type == "inference":
        association.has_confidence_score = float(record["InferenceScore"])
    return KnowledgeGraph(nodes=[chemical, disease], edges=[association])

@koza.transform_record(tag="exposure_events")
def transform_exposure_events(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # get the exposurestressorid and outcomerelationship first, bail if we can't use both
    exposure_chemical_id = f'MESH:{record['exposurestressorid']}' if record['exposurestressorid'] else None
    outcome_relationship = record['outcomerelationship']
    # map the outcomerelationship to a predicate
    predicate = EXPOSURE_EVENTS_PREDICATES.get(outcome_relationship)
    if not (predicate and exposure_chemical_id):
        return None

    nodes = [ChemicalEntity(id=exposure_chemical_id)]
    edges = []
    publications = [f'PMID:{record['reference']}'] if record['reference'] else None

    # diseaseid is a "(MeSH or OMIM identifier)" but doesn't include curie prefixes
    disease_id = record['diseaseid']
    if disease_id:
        # MeSH ids should start with D
        if disease_id.startswith("D") or disease_id.startswith("C"):
            disease_id = f'MESH:{record['diseaseid']}'
        # OMIM ids should just be numbers
        elif disease_id.isdigit():
            disease_id = f'OMIM:{record['diseaseid']}'
        else:
            koza.log(f'Could not determine what kind of diseaseid this is: {disease_id}', level="WARNING")
            disease_id = None
    if disease_id:
        nodes.append(Disease(id=disease_id))
        c_to_d_association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
                id=entity_id(),
                subject=exposure_chemical_id,
                predicate=predicate,
                object=disease_id,
                sources=build_association_knowledge_sources(primary=INFORES_CTD),
                knowledge_level=KnowledgeLevelEnum.statistical_association,
                agent_type=AgentTypeEnum.manual_agent
        )
        if publications:
            c_to_d_association.publications = publications
        edges.append(c_to_d_association)

    # phenotype ids have the "GO:" curie prefix here unlike diseases
    phenotype_id = f'{record['phenotypeid']}' if record['phenotypeid'] else None
    if phenotype_id:
        nodes.append(PhenotypicFeature(id=phenotype_id))
        c_to_p_association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
                id=entity_id(),
                subject=exposure_chemical_id,
                predicate=predicate,
                object=phenotype_id,
                sources=build_association_knowledge_sources(primary=INFORES_CTD),
                knowledge_level=KnowledgeLevelEnum.statistical_association,
                agent_type=AgentTypeEnum.manual_agent
        )
        if publications:
            c_to_p_association.publications = publications
        edges.append(c_to_p_association)

    if edges:
        return KnowledgeGraph(nodes=nodes, edges=edges)
    return None

@koza.on_data_begin(tag="chem_gene_ixns")
def on_chem_gene_ixns_begin(koza: koza.KozaTransform):
    koza.transform_metadata['unmapped_chem_gene_ixns'] = set()

@koza.on_data_end(tag="chem_gene_ixns")
def on_chem_gene_ixns_end(koza: koza.KozaTransform):
    koza.transform_metadata['unmapped_chem_gene_ixns'] = (
        list(koza.transform_metadata['unmapped_chem_gene_ixns']))

@koza.transform_record(tag="chem_gene_ixns")
def transform_chem_gene_ixns(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # chemical ids are mesh ids without the curie prefix
    chemical_id = f'MESH:{record['ChemicalID']}'
    # gene ids are NCBIGene ids without the curie prefix
    gene_id = f'NCBIGene:{record['GeneID']}'
    # organism ids are NCBITaxon ids without the curie prefix
    taxon_id = f'NCBITaxon:{record['OrganismID']}'

    interactions = record['InteractionActions'].split('|')
    if len(interactions) > 1:
        # TODO these interactions involve multiple chemicals or terms,
        #  most of them are hard/impossible to parse into self-contained edges, but we may be able to do some of them.
        return None
    interaction = interactions[0]
    interaction_direction, interaction_aspect = interaction.split("^")

    object_direction_qualifier = None
    object_aspect_qualifier = None

    match interaction_direction:
        case 'increases':
            object_direction_qualifier = DirectionQualifierEnum.increased
        case 'decreases':
            object_direction_qualifier = DirectionQualifierEnum.decreased
        case 'affects':
            pass
        case _:
            koza.transform_metadata['unmapped_chem_gene_ixns'].add(interaction)

    match interaction_aspect:
        case 'activity':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.activity
        case 'expression':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.expression
        case 'phosphorylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.phosphorylation
        case 'lipidation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.lipidation
        case 'sumoylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.sumoylation
        case 'N-linked glycosylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.n_linked_glycosylation
        case 'glycosylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.glycosylation
        case 'uptake':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.uptake
        case 'methylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.methylation
        case 'carbamoylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.carbamoylation
        case 'secretion':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.secretion
        case 'abundance':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
        case 'amination':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.amination
        case 'carboxylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.carboxylation
        case 'farnesylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.farnesylation
        case 'localization':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.localization
        case 'acylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.acylation
        case 'ethylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.ethylation
        case 'glucuronidation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.glucuronidation
        case 'splicing':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.splicing
        case 'stability':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.stability
        case 'folding':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.folding
        case 'acetylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.acetylation
        case 'ADP-ribosylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.ADP_ribosylation
        case 'ubiquitination':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.ubiquitination
        case 'reduction':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.reduction
        case 'cleavage':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.cleavage
        case 'nitrosation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.nitrosation
        case 'glycation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.glycation
        case 'hydroxylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.hydroxylation
        case 'oxidation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.oxidation
        case 'hydrolysis':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.hydrolysis
        case 'metabolic processing':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.metabolic_processing
        case 'glutathionylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.glutathionylation
        case 'prenylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.prenylation
        case 'degradation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.degradation
        case 'ribosylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.ribosylation
        case 'geranoylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.geranoylation
        case 'sulfation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.sulfation
        case 'O-linked glycosylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.o_linked_glycosylation
        case 'palmitoylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.palmitoylation
        case 'transport':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.transport
        case 'alkylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.alkylation
        case 'myristoylation':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.myristoylation
        # these next two were not exact matches to biolink aspects
        case 'chemical synthesis':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.synthesis
        case 'mutagenesis':
            object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.mutation_rate
        case _:
            koza.transform_metadata['unmapped_chem_gene_ixns'].add(interaction)

    publications = [f'PMID:{pmid}' for pmid in record['PubMedIDs'].split('|')]

    association = ChemicalAffectsGeneAssociation(
        id=entity_id(),
        subject=chemical_id,
        predicate="biolink:affects",
        object=gene_id,
        qualified_predicate="causes",
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        publications=publications,
        species_context_qualifier=taxon_id
    )
    if object_aspect_qualifier:
        association.object_aspect_qualifier = object_aspect_qualifier
    if object_direction_qualifier:
        association.object_direction_qualifier = object_direction_qualifier

    return KnowledgeGraph(nodes=[ChemicalEntity(id=chemical_id),
                                 Gene(id=gene_id)],
                          edges=[association])

@koza.transform_record(tag="chem_go_enriched")
def transform_chem_go_enriched(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # chemical ids are mesh ids without the curie prefix
    chemical_id = f'MESH:{record['ChemicalID']}'
    # GO curies
    go_term = record['GOTermID']
    p_value = record['PValue']
    corrected_p_value = record['CorrectedPValue']
    edge = ChemicalEntityToBiologicalProcessAssociation(
        id=entity_id(),
        subject=chemical_id,
        predicate="biolink:affects",
        object=go_term,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.statistical_association,
        agent_type=AgentTypeEnum.data_analysis_pipeline,
        p_value=p_value,
        adjusted_p_value=corrected_p_value
    )
    return KnowledgeGraph(nodes=[ChemicalEntity(id=chemical_id),
                                 Pathway(id=go_term)],
                          edges=[edge])


@koza.transform_record(tag="chem_pathways_enriched")
def transform_chem_pathways_enriched(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # chemical ids are mesh ids without the curie prefix
    chemical_id = f'MESH:{record['ChemicalID']}'
    # these are curies, either REACT: or KEGG:
    # replace KEGG with KEGG.PATHWAY as it is in the biolink model
    pathway_id = record['PathwayID'].replace('KEGG', 'KEGG.PATHWAY')
    p_value = record['PValue']
    corrected_p_value = record['CorrectedPValue']
    edge = ChemicalEntityToPathwayAssociation(
        id=entity_id(),
        subject=chemical_id,
        predicate="biolink:associated_with",
        object=pathway_id,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.statistical_association,
        agent_type=AgentTypeEnum.data_analysis_pipeline,
        p_value=p_value,
        adjusted_p_value=corrected_p_value
    )
    return KnowledgeGraph(nodes=[ChemicalEntity(id=chemical_id),
                                 Pathway(id=pathway_id)],
                          edges=[edge])


@koza.on_data_begin(tag="pheno_term_ixns")
def on_pheno_ixns_begin(koza: koza.KozaTransform):
    koza.transform_metadata['unmapped_pheno_ixn_types'] = set()


@koza.on_data_end(tag="pheno_term_ixns")
def on_pheno_ixns_end(koza: koza.KozaTransform):
    koza.transform_metadata['unmapped_pheno_ixn_types'] = list(koza.transform_metadata['unmapped_pheno_ixn_types'])


@koza.transform_record(tag="pheno_term_ixns")
def transform_pheno_term_ixns(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # chemical ids are mesh ids without the curie prefix
    chemical_id = f'MESH:{record['chemicalid']}'
    # phenotypes are GO curies
    phenotype_id = record['phenotypeid']
    # organismid is an ncbitaxon id
    species = f"NCBITaxon:{record['organismid']}"
    publications = [f'PMID:{pmid}' for pmid in record['pubmedids'].split('|')]
    # AnatomyTerms (MeSH term; '|'-delimited list) entries formatted as SequenceOrder^Name^Id
    # example: 1^Lung^D008168|2^Cell Line, Tumor^D045744
    # extract the mesh ids and make them a list of curies
    anatomies = [f'MESH:{anatomy_entry.split('^')[-1]}' for anatomy_entry in record['anatomyterms'].split("|")]
    interactions = record['interactionactions'].split('|')
    if len(interactions) > 1:
        # TODO these interactions involve multiple chemicals or terms,
        #  most of them are hard/impossible to parse into self-contained edges, but we may be able to do some of them.
        return
    interaction = interactions[0]
    object_direction_qualifier = None
    match interaction:
        case 'increases^phenotype':
            object_direction_qualifier = "increased"
        case 'decreases^phenotype':
            object_direction_qualifier = "decreased"
        case 'affects^phenotype':
            pass
        case _:
            koza.transform_metadata['unmapped_pheno_ixn_types'].add(interaction)

    edge = ChemicalEntityToBiologicalProcessAssociation(
        id=entity_id(),
        subject=chemical_id,
        predicate="biolink:affects",
        object=phenotype_id,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        publications=publications,
        species_context_qualifier=species,
        anatomical_context_qualifier=anatomies
    )
    if object_direction_qualifier:
        edge.object_direction_qualifier=object_direction_qualifier
        edge.qualified_predicate = BIOLINK_CAUSES
    return KnowledgeGraph(nodes=[ChemicalEntity(id=chemical_id),
                                 PhenotypicFeature(id=phenotype_id)],
                          edges=[edge])
