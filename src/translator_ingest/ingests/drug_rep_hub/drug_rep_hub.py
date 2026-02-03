import koza
import json
import re

from typing import Any, Iterable
from collections import defaultdict
from translator_ingest import INGESTS_PARSER_PATH

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    ChemicalAffectsGeneAssociation,
    DiseaseOrPhenotypicFeature,
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id, build_association_knowledge_sources


inchikey_regex = re.compile('^A-Z]{14}-[A-Z]{10}-[A-Z]$')

INFORES_DRUG_REP_HUB = "infores:drug-repurposing-hub"
PUBCHEM_PREFIX = "PUBCHEM.COMPOUND:"
INCHIKEY_PREFIX = "INCHIKEY:"
SMILES_PREFIX = "SMILES:"

SAMPLES = defaultdict(dict)

def load_json_config(filename: str) -> dict:
    path = INGESTS_PARSER_PATH / 'drug_rep_hub' / filename
    """Load a JSON config file and return its contents."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


INDICATIONS = load_json_config('indications_config.json')
TARGETS = load_json_config('target_config.json')


predicate_map = {
    'Launched':'biolink:treats',
    'Phase 1':'biolink:in_clinical_trial_for',
    'Phase 1/Phase 2':'biolink:in_clinical_trial_for',
    'Phase 2':'biolink:in_clinical_trial_for',
    'Phase 2/Phase 3':'biolink:in_clinical_trial_for',
    'Phase 3':'biolink:in_clinical_trial_for',
    'Preclinical':'biolink:in_preclinical_trials_for',
    'Withdrawn':'biolink:treats_or_applied_or_studied_to_treat',
    '':'biolink:treats_or_applied_or_studied_to_treat'
}

clinical_approval_map = {
    'Launched':'approved_for_condition',
    'Withdrawn':'post_approval_withdrawal',
}

research_phase_map = {
    'Phase 1':'clinical_trial_phase_1',
    'Phase 1/Phase 2':'clinical_trial_phase_1_to_2',
    'Phase 2':'clinical_trial_phase_2',
    'Phase 2/Phase 3':'clinical_trial_phase_2_to_3',
    'Phase 3':'clinical_trial_phase_3',
    'Preclinical':'pre_clinical_research_phase',
}


# Always implement a function that returns a string representing the latest version of the source data.
# Ideally, this is the version provided by the knowledge source, directly associated with a specific data download.
# If a source does not implement versioning, we need to do it. For static datasets, assign a version string
# corresponding to the current version. For sources that are updated regularly, use file modification dates if
# possible, or the current date. Versions should (ideally) be sortable (ie YYYY-MM-DD) and should contain no spaces.
def get_latest_version() -> str:
    return "2025-08-19"



@koza.transform(tag="ingest_drug_rep_hub_samples")
def transform_drug_rep_hub_samples(
    koza: koza.KozaTransform, data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    global SAMPLES
    for record in data:
        nodes = []
        name = record["pert_iname"]
        inchikey = record["InChIKey"] if inchikey_regex.match(record["InChIKey"]) else None
        source_name = record["vendor_name"]
        pubchem_id = record["pubchem_cid"]

        id = None
        xref = None
        if pubchem_id.isdigit():
            id = PUBCHEM_PREFIX + str(pubchem_id)
            xref = [INCHIKEY_PREFIX + inchikey] if inchikey else None
        elif inchikey:
            id = INCHIKEY_PREFIX + inchikey

        if id:
            synonyms = None
            if source_name and source_name.lower() != name.lower():
                synonyms = [source_name] if source_name != name else None
            chemical = ChemicalEntity(
                id=id,
                name=name,
                synonym=synonyms,
                xref=xref
            )
            # nodes.append(chemical)
            SAMPLES[name][id] = chemical
        yield KnowledgeGraph(nodes=nodes, edges=[])


def create_disease_association(chemical, indication, indication_info, predicate, clinical_phase, disease_area):
    """
    Create a disease association between a chemical and a disease/phenotypic feature.
    """
    disease = DiseaseOrPhenotypicFeature(
        id=indication_info['xref'],
        name=indication_info['primary_name'] if indication_info['primary_name'] else indication,
    )
    if predicate is None:
        return disease, None
    #TODO: clinical_approval_status = clinical_approval_map.get(clinical_phase, None)
    #TODO: max_research_phase = research_phase_map.get(clinical_phase, None)
    association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id = entity_id(),
        subject=chemical.id,
        object=disease.id,
        predicate=predicate,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        sources=build_association_knowledge_sources(INFORES_DRUG_REP_HUB),
        original_object = indication,
        #TODO: clinical_approval_status=clinical_approval_status,
        #TODO: max_research_phase=max_research_phase,
        #TODO: disease_area=disease_area,
    )
    return disease, association


def create_chemical_role_association(chemical, indication, indication_info, predicate):
    chemical_role = ChemicalEntity(
        id=indication_info['xref'],
        name=indication_info['primary_name'] if indication_info['primary_name'] else indication,
    )
    association = ChemicalEntityToChemicalEntityAssociation(
        id = entity_id(),
        subject=chemical.id,
        predicate=predicate,
        object=chemical_role.id,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        sources=build_association_knowledge_sources(INFORES_DRUG_REP_HUB),
        original_object = indication
    )
    return chemical, association


def create_target_association(chemical, target_gene_symbol, moa):
    if target_gene_symbol not in TARGETS:
        return None, None
    target_id = TARGETS[target_gene_symbol]
    target = Gene(
        id=target_id,
        name=target_gene_symbol,
        symbol = target_gene_symbol
    )
    association = ChemicalAffectsGeneAssociation(
        id = entity_id(),
        subject=chemical.id,
        predicate='biolink:affects',
        object=target.id,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        sources=build_association_knowledge_sources(INFORES_DRUG_REP_HUB),
        description = moa
    )
    return target, association


def build_indication_association(chemical: ChemicalEntity, clinical_phase: str, indication: str, disease_area: str):
    if indication not in INDICATIONS:
        return None, None
    indication_info = INDICATIONS[indication]
    predicate = indication_info['predicate']
    if predicate == 'biolink:treats':
        predicate = predicate_map[clinical_phase]
    if predicate == 'biolink:has_chemical_role':
        return create_chemical_role_association(chemical, indication, indication_info, predicate)
    else:
        return create_disease_association(chemical, indication, indication_info, predicate, clinical_phase, disease_area)


@koza.prepare_data(tag="ingest_drug_rep_hub_annotations")
def prepare_complexes(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    koza.state['samples'] = SAMPLES
    for record in data:
        yield record


@koza.transform(tag="ingest_drug_rep_hub_annotations")
def transform_drug_rep_hub_annotations(
    koza: koza.KozaTransform, data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    for record in data:
        nodes = []
        edges = []
        pert_iname = record["pert_iname"]
        clinical_phase = record["clinical_phase"]
        moa = record["moa"]
        targets = record["target"].strip()
        disease_area = record["disease_area"]
        indications = record["indication"].strip()
        if pert_iname in koza.state['samples']:
            for chem_id, chemical in koza.state['samples'][pert_iname].items():
                nodes.append(chemical)
                for indication in [ind.strip() for ind in indications.split('|')]:
                    disease, indication_association = build_indication_association(chemical, clinical_phase, indication, disease_area)
                    if indication_association:
                        nodes.append(disease)
                        edges.append(indication_association)
                if targets:
                    target_gene_symbols = [gene.strip() for gene in targets.split('|')]
                    for target_gene_symbol in target_gene_symbols:
                        target, target_association = create_target_association(chemical, target_gene_symbol, moa)
                        if target_association:
                            nodes.append(target)
                            edges.append(target_association)

        if len(edges) == 0:
            nodes = []  # no associations, skip
        yield KnowledgeGraph(nodes=nodes, edges=edges)

