from typing import Any

import requests
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from translator_ingest.util.biolink import (
    INFORES_CTD,
    entity_id,
    build_association_knowledge_sources
)

from bs4 import BeautifulSoup
from koza.model.graphs import KnowledgeGraph


def get_latest_version():
    # CTD doesn't provide a great programmatic way to determine the latest version, but it does have a Data Status page
    # with a version on it. Fetch the html and parse it to determine the current version.
    html_page: requests.Response = requests.get('http://ctdbase.org/about/dataStatus.go')
    resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')
    version_header: BeautifulSoup.Tag = resp.find(id='pgheading')
    if version_header is not None:
        # pgheading looks like "Data Status: July 2025", convert it to "July_2025"
        return version_header.text.split(':')[1].strip().replace(' ', '_')
    else:
        raise RuntimeError('Could not determine latest version for CTD, "pgheading" header was missing...')

@koza.on_data_begin(tag="chemical_to_disease")
def on_begin_chemical_to_disease(koza: koza.KozaTransform) -> None:
    koza.state["rows_missing_publications"] = {}
    koza.state["rows_missing_publications"]["therapeutic"] = 0
    koza.state["rows_missing_publications"]["marker_mechanism"] = 0
    koza.state["rows_missing_publications"]["inference"] = 0

@koza.on_data_end(tag="chemical_to_disease")
def on_end_chemical_to_disease(koza: koza.KozaTransform) -> None:
    for row_type, count in koza.state['rows_missing_publications'].items():
        koza.log(f"CTD chemical_to_disease: {count} {row_type} rows with 0 publications", level="WARNING")

CTD_PREDICATES_BY_EVIDENCE_TYPE = {
    "therapeutic": "biolink:treats_or_applied_or_studied_to_treat",
    "marker/mechanism": "biolink:correlated_with",
    "inference": "biolink:associated_with"  # the files don't have "inference" but we use it in the transform
}

@koza.transform_record(tag="chemical_to_disease")
def transform_record_chemical_to_disease(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    chemical = ChemicalEntity(id=f"MESH:{record["ChemicalID"]}", name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])

    # check the evidence type and assign a predicate based on that
    evidence_type = record["DirectEvidence"] if record["DirectEvidence"] else "inference"
    predicate = CTD_PREDICATES_BY_EVIDENCE_TYPE[evidence_type]

    publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    if not publications:
        koza.state["rows_missing_publications"][evidence_type] += 1

    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate=predicate,
        object=disease.id,
        publications=publications,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    if evidence_type == "inference":
        association.has_confidence_score = float(record["InferenceScore"])
    return KnowledgeGraph(nodes=[chemical, disease], edges=[association])


# @koza.on_data_begin(tag="exposure_events")
# def on_begin_exposure_events(koza: koza.KozaTransform) -> None:
#     koza.log('On Data Begin... exposure_events', level="INFO")
#     koza.state['missing_predicate'] = 0
#     koza.state['missing_disease'] = 0
#     koza.state['all_predicates_labels'] = set()
#
#
# Functions decorated with @koza.on_data_end() run after transform or transform_record
# @koza.on_data_end(tag="exposure_events")
# def on_end_exposure_events(koza: koza.KozaTransform) -> None:
#     koza.log('On Data End.. exposure_events', level="INFO")
#     koza.log(f'all CTD predicate values: {koza.state['all_predicates_labels']}', level="INFO")
#
#
# @koza.transform_record(tag="exposure_events")
# def transform_record_exposure_events(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
#     disease_id = f'MESH:{record['diseaseid']}'
#     if not disease_id:
#         koza.state['missing_disease'] += 1
#
#     predicate_label = record['outcomerelationship']
#     if not predicate_label:
#         koza.state['missing_predicate'] += 1
#
#     koza.state['all_predicates_labels'].add(predicate_label)
#
#     exposure_id = f'MESH:{record['exposurestressorid']}'
#     publications = f'PMID:{record['reference']}'
