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


#Functions decorated with @koza.on_data_begin() run before transform or transform_record

#koza.state is a dictionary that can be used to store arbitrary variables
@koza.on_data_begin(tag="chemical_to_disease")
def on_begin_chemical_to_disease(koza: koza.KozaTransform) -> None:
    koza.state['example_error_counter'] = 1
    koza.log('On Data Begin... chemical_to_disease', level="INFO")


#Functions decorated with @koza.on_data_end() run after transform or transform_record
@koza.on_data_end(tag="chemical_to_disease")
def on_end_chemical_to_disease(koza: koza.KozaTransform) -> None:
    koza.log('On Data End... chemical_to_disease', level="INFO")
    if koza.state['example_error_counter'] > 1:
        koza.log(f'Uh oh, {koza.state['example_error_counter']} things happened!', level="WARNING")


@koza.transform_record(tag="chemical_to_disease")
def transform_record_chemical_to_disease(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
    publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate="biolink:treats_or_applied_or_studied_to_treat",
        object=disease.id,
        publications=publications,
        # is this code/repo an aggregator in this context? feels like no, but maybe yes?
        # aggregator_knowledge_source=["infores:???"],
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    return KnowledgeGraph(nodes=[chemical, disease], edges=[association])



@koza.on_data_begin(tag="exposure_events")
def on_begin_exposure_events(koza: koza.KozaTransform) -> None:
    koza.log('On Data Begin... exposure_events', level="INFO")
    koza.state['missing_predicate'] = 0
    koza.state['missing_disease'] = 0
    koza.state['all_predicates_labels'] = set()


#Functions decorated with @koza.on_data_end() run after transform or transform_record
@koza.on_data_end(tag="exposure_events")
def on_end_exposure_events(koza: koza.KozaTransform) -> None:
    koza.log('On Data End.. exposure_events', level="INFO")
    koza.log(f'all CTD predicate values: {koza.state['all_predicates_labels']}', level="INFO")


@koza.transform_record(tag="exposure_events")
def transform_record_exposure_events(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    disease_id = f'MESH:{record['diseaseid']}'
    if not disease_id:
        koza.state['missing_disease'] += 1

    predicate_label = record['outcomerelationship']
    if not predicate_label:
        koza.state['missing_predicate'] += 1

    koza.state['all_predicates_labels'].add(predicate_label)
    return None

    # exposure_id = f'MESH:{record['exposurestressorid']}'
    # publications = f'PMID:{record['reference']}'
