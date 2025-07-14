import uuid
from typing import Iterator

import requests

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    Entity,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association
)
from bs4 import BeautifulSoup

# ideally we'll use a predicate enum, maybe an infores enum?
BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"
INFORES_CTD = "infores:ctd"


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

"""
def prepare(records: Iterator[dict] = None) -> Iterator[dict] | None:
    # prepare is just a function that gets run before transform or transform_record ie to seed a database
    # return an iterator of dicts if that makes sense,
    # or we could use env vars to just provide access to the data/db in transform()
    return records
"""

def transform_record(record: dict) -> (Iterator[Entity], Iterator[Association]):
    chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=str(uuid.uuid4()),
        subject=chemical.id,
        predicate=BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
        object=disease.id,
        publications=["PMID:" + p for p in record["PubMedIDs"].split("|")],
        # is this code/repo an aggregator in this context? feels like no, but maybe yes?
        # aggregator_knowledge_source=["infores:???"],
        primary_knowledge_source=INFORES_CTD,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    return [chemical, disease], [association]

"""
this is just an example of the interface, using transform() offers the opportunity to do something more efficient
def transform(records: Iterator[dict]) -> Iterator[tuple[Iterator[Entity], Iterator[Association]]]:
    for record in records:
        chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
        disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
        association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
            id=str(uuid.uuid4()),
            subject=chemical.id,
            predicate=BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
            object=disease.id,
            publications=["PMID:" + p for p in record["PubMedIDs"].split("|")],
            # is this code/repo an aggregator in this context? feels like no, but maybe yes?
            # aggregator_knowledge_source=["infores:???"],
            primary_knowledge_source=INFORES_CTD,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        yield [chemical, disease], [association]
"""