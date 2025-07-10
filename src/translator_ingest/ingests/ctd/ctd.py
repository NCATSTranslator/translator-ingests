import uuid
from typing import Iterable

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

# hack because koza doesn't yet have a feature to avoid rewriting nodes
seen_nodes = set()

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


def transform_record(record: dict) -> (Iterable[Entity], Iterable[Association]):
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
def transform(records: Iterable[dict]) -> (Iterable[Entity], Iterable[Association]):
    nodes = []
    edges = []
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
        nodes.append(chemical)
        nodes.append(disease)
        edges.append(association)
    return nodes, edges
"""