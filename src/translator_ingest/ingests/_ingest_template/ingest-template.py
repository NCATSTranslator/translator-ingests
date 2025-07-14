import uuid
from typing import Iterator

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    Entity,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association
)

BIOLINK_RELATED_TO = "biolink:related_to"
INFORES_TEMPLATE = "infores:template-example"


# Retrieve and return the latest version from the source.
# If a source does not implement versioning, we need to do it. For static datasets assign a version string
# corresponding to the current version. For sources that are updated regularly use file modified dates if
# possible, or the current date.
def get_latest_version() -> str:
    return "v1"

# Prepare is optional. If implemented it will be called before transform or transform_record, for example to seed a
# database or reformat data. Return an iterator of dicts or set env vars that can be used by a transform to provide
# access to the data.
def prepare(records: Iterator[dict] = None) -> Iterator[dict] | None:
    return records

# Each ingest must implement transform or transform_record

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
def transform(records: Iterator[Dict]) -> Iterator[tuple[Iterator[Entity], Iterator[Association]]]:
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