import uuid
from typing import Iterator, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association
)


# Biolink predicates and infores identifiers will eventually be imported from automatically updated modules,
# but for now use hardcoded constants.
BIOLINK_RELATED_TO = "biolink:related_to"
INFORES_CTD = "infores:ctd"


# Retrieve and return a string representing the latest version of the source data.
# If a source does not implement versioning, we need to do it. For static datasets assign a version string
# corresponding to the current version. For sources that are updated regularly, use file modification dates if
# possible or the current date. Versions should (ideally) be sortable (ie YYYY-MM-DD) and should contain no spaces.
def get_latest_version() -> str:
    return "v1"

# Prepare is optional. If implemented it will be called before transform or transform_record.
# Prepare should be used for things like reformatting data, seeding a database from a dump file, or converting database
# query results into records for the transform. It should not be used for creating NamedThings or Associations.
#
# Return an iterator of dicts (records for transform) or set up env vars that can be used later to access to the data.
def prepare(records: Iterator[dict] = None) -> Iterator[dict] | None:
    return records

# Ingests must implement transform OR transform_record (not both). These functions should contain the core data
# transformation logic generating NamedThings (nodes) and Associations (edges) from source data.
#
# The transform_record function takes a single record, a dictionary typically corresponding to a row in a source data
# file, and returns a tuple of NamedThings and Associations. Any number of NamedThings and/or Associations can be
# returned.
def transform_record(record: dict) -> (Iterable[NamedThing], Iterable[Association]):
    # The following is just an example from the CTD ingest, replace it.
    chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=str(uuid.uuid4()),
        subject=chemical.id,
        predicate=BIOLINK_RELATED_TO,
        object=disease.id,
        publications=["PMID:" + p for p in record["PubMedIDs"].split("|")],
        primary_knowledge_source=INFORES_CTD,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    return [chemical, disease], [association]


# As an alternative to transform_record - the transform function takes an Iterator of records, dictionaries typically
# corresponding to all the rows in a source data file, and returns an iterable of tuples of NamedThings and
# Associations. Any number of NamedThings and/or Associations can be returned in as many separate tuples as desired.
# This offers significant flexibility for ingest implementers. Results can be returned all at once, in batches, or using
# a generator for streaming.
def transform(records: Iterator[dict]) -> Iterable[tuple[Iterable[NamedThing], Iterable[Association]]]:
    # The following is just an example from the CTD ingest, replace it.
    for record in records:
        chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
        disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
        association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
            id=str(uuid.uuid4()),
            subject=chemical.id,
            predicate=BIOLINK_RELATED_TO,
            object=disease.id,
            publications=["PMID:" + p for p in record["PubMedIDs"].split("|")],
            # is this code/repo an aggregator in this context? feels like no, but maybe yes?
            # aggregator_knowledge_source=["infores:???"],
            primary_knowledge_source=INFORES_CTD,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        yield [chemical, disease], [association]
