import uuid
import koza
from typing import Iterable, Tuple

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

# Functions decorated with @koza.on_data_begin() or @koza.on_data_end() are optional. If implemented they will be called
# before and/or after transform or transform_record.
@koza.on_data_begin()
def prepare(koza: koza.KozaTransform) -> None:
    koza.state['example_counter'] = 0

@koza.on_data_end()
def clean_up(koza: koza.KozaTransform) -> None:
    if koza.state['example_counter'] > 0:
        koza.log(f'Uh oh, {koza.state['example_counter']} things happened!', level="WARNING")

# Ingests must implement a function decorated with @koza.transform() OR @koza.transform_record() (not both).
# These functions should contain the core data transformation logic generating and returning NamedThings (nodes) and
# Associations (edges) from source data.
#
# The transform_record function takes the KozaTransform and a single record, a dictionary typically corresponding to a
# row in a source data file, and returns a tuple of NamedThings and Associations. Any number of NamedThings and/or
# Associations can be returned.
@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict) -> (Iterable[NamedThing], Iterable[Association]):
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


# As an alternative to transform_record, functions decorated with @koza.transform() take a KozaTransform and expect the
# function to access its data, KozaTransform.data is an iterable of dictionaries typically corresponding to all the rows
# in a source data file, and returns an iterable of tuples of NamedThings and Associations. Any number of NamedThings
# and/or Associations can be returned in as many separate tuples as desired. This offers flexibility for ingest
# implementers. Results can be returned all at once, in batches, or using a generator for streaming.
@koza.transform()
def transform(koza: koza.KozaTransform) -> Iterable[Tuple[Iterable[NamedThing], Iterable[Association]]]:
    for record in koza.data:
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
