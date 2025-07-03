import uuid

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

from koza.runner import KozaTransform

# ideally we'll use a predicate enum, maybe an infores enum?
BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"
INFORES_CTD = "infores:ctd"

# hack because koza doesn't yet have a feature to avoid rewriting nodes
seen_nodes = set()


def transform_record(koza: KozaTransform, record: dict):
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

    # todo: don't write conflicting associations, out of scope for this demo
    to_write = [association]
    # todo: an inline hack to avoid rewriting nodes, koza needs support to do this explicitly
    if chemical.id not in seen_nodes:
        to_write.append(chemical)
        seen_nodes.add(chemical.id)
    if disease.id not in seen_nodes:
        to_write.append(disease)
        seen_nodes.add(disease.id)
    koza.write(*to_write)
