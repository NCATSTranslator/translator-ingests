import uuid
import koza
import pandas as pd
from typing import Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association,
)
from translator_ingest.util.biolink import INFORES_CTD, entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph


def get_latest_version() -> str:
    return "2024-08-20"  # last Phase 2 ICEES release

@koza.transform_record(tag="icees_nodes")
def transform_ingest_by_record(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:

    # here is an example of skipping a record based off of some condition
    publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    if not publications:
        koza.state["example_counter"] += 1
        return None

    chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])

    return KnowledgeGraph(nodes=[chemical, disease])

@koza.transform_record(tag="icees_edges")
def transform_ingest_by_record(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:

    # here is an example of skipping a record based off of some condition
    publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    if not publications:
        koza.state["example_counter"] += 1
        return None

    chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate="biolink:related_to",
        object=disease.id,
        publications=publications,
        sources=build_association_knowledge_sources(primary=INFORES_CTD),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    return KnowledgeGraph(edges=[association])
