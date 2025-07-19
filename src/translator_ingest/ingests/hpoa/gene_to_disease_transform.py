import uuid
from typing import Dict, Iterator

from biolink_model.datamodel.pydanticmodel_v2 import (
    Entity,
    Association,
    Gene,
    Disease,
    CausalGeneToDiseaseAssociation,
    CorrelatedGeneToDiseaseAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from src.translator_ingest.util.monarch.constants import (
    INFORES_MONARCHINITIATIVE,
    BIOLINK_CAUSES
)
from phenotype_ingest_utils import get_knowledge_sources, get_predicate

# All HPOA ingest submodules share one
# simplistic ingest versioning (for now)
from . import get_latest_version


"""
def prepare(records: Iterator[Dict] = None) -> Iterator[Dict] | None:
    # prepare is just a function that gets run before transform or transform_record ie to seed a database
    # return an iterator of dicts if that makes sense,
    # or we could use env vars to just provide access to the data/db in transform()
    return records
"""

#
##### ORIGINAL Koza-centric ingest code
#
# koza_app = get_koza_app("hpoa_gene_to_disease")
#
#
# while (row := koza_app.get_row()) is not None:
#     gene_id = row["ncbi_gene_id"]
#     disease_id = row["disease_id"].replace("ORPHA:", "Orphanet:")
#
#     predicate = get_predicate(row["association_type"])
#     primary_knowledge_source, aggregator_knowledge_source = get_knowledge_sources(row["source"],
#                                                                                   INFORES_MONARCHINITIATIVE)
#
#     if predicate == BIOLINK_CAUSES:
#         association_class = CausalGeneToDiseaseAssociation
#     else:
#         association_class = CorrelatedGeneToDiseaseAssociation
#
#     association = association_class(id="uuid:" + str(uuid.uuid1()),
#                                     subject=gene_id,
#                                     predicate=predicate,
#                                     object=disease_id,
#                                     primary_knowledge_source=primary_knowledge_source,
#                                     aggregator_knowledge_source=aggregator_knowledge_source,
#                                     knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
#                                     agent_type=AgentTypeEnum.manual_agent)
#
#     koza_app.write(association)
#

#
## Sample template record parser (from CTD); used as a guide for coding (to be delated later)
#
# def transform_record(record: Dict) -> (Iterator[Entity], Iterator[Association]):
#     chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
#     disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
#     association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
#         id=str(uuid.uuid4()),
#         subject=chemical.id,
#         predicate=BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
#         object=disease.id,
#         publications=["PMID:" + p for p in record["PubMedIDs"].split("|")],
#         # is this code/repo an aggregator in this context? feels like no, but maybe yes?
#         primary_knowledge_source=INFORES_CTD,
#         knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
#         agent_type=AgentTypeEnum.manual_agent,
#     )
#     return [chemical, disease], [association]

def transform_record(record: Dict) -> (Iterator[Entity], Iterator[Association]):

    gene_id = record["ncbi_gene_id"]
    gene = Gene(id=gene_id, name=record["gene_symbol"],**{})

    predicate = get_predicate(record["association_type"])

    disease_id = record["disease_id"].replace("ORPHA:", "Orphanet:")
    disease = Disease(id=disease_id, **{})

    primary_knowledge_source, aggregator_knowledge_source = \
        get_knowledge_sources(record["source"],INFORES_MONARCHINITIATIVE)

    if predicate == BIOLINK_CAUSES:
        association_class = CausalGeneToDiseaseAssociation
    else:
        association_class = CorrelatedGeneToDiseaseAssociation

    association = association_class(
        id="uuid:" + str(uuid.uuid1()),
        subject=gene_id,
        predicate=predicate,
        object=disease_id,
        primary_knowledge_source=primary_knowledge_source,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        **{}
    )

    return [gene,disease],[association]

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
            primary_knowledge_source=INFORES_CTD,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        yield [chemical, disease], [association]
"""
