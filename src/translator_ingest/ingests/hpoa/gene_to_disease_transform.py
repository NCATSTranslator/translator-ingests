"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between genes and associated diseases.
"""
from loguru import logger
from typing import Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Gene,
    Disease,
    CausalGeneToDiseaseAssociation,
    CorrelatedGeneToDiseaseAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from translator_ingest.util.biolink import entity_id, BIOLINK_CAUSES

from translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    get_hpoa_genetic_predicate,
    get_hpoa_association_sources
)

# All HPOA ingest submodules share one simplistic ingest versioning (for now)
from translator_ingest.ingests.hpoa import get_latest_version

"""
def prepare(records: Iterator[dict] = None) -> Iterator[dict] | None:
    # prepare is just a function that gets run before transform or transform_record ie to seed a database
    # return an iterator of dicts if that makes sense,
    # or we could use env vars to just provide access to the data/db in transform()
    return records
"""

def transform_record(record: dict) -> tuple[Iterable[NamedThing], Iterable[Association]]:

    try:
        gene_id = record["ncbi_gene_id"]
        gene = Gene(id=gene_id, name=record["gene_symbol"],**{})

        predicate = get_hpoa_genetic_predicate(record["association_type"])

        disease_id = record["disease_id"].replace("ORPHA:", "Orphanet:")
        disease = Disease(id=disease_id, **{})

        if predicate == BIOLINK_CAUSES:
            association_class = CausalGeneToDiseaseAssociation
        else:
            association_class = CorrelatedGeneToDiseaseAssociation

        association = association_class(
            id=entity_id(),
            subject=gene_id,
            predicate=predicate,
            object=disease_id,
            sources=get_hpoa_association_sources(record["source"]),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
            **{}
        )

        return [gene, disease],[association]

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return [], []
