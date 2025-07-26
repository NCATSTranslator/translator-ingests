"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between genes and associated diseases.
"""
from loguru import logger
from typing import Dict, Iterable
import uuid

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

from translator_ingest.util.monarch.constants import (
    INFORES_MONARCHINITIATIVE,
    BIOLINK_CAUSES
)
from translator_ingest.ingests.hpoa.phenotype_ingest_utils import get_knowledge_sources, get_predicate

# All HPOA ingest submodules share one simplistic ingest versioning (for now)
from src.translator_ingest.ingests.hpoa import get_latest_version
from translator_ingest.util.biolink import build_association_sources

"""
def prepare(records: Iterator[dict] = None) -> Iterator[dict] | None:
    # prepare is just a function that gets run before transform or transform_record ie to seed a database
    # return an iterator of dicts if that makes sense,
    # or we could use env vars to just provide access to the data/db in transform()
    return records
"""

def transform_record(record: Dict) -> (Iterable[NamedThing], Iterable[Association]):

    try:
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
            sources=build_association_sources(),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
            **{}
        )

        return [gene, disease],[association]

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return [], []
