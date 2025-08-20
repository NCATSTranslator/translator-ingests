"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between genes and associated diseases.
"""
from loguru import logger
from typing import Any, Iterable

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

import koza

from translator_ingest.util.biolink import entity_id, BIOLINK_CAUSES

from translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    get_hpoa_genetic_predicate,
    get_hpoa_association_sources
)

# All HPOA ingest submodules share one simplistic ingest versioning (for now)
from translator_ingest.ingests.hpoa import get_latest_version

@koza.transform_record()
def transform_record(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> tuple[Iterable[NamedThing], Iterable[Association]]:
    """
    Transform an HPOA 'genes_to_disease.txt' data entry into a
    (Pydantic encapsulated) Biolink knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: 2-Tuple of Iterable instances for generated node (NamedThing) and edge (Association)
    """
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
