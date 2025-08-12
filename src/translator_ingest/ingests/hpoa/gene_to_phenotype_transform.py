"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between genes and associated phenotypes.
"""
from loguru import logger
from typing import Any, Iterable

from translator_ingest.util.biolink import entity_id

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Gene,
    PhenotypicFeature,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

import koza

from translator_ingest.util.biolink import build_association_knowledge_sources, INFORES_HPOA

from src.translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    Frequency,
    phenotype_frequency_to_hpo_term
)

# All HPOA ingest submodules share one simplistic ingest versioning (for now)
from translator_ingest.ingests.hpoa import get_latest_version


@koza.transform_record()
def transform_record(
        koza: koza.KozaTransform,
        record: dict[str, Any]
) -> tuple[Iterable[NamedThing], Iterable[Association]]:

    try:
        gene_id = "NCBIGene:" + record["ncbi_gene_id"]
        gene = Gene(id=gene_id, name=record["gene_symbol"],**{})

        hpo_id = record["hpo_id"]
        assert hpo_id, "HPOA Disease to Phenotype has missing HP ontology ('HPO_ID') field identifier?"
        phenotype = PhenotypicFeature(id=hpo_id, **{})

        # No frequency data provided
        if record["frequency"] == "-":
            frequency = Frequency()
        else:
            # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
            frequency: Frequency = phenotype_frequency_to_hpo_term(record["frequency"])

        # Convert to mondo id if possible, otherwise leave as is
        dis_id = record["disease_id"].replace("ORPHA:", "Orphanet:")

        publications = [pub.strip() for pub in record["publications"].split(";")] if record["publications"] else []

        association = GeneToPhenotypicFeatureAssociation(
            id=entity_id(),
            subject=gene_id,
            predicate="biolink:has_phenotype",
            object=hpo_id,
            frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
            has_percentage=frequency.has_percentage,
            has_quotient=frequency.has_quotient,
            has_count=frequency.has_count,
            has_total=frequency.has_total,
            disease_context_qualifier=dis_id,
            publications=publications,
            sources=build_association_knowledge_sources(primary=INFORES_HPOA),
            knowledge_level=KnowledgeLevelEnum.logical_entailment,
            agent_type=AgentTypeEnum.automated_agent,
            **{}
        )

        return [gene, phenotype],[association]

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return [], []
