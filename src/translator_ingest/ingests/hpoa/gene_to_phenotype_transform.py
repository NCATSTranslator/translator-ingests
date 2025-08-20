"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between genes and associated phenotypes.
"""
from loguru import logger
from typing import Any, Iterable

import duckdb

from translator_ingest.ingests.hpoa import (
    HPOA_PHENOTYPE_FILE,
    HPOA_GENES_TO_DISEASE_FILE,
    HPOA_GENES_TO_PHENOTYPE_FILE,
    HPOA_GENES_TO_PHENOTYPE_PREPROCESSED_FILE
)
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


@koza.on_data_begin()
def prepare(koza: koza.KozaTransform) -> None:
    """
    For HPOA, we need to preprocess data to join data from two files: phenotype.hpoa and genes_to_phenotype.txt
    :param koza: koza.KozaTransform
    :return: None
    """
    db = duckdb.connect(":memory:", read_only=False)
    db.execute(f"""
    copy (
    with
      hpoa as (select * from read_csv('{HPOA_PHENOTYPE_FILE}')),
      g2p as (select * from read_csv('{HPOA_GENES_TO_DISEASE_FILE}')),
      g2d as (select 
        replace(ncbi_gene_id, 'NCBIGene:', '') as ncbi_gene_id_clean,
        disease_id, 
        association_type 
        from read_csv('{HPOA_GENES_TO_PHENOTYPE_FILE}')),
      g2d_grouped as (select 
        ncbi_gene_id_clean,
        disease_id,
        array_to_string(list(distinct association_type), ';') as association_types
        from g2d 
        group by ncbi_gene_id_clean, disease_id)
    select g2p.*, 
           array_to_string(list(hpoa.reference),';') as publications,
           coalesce(g2d_grouped.association_types, '') as gene_to_disease_association_types
    from g2p
         left outer join hpoa on hpoa.hpo_id = g2p.hpo_id
                     and g2p.disease_id = hpoa.database_id
    		             and hpoa.frequency = g2p.frequency
         left outer join g2d_grouped on g2p.ncbi_gene_id = g2d_grouped.ncbi_gene_id_clean
                     and g2p.disease_id = g2d_grouped.disease_id
    group by all
    ) to '{HPOA_GENES_TO_PHENOTYPE_PREPROCESSED_FILE}' (delimiter '\t', header true)
    """)


@koza.transform_record()
def transform_record(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> tuple[Iterable[NamedThing], Iterable[Association]]:
    """
    Transform a (preprocessed) genes_to_disease.txt data entry into a
    (Pydantic encapsulated) Biolink knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: 2-Tuple of Iterable instances for generated node (NamedThing) and edge (Association)
    """

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
