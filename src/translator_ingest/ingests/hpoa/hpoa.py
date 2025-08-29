"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and phenotypic features, together with their evidence,
and age of onset and frequency (if known).

The general design of this code comes from the Monarch Initiative, in particular,
https://github.com/monarch-initiative/monarch-phenotype-profile-ingest
"""

from loguru import logger
from typing import Optional, Any, Iterable
from os.path import join, abspath

import duckdb

import koza
from koza.utils.exceptions import MapItemException
from koza.model.graphs import KnowledgeGraph

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Disease,
    PhenotypicFeature,
    DiseaseToPhenotypicFeatureAssociation,
    CausalGeneToDiseaseAssociation,
    CorrelatedGeneToDiseaseAssociation,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from translator_ingest import PRIMARY_DATA_PATH
from translator_ingest.util.github import GitHubReleases
from translator_ingest.util.biolink import (
    INFORES_HPOA,
    entity_id,
    build_association_knowledge_sources
)

from translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    get_hpoa_association_sources,
    evidence_to_eco,
    sex_format,
    sex_to_pato,
    Frequency,
    phenotype_frequency_to_hpo_term,
    get_hpoa_genetic_predicate,
    hpo_to_mode_of_inheritance
)

HPOA_PHENOTYPE_FILE: str = abspath(join(PRIMARY_DATA_PATH, "hpoa", "phenotype.hpoa"))
HPOA_GENES_TO_DISEASE_FILE: str = abspath(join(PRIMARY_DATA_PATH, "hpoa", "genes_to_disease.txt"))
HPOA_GENES_TO_PHENOTYPE_FILE: str = abspath(join(PRIMARY_DATA_PATH, "hpoa", "genes_to_phenotype.txt"))
HPOA_GENES_TO_PHENOTYPE_PREPROCESSED_FILE: str = \
    abspath(join(PRIMARY_DATA_PATH, "hpoa", "genes_to_phenotype_preprocessed.tsv"))


def get_latest_version() -> str:
    ghr = GitHubReleases(git_org="obophenotype", git_repo="human-phenotype-ontology")
    return ghr.get_latest_version()

"""
This particular Translator Ingest module targets the "phenotype.hpoa" file for parsing.

This parser only processes out "phenotypic anomaly" (aspect == 'P')
and "inheritance" (aspect == 'I') annotation records.
Association to "remarkable normality" may be added later.
"""
@koza.transform_record(tag="disease_to_phenotype")
def transform_record_disease_to_phenotype(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform a 'phenotype.hpoa' data entry into a
    (Pydantic encapsulated) Biolink knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: 2-Tuple of Iterable instances for generated node (NamedThing) and edge (Association)
    """
    try:
        ## Subject: Disease

        disease_id = record["database_id"]
        disease_name = record["disease_name"]
        disease: Disease = Disease(
            id=disease_id,
            name=disease_name,
            provided_by=get_hpoa_association_sources(source_id=disease_id, as_list=True),
            **{}
        )

        ## Object: PhenotypicFeature defined by an HPO term
        hpo_id = record["hpo_id"]
        assert hpo_id, "HPOA phenotype annotation record has missing HP ontology ('HPO_ID') field identifier?"

        if record["aspect"] == "P":

            # Disease to Phenotypic anomaly relationship

            ## Object: PhenotypicFeature
            phenotype: PhenotypicFeature = PhenotypicFeature(id=hpo_id, **{})

            ## Annotations

            ### Predicate negation
            negated: bool
            if record["qualifier"] == "NOT":
                negated = True
            else:
                negated = False

            ## Biological gender
            ### female -> PATO:0000383
            ### male -> PATO:0000384
            sex: Optional[str] = record["sex"] if record["sex"] else None  # may be translated by local table
            sex_qualifier = sex_to_pato[sex_format[sex]] if sex and sex in sex_format else None
            #sex_qualifier = sex_format[sex] if sex in sex_format else None

            ## Onset
            onset = record["onset"] if record["onset"] else None

            ## Frequency of occurrence
            # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
            frequency: Frequency = phenotype_frequency_to_hpo_term(record["frequency"])

            ## Evidence Code
            # Three letter Evidence Code Ontology ("ECO") term translated
            # to ECO class CURIE based on HPO documentation
            evidence_curie = evidence_to_eco[record["evidence"]]

            ## Publications
            references: str = record["reference"]
            publications: list[str] = references.split(";")

            ## don't populate the reference with the database_id / disease id
            publications = [p for p in publications if not p == record["database_id"]]

            ## Filter out NCBI web publication endpoints
            publications = [p for p in publications if not p.startswith("http")]

            # Association/Edge
            association = DiseaseToPhenotypicFeatureAssociation(
                id=entity_id(),
                subject=disease_id.replace("ORPHA:", "Orphanet:"),  # match `Orphanet` as used in Mondo SSSOM
                predicate="biolink:has_phenotype",
                negated=negated,
                object=hpo_id,
                publications=publications,
                has_evidence=[evidence_curie],
                sex_qualifier=sex_qualifier,
                onset_qualifier=onset,
                has_percentage=frequency.has_percentage,
                has_quotient=frequency.has_quotient,
                frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
                has_count=frequency.has_count,
                has_total=frequency.has_total,
                sources=get_hpoa_association_sources(disease_id),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                **{}
            )
            return KnowledgeGraph(nodes=[disease, phenotype], edges=[association])

        elif record["aspect"] == "I":

            # We ignore records that don't map to a known HPO term for Genetic Inheritance
            # (as recorded in the locally bound 'hpoa-modes-of-inheritance' table)
            if hpo_id and hpo_id in hpo_to_mode_of_inheritance:

                # Rather than an association, we simply record a
                # genetic inheritance node property directly on the disease node...
                disease.inheritance = hpo_to_mode_of_inheritance[hpo_id]

            else:
                raise RuntimeWarning(
                    f"HPOA ID field value '{str(hpo_id)}' is missing or is an unknown disease mode of inheritance?")

            # ...only the disease node - annotated with its inheritance - is returned
            return KnowledgeGraph(nodes=[disease])
        else:
            # Specified record 'aspect' is not of interest to us at this time
            return None

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return None

@koza.transform_record(tag="gene_to_disease")
def transform_record_gene_to_disease(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
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

        if predicate == "biolink:causes":
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

        return KnowledgeGraph(nodes=[gene, disease], edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return None

@koza.prepare_data(tag="gene_to_phenotype")
def prepare_data_gene_to_phenotype(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[dict[str, Any]] | None:
    """
    For HPOA, we need to preprocess data to join data
    from two files: phenotype.hpoa and genes_to_phenotype.txt
    :param koza_transform: koza.KozaTransform
    :param data: Iterable[dict[str, Any]]
    :return: Iterable[dict[str, Any]] | None
    """
    db = duckdb.connect(":memory:", read_only=False)
    return db.execute(f"""
    with
      hpoa as (select * from read_csv('{HPOA_PHENOTYPE_FILE}')),
      g2p as (select * from read_csv('{HPOA_GENES_TO_PHENOTYPE_FILE}')),
      g2d as (select 
        replace(ncbi_gene_id, 'NCBIGene:', '') as ncbi_gene_id_clean,
        disease_id, 
        association_type 
        from read_csv('{HPOA_GENES_TO_DISEASE_FILE}')),
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
    """).fetchall()

@koza.transform_record(tag="gene_to_phenotype")
def transform_record_gene_to_phenotype(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
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

        dis_id = record["disease_id"].replace("ORPHA:", "Orphanet:")
        try:
            # Convert disease identifier to mondo term identifier if possible...
            dis_id = koza_transform.lookup(name=dis_id, map_column="subject_id", map_name="mondo_map")
        except MapItemException:
            # ...otherwise leave as is
            pass

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

        return KnowledgeGraph(nodes=[gene, phenotype], edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return None
