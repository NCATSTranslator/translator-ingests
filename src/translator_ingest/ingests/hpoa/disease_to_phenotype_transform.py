"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and phenotypic features, together with their evidence,
and age of onset and frequency (if known).

The general design of this code comes from the Monarch Initiative, in particular,
https://github.com/monarch-initiative/monarch-phenotype-profile-ingest

This particular Translator Ingest module targets the "phenotype.hpoa" file for parsing.

This parser only processes out "phenotypic anomaly" (aspect == 'P')
and "inheritance" (aspect == 'I') annotation records.
Association to "remarkable normality" may be added later.
"""
from loguru import logger
from typing import Optional, Tuple, List, Dict, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Disease,
    PhenotypicFeature,
    DiseaseToPhenotypicFeatureAssociation,
    DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from translator_ingest.util.biolink import entity_id
from translator_ingest.util.ontology import read_ontology_to_exclusion_terms

from translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    HPO_FILE_PATH,
    evidence_to_eco,
    sex_format,
    sex_to_pato,
    phenotype_frequency_to_hpo_term,
    Frequency,
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

# Read hpo mode of inheritance terms into memory using the
# pronto library + hp.obo file + HP:0000005 (Mode of Inheritance) root term
# TODO: how do I best configure this for mock data for unit testing?
modes_of_inheritance = read_ontology_to_exclusion_terms(ontology_obo_file=HPO_FILE_PATH)


def transform_record(record: Dict) -> Tuple[Iterable[NamedThing], Iterable[Association]]:
    """
    Transform a 'phenotype.hpoa' data entry into a
    (Pydantic encapsulated) Biolink knowledge graph statement.
    :param record: Dict of the contents of a single input data record
    :return: 2-Tuple of Iterable instances for generated node (NamedThing) and edge (Association)
    """
    try:
        # Nodes
        ## Subject: Disease
        disease_id = record["database_id"]
        disease: Disease = Disease(id=disease_id, **{})

        ## Object: PhenotypicFeature defined by an HPO term
        hpo_id = record["hpo_id"]
        assert hpo_id, "HPOA phenotype annotation record has missing HP ontology ('HPO_ID') field identifier?"
        phenotype: PhenotypicFeature = PhenotypicFeature(id=hpo_id, **{})

        # Edge

        ## Edge annotation common to all 'aspect' types

        ## Evidence Code
        # Three letter Evidence Code Ontology ("ECO") term translated
        # to ECO class CURIE based on HPO documentation
        evidence_curie = evidence_to_eco[record["evidence"]]

        ## Publications
        references: str = record["reference"]
        publications: List[str] = references.split(";")

        ## don't populate the reference with the database_id / disease id
        publications = [p for p in publications if not p == record["database_id"]]

        ## Filter out NCBI web publication endpoints
        publications = [p for p in publications if not p.startswith("http")]

        association: Association

        if record["aspect"] == "P":

            # Disease to Phenotypic anomaly relationship

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

        elif record["aspect"] == "I":
            # Potential statement specifying Disease Genetic Inheritance

            # We ignore records that don't map to a known HPO term for Genetic Inheritance
            # (as recorded in the locally bound 'hpoa-modes-of-inheritance' table)
            if hpo_id and hpo_id in modes_of_inheritance:

                # Association/Edge
                association = DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation(
                    id=entity_id(),
                    subject=disease_id,
                    predicate="biolink:has_mode_of_inheritance", # Predicate (canonical direction)
                    object=hpo_id,
                    publications=publications,
                    has_evidence=[evidence_curie],
                    sources=get_hpoa_association_sources(disease_id),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                    **{}
                )
            else:
                raise RuntimeWarning(
                    f"HPOA ID field value '{str(hpo_id)}' is missing or an invalid disease mode of inheritance?")

        else:
            # Specified record 'aspect' is not of interest to us at this time
            return [], []

        return [disease,phenotype], [association]

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return [], []
