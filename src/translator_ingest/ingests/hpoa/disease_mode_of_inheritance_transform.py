"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and their mode of inheritance.

This parser only processes out the "inheritance" (aspect == 'I') annotation records.

Ideally, the Translator Ingest pipeline runner can prefilter
using ingest YAML directives (like in Monarch) but for now,
we implement the filtering here.
"""

from typing import List, Dict, Iterable
from loguru import logger
import uuid

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Disease,
    PhenotypicFeature,
    DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from phenotype_ingest_utils import (
    evidence_to_eco,
    read_ontology_to_exclusion_terms
)

# All HPOA ingest submodules share one
# simplistic ingest versioning (for now)
from . import get_latest_version


# Read hpo mode of inheritance terms into memory using the
# pronto library + hp.obo file + HP:0000005 (Mode of Inheritance) root term
modes_of_inheritance = read_ontology_to_exclusion_terms(
    ontology_obo_file="data/hp.obo", umbrella_term="HP:0000005", include=True
)

def transform_record(record: Dict) -> (Iterable[NamedThing], Iterable[Association]):

    try:
        # TODO: this record filter condition was declared externally in the original Monarch ingest;
        #       should this be done again in the Translator Ingest framework
        if not record["aspect"] or record["aspect"] != "I":
            # Skip this record
            return [],[]

        # Object: Actually a Genetic Inheritance (as should be specified by a suitable HPO term)
        # TODO: perhaps load the proper (Genetic Inheritance) node concepts into the Monarch Graph (simply as Ontology terms?).
        hpo_id = record["hpo_id"]

        # We ignore records that don't map to a known HPO term for Genetic Inheritance
        # (as recorded in the locally bound 'hpoa-modes-of-inheritance' table)
        if hpo_id and hpo_id in modes_of_inheritance:

            # Nodes

            # Subject: Disease
            disease_id = record["database_id"]
            disease = Disease(id=disease_id,**{})

            # Predicate (canonical direction)
            predicate = "biolink:has_mode_of_inheritance"

            # Object: PhenotypicFeature defined by HPO
            phenotype = PhenotypicFeature(id=hpo_id,**{})

            # Annotations

            # Three letter ECO code to ECO class based on HPO documentation
            evidence_curie = evidence_to_eco[record["evidence"]]

            # Publications
            publications_field: str = record["reference"]
            publications: List[str] = publications_field.split(";")

            # Filter out some weird NCBI web endpoints
            publications = [p for p in publications if not p.startswith("http")]

            # Association/Edge
            association = DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation(
                id="uuid:" + str(uuid.uuid1()),
                subject=disease_id,
                predicate=predicate,
                object=hpo_id,
                publications=publications,
                has_evidence=[evidence_curie],
                primary_knowledge_source="infores:hpo-annotations",
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                **{}
            )
            return [disease, phenotype], [association]

        else:
            raise RuntimeWarning(f"HPOA ID field value '{str(hpo_id)}' is missing or an invalid disease mode of inheritance?")

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(str(e))
        return [], []
