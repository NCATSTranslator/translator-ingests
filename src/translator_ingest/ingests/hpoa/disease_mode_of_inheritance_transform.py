"""
The [Human Phenotype Ontology](http://human-phenotype-ontology.org) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and their mode of inheritance.

This parser only processes out the "inheritance" (aspect == 'I') annotation records.

filters:
  - inclusion: 'include'
    column: 'aspect'
    filter_code: 'eq'
    value: 'I'

Usage:
poetry run koza transform \
  --global-table src/translator_ingest/util/monarch/translation_table.yaml \
  --local-table src/translator_ingest/ingests/hpoa/hpoa_translation.yaml \
  --source src/translator_ingest/ingests/hpoa/disease_mode_of_inheritance.yaml \
  --output-format tsv
"""

from typing import List
from loguru import logger
import uuid

from biolink_model.datamodel.pydanticmodel_v2 import (
    DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

# from koza.cli_utils import get_koza_app
from koza.runner import KozaTransform

from phenotype_ingest_utils import (
    evidence_to_eco,
    read_ontology_to_exclusion_terms
)

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


# Read hpo mode of inheritance terms into memory using pronto + hp.obo file + HP:0000005 (Mode of Inheritance)
modes_of_inheritance = read_ontology_to_exclusion_terms("data/hp.obo", umbrella_term="HP:0000005", include=True)
koza_app = get_koza_app("hpoa_disease_mode_of_inheritance")

while (row := koza_app.get_row()) is not None:

    # Object: Actually a Genetic Inheritance (as should be specified by a suitable HPO term)
    # TODO: perhaps load the proper (Genetic Inheritance) node concepts into the Monarch Graph (simply as Ontology terms?).
    hpo_id = row["hpo_id"]

    # We ignore records that don't map to a known HPO term for Genetic Inheritance
    # (as recorded in the locally bound 'hpoa-modes-of-inheritance' table)
    if hpo_id and hpo_id in modes_of_inheritance:

        # Nodes

        # Subject: Disease
        disease_id = row["database_id"]

        # Predicate (canonical direction)
        predicate = "biolink:has_mode_of_inheritance"

        # Annotations

        # Three letter ECO code to ECO class based on HPO documentation
        evidence_curie = evidence_to_eco[row["evidence"]]

        # Publications
        publications_field: str = row["reference"]
        publications: List[str] = publications_field.split(";")

        # Filter out some weird NCBI web endpoints
        publications = [p for p in publications if not p.startswith("http")]

        # Association/Edge
        association = DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation(id="uuid:" + str(uuid.uuid1()),
                                                                                subject=disease_id,
                                                                                predicate=predicate,
                                                                                object=hpo_id,
                                                                                publications=publications,
                                                                                has_evidence=[evidence_curie],
                                                                                aggregator_knowledge_source=["infores:monarchinitiative"],
                                                                                primary_knowledge_source="infores:hpo-annotations",
                                                                                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                                                                                agent_type=AgentTypeEnum.manual_agent)
        koza_app.write(association)

    else:
        logger.warning(f"HPOA ID field value '{str(hpo_id)}' is missing or an invalid disease mode of inheritance?")


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
            # aggregator_knowledge_source=["infores:???"],
            primary_knowledge_source=INFORES_CTD,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        yield [chemical, disease], [association]
"""
