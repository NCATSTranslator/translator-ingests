"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and phenotypic features, together with their evidence,
and age of onset and frequency (if known).

The general design of this code comes from the Monarch Initiative, in particular,
https://github.com/monarch-initiative/monarch-phenotype-profile-ingest

The parser currently only processes the "abnormal" annotations.
Association to "remarkable normality" will be added soon.

This parser only processes out "phenotypic anomaly" (aspect == 'P')
and "inheritance" (aspect == 'I') annotation records.
"""

from typing import Optional, List, Dict, Iterable
from loguru import logger
import uuid

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

from .phenotype_ingest_utils import (
    evidence_to_eco,
    sex_format,
    sex_to_pato,
    phenotype_frequency_to_hpo_term,
    Frequency,
    read_ontology_to_exclusion_terms
)

# All HPOA ingest submodules share one
# simplistic ingest versioning (for now)
from . import get_latest_version


"""
def prepare(records: Iterator[dict] = None) -> Iterator[dict] | None:
    # prepare is just a function that gets run before transform or transform_record ie to seed a database
    # return an iterator of dicts if that makes sense,
    # or we could use env vars to just provide access to the data/db in transform()
    return records
"""

#
# TODO: perhaps eventually delete this commented out code
##### ORIGINAL Koza-centric ingest code for disease-phenotype relationship
#
#
# koza_app = get_koza_app("hpoa_disease_to_phenotype")
#
# while (row := koza_app.get_row()) is not None:
#
#     # Nodes
#     disease_id = row["database_id"]
#
#     predicate = "biolink:has_phenotype"
#
#     hpo_id = row["hpo_id"]
#     assert hpo_id, "HPOA Disease to Phenotype has missing HP ontology ('HPO_ID') field identifier?"
#
#     # Predicate negation
#     negated: Optional[bool]
#     if row["qualifier"] == "NOT":
#         negated = True
#     else:
#         negated = False
#
#     # Annotations
#
#     # Translations to curies
#     # Three letter ECO code to ECO class based on hpo documentation
#     evidence_curie = evidence_to_eco[row["evidence"]]
#
#     # female -> PATO:0000383
#     # male -> PATO:0000384
#     sex: Optional[str] = row["sex"]  # may be translated by local table
#     sex_qualifier = sex_to_pato[sex_format[sex]] if sex in sex_format else None
#     #sex_qualifier = sex_format[sex] if sex in sex_format else None
#
#     onset = row["onset"]
#
#     # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
#     frequency: Frequency = phenotype_frequency_to_hpo_term(row["frequency"])
#
#     # Publications
#     publications_field: str = row["reference"]
#     publications: List[str] = publications_field.split(";")
#
#     # don't populate the reference with the database_id / disease id
#     publications = [p for p in publications if not p == row["database_id"]]
#
#     primary_knowledge_source = get_supporting_knowledge_source(disease_id )
#
#     # Association/Edge
#     association = DiseaseToPhenotypicFeatureAssociation(id="uuid:" + str(uuid.uuid1()),
#                                                         subject=disease_id.replace("ORPHA:", "Orphanet:"),  # match `Orphanet` as used in Mondo SSSOM
#                                                         predicate=predicate,
#                                                         negated=negated,
#                                                         object=hpo_id,
#                                                         publications=publications,
#                                                         has_evidence=[evidence_curie],
#                                                         sex_qualifier=sex_qualifier,
#                                                         onset_qualifier=onset,
#                                                         has_percentage=frequency.has_percentage,
#                                                         has_quotient=frequency.has_quotient,
#                                                         frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
#                                                         has_count=frequency.has_count,
#                                                         has_total=frequency.has_total,
#                                                         aggregator_knowledge_source=["infores:monarchinitiative","infores:hpo-annotations"],
#                                                         primary_knowledge_source=primary_knowledge_source,
#                                                         knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
#                                                         agent_type=AgentTypeEnum.manual_agent)
#
#     koza_app.write(association)
#
#
##### ORIGINAL Koza-centric ingest code for disease inheritance
#
# koza_app = get_koza_app("hpoa_disease_mode_of_inheritance")
#
# while (row := koza_app.get_row()) is not None:
#
#     # Object: Actually a Genetic Inheritance (as should be specified by a suitable HPO term)
#     # TODO: perhaps load the proper (Genetic Inheritance) node concepts into the Monarch Graph (simply as Ontology terms?).
#     hpo_id = row["hpo_id"]
#
#     # We ignore records that don't map to a known HPO term for Genetic Inheritance
#     # (as recorded in the locally bound 'hpoa-modes-of-inheritance' table)
#     if hpo_id and hpo_id in modes_of_inheritance:
#
#         # Nodes
#
#         # Subject: Disease
#         disease_id = row["database_id"]
#
#         # Predicate (canonical direction)
#         predicate = "biolink:has_mode_of_inheritance"
#
#         # Annotations
#
#         # Three letter ECO code to ECO class based on HPO documentation
#         evidence_curie = evidence_to_eco[row["evidence"]]
#
#         # Publications
#         publications_field: str = row["reference"]
#         publications: List[str] = publications_field.split(";")
#
#         # Filter out some weird NCBI web endpoints
#         publications = [p for p in publications if not p.startswith("http")]
#
#         # Association/Edge
#         association = DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation(id="uuid:" + str(uuid.uuid1()),
#                                                                                 subject=disease_id,
#                                                                                 predicate=predicate,
#                                                                                 object=hpo_id,
#                                                                                 publications=publications,
#                                                                                 has_evidence=[evidence_curie],
#                                                                                 aggregator_knowledge_source=["infores:monarchinitiative"],
#                                                                                 primary_knowledge_source="infores:hpo-annotations",
#                                                                                 knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
#                                                                                 agent_type=AgentTypeEnum.manual_agent)
#         koza_app.write(association)
#
#     else:
#         logger.warning(f"HPOA ID field value '{str(hpo_id)}' is missing or an invalid disease mode of inheritance?")
#

# Read hpo mode of inheritance terms into memory using the
# pronto library + hp.obo file + HP:0000005 (Mode of Inheritance) root term
modes_of_inheritance = read_ontology_to_exclusion_terms()

def get_supporting_knowledge_source(disease_id: str) -> str:
    if disease_id.startswith("OMIM"):
        return "infores:omim"
    elif disease_id.startswith("ORPHA") or "orpha" in disease_id.lower():
        return "infores:orphanet"
    elif disease_id.startswith("DECIPHER"):
        return "infores:decipher"
    else:
        raise ValueError(f"Unknown disease ID prefix for {disease_id}, can't set primary_knowledge_source")

def transform_record(record: Dict) -> (Iterable[NamedThing], Iterable[Association]):
    """

    :param record:
    :return:
    """
    disease: Disease
    try:
        # Nodes

        ## Subject: Disease
        disease_id = record["database_id"]
        disease = Disease(id=disease_id, **{})

        # Track the original source of the disease-phenotype/inheritance relationship
        supporting_knowledge_source = get_supporting_knowledge_source(disease_id)

        ## Object: PhenotypicFeature defined by an HPO term
        hpo_id = record["hpo_id"]
        assert hpo_id, "HPOA phenotype annotation record has missing HP ontology ('HPO_ID') field identifier?"
        phenotype = PhenotypicFeature(id=hpo_id, **{})

        # Edge

        # Edge annotation common to all 'aspect' types

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
            negated: Optional[bool]
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
                id="uuid:" + str(uuid.uuid1()),
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
                # TODO: the Biolink Model for edge provenance is under some revision,
                #       deprecating the use of direct *_knowledge_source tags
                primary_knowledge_source="infores:hpo-annotations",
                # supporting_knowledge_source=supporting_knowledge_source,
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
                    id="uuid:" + str(uuid.uuid1()),
                    subject=disease_id,
                    predicate="biolink:has_mode_of_inheritance", # Predicate (canonical direction)
                    object=hpo_id,
                    publications=publications,
                    has_evidence=[evidence_curie],
                    primary_knowledge_source="infores:hpo-annotations",
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


"""
this is just an example of the interface, using transform() offers the opportunity to do something more efficient
def transform(records: Iterator[Dict]) -> Iterable[tuple[Iterable[NamedThing], Iterable[Association]]]:
    for record in records:
        # Nodes
        disease_id = record["database_id"]
        disease = Disease(id=disease_id, **{})
    
        predicate = "biolink:has_phenotype"
    
        hpo_id = record["hpo_id"]
        assert hpo_id, "HPOA Disease to Phenotype has missing HP ontology ('HPO_ID') field identifier?"
        phenotype = PhenotypicFeature(id=hpo_id, **{})
    
        # Predicate negation
        negated: Optional[bool]
        if record["qualifier"] == "NOT":
            negated = True
        else:
            negated = False
    
        # Annotations
    
        # Translations to curies
        # Three letter ECO code to ECO class based on hpo documentation
        evidence_curie = evidence_to_eco[record["evidence"]]
    
        # female -> PATO:0000383
        # male -> PATO:0000384
        sex: Optional[str] = record["sex"]  # may be translated by local table
        sex_qualifier = sex_to_pato[sex_format[sex]] if sex in sex_format else None
        #sex_qualifier = sex_format[sex] if sex in sex_format else None
    
        onset = record["onset"]
    
        # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
        frequency: Frequency = phenotype_frequency_to_hpo_term(record["frequency"])
    
        # Publications
        publications_field: str = record["reference"]
        publications: List[str] = publications_field.split(";")
    
        # don't populate the reference with the database_id / disease id
        publications = [p for p in publications if not p == record["database_id"]]
    
        supporting_knowledge_source = get_supporting_knowledge_source(disease_id)
    
        # Association/Edge
        association = DiseaseToPhenotypicFeatureAssociation(
            id="uuid:" + str(uuid.uuid1()),
            subject=disease_id.replace("ORPHA:", "Orphanet:"),  # match `Orphanet` as used in Mondo SSSOM
            predicate=predicate,
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
            # TODO: the Biolink Model for edge provenance is under some revision,
            #       deprecating the use of direct *_knowledge_source tags
            primary_knowledge_source="infores:hpo-annotations",
            # supporting_knowledge_source=supporting_knowledge_source,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
            **{}
        )
    
        yield [disease,phenotype], [association]
"""
