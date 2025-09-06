"""
HPOA processing utility methods
"""
from typing import Optional, Union
from loguru import logger

from pydantic import BaseModel
from biolink_model.datamodel.pydanticmodel_v2 import RetrievalSource

# Module imports solely used for Option 1: Read hpo mode of inheritance from HP.OBO
# from os import sep
# from translator_ingest import PRIMARY_DATA_PATH
# from translator_ingest.util.ontology import read_ontology_to_exclusion_terms

from translator_ingest.util.biolink import (
    INFORES_MEDGEN,
    INFORES_OMIM,
    INFORES_ORPHANET,
    INFORES_DECIFER,
    INFORES_HPOA,
    build_association_knowledge_sources
)


def get_hpoa_association_sources(source_id: str, as_list: bool = False) -> Union[list[RetrievalSource], list[str]]:
    """
    The primary knowledge source may either be inferred from the 'source' string
    matching a characteristic port of a HPOA-coded record['source'] encoded URI
    or from the CURIE namespace of a 'source' string, which is a disease identifier.

    :param source_id: HPOA data field value encoding the primary knowledge source
    :param as_list: boolean if True (default: False), returns the sources as a simple flat list of
                    infores strings (for use as a 'provided_by' node property value)
    :return: Union[list[RetrievalSource], list[str]] of source infores identifiers
    """
    if "medgen" in source_id:
        if as_list:
            return [INFORES_HPOA, INFORES_MEDGEN, INFORES_OMIM]
        else:
            return build_association_knowledge_sources(
                primary=INFORES_HPOA,
                supporting=[INFORES_MEDGEN, INFORES_OMIM]
            )

    elif source_id.startswith("OMIM"):
        if as_list:
            return [INFORES_HPOA, INFORES_OMIM]
        else:
            return build_association_knowledge_sources(
                primary=INFORES_HPOA,
                supporting=[INFORES_OMIM]
            )

    elif "orphadata" in source_id or source_id.startswith("ORPHA") or "orpha" in source_id.lower():
        if as_list:
            return [INFORES_HPOA, INFORES_ORPHANET]
        else:
            return build_association_knowledge_sources(
                primary=INFORES_HPOA,
                supporting=[INFORES_ORPHANET]
            )

    elif source_id.startswith("DECIPHER"):
        if as_list:
            return [INFORES_HPOA, INFORES_DECIFER]
        else:
            return build_association_knowledge_sources(
                primary=INFORES_HPOA,
                supporting=[INFORES_DECIFER]
            )

    else:
        raise ValueError(f"Unknown source '{source_id}' value, can't set the primary knowledge source")


# Evidence Code translations - https://www.ebi.ac.uk/ols4/ontologies/eco
evidence_to_eco: dict = {"IEA": "ECO:0000501", # "inferred from electronic annotation",
                         "PCS": "ECO:0006017", # "published clinical study evidence",
                         "TAS": "ECO:0000304", # "traceable author statement",
                         "ICE": "ECO:0006019"} # "individual clinical experience evidence"

# Sex (right now both all uppercase and all lowercase
sex_format: dict = {"male": "male",
                    "MALE": "male",
                    "female": "female",
                    "FEMALE": "female"}

sex_to_pato: dict = {"female": "PATO:0000383",
                     "male":   "PATO:0000384"}


class FrequencyHpoTerm(BaseModel):
    """
    Data class to store relevant information
    """
    curie: str
    name: str
    lower: float
    upper: float


class Frequency(BaseModel):
    """
    Converts fields to pydantic field declarations
    """
    frequency_qualifier: Optional[str] = None
    has_percentage: Optional[float] = None
    has_quotient: Optional[float] = None
    has_count: Optional[int] = None
    has_total: Optional[int] = None


# HPO "HP:0040279": representing the frequency of phenotypic abnormalities within a patient cohort.
hpo_term_to_frequency: dict = {"HP:0040280": FrequencyHpoTerm(curie="HP:0040280",
                                                              name="Obligate", 
                                                              lower=100.0, 
                                                              upper=100.0), # Always present, i.e., 100% of the cases.
                               "HP:0040281": FrequencyHpoTerm(curie="HP:0040281", 
                                                              name="Very frequent", 
                                                              lower=80.0, 
                                                              upper=99.0), # Present in 80% to 99% of the cases.
                               "HP:0040282": FrequencyHpoTerm(curie="HP:0040282", 
                                                              name="Frequent", 
                                                              lower=30.0, 
                                                              upper=79.0),# Present in 30% to 79% of the cases.
                               "HP:0040283": FrequencyHpoTerm(curie="HP:0040283", 
                                                              name="Occasional", 
                                                              lower=5.0, 
                                                              upper=29.0), # Present in 5% to 29% of the cases.
                               "HP:0040284": FrequencyHpoTerm(curie="HP:0040284", 
                                                              name="Very rare", 
                                                              lower=1.0, 
                                                              upper=4.0), # Present in 1% to 4% of the cases.
                               "HP:0040285": FrequencyHpoTerm(curie="HP:0040285", 
                                                              name="Excluded", 
                                                              lower=0.0, 
                                                              upper=0.0)}  # Present in 0% of the cases.


def get_frequency_hpo_term(hpo_id: str) -> FrequencyHpoTerm:
    """
    :param hpo_id: Candidate "frequency" HPO ID to be looked up
    :return: FrequencyHpoTerm defined by the HPO ID (if available)
    :raise ValueError: when no valid FrequencyHpoTerm is found
    """
    if hpo_id:
        return hpo_term_to_frequency[hpo_id] if hpo_id in hpo_term_to_frequency else None
    else:
        raise ValueError(f"Invalid HPO ID: {hpo_id}")


def map_percentage_frequency_to_hpo_term(percentage: Optional[float]) -> FrequencyHpoTerm:
    """
    Map phenotypic percentage frequency to a corresponding HPO term corresponding to (HP:0040280 to HP:0040285).

    :param percentage: The float number should be in range 0.0 to 100.0 (otherwise, returns None)
    :return: FrequencyHpoTerm mapping onto the percentage range of the term definition
    :raise ValueError: when percentage lies outside any valid FrequencyHpoTerm range
    """
    if percentage is not None:
        for hpo_id, fht in hpo_term_to_frequency.items():
            if fht.lower <= percentage <= fht.upper:
                return fht
    raise ValueError(f"Out-of-bound phenotypic frequency percentage: {percentage}")


def phenotype_frequency_to_hpo_term(frequency_field: Optional[str]) -> Frequency:
    """
    Maps a raw frequency field onto an HPO term, for consistency, since **phenotypes.hpoa** file field 8,
    which tracks phenotypic frequency, has variable values.   There are three allowed options for this field:

    1. A term-id from the HPO-sub-ontology below the term “Frequency” (HP:0040279).
      (since December 2016; before it was a mixture of values). The terms for frequency are in alignment with Orphanet;

    2. A percentage value such as 17%.

    3. A count of patients affected within a cohort. For instance, 7/13 would indicate that 7 of the 13 patients
       with the specified disease were found to have the phenotypic abnormality referred to by the HPO term
       in question in the study referred to by the DB_Reference;

        :param frequency_field: String raw frequency value in one of the three above forms
        :return: Frequency containing the resolved FrequencyHpoTerm range and/or
                 interpreted value (empty Frequency object, if not found)
    """
    quotient: Optional[float] = None
    percentage: Optional[float] = None
    has_count: Optional[int] = None
    has_total: Optional[int] = None
    if frequency_field:
        try:

            if frequency_field.startswith("HP:"):
                hpo_term = get_frequency_hpo_term(hpo_id=frequency_field)

            else:
                if frequency_field.endswith("%"):
                    percentage = round(float(frequency_field.removesuffix("%")),1)
                    quotient = round(percentage / 100.0, 2)

                else:
                    # assume a ratio
                    ratio_parts = frequency_field.split("/")
                    assert len(ratio_parts) == 2, \
                        f"phenotype_frequency_to_hpo_term(): invalid frequency ratio '{frequency_field}'"
                    has_count = int(ratio_parts[0])
                    has_total = int(ratio_parts[1])
                    quotient = round(float(has_count / has_total), 2)
                    percentage = round(quotient * 100.0, 1)

                # This should map onto a non-null HPO term
                hpo_term = map_percentage_frequency_to_hpo_term(percentage)

        except Exception as e:
            # the expected ratio is not recognized
            logger.error(
                "phenotype_frequency_to_hpo_term(): invalid frequency field value " +
                f"'{frequency_field}' of type {type(frequency_field)}, {type(e)} message: {e}"
            )
            return Frequency()

        return Frequency(
            frequency_qualifier=hpo_term.curie if hpo_term else None,
            has_percentage=percentage,
            has_quotient=quotient,
            has_count=has_count,
            has_total=has_total,
        )

    else:
        # may return an empty Frequency object if
        # the original field was empty or has an invalid value
        return Frequency()


def get_hpoa_genetic_predicate(original_predicate: str) -> str:
    """
    Convert the association column into a Biolink Model predicate
    """
    if original_predicate == 'MENDELIAN':
        return "biolink:causes"
    elif original_predicate == 'POLYGENIC':
        return "biolink:contributes_to"
    elif original_predicate == 'UNKNOWN':
        return "biolink:associated_with"
    else:
        raise ValueError(f"Unknown predicate: {original_predicate}")

## MODES OF INHERITANCE

#
# Although Option 1 was adopted by Monarch, option 2 is still used in the initial (August 2025)
# Translator Ingest pipeline HPOA ingest implementation, for practical convenience (for now)
#
# Option 1: Read hpo mode of inheritance terms into memory using the
#           pronto library + hp.obo file + HP:0000005 (Mode of Inheritance) root term
#           from hp ontology using the util.ontology 'read_ontology_to_exclusion_terms' function
#
# Human Phenotype Ontology local file path
# This path should perhaps be dynamically resolved by the Translator Ingest pipeline (Koza?) library
# HPO_FILE_PATH = f"{PRIMARY_DATA_PATH}{sep}hpoa{sep}hp.obo"
# hpo_to_mode_of_inheritance = read_ontology_to_exclusion_terms(ontology_obo_file=HPO_FILE_PATH)

# Option 2: Hardcoded table of HPO "Mode of Inheritance" terms - https://www.ebi.ac.uk/ols4/ontologies/hp
hpo_to_mode_of_inheritance: dict = {
    "HP:0001417": "X-linked inheritance",
    "HP:0000005": "Mode of inheritance",
    "HP:0001423": "X-linked dominant inheritance",
    "HP:0010982": "Polygenic inheritance",
    "HP:0010984": "Digenic inheritance",
    "HP:0001450": "Y-linked inheritance",
    "HP:0001475": "Male-limited autosomal dominant",
    "HP:0032384": "Uniparental isodisomy",
    "HP:0001426": "Multifactorial inheritance",
    "HP:0000006": "Autosomal dominant inheritance",
    "HP:0032113": "Semidominant inheritance",
    "HP:0032382": "Uniparental disomy",
    "HP:0032383": "Uniparental heterodisomy",
    "HP:0001452": "Autosomal dominant contiguous gene syndrome",
    "HP:0003745": "Sporadic",
    "HP:0001425": "Heterogeneous",
    "HP:0001466": "Contiguous gene syndrome",
    "HP:0003744": "Genetic anticipation with paternal anticipation bias",
    "HP:0012274": "Autosomal dominant inheritance with paternal imprinting",
    "HP:0000007": "Autosomal recessive inheritance",
    "HP:0003743": "Genetic anticipation",
    "HP:0001419": "X-linked recessive inheritance",
    "HP:0001442": "Somatic mosaicism",
    "HP:0001428": "Somatic mutation",
    "HP:0010983": "Oligogenic inheritance",
    "HP:0001444": "Autosomal dominant somatic cell mutation",
    "HP:0031362": "Sex-limited autosomal recessive inheritance",
    "HP:0025352": "Autosomal dominant germline de novo mutation",
    "HP:0001470": "Sex-limited autosomal dominant",
    "HP:0012275": "Autosomal dominant inheritance with maternal imprinting",
    "HP:0001427": "Mitochondrial inheritance",
    "HP:0010985": "Gonosomal inheritance",
    "HP:0003829": "Typified by incomplete penetrance",
    "HP:0003831": "Typified by age-related disease onset",
    "HP:0034344": "Female-limited expression",
    "HP:4000158": "Typified by high penetrance"
}
