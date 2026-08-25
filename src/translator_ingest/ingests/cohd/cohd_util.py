"""
This file contains utility functions for COHD data processing
"""

def to_curie(vocabulary_id: str, concept_code: int):
    # TODO: probably need a more complex mapping here
    return f"{vocabulary_id}:{concept_code!s}"

_omop_domain_to_biolink_category: set[str] = { }

def omop_to_biolink_category(omop_domain: str, omop_concept_class: sr)-> str:
    # TODO: first cut is to just use the OMOP domain to select the biolink category
    if omop_domain == "Condition":
        return "biolink:DiseaseOrPhenotypicFeature"
    if omop_domain not in _omop_domain_to_biolink_category:
        return "biolink:NamedThing"
    return f"biolink:{omop_domain}"
