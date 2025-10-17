from loguru import logger
from pronto import Ontology


# General function to read an .obo ontology file into memory
# using pronto to gather all terms that do not fall under a particular parent class
# TODO: we somehow need to allow the substitution of OBO mock data for unit testing
#       This file access is also tricky to generally manage since, like mapping files,
#       it is an additional data file used for an ingest. Maybe Koza integration will help.
def read_ontology_to_exclusion_terms(
    ontology_obo_file: str,
    umbrella_term: str = "HP:0000005",  # original default value  "HP:0000118" from Monarch code
    include: bool = True,  # original default value False from Monarch code
):

    # Read the ontology file into memory
    onto = Ontology(ontology_obo_file)
    exclude_terms = {}
    term_count = len(list(onto.terms()))

    for term in onto.terms():

        # Gather ancestor terms and update our filtering data structure accordingly
        parent_terms = {ancestor.id: ancestor.name for ancestor in term.superclasses()}
        if not include:
            if umbrella_term not in parent_terms:
                exclude_terms.update({term.id: term.name})

        elif umbrella_term in parent_terms:
            exclude_terms.update({term.id: term.name})

    logger.info(
        "- Terms from ontology found that "
        "do not belong to parent class {} {}/{}".format(umbrella_term, format(len(exclude_terms)), format(term_count))
    )
    return exclude_terms
