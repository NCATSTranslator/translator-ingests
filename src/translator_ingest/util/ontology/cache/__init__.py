"""
Code supporting multi-level access of name-to-term mappings for public ontology terms
using the (Translator Phase 2 built) RENCI SRI Name Resolution Service (NRS)
at the end point at https://name-resolution-sri.renci.org/docs#
"""
from typing import Any
from functools import lru_cache
import requests

from .file_cache import file_cache

NRS_ENDPOINT = "https://name-resolution-sri.renci.org"
NRS_LOOKUP = f"{NRS_ENDPOINT}/lookup"

def _wrap_result(query: str, ontology: str, entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "ontology": ontology,
        "input": query,
        "label": entry.get("label"),
        "id": entry.get("obo_id"),
        "iri": entry.get("iri"),
        "score": entry.get("score")
    }


def api_lookup(
    query: str,
    ontology: str,
    only_taxa: str | None = None
) -> dict[str, Any] | None:
    """
    Name Resolution Service API lookup for ontology term entries.

    Typical output is a single entry, with the following keys (from the Translator Name Resolver Service (NRS) API):
        curie, label, highlighting, synonyms, taxa, score, clique_identifier_count
    
    :param query: str, The (hopefully normalized?) query concept string to look up
    :param ontology: str, The ontology namespace to query (e.g., "GO", "MONDO", "UBERON","NCBIGene","CHEMBL")
    :param only_taxa: str | None, Pipe-delimited string of the taxa to query (e.g., "NCBITaxon:9606|NCBITaxon:10090")
                                  Usually None or only one taxon is specified since method returns only one entry.

    :return: dict[str, Any] | None, the best ranked ontology term match, if any, with match metadata (as noted above)
    """
    assert query, "Query must be a non-empty string"

    params = {
        "string": query,
        "autocomplete": "false",
        "highlighting": "false",
        "offset": 0,
        "limit": 10,
        "only_prefixes": ontology
    }

    # only_taxa is optional, but if specified, it can be a
    # pipe-delimited string of taxon IDs, but is usually just one ID
    if only_taxa is not None:
        params["only_taxa"] = only_taxa

    r = requests.get(NRS_LOOKUP, params=params)
    r.raise_for_status()

    candidates: list[dict[str, Any]] = r.json()
    
    if not candidates:
        return None
    
    # Assume that all NRS candidates have a score for ranking,
    # and that the largest score is the best match
    ranked = sorted(candidates, key=lambda x: x["score"], reverse=True)

    # 
    # Example of a best match entry returned, from UBERON, using query term 'Placenta'
    #
    # {
    #     "curie": "UBERON:0001987",
    #     "label": "placenta",
    #     "highlighting": {},
    #     "synonyms": [
    #         "PLACENTA",
    #         "Placenta",
    #         "placenta",
    #         "placentas",
    #         "Placentas",
    #         "Placental",
    #         "placentome",
    #         "Placentome",
    #         "Placentomes",
    #         "Placenta, NOS",
    #         "eutherian placenta",
    #         "allantoic placenta",
    #         "Placental structure",
    #         "Placentomes (body structure)",
    #         "Placental structure (body structure)",
    #     ],
    #     "taxa": [],
    #     "types": [
    #         "biolink:GrossAnatomicalStructure",
    #         "biolink:AnatomicalEntity",
    #         "biolink:PhysicalEssence",
    #         "biolink:OrganismalEntity",
    #         "biolink:SubjectOfInvestigation",
    #         "biolink:BiologicalEntity",
    #         "biolink:ThingWithTaxon",
    #         "biolink:NamedThing",
    #         "biolink:Entity",
    #         "biolink:PhysicalEssenceOrOccurrent",
    #     ],
    #     "score": 1720.6982,
    #     "clique_identifier_count": 4,
    # }
    return ranked[0]


@lru_cache(maxsize=10000)
def cache_lookup(
    query: str,
    ontology: str,
    only_taxa: str | None = None
) -> dict[str, Any] | None:
    """
    Multi-level cached lookup of ontology term entries whose names best match the query string.

    Typical output is a single dictionary with the following keys (from the Translator Name Resolver Service (NRS)):
        curie, label, highlighting, synonyms, taxa, score, clique_identifier_count

    :param query: str, the (hopefully normalized?) query concept string to look up
    :param ontology: str, the ontology to query (e.g., "GO", "MONDO", "UBERON","NCBIGene","CHEMBL")
    :param only_taxa: str | None, pipe-delimited string of the taxa to query (e.g., "NCBITaxon:9606|NCBITaxon:10090")
                                  Usually None or only one taxon is specified since method returns only one entry.

    :return: dict[str, Any] | None, the best ranked ontology term match, if any, with match metadata (as noted above)
    """
    if not file_cache.contains(query, ontology, only_taxa):
        ontology_term = api_lookup(query, ontology, only_taxa)
        file_cache.save(query, ontology, ontology_term, only_taxa)
        return ontology_term

    return file_cache.retrieve(query, ontology, only_taxa)
