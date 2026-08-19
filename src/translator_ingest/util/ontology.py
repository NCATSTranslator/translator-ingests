"""
A module of utilities for accessing public ontology terms
using the EBI Ontology Lookup Service (OLS)
"""

# TODO: this initial implementation is primarily exact matching
#       (with some ad hoc (hard coded) map normalization of some outlier names)
#       However, some use cases may benefit from inexact matches and scoring of hits.

from functools import lru_cache
import requests

OLS_SEARCH = "https://www.ebi.ac.uk/ols4/api/search"

_QUERY_REMAP = {
    "go": {},
    "mondo": {},
    "uberon":
    {
        "csf": "cerebrospinal fluid",
        "cerebrospinal fluid (csf)": "cerebrospinal fluid",
        "faeces": "feces"
    }
}

def _remap(query: str, ontology: str)->str:
    # This method remaps some encountered alias names
    # to more canonical terms for a given ontology
    mappings = _QUERY_REMAP.get(ontology.lower(), {})
    return mappings.get(query.lower(), query)


@lru_cache(maxsize=10000)
def lookup(query: str, ontology: str | None = None)->dict[str,str] | None:

    assert ontology is not None, "Ontology must be specified"

    normalized_query = _remap(query, ontology)

    params = {
        "q": normalized_query,
        "ontology": ontology,
        "exact": "true"
    }

    r = requests.get(OLS_SEARCH, params=params)
    r.raise_for_status()

    docs = r.json()["response"]["docs"]

    if not docs:
        return None

    best = docs[0]

    return {
        "ontology": ontology,
        "input": query,
        "normalization": normalized_query,
        "label": best.get("label"),
        "id": best.get("obo_id"),
        "iri": best.get("iri")
    }

# Not sure how large the caches should be here, but
# there are likely only a modest set of terms accessed per run

@lru_cache(maxsize=None)
def lookup_go(query: str)->dict[str,str] | None:
    """
    Gene Ontology (GO) name to term lookup
    """
    return lookup(query, "go")


@lru_cache(maxsize=None)
def lookup_mondo(query: str)->dict[str,str] | None:
    return lookup(query, "mondo")


@lru_cache(maxsize=None)
def lookup_uberon(query: str)->dict[str,str] | None:
    return lookup(query, "uberon")