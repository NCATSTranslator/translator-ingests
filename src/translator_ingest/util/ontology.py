"""
A module of utilities for accessing public ontology terms.
"""

from functools import lru_cache
import requests

OLS_SEARCH = "https://www.ebi.ac.uk/ols4/api/search"

UBERON_NORMALIZATION = {
    "csf": "cerebrospinal fluid",
    "faeces": "feces"
}

def _normalize_uberon_query(query):
    return UBERON_NORMALIZATION.get(query.lower(), query)

# Not sure how large a cache is appropriate here, but it's likely a small set of terms accessed per run'
@lru_cache
def lookup_uberon(query: str):

    normalized_query = _normalize_uberon_query(query)

    params = {
        "q": normalized_query,
        "ontology": "uberon",
        "exact": "true"
    }

    r = requests.get(OLS_SEARCH, params=params)
    r.raise_for_status()

    docs = r.json()["response"]["docs"]

    if not docs:
        return None

    best = docs[0]

    return {
        "input": query,
        "normalization": normalized_query,
        "label": best.get("label"),
        "uberon_id": best.get("obo_id"),
        "iri": best.get("iri")
    }
