"""
A module of utilities for accessing public ontology terms
using the EBI Ontology Lookup Service (OLS)
"""

# TODO: this initial implementation is primarily exact matching
#       (with some ad hoc (hard coded) map normalization of some outlier names)
#       However, some use cases may benefit from inexact matches and scoring of hits.
from typing import Any
from functools import lru_cache
import re
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


def normalize(text):
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9 ]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text

def rank_candidate(query, candidate)->int:

    q = normalize(query)

    label = normalize(candidate["label"])

    exact_synonyms = {normalize(x) for x in candidate.get("exact_synonyms", [])}

    narrow_synonyms = {normalize(x) for x in candidate.get("narrow_synonyms", [])}

    broad_synonyms = {normalize(x) for x in candidate.get("broad_synonyms", [])}

    related_synonyms = {normalize(x) for x in candidate.get("related_synonyms", [])}

    if q == label:
        return 100

    if q in exact_synonyms:
        return 90

    if q in related_synonyms:
        return 80

    if q in narrow_synonyms:
        return 70

    if q in broad_synonyms:
        return 60

    if q in label:
        return 50

    return 0


def _wrap_result(query: str, ontology:str, entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "ontology": ontology,
        "input": query,
        "label": entry.get("label"),
        "id": entry.get("obo_id"),
        "iri": entry.get("iri"),
        "rank": 100,
    }


@lru_cache(maxsize=10000)
def lookup(
        query: str,
        ontology: str | None = None,
        exact_match: bool = True
)->list[dict[str,Any]] | None:
    """

    :param query: str, the term to lookup
    :param ontology: str, the ontology to query (e.g., "go", "mondo", "uberon")
    :param exact_match: bool, if True, only exact match to term label is returned (default: True)
    :return: list[dict[str, str]] | None, the best match(es), if any, with match metadata
    """
    assert ontology is not None, "Ontology must be specified"

    normalized_query = _remap(query, ontology)

    params = {
        "q": normalized_query,
        "ontology": ontology,
        "exact": exact_match
    }

    r = requests.get(OLS_SEARCH, params=params)
    r.raise_for_status()

    candidates: list[dict[str,Any]] = r.json()["response"]["docs"]

    if not candidates:
        return None

    if not exact_match:
        for candidate in candidates:
            candidate["rank"] = rank_candidate(query, candidate)
    
        sorted_candidates = sorted(candidates, key=lambda x: x["rank"], reverse=True)

        ranked = [_wrap_result(query,ontology, c) for c in sorted_candidates]

    else:
        best = candidates[0]
        best["rank"] = 100
        ranked = [_wrap_result(query,ontology, best)]

    return ranked

# Not sure how large the caches should be here, but
# there are likely only a modest set of terms accessed per run

@lru_cache(maxsize=None)
def lookup_go(query: str, exact_match: bool = False)->list[dict[str,Any]] | None:
    """
    Gene Ontology (GO) name to term lookup
    :param query: str, the term to look up
    :param exact_match: str, if True, only exact match to term label is returned
                        (default: False to encourage exact matches to GO curated synonyms)
    :return: list[dict[str, Any]] | None, the best match, if any, with match metadata
    """
    return lookup(query, "go", exact_match=exact_match)


@lru_cache(maxsize=None)
def lookup_mondo(query: str, exact_match: bool = True)->list[dict[str,Any]] | None:
    """
    Monarch Disease Ontology (MONDO) name to ontology term lookup
    :param query: str, the term to look up
    :param exact_match: str, if True, only exact match to term label is returned
                        (default: True to encourage exact matches to canonical MONDO term names)
    :return: list[dict[str, Any]] | None, the best match, if any, with match metadata
    """
    return lookup(query, "mondo", exact_match=exact_match)


@lru_cache(maxsize=None)
def lookup_uberon(query: str, exact_match: bool = True)->list[dict[str,Any]] | None:
    """
    UBERON name to term lookup
    :param query: str, the term to look up
    :param exact_match: str, if True, only exact match to term label is returned
                        (default: True to encourage exact matches to canonical UBERON term names)
    :return: list[dict[str, Any]] | None, the best match, if any, with match metadata
    """
    return lookup(query, "uberon", exact_match=exact_match)