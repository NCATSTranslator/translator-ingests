"""
A module of utilities for accessing public ontology terms
using the EBI Ontology Lookup Service (OLS)
"""
from typing import Any
from functools import lru_cache
import re
import requests
from rapidfuzz import fuzz

OLS_SEARCH = "https://www.ebi.ac.uk/ols4/api/search"


# This is just an ad hoc (hard coded) map of some outlier names to canonical terms
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

def rank_candidate(query, candidate)->tuple[tuple[int, float | int, int], str]:

    q = normalize(query)

    label = normalize(candidate["label"])

    exact_synonyms = {normalize(x) for x in candidate.get("exact_synonyms", [])}

    narrow_synonyms = {normalize(x) for x in candidate.get("narrow_synonyms", [])}

    broad_synonyms = {normalize(x) for x in candidate.get("broad_synonyms", [])}

    related_synonyms = {normalize(x) for x in candidate.get("related_synonyms", [])}

    similarity = fuzz.token_set_ratio(q, label)

    if q == label:
        return (100, similarity, -len(label)), "exact match to label"

    if q in exact_synonyms:
        # catches exact matches to any exact synonym
        return (90, similarity, -len(label)), "exact match to exact synonym"
    
    if any(q in es.split() for es in exact_synonyms):
        # catches exact matches to any distinct words within the exact synonyms
        return (85, similarity, -len(label)), "exact match to exact synonym word"

    if q in related_synonyms:
        # catches exact matches to any related synonym
        return (80, similarity, -len(label)), "exact match to related synonym"
    
    if any(q in rs.split() for rs in related_synonyms):
        # catches exact matches to any distinct words within the related synonyms
        return (75, similarity, -len(label)), "exact match to related synonym word"

    if q in narrow_synonyms:
        # catches exact matches to any narrow synonym
        return (70, similarity, -len(label)), "exact match to narrow synonym"

    if any(q in ns.split() for ns in narrow_synonyms):
        # catches exact matches to any distinct words within the narrow synonyms
        return (65, similarity, -len(label)), "exact match to narrow synonym word"

    if q in broad_synonyms:
        # catches exact matches to any broad synonym
        return (60, similarity, -len(label)), "exact match to broad synonym"
    
    if any(q in bs.split() for bs in broad_synonyms):
        # catches exact matches to any distinct words within the broad synonyms
        return (55, similarity, -len(label)), "exact match to broad synonym word"

    if q in label:
        # catches partial matches to any substring within the canonical label
        return (50, similarity, -len(label)), "partial match to label"

    if any(q in es for es in exact_synonyms):
        # catches partial substring matches to any exact synonym
        return (45, similarity, -len(label)), "partial match to exact synonym"

    if any(q in rs for rs in related_synonyms):
        # catches partial substring matches to any related synonym
        return (40, similarity, -len(label)), "partial match to related synonym"

    if any(q in ns for ns in narrow_synonyms):
        # catches partial substring matches to any narrow synonym
        return (35, similarity, -len(label)), "partial match to narrow synonym"

    if any(q in bs for bs in broad_synonyms):
        # catches partial substring matches to any broad synonym
        return (30, similarity, -len(label)), "partial match to broad synonym"

    if similarity > 60:
        return (25, similarity, -len(label)), "fuzzy match to label"

    return (0, similarity, -len(label)), "poor or no match"


def _wrap_result(query: str, ontology:str, entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "ontology": ontology,
        "input": query,
        "label": entry.get("label"),
        "id": entry.get("obo_id"),
        "iri": entry.get("iri"),
        "rank": entry.get("rank"),
        "match_type": entry.get("match_type")
    }


@lru_cache(maxsize=10000)
def lookup(query: str, ontology: str | None = None)->dict[str,Any] | None:
    """

    :param query: str, the term to lookup
    :param ontology: str, the ontology to query (e.g., "go", "mondo", "uberon")
    :return: dict[str, Any] | None, the best ranked ontology term match, if any, with match metadata
    """
    assert ontology is not None, "Ontology must be specified"

    normalized_query = _remap(query, ontology)

    params = {
        "q": normalized_query,
        "ontology": ontology,
        "exact": "false"
    }

    r = requests.get(OLS_SEARCH, params=params)
    r.raise_for_status()

    candidates: list[dict[str,Any]] = r.json()["response"]["docs"]

    if not candidates:
        return None

    for candidate in candidates:
        candidate["rank"], candidate["match_type"]  = rank_candidate(query, candidate)

    ranked = sorted(candidates, key=lambda x: x["rank"], reverse=True)

    return _wrap_result(query,ontology, ranked[0])

# Not sure how large the caches should be here, but
# there are likely only a modest set of terms accessed per run

@lru_cache(maxsize=None)
def lookup_go(query: str)->dict[str,Any] | None:
    """
    Gene Ontology (GO) name to term lookup
    :param query: str, the term to look up
    :return: dict[str, Any] | None, the best match, if any, with match metadata
    """
    return lookup(query, "go")


@lru_cache(maxsize=None)
def lookup_mondo(query: str)->dict[str,Any] | None:
    """
    Monarch Disease Ontology (MONDO) name to ontology term lookup
    :param query: str, the term to look up
    :return: dict[str, Any] | None, the best match, if any, with match metadata
    """
    return lookup(query, "mondo")


@lru_cache(maxsize=None)
def lookup_uberon(query: str)->dict[str,Any] | None:
    """
    UBERON name to term lookup
    :param query: str, the term to look up
    :return: dict[str, Any] | None, the best match, if any, with match metadata
    """
    return lookup(query, "uberon")