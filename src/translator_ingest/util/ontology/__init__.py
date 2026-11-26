"""
A module of utilities for looking up ontology terms
corresponding to concepts defined in the query.
"""
from typing import Any
from functools import lru_cache
from .cache import cache_lookup

#
# RMB: Normalization of query strings does not seem to be necessary for the NRS
#
# def normalize(text):
#     text = text.lower().strip()
#     text = re.sub(r"[^a-z0-9 ]", " ", text)
#     text = re.sub(r"\s+", " ", text)
#     return text

@lru_cache(maxsize=10000)
def lookup(
    query: str,
    ontology: str,
    only_taxa: str | None = None
) -> dict[str, Any] | None:
    """
    Multi-level lookup for ontology term entries.

    Typical output is a single entry, with the following keys (from the Translator Name Resolver Service (NRS)):
        curie, label, highlighting, synonyms, taxa, score, clique_identifier_count

    :param query: str, the (hopefully normalized?) query concept string to look up
    :param ontology: str, the (non-empty string) ontology to query (e.g., "GO", "MONDO", "UBERON","NCBIGene","CHEMBL")
    :param only_taxa: str | None, pipe-delimited string of the taxa to query (e.g., "NCBITaxon:9606|NCBITaxon:10090")
                                  Usually None or only one taxon is specified since method returns only one entry.

    :return: dict[str, Any] | None, the best ranked ontology term match, if any, with match metadata (as noted above)
    """
    assert ontology, "Non-empty ontology must be specified"

    match: dict[str, Any] | None = cache_lookup(query, ontology, only_taxa)

    return match


# Not sure how large the caches should be here, but
# there are likely only a modest set of terms accessed per run

@lru_cache(maxsize=None)
def lookup_go(query: str) -> dict[str, Any] | None:
    """
    Gene Ontology (GO) name to term lookup
    :param query: str, the term to look up
    :return: dict[str, Any] | None, the best match, if any, with match metadata
    """
    return lookup(query=query, ontology="GO")


@lru_cache(maxsize=None)
def lookup_mondo(query: str) -> dict[str, Any] | None:
    """
    Monarch Disease Ontology (MONDO) name to ontology term lookup
    :param query: str, the term to look up
    :return: dict[str, Any] | None, the best match, if any, with match metadata
    """
    return lookup(query=query, ontology="MONDO")


@lru_cache(maxsize=None)
def lookup_uberon(query: str) -> dict[str, Any] | None:
    """
    UBERON name to term lookup
    :param query: str, the term to look up
    :return: dict[str, Any] | None, the best match, if any, with match metadata
    """
    return lookup(query=query, ontology="UBERON")
