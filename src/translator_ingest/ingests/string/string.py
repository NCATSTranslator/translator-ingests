"""
STRING protein–protein interaction ingest (human-only, first iteration).

Scope, rationale, and the design alternatives we considered are documented in
[CHANGELOG.md](./CHANGELOG.md) and [string_rig.yaml](./string_rig.yaml).

Reference implementations consulted:
  * https://github.com/monarch-initiative/string-ingest/blob/main/src/protein_links.py
  * https://github.com/RobokopU24/ORION/blob/master/parsers/STRING/src/loadSTRINGDB.py
  * RENCI Automat production graph: https://automat.renci.org/string-db/
"""

import requests

from translator_ingest.util.biolink import (
    INFORES_STRING,
    build_association_knowledge_sources,
)

STRING_VERSION_API_URL = "https://string-db.org/api/json/version"

HUMAN_TAXON_PREFIX = "9606"
HUMAN_TAXON_CURIE = "NCBITaxon:9606"

# STRING's medium-confidence cutoff; matches the default offered on the STRING web UI.
COMBINED_SCORE_THRESHOLD = 500

STRING_SOURCES = build_association_knowledge_sources(primary=INFORES_STRING)


def get_latest_version() -> str:
    """
    Return the current STRING release version (e.g. ``"v12.0"``).

    STRING exposes a JSON version endpoint that returns
    ``[{"string_version": "12.0", "stable_address": "https://version-12-0.string-db.org"}]``.
    The ``v`` prefix is added to match the convention used in STRING's download URLs
    (e.g. ``protein.links.v12.0/``).

    >>> v = get_latest_version()
    >>> v.startswith("v") and "." in v
    True
    """
    response = requests.get(STRING_VERSION_API_URL, timeout=30)
    response.raise_for_status()
    return f"v{response.json()[0]['string_version']}"


def parse_string_protein_id(string_id: str, taxon_prefix: str = HUMAN_TAXON_PREFIX) -> str:
    """
    Convert a STRING-prefixed protein identifier into an ENSEMBL CURIE.

    STRING ships protein IDs as ``{taxid}.{ENSEMBL_protein_id}`` (e.g.
    ``9606.ENSP00000478725``). We strip the taxon prefix and emit a Biolink-style
    ``ENSEMBL:`` CURIE.

    >>> parse_string_protein_id("9606.ENSP00000478725")
    'ENSEMBL:ENSP00000478725'
    >>> parse_string_protein_id("9606.ENSP00000000001", taxon_prefix="9606")
    'ENSEMBL:ENSP00000000001'
    """
    prefix = f"{taxon_prefix}."
    if not string_id.startswith(prefix):
        raise ValueError(
            f"Expected STRING ID prefixed with '{prefix}', got: {string_id!r}"
        )
    return f"ENSEMBL:{string_id[len(prefix):]}"


def passes_combined_score(
    combined_score: str | int, threshold: int = COMBINED_SCORE_THRESHOLD
) -> bool:
    """
    Whether a row's combined_score exceeds the inclusion threshold.

    Uses strict greater-than (``>``), matching STRING's web-UI convention where
    the slider value is the lower exclusive bound.

    >>> passes_combined_score("540")
    True
    >>> passes_combined_score("500")
    False
    >>> passes_combined_score("499")
    False
    >>> passes_combined_score(999)
    True
    """
    return int(combined_score) > threshold


def sorted_pair_key(p1: str, p2: str) -> tuple[str, str]:
    """
    Order-independent key for deduping symmetric protein pairs.

    STRING's ``protein.links`` file lists each unordered pair in both directions
    (``p1 p2`` and ``p2 p1``). We emit one undirected edge per pair, so we need
    a key that collapses the two rows together.

    >>> sorted_pair_key("ENSEMBL:B", "ENSEMBL:A")
    ('ENSEMBL:A', 'ENSEMBL:B')
    >>> sorted_pair_key("ENSEMBL:A", "ENSEMBL:B")
    ('ENSEMBL:A', 'ENSEMBL:B')
    """
    return tuple(sorted([p1, p2]))
