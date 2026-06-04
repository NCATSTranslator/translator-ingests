"""
STRING protein–protein interaction ingest (human, mouse, rat).

Reads STRING's '{taxon}.protein.links.full.v12.0.txt.gz' (16 columns: 7
evidence channels + 6 orthology-transferred variants + combined_score) and emits
per-channel biolink predicates between 'Protein' nodes, following ORION's
STRING parser. See [CHANGELOG.md](./CHANGELOG.md) and
[string_rig.yaml](./string_rig.yaml) for scope, rationale, and alternatives.

Note: the STITCH protein–chemical sibling ingest task ('stitch_pcl' tag) was
developed alongside this STRING ingest task but has been split out of this PR for a
later, properly scoped effort (with mode-of-action predicates from
'actions.v5.0.tsv'). The complete STITCH implementation is preserved on the
'stitch-ingest' branch; see the CHANGELOG entry dated 2026-05-28 for the
reintegration pointer.

Reference implementations consulted:
  * https://github.com/monarch-initiative/string-ingest/blob/main/src/protein_links.py
  * https://github.com/RobokopU24/ORION/blob/master/parsers/STRING/src/loadSTRINGDB.py
  * RENCI Automat production graph: https://automat.renci.org/string-db/
"""

from pathlib import Path
from typing import Any

import koza
import requests
from biolink_model.datamodel.pydanticmodel_v2 import Protein

from koza.model.graphs import KnowledgeGraph

from translator_ingest.ingests.string.string_utils import load_string_to_entrez_mapping, passes_combined_score, \
    parse_string_protein_id, predicates_for_row, sorted_pair_key, knowledge_level_and_agent_type_for_row, \
    make_string_ppi_edge


STRING_VERSION_API_URL = "https://string-db.org/api/json/version"

# Filename of the STRING ↔ Entrez gene-ID mapping (universal across species).
# Downloaded by download.yaml into 'koza.input_files_dir'. Loaded once at the
# start of the transform to populate 'equivalent_identifiers' on Protein nodes.
ENTREZ_MAPPING_FILENAME = "all_organisms.entrez_2_string.tsv"


def get_latest_version() -> str:
    """
    Return the current STRING release version (e.g. '"v12.0"').

    STRING exposes a JSON version endpoint that returns
    '[{"string_version": "12.0", "stable_address": "https://version-12-0.string-db.org"}]'.
    The 'v' prefix is added to match the convention used in STRING's download URLs
    (e.g. 'protein.links.v12.0/').

    >>> v = get_latest_version()
    >>> v.startswith("v") and "." in v
    True
    """
    response = requests.get(STRING_VERSION_API_URL, timeout=30)
    response.raise_for_status()
    return f"v{response.json()[0]['string_version']}"


@koza.on_data_begin(tag="string_ppi")
def on_data_begin_string_ppi(koza_transform: koza.KozaTransform) -> None:
    """
    Load the STRING ↔ Entrez mapping into 'koza_transform.state["string_to_entrez"]'
    once per 'string_ppi' transform run. Used by 'transform_string_ppi' to
    populate 'equivalent_identifiers' on Protein nodes with their NCBIGene
    equivalents.

    Tests bypass this hook by setting 'koza_transform.state["string_to_entrez"]'
    directly to a small fixture dict.
    """
    assert koza_transform.input_files_dir is not None, "Koza Transform 'input_files_dir' variable cannot be null!"
    mapping_path = Path(koza_transform.input_files_dir) / ENTREZ_MAPPING_FILENAME
    koza_transform.state["string_to_entrez"] = load_string_to_entrez_mapping(mapping_path)


@koza.transform_record(tag="string_ppi")
def transform_string_ppi(
    koza_transform: koza.KozaTransform, record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform one row of STRING's 'protein.links.full.v12.0.txt.gz' file into two
    'Protein' nodes and one or more per-channel edges.

    Predicate selection follows ORION's STRING parser: any evidence channel whose
    score exceeds 'CHANNEL_HIGH_CONF_THRESHOLD' (750) fires the channel's
    corresponding predicate (see 'CHANNEL_PREDICATES'). If no channel exceeds
    the high-confidence threshold but the row passes the combined_score gate
    (>500), a single fallback 'physically_interacts_with' edge is emitted.
    Up to 6 edges per pair (one per fired predicate); typically 1–2.

    Each edge carries a per-row knowledge_level / agent_type derived from the
    dominant evidence channel (see 'knowledge_level_and_agent_type_for_row').

    Dedup is per (sorted_pair, predicate), so multiple predicates can fire for the
    same pair without colliding, while symmetric duplicate rows still collapse.
    """
    # NOTE: the combined_score > 500 gate is also applied as a reader-level filter
    # in string.yaml (the production path — Koza skips sub-threshold rows before
    # they reach here). This guard keeps the transform correct when called
    # directly in unit tests, where rows aren't pre-filtered.
    if not passes_combined_score(record["combined_score"]):
        return None

    subject_id, subject_taxon = parse_string_protein_id(record["protein1"])
    object_id, object_taxon = parse_string_protein_id(record["protein2"])

    # STRING's per-organism link files only contain intra-species pairs.
    # Catch corrupt or cross-species rows loudly.
    if subject_taxon != object_taxon:
        raise ValueError(
            f"Cross-species pair in STRING row: {record['protein1']!r} vs {record['protein2']!r}"
        )

    predicates = predicates_for_row(record)

    # Per-pair-per-predicate dedup. The dedup set lives on koza_transform.state
    # and grows with the number of unique (pair, predicate) tuples — bounded by
    # the above-threshold edge count (~1-2M for human PPI). If memory becomes a
    # constraint at the full multi-organism scale, swap for an on-disk set (sqlite)
    # or an integer-keyed roaring bitmap; the key is already a hashable tuple.
    seen_pairs: set = koza_transform.state.setdefault("seen_pairs", set())
    new_predicates = [
        p for p in predicates
        if sorted_pair_key(subject_id, object_id, p) not in seen_pairs
    ]
    if not new_predicates:
        return None
    for p in new_predicates:
        seen_pairs.add(sorted_pair_key(subject_id, object_id, p))

    # Look up NCBIGene equivalents from the entrez_2_string mapping. Loaded
    # at transform start by on_data_begin; tests may inject a fixture dict.
    # Missing entries are normal (some STRING proteins have no Entrez mapping;
    # downstream NodeNormalizer still resolves most of them via UniProtKB).
    entrez_map: dict[str, list[str]] = koza_transform.state.get("string_to_entrez", {})
    subject_equivalents = entrez_map.get(record["protein1"]) or None
    object_equivalents = entrez_map.get(record["protein2"]) or None

    subject_node = Protein(
        id=subject_id,
        category=["biolink:Protein"],
        in_taxon=[subject_taxon],
        equivalent_identifiers=subject_equivalents,
    )
    object_node = Protein(
        id=object_id,
        category=["biolink:Protein"],
        in_taxon=[object_taxon],
        equivalent_identifiers=object_equivalents,
    )

    # KL/AT is a row-level property (derived from the dominant evidence channel),
    # shared by all edges emitted from this row.
    knowledge_level, agent_type = knowledge_level_and_agent_type_for_row(record)
    edges = [
        make_string_ppi_edge(subject_id, predicate, object_id, knowledge_level, agent_type)
        for predicate in new_predicates
    ]
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=edges)
