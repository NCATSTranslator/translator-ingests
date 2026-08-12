"""
STRING protein–protein interaction ingest (human, mouse, rat).

Reads STRING's '{taxon}.protein.links.full.v12.0.txt.gz' (16 columns: 7
evidence channels + 6 orthology-transferred variants + combined_score) and emits
three edge types per qualifying protein pair, following the 2026-07-17 call decisions:

  1. biolink:associated_with + association_basis_qualifier:Functional (always, combined_score > 700)
  2. biolink:directly_physically_interacts_with (conditional on experiments channel, TBD threshold)
  3. biolink:coexpressed_with (conditional on coexpression channel, TBD threshold)

See CHANGELOG.md for full design rationale. See string_rig.yaml for the graph specification.

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

from translator_ingest.ingests.string.string_utils import (
    load_string_to_entrez_mapping,
    parse_string_protein_id,
    edges_for_row,
    sorted_pair_key,
    make_string_ppi_edge,
    EDGE_KL_AT,
    COMBINED_SCORE_THRESHOLD
)


STRING_VERSION_API_URL = "https://string-db.org/api/json/version"

# Filename of the STRING to Entrez gene-ID mapping (universal across species).
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
    Load the STRING to Entrez mapping into 'koza_transform.state["string_to_entrez"]'
    once per 'string_ppi' transform run. Used by 'transform_string_ppi' to
    populate 'equivalent_identifiers' on Protein nodes with their NCBIGene
    equivalents.
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
    'Protein' nodes and one to three edges per the 2026-07-17 edge model.

    Edge emission:
      - always: biolink:associated_with (combined_score > 700)
      - conditional: biolink:directly_physically_interacts_with (experiments > threshold)
      - conditional: biolink:coexpressed_with (coexpression > threshold)

    KL/AT is fixed per edge type (not row-derived for this iteration).
    See EDGE_KL_AT in string_utils.py.

    Dedup is per (sorted_pair, predicate), so multiple predicates can coexist
    on the same pair without colliding, while symmetric duplicate rows collapse.
    """
    # The combined_score gate is also applied as a reader-level filter in
    # string.yaml (the production efficiency path). This guard keeps the
    # transform correct in unit tests where rows aren't pre-filtered.
    if not int(record["combined_score"]) > COMBINED_SCORE_THRESHOLD:
        return None

    subject_id, subject_taxon = parse_string_protein_id(record["protein1"])
    object_id, object_taxon = parse_string_protein_id(record["protein2"])

    if subject_taxon != object_taxon:
        raise ValueError(
            f"Cross-species pair in STRING row: {record['protein1']!r} vs {record['protein2']!r}"
        )

    # Collect (predicate, channel_score) pairs for this row. Always has at
    # least the associated_with entry; may add directly_physically_interacts_with
    # and/or coexpressed_with when the corresponding channels fire.
    # includes the ALWAYS edge with the combined score
    row_edges = edges_for_row(record)

    # Per-pair-per-predicate dedup. The dedup set lives on koza_transform.state
    # and grows with the number of unique (pair, predicate) tuples.
    seen_edges: set = koza_transform.state.setdefault("seen_edges", set())
    new_edges = [
        (pred, score)
        for pred, score in row_edges
        if sorted_pair_key(subject_id, object_id, pred) not in seen_edges
    ]
    if not new_edges:
        return None
    for pred, _ in new_edges:
        seen_edges.add(sorted_pair_key(subject_id, object_id, pred))

    entrez_map: dict[str, list[str]] = koza_transform.state.setdefault("string_to_entrez", {})
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

    edges = [
        make_string_ppi_edge(
            subject_id, pred, score, object_id,
            *EDGE_KL_AT[pred],
        )
        for pred, score in new_edges
    ]
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=edges)
