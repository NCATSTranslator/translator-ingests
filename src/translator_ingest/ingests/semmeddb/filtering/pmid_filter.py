"""SemMedDB post-normalization filter: drop publications the LLM PMID-checker rejected.

Rule B: drop a publication only when its verdict is ``no``; keep ``yes`` / ``maybe`` (and any
PMID with no verdict, e.g. a paper the checker could not read). Also drop the matching
``TextMiningStudyResult`` from ``has_supporting_studies``. Drop an edge when no publication
remains. Trim an edge to the most recent ``MAX_PUBLICATIONS_PER_EDGE`` publications when it has
more than that, then prune nodes left without any edge.

The filter overwrites ``normalized_nodes.jsonl`` / ``normalized_edges.jsonl`` in place (no raw
backup), so merge and every downstream stage read the filtered output with no path change.
"""

import json
import os
from pathlib import Path
from typing import Any

import polars as pl

# LLM PMID-checker results parquet downloaded into source_data/ (see download.yaml).
# Used columns: subject_curie, predicate, object_curie, PMID, support. Rule B only needs
# the "no" rows, which all live here, so no separate no-abstract file is required.
VERDICT_ARTIFACT_FILENAME = "semmeddb_pmid_checker_results.parquet"

# only this verdict removes a publication; every other value (and any PMID absent from the
# results, e.g. a no-abstract paper) is kept (Rule B)
DROP_SUPPORT_VALUE = "no"

# edges left with more than this many publications after filtering are trimmed to the most
# recent ones (highest PMID number, since PMIDs are assigned chronologically). This bounds the
# small tail of very large edges without touching the vast majority.
MAX_PUBLICATIONS_PER_EDGE = 1000

EdgeKey = tuple[str, str, str]
DropSet = dict[EdgeKey, set[str]]


def load_drop_set(artifact_file: Path) -> DropSet:
    """Build ``{(subject, predicate, object) -> {rejected PMIDs}}`` from the ``no`` verdicts.

    Keyed per edge so each edge needs a single dict lookup rather than one membership
    test per publication.
    """
    verdicts = (
        pl.read_parquet(
            artifact_file,
            columns=["subject_curie", "predicate", "object_curie", "PMID", "support"],
        )
        .with_columns(pl.col("support").cast(pl.Utf8).str.to_lowercase().str.strip_chars())
        .filter(pl.col("support") == DROP_SUPPORT_VALUE)
    )

    drop_set: DropSet = {}
    for subject, predicate, obj, pmid in verdicts.select(
        "subject_curie", "predicate", "object_curie", "PMID"
    ).iter_rows():
        drop_set.setdefault((subject, predicate, obj), set()).add(pmid)
    return drop_set


def _prune_supporting_studies(
    has_supporting_studies: dict[str, Any], dropped_pmids: set[str]
) -> dict[str, Any]:
    """Drop TextMiningStudyResults whose xref PMID was rejected, and any emptied Study.

    >>> studies = {"s1": {"id": "s1", "has_study_results": [
    ...     {"xref": ["PMID:1"], "supporting_text": ["a"]},
    ...     {"xref": ["PMID:2"], "supporting_text": ["b"]}]}}
    >>> _prune_supporting_studies(studies, {"PMID:1"})
    {'s1': {'id': 's1', 'has_study_results': [{'xref': ['PMID:2'], 'supporting_text': ['b']}]}}
    >>> _prune_supporting_studies(studies, {"PMID:1", "PMID:2"})
    {}
    """
    pruned: dict[str, Any] = {}
    for study_id, study in has_supporting_studies.items():
        kept_results = [
            result
            for result in study.get("has_study_results", [])
            if not any(xref in dropped_pmids for xref in result.get("xref", []))
        ]
        if kept_results:
            pruned[study_id] = {**study, "has_study_results": kept_results}
    return pruned


def _pmid_number(pmid: str) -> int:
    """Return the numeric part of a PMID for recency ordering, or -1 if not numeric.

    PMIDs are assigned chronologically, so a higher number is a more recent paper.

    >>> _pmid_number("PMID:12345")
    12345
    >>> _pmid_number("PMID:not-a-number")
    -1
    """
    digits = pmid.rsplit(":", 1)[-1]
    return int(digits) if digits.isdigit() else -1


def _cap_by_recency(publications: list[str], limit: int) -> list[str]:
    """Keep the ``limit`` most recent publications (highest PMID number), in original order.

    >>> _cap_by_recency(["PMID:5", "PMID:1", "PMID:9", "PMID:3"], 2)
    ['PMID:5', 'PMID:9']
    >>> _cap_by_recency(["PMID:1", "PMID:2"], 5)
    ['PMID:1', 'PMID:2']
    """
    if len(publications) <= limit:
        return publications
    kept = set(sorted(publications, key=_pmid_number, reverse=True)[:limit])
    return [pmid for pmid in publications if pmid in kept]


def filter_edge(edge: dict[str, Any], drop_set: DropSet) -> dict[str, Any] | None:
    """Remove rejected publications and cap oversized edges, or return None if none remain.

    Drops publications the checker marked ``no``. If more than ``MAX_PUBLICATIONS_PER_EDGE``
    remain, keeps only the most recent ones (highest PMID). Matching ``TextMiningStudyResult``
    entries are pruned for every removed publication.

    >>> drop = {("A", "p", "B"): {"PMID:2"}}
    >>> filter_edge({"subject": "A", "predicate": "p", "object": "B",
    ...              "publications": ["PMID:1", "PMID:2"]}, drop)
    {'subject': 'A', 'predicate': 'p', 'object': 'B', 'publications': ['PMID:1']}
    >>> filter_edge({"subject": "A", "predicate": "p", "object": "B",
    ...              "publications": ["PMID:2"]}, drop) is None
    True
    >>> filter_edge({"subject": "X", "predicate": "p", "object": "Y",
    ...              "publications": ["PMID:9"]}, drop)
    {'subject': 'X', 'predicate': 'p', 'object': 'Y', 'publications': ['PMID:9']}
    """
    # normalized KGX edges always carry subject/predicate/object
    edge_key = (edge["subject"], edge["predicate"], edge["object"])
    dropped_pmids = drop_set.get(edge_key) or set()

    original_publications = edge.get("publications", [])
    publications = [pmid for pmid in original_publications if pmid not in dropped_pmids]
    if not publications:
        # every publication was rejected -> drop the whole edge
        return None

    removed_pmids = set(dropped_pmids)
    if len(publications) > MAX_PUBLICATIONS_PER_EDGE:
        capped = _cap_by_recency(publications, MAX_PUBLICATIONS_PER_EDGE)
        removed_pmids |= set(publications) - set(capped)
        publications = capped

    if len(publications) == len(original_publications):
        # nothing dropped and nothing capped; pass the edge through untouched
        return edge

    filtered_edge = {**edge, "publications": publications}
    supporting_studies = filtered_edge.get("has_supporting_studies")
    if supporting_studies and removed_pmids:
        pruned_studies = _prune_supporting_studies(supporting_studies, removed_pmids)
        if pruned_studies:
            filtered_edge["has_supporting_studies"] = pruned_studies
        else:
            filtered_edge.pop("has_supporting_studies", None)
    return filtered_edge


def _rewrite_edges(
    edges_file: Path, drop_set: DropSet
) -> tuple[set[str], dict[str, int]]:
    """Stream edges through the filter in place, returning surviving node ids and stats."""
    surviving_node_ids: set[str] = set()
    edges_before = edges_after = 0
    publications_before = publications_after = 0

    edges_tmp = edges_file.with_name(edges_file.name + ".tmp")
    with edges_file.open() as source, edges_tmp.open("w") as destination:
        for line in source:
            line = line.strip()
            if not line:
                continue
            edge = json.loads(line)
            edges_before += 1
            publications_before += len(edge.get("publications", []))

            filtered_edge = filter_edge(edge, drop_set)
            if filtered_edge is None:
                continue

            edges_after += 1
            publications_after += len(filtered_edge["publications"])
            surviving_node_ids.add(filtered_edge["subject"])
            surviving_node_ids.add(filtered_edge["object"])
            destination.write(json.dumps(filtered_edge) + "\n")
    os.replace(edges_tmp, edges_file)

    stats = {
        "edges_before": edges_before,
        "edges_after": edges_after,
        "edges_dropped": edges_before - edges_after,
        "publications_before": publications_before,
        "publications_after": publications_after,
        "publications_removed": publications_before - publications_after,
    }
    return surviving_node_ids, stats


def _rewrite_nodes(nodes_file: Path, surviving_node_ids: set[str]) -> dict[str, int]:
    """Stream nodes in place, keeping only those referenced by a surviving edge."""
    nodes_before = nodes_after = 0
    nodes_tmp = nodes_file.with_name(nodes_file.name + ".tmp")
    with nodes_file.open() as source, nodes_tmp.open("w") as destination:
        for line in source:
            line = line.strip()
            if not line:
                continue
            node = json.loads(line)
            nodes_before += 1
            if node.get("id") in surviving_node_ids:
                nodes_after += 1
                destination.write(json.dumps(node) + "\n")
    os.replace(nodes_tmp, nodes_file)
    return {
        "nodes_before": nodes_before,
        "nodes_after": nodes_after,
        "nodes_pruned": nodes_before - nodes_after,
    }


def filter_normalized_kgx(
    nodes_file: Path,
    edges_file: Path,
    source_data_dir: Path,
) -> dict[str, Any]:
    """Apply the PMID-checker filter in place over normalized KGX nodes and edges.

    Reads the verdict artifact from ``source_data_dir``, rewrites ``edges_file`` (dropping
    rejected publications and edges that lose all of them), then rewrites ``nodes_file`` to
    keep only nodes still referenced by a surviving edge. Returns filtering statistics.
    """
    artifact_file = source_data_dir / VERDICT_ARTIFACT_FILENAME
    if not artifact_file.exists():
        raise FileNotFoundError(
            f"Verdict artifact not found at {artifact_file}; check semmeddb download.yaml."
        )

    drop_set = load_drop_set(artifact_file)
    surviving_node_ids, edge_stats = _rewrite_edges(edges_file, drop_set)
    node_stats = _rewrite_nodes(nodes_file, surviving_node_ids)
    return {**edge_stats, **node_stats}
