"""SemMedDB pre-normalization filter: drop publications the LLM PMID-checker rejected.

Runs between transform and normalization, so it joins the verdict artifact on the raw
kg2.10.3 identifiers the transform emits. That matters because the artifact is keyed by node
ids: keying it by normalized ids would tie it to one Babel release, and since a key that does
not match is simply "no verdict" (which means keep), every id that changed in a later Babel
release would silently stop being filtered and the filter would decay without any error. Raw
ids from the frozen kg2.10.3 source never change, so the join stays stable. The artifact itself
is re-keyed from normalized ids to raw ids by ``analysis/rekey_pmid_verdicts.py``.

Drop a publication when its verdict is ``no`` or ``maybe``; keep ``yes``, ``no_abstract`` and
any PMID with no verdict row at all. The ``maybe`` bucket is dropped for now and will be
refined in a later second pass. Also drop the matching ``TextMiningStudyResult`` from
``has_supporting_studies``. Drop an edge when no publication remains. Trim an edge to the most
recent ``MAX_PUBLICATIONS_PER_EDGE`` publications when it has more than that, then prune nodes
left without any edge.

Because a non-matching key silently means "keep", a coverage guard backstops the join: the
fraction of input edges that have any verdict row must be at least ``MIN_EDGE_COVERAGE``, or
the filter raises instead of quietly passing everything through.

Inputs are never modified. The filtered nodes and edges are written to the output paths the
pipeline supplies, which live in a filter directory keyed by the filter code hash.
"""

import json
import os
from pathlib import Path
from typing import Any

import polars as pl

# LLM PMID-checker results parquet downloaded into source_data/ (see download.yaml), re-keyed
# from normalized ids to raw kg2.10.3 ids. Used columns: subject_curie, predicate, object_curie,
# PMID, support.
VERDICT_ARTIFACT_FILENAME = "semmeddb_pmid_checker_results_raw_keyed.parquet"

# these verdicts remove a publication; "yes", "no_abstract" and any PMID absent from the results
# are kept. "maybe" is dropped now and refined in a later second pass.
DROP_SUPPORT_VALUES = frozenset({"no", "maybe"})

# minimum fraction of input edges that must carry at least one verdict row. A key the artifact
# does not cover is treated as "keep", so a broken join would silently disable the filter rather
# than fail; this guard turns that into a loud error. Measured on kg2.10.3 with the re-keyed
# artifact: 99.7% of edges covered (the rest have only no-abstract publications the checker could
# not judge). The same edges joined on the original normalized keys reach only 89.3%, so this
# threshold catches a broken or drifted join with a wide margin on both sides.
MIN_EDGE_COVERAGE = 0.95

# edges left with more than this many publications after filtering are trimmed to the most
# recent ones (highest PMID number, since PMIDs are assigned chronologically). This bounds the
# small tail of very large edges without touching the vast majority.
MAX_PUBLICATIONS_PER_EDGE = 1000

# the cap is on by default. Set SEMMEDDB_UNCAPPED=1 (or true/yes) to disable it and keep every
# surviving publication, for example to regenerate uncapped output for a fresh PMID-checker run.
# Switching this between runs requires OVERWRITE, since it does not change the filter version hash.
CAP_ENABLED = os.environ.get("SEMMEDDB_UNCAPPED", "").lower() not in ("1", "true", "yes")

EdgeKey = tuple[str, str, str]
DropSet = dict[EdgeKey, set[str]]

VERDICT_ARTIFACT_COLUMNS = ["subject_curie", "predicate", "object_curie", "PMID", "support"]
EDGE_KEY_COLUMNS = ["subject_curie", "predicate", "object_curie"]


def load_drop_set(artifact_file: Path) -> DropSet:
    """Build ``{(subject, predicate, object) -> {rejected PMIDs}}`` from the ``no``/``maybe`` verdicts.

    Keyed per edge so each edge needs a single dict lookup rather than one membership
    test per publication.
    """
    verdicts = (
        pl.read_parquet(artifact_file, columns=VERDICT_ARTIFACT_COLUMNS)
        .with_columns(pl.col("support").cast(pl.Utf8).str.to_lowercase().str.strip_chars())
        .filter(pl.col("support").is_in(DROP_SUPPORT_VALUES))
    )

    drop_set: DropSet = {}
    for subject, predicate, obj, pmid in verdicts.select(
        "subject_curie", "predicate", "object_curie", "PMID"
    ).iter_rows():
        drop_set.setdefault((subject, predicate, obj), set()).add(pmid)
    return drop_set


def load_covered_edge_keys(artifact_file: Path) -> set[EdgeKey]:
    """Return every distinct edge key the verdict artifact mentions, for any support value.

    Wider than the drop set on purpose: an edge whose verdicts are all ``yes`` is still covered
    by the checker, so it counts toward the coverage guard.
    """
    covered = pl.read_parquet(artifact_file, columns=EDGE_KEY_COLUMNS).unique()
    covered_keys: set[EdgeKey] = set(covered.iter_rows())
    return covered_keys


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


def edge_key_of(edge: dict[str, Any]) -> EdgeKey:
    """Return the ``(subject, predicate, object)`` key a verdict row is joined on.

    >>> edge_key_of({"subject": "A", "predicate": "p", "object": "B"})
    ('A', 'p', 'B')
    """
    # transform-stage KGX edges always carry subject/predicate/object
    return edge["subject"], edge["predicate"], edge["object"]


def filter_edge(edge: dict[str, Any], drop_set: DropSet) -> dict[str, Any] | None:
    """Remove rejected publications and cap oversized edges, or return None if none remain.

    Drops publications the checker marked ``no`` or ``maybe``. If the cap is enabled
    (``CAP_ENABLED``) and more than ``MAX_PUBLICATIONS_PER_EDGE`` remain, keeps only the most
    recent ones (highest PMID). Matching ``TextMiningStudyResult`` entries are pruned for every
    removed publication.

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
    dropped_pmids = drop_set.get(edge_key_of(edge)) or set()

    original_publications = edge.get("publications", [])
    publications = [pmid for pmid in original_publications if pmid not in dropped_pmids]
    if not publications:
        # every publication was rejected -> drop the whole edge
        return None

    removed_pmids = set(dropped_pmids)
    if CAP_ENABLED and len(publications) > MAX_PUBLICATIONS_PER_EDGE:
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


def _write_filtered_edges(
    edges_file: Path,
    output_edges_file: Path,
    drop_set: DropSet,
    covered_edge_keys: set[EdgeKey],
) -> tuple[set[str], dict[str, Any]]:
    """Stream edges through the filter into the output file, returning surviving node ids and stats.

    The input file is only read. Verdict coverage is counted during the same pass.
    """
    surviving_node_ids: set[str] = set()
    edges_before = edges_after = 0
    edges_with_verdicts = 0
    publications_before = publications_after = 0

    edges_tmp = output_edges_file.with_name(output_edges_file.name + ".tmp")
    with edges_file.open() as source, edges_tmp.open("w") as destination:
        for line in source:
            line = line.strip()
            if not line:
                continue
            edge = json.loads(line)
            edges_before += 1
            publications_before += len(edge.get("publications", []))
            if edge_key_of(edge) in covered_edge_keys:
                edges_with_verdicts += 1

            filtered_edge = filter_edge(edge, drop_set)
            if filtered_edge is None:
                continue

            edges_after += 1
            publications_after += len(filtered_edge["publications"])
            surviving_node_ids.add(filtered_edge["subject"])
            surviving_node_ids.add(filtered_edge["object"])
            destination.write(json.dumps(filtered_edge) + "\n")
    os.replace(edges_tmp, output_edges_file)

    stats: dict[str, Any] = {
        "edges_before": edges_before,
        "edges_after": edges_after,
        "edges_dropped": edges_before - edges_after,
        "edges_with_verdicts": edges_with_verdicts,
        "edge_coverage": edges_with_verdicts / edges_before if edges_before else 0.0,
        "publications_before": publications_before,
        "publications_after": publications_after,
        "publications_removed": publications_before - publications_after,
    }
    return surviving_node_ids, stats


def _write_filtered_nodes(
    nodes_file: Path, output_nodes_file: Path, surviving_node_ids: set[str]
) -> dict[str, int]:
    """Stream nodes into the output file, keeping only those referenced by a surviving edge."""
    nodes_before = nodes_after = 0
    nodes_tmp = output_nodes_file.with_name(output_nodes_file.name + ".tmp")
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
    os.replace(nodes_tmp, output_nodes_file)
    return {
        "nodes_before": nodes_before,
        "nodes_after": nodes_after,
        "nodes_pruned": nodes_before - nodes_after,
    }


def filter_transform_kgx(
    nodes_file: Path,
    edges_file: Path,
    output_nodes_file: Path,
    output_edges_file: Path,
    source_data_dir: Path,
) -> dict[str, Any]:
    """Apply the PMID-checker filter to transform-stage KGX nodes and edges.

    Reads the raw-keyed verdict artifact from ``source_data_dir``, writes ``output_edges_file``
    (dropping rejected publications and edges that lose all of them), then writes
    ``output_nodes_file`` with only the nodes still referenced by a surviving edge. The input
    files are never modified. Returns filtering statistics.

    Raises ``RuntimeError`` when the fraction of input edges covered by the artifact falls below
    ``MIN_EDGE_COVERAGE``, which means the artifact keys no longer line up with the source ids.
    The guard fires after the output files are written, which is harmless: the pipeline only
    marks the stage complete once it has written the filter metadata, and that never happens
    when this raises.
    """
    artifact_file = source_data_dir / VERDICT_ARTIFACT_FILENAME
    if not artifact_file.exists():
        raise FileNotFoundError(
            f"Verdict artifact not found at {artifact_file}; check semmeddb download.yaml."
        )

    drop_set = load_drop_set(artifact_file)
    covered_edge_keys = load_covered_edge_keys(artifact_file)
    surviving_node_ids, edge_stats = _write_filtered_edges(
        edges_file, output_edges_file, drop_set, covered_edge_keys
    )
    node_stats = _write_filtered_nodes(nodes_file, output_nodes_file, surviving_node_ids)
    stats = {**edge_stats, **node_stats}

    edge_coverage = edge_stats["edge_coverage"]
    if edge_coverage < MIN_EDGE_COVERAGE:
        raise RuntimeError(
            f"PMID-checker verdict coverage is {edge_coverage:.4f}, below the required "
            f"{MIN_EDGE_COVERAGE}: only {edge_stats['edges_with_verdicts']} of "
            f"{edge_stats['edges_before']} edges have any verdict row. The verdict artifact "
            f"({artifact_file.name}) keys no longer match the source edge ids, most likely a "
            f"re-keyed artifact mismatch or a source version change. Uncovered edges are kept "
            f"unfiltered, so this would silently disable the filter."
        )
    return stats
