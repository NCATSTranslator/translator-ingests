"""Offline verification harness for the SemMedDB PMID-checker filter.

This script does NOT touch the pipeline. It joins the LLM PMID-checker verdicts
(``results.parquet`` + ``results_no_abstract.parquet`` from RTXteam/LLM_PMID_Checker)
against our normalized SemMedDB edges and reports exactly what the filter would do,
so the in-pipeline implementation can be validated against these numbers later.

It lives in a subdirectory (not directly under ``semmeddb/``) on purpose:
``get_transform_version`` hashes ``*.py`` and ``*.json`` in the ingest directory
non-recursively, so a script placed here does not change the transform version.

Locked filter rules (the script reports both so the team can confirm the cutoff):

- Rule B (default / shipping): a PMID is dropped only if its verdict is ``no``.
  ``yes``/``maybe``/``no-abstract``/absent are kept. An edge survives if at least
  one PMID remains (i.e. not every PMID was ``no``).
- Rule C (stricter, for comparison): keep only ``yes``/``maybe`` PMIDs; an edge
  survives only if it has at least one such positively-supported PMID.

The baseline edges must be the UNCAPPED normalized output (``SEMMEDDB_UNCAPPED=1``),
because the checker evaluated uncapped edges.

Inputs:

- ``--results-parquet``    LLM_PMID_Checker ``results.parquet`` (evaluated triples).
- ``--no-abstract-parquet`` (optional) ``results_no_abstract.parquet`` (unevaluated;
  lets the report distinguish "no abstract" from "never seen / join miss").
- ``--normalized-edges``   our ``normalized_edges.jsonl`` (uncapped).
- ``--normalized-nodes``   (optional) our ``normalized_nodes.jsonl`` for orphan-node counts.
- ``--out``                (optional) path to write the full JSON report. Write it under
  ``data/`` (gitignored) so reports never clutter the repo root.

Memory note: peak usage is a few GB (two ~28M-row tables plus the join). Prefer
running on the pod or a machine with >=16 GB RAM.

Run:

    uv run python src/translator_ingest/ingests/semmeddb/analysis/verify_pmid_filter.py \
        --results-parquet data/pmid_checker/results.parquet \
        --no-abstract-parquet data/pmid_checker/results_no_abstract.parquet \
        --normalized-edges  data/.../normalization_.../normalized_edges.jsonl \
        --normalized-nodes  data/.../normalization_.../normalized_nodes.jsonl \
        --out data/pmid_checker/pmid_filter_verification.json
"""

import argparse
import gzip
import json
from collections.abc import Iterator
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, TextIO

import polars as pl

JOIN_KEYS: list[str] = ["subject", "predicate", "object", "pmid_key"]
SUPPORTED_VALUES: list[str] = ["yes", "maybe"]
DROP_VALUE: str = "no"
NO_ABSTRACT: str = "no_abstract"
ABSENT: str = "absent"

# Edge-size thresholds used to decide whether (and at what N) the post-filter
# publication cap is still needed.
CAP_THRESHOLDS: tuple[int, ...] = (200, 500, 1000, 5000)

# Rows accumulated in Python before flushing into a polars batch frame.
BATCH_ROWS: int = 2_000_000


def open_text(path: Path) -> AbstractContextManager[TextIO]:
    """Open a possibly gzipped text file for reading."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("rt", encoding="utf-8")


def iter_edge_pmid_pairs(record: dict[str, Any]) -> Iterator[tuple[str, str, str, str]]:
    """Yield ``(subject, predicate, object, pmid)`` for each publication on a KGX edge.

    ``publications`` may be a JSON list (normalized KGX) or a pipe-delimited
    string (KGX TSV-style). Edges missing subject/predicate/object, or with no
    publications, yield nothing.

    >>> rec = {"subject": "CHEBI:1", "predicate": "biolink:affects",
    ...        "object": "NCBIGene:2", "publications": ["PMID:10", "PMID:20"]}
    >>> list(iter_edge_pmid_pairs(rec))
    [('CHEBI:1', 'biolink:affects', 'NCBIGene:2', 'PMID:10'), ('CHEBI:1', 'biolink:affects', 'NCBIGene:2', 'PMID:20')]
    >>> list(iter_edge_pmid_pairs(
    ...     {"subject": "A", "predicate": "p", "object": "B", "publications": "PMID:1|PMID:2"}))
    [('A', 'p', 'B', 'PMID:1'), ('A', 'p', 'B', 'PMID:2')]
    >>> list(iter_edge_pmid_pairs({"subject": "A", "predicate": "p", "object": "B"}))
    []
    """
    subject = record.get("subject")
    predicate = record.get("predicate")
    obj = record.get("object")
    if not (subject and predicate and obj):
        return
    publications = record.get("publications") or []
    if isinstance(publications, str):
        publications = [p for p in publications.split("|") if p]
    for pmid in publications:
        if pmid:
            yield subject, predicate, obj, pmid


def pmid_key_expr(column: str) -> pl.Expr:
    """Return a polars expression normalizing a PMID column to a bare-id join key.

    Strips a leading ``PMID:`` (any case) and surrounding whitespace so that
    ``"PMID:123"``, ``"123"`` and the integer ``123`` all collapse to ``"123"``.
    """
    return (
        pl.col(column)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r"(?i)^pmid:", "")
        .alias("pmid_key")
    )


def load_edge_pairs(edges_path: Path) -> tuple[pl.DataFrame, int, int]:
    """Stream the normalized edges JSONL into an exploded (edge, PMID) table.

    Returns the pairs DataFrame (``edge_idx, subject, predicate, object, pmid_key``),
    the number of edges that had at least one publication, and the total number of
    edge records read. Edges without any publication are not represented in the
    pairs table (they carry no PMID to check) but are still counted in the total.
    """
    columns: dict[str, list[Any]] = {
        "edge_idx": [],
        "subject": [],
        "predicate": [],
        "object": [],
        "pmid": [],
    }
    frames: list[pl.DataFrame] = []
    edge_idx = 0
    total_edges_read = 0
    with open_text(edges_path) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            total_edges_read += 1
            record = json.loads(line)
            emitted = False
            for subject, predicate, obj, pmid in iter_edge_pmid_pairs(record):
                columns["edge_idx"].append(edge_idx)
                columns["subject"].append(subject)
                columns["predicate"].append(predicate)
                columns["object"].append(obj)
                columns["pmid"].append(pmid)
                emitted = True
            if emitted:
                edge_idx += 1
            if len(columns["edge_idx"]) >= BATCH_ROWS:
                frames.append(pl.DataFrame(columns))
                columns = {key: [] for key in columns}
    if columns["edge_idx"]:
        frames.append(pl.DataFrame(columns))

    schema = {
        "edge_idx": pl.Int64,
        "subject": pl.Utf8,
        "predicate": pl.Utf8,
        "object": pl.Utf8,
        "pmid": pl.Utf8,
    }
    pairs = pl.concat(frames, rechunk=True) if frames else pl.DataFrame(schema=schema)
    pairs = pairs.with_columns(pmid_key_expr("pmid")).drop("pmid")
    return pairs, edge_idx, total_edges_read


def build_verdicts(results_path: Path, no_abstract_path: Path | None) -> pl.DataFrame:
    """Load and union the checker verdicts into one key->support lookup table."""
    rename = {"subject_curie": "subject", "object_curie": "object"}
    results = (
        pl.read_parquet(
            results_path,
            columns=["subject_curie", "predicate", "object_curie", "PMID", "support"],
        )
        .rename(rename)
        .with_columns(
            pmid_key_expr("PMID"),
            pl.col("support").cast(pl.Utf8).str.to_lowercase().str.strip_chars().alias("support"),
        )
        .select([*JOIN_KEYS, "support"])
    )
    frames = [results]
    if no_abstract_path is not None:
        no_abstract = (
            pl.read_parquet(
                no_abstract_path,
                columns=["subject_curie", "predicate", "object_curie", "PMID"],
            )
            .rename(rename)
            .with_columns(pmid_key_expr("PMID"), pl.lit(NO_ABSTRACT).alias("support"))
            .select([*JOIN_KEYS, "support"])
        )
        frames.append(no_abstract)
    return pl.concat(frames, how="vertical", rechunk=True).unique(subset=JOIN_KEYS, keep="first")


def aggregate_edges(joined: pl.DataFrame) -> pl.DataFrame:
    """Collapse the joined pair table to one row per edge with verdict counts."""
    edges = joined.group_by("edge_idx").agg(
        pl.col("subject").first(),
        pl.col("predicate").first(),
        pl.col("object").first(),
        pl.len().alias("n_pubs"),
        (pl.col("support") == DROP_VALUE).sum().alias("n_no"),
        pl.col("support").is_in(SUPPORTED_VALUES).sum().alias("n_supported"),
        (pl.col("support") == NO_ABSTRACT).sum().alias("n_no_abstract"),
        (pl.col("support") == ABSENT).sum().alias("n_absent"),
    )
    return edges.with_columns(
        (pl.col("n_pubs") - pl.col("n_no")).alias("n_kept_b"),
        (pl.col("n_pubs") - pl.col("n_no") >= 1).alias("survive_b"),
        (pl.col("n_supported") >= 1).alias("survive_c"),
    )


def count_orphan_nodes(nodes_path: Path, surviving_node_ids: set[str]) -> dict[str, int]:
    """Count nodes that no surviving edge references."""
    total = 0
    referenced = 0
    with open_text(nodes_path) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            node_id = json.loads(line).get("id")
            if node_id is None:
                continue
            total += 1
            if node_id in surviving_node_ids:
                referenced += 1
    return {"total_nodes": total, "referenced_by_surviving": referenced, "orphaned": total - referenced}


def _predicate_breakdown(edges: pl.DataFrame) -> list[dict[str, Any]]:
    """Per-predicate edge/pub deltas under Rule B."""
    grouped = (
        edges.group_by("predicate")
        .agg(
            pl.len().alias("edges_before"),
            pl.col("survive_b").sum().alias("edges_after"),
            pl.col("n_pubs").sum().alias("pubs_before"),
            pl.col("n_kept_b").sum().alias("pubs_after"),
        )
        .with_columns((pl.col("edges_before") - pl.col("edges_after")).alias("edges_dropped"))
        .sort("edges_before", descending=True)
    )
    return grouped.to_dicts()


def analyze(
    pairs: pl.DataFrame,
    verdicts: pl.DataFrame,
    edges_with_pubs: int,
    total_edges_read: int,
    nodes_path: Path | None,
) -> dict[str, Any]:
    """Run the full join + Rule-B/Rule-C analysis and return a structured report."""
    joined = pairs.join(verdicts, on=JOIN_KEYS, how="left").with_columns(
        pl.col("support").fill_null(ABSENT)
    )

    total_pairs = joined.height
    verdict_counts = {
        row["support"]: row["len"]
        for row in joined.group_by("support").len().sort("len", descending=True).to_dicts()
    }
    matched = total_pairs - verdict_counts.get(ABSENT, 0)

    edges = aggregate_edges(joined)
    total_edges = edges.height
    total_pubs = int(edges["n_pubs"].sum())

    surviving_b = edges.filter("survive_b")
    edges_after_b = surviving_b.height
    pubs_after_b = int(edges["n_kept_b"].sum())
    edges_after_c = int(edges["survive_c"].sum())
    # Edges kept by B but not C: they survive only on no-abstract/absent PMIDs.
    # This is the entire recall difference between the two rules.
    b_only = edges.filter(pl.col("survive_b") & ~pl.col("survive_c")).height

    kept = surviving_b["n_kept_b"]
    cap_distribution = {
        "median": int(kept.median() or 0),
        "p90": int(kept.quantile(0.90) or 0),
        "p99": int(kept.quantile(0.99) or 0),
        "max": int(kept.max() or 0),
        "edges_over": {str(t): int((kept > t).sum()) for t in CAP_THRESHOLDS},
    }

    report: dict[str, Any] = {
        "inputs": {
            "edges_read": total_edges_read,
            "edges_without_publications": total_edges_read - edges_with_pubs,
            "edges_with_publications": edges_with_pubs,
            "edge_pmid_pairs": total_pairs,
            "verdict_rows": verdicts.height,
        },
        "join_health": {
            "pairs_matched": matched,
            "pairs_absent": verdict_counts.get(ABSENT, 0),
            "match_rate": round(matched / total_pairs, 4) if total_pairs else 0.0,
            "verdict_breakdown": verdict_counts,
        },
        "rule_b_default": {
            "edges_before": total_edges,
            "edges_after": edges_after_b,
            "edges_dropped": total_edges - edges_after_b,
            "pubs_before": total_pubs,
            "pubs_after": pubs_after_b,
            "pubs_removed": total_pubs - pubs_after_b,
        },
        "rule_c_comparison": {
            "edges_after": edges_after_c,
            "edges_dropped": total_edges - edges_after_c,
            "edges_b_keeps_c_drops": b_only,
        },
        "cap_sizing_post_filter": cap_distribution,
        "predicate_breakdown": _predicate_breakdown(edges),
    }

    if nodes_path is not None:
        surviving_ids = set(surviving_b["subject"].to_list()) | set(surviving_b["object"].to_list())
        report["orphan_nodes"] = count_orphan_nodes(nodes_path, surviving_ids)

    return report


def format_summary(report: dict[str, Any]) -> str:
    """Render the key numbers as a human-readable console summary."""
    inp = report["inputs"]
    jh = report["join_health"]
    rb = report["rule_b_default"]
    rc = report["rule_c_comparison"]
    cap = report["cap_sizing_post_filter"]
    lines = [
        "=== SemMedDB PMID-filter verification ===",
        f"edges (with pubs): {inp['edges_with_publications']:,}   edge-PMID pairs: {inp['edge_pmid_pairs']:,}",
        f"join match rate:   {jh['match_rate']:.2%}   absent pairs: {jh['pairs_absent']:,}",
        f"verdict breakdown: {jh['verdict_breakdown']}",
        "",
        "-- Rule B (ship): drop only 'no'; edge dies only if all PMIDs are 'no' --",
        f"  edges  {rb['edges_before']:,} -> {rb['edges_after']:,}  (dropped {rb['edges_dropped']:,})",
        f"  pubs   {rb['pubs_before']:,} -> {rb['pubs_after']:,}  (removed {rb['pubs_removed']:,})",
        "",
        "-- Rule C (compare): keep only yes/maybe; edge needs >=1 supported --",
        f"  edges_after {rc['edges_after']:,}  (dropped {rc['edges_dropped']:,})",
        f"  B-keeps / C-drops (survive only on no-abstract): {rc['edges_b_keeps_c_drops']:,}",
        "",
        "-- Post-filter PMID counts on surviving edges (cap sizing) --",
        f"  median {cap['median']}  p90 {cap['p90']}  p99 {cap['p99']}  max {cap['max']:,}",
        f"  edges over threshold: {cap['edges_over']}",
    ]
    if "orphan_nodes" in report:
        orphan = report["orphan_nodes"]
        lines += [
            "",
            f"-- Orphan nodes after edge drops: {orphan['orphaned']:,} of {orphan['total_nodes']:,} --",
        ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-parquet", type=Path, required=True, help="LLM_PMID_Checker results.parquet")
    parser.add_argument("--no-abstract-parquet", type=Path, default=None, help="results_no_abstract.parquet")
    parser.add_argument("--normalized-edges", type=Path, required=True, help="uncapped normalized_edges.jsonl")
    parser.add_argument("--normalized-nodes", type=Path, default=None, help="normalized_nodes.jsonl (orphan counts)")
    parser.add_argument(
        "--out", type=Path, default=None,
        help="write full JSON report here; use a data/ path (gitignored) to avoid cluttering the repo root",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point: load inputs, run the analysis, print and optionally persist the report."""
    args = parse_args(argv)
    pairs, edges_with_pubs, total_edges_read = load_edge_pairs(args.normalized_edges)
    verdicts = build_verdicts(args.results_parquet, args.no_abstract_parquet)
    report = analyze(pairs, verdicts, edges_with_pubs, total_edges_read, args.normalized_nodes)

    print(format_summary(report))
    if args.out is not None:
        args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\nFull report written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
