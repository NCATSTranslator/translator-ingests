"""Re-key the SemMedDB LLM PMID-checker verdicts from normalized ids back to raw kg2.10.3 ids.

The checker artifact (``results.parquet``, ~26.7M rows) is keyed by NORMALIZED CURIEs as
Babel / Node Normalizer release ``2025sep1`` produced them. Node ids are rewritten by every
Babel release, so a filter that joins on them silently decays: a renamed id turns into a join
miss, the verdict becomes "absent", and an absent verdict keeps the publication. The filter
fails open with no error.

This script translates the verdict keys back to the RAW transform-stage ids of kg2.10.3, which
are frozen forever, using the normalization map of the exact run that fed the checker. The
pipeline filter can then run before normalization and join on ids that cannot drift.

Because normalization conflates identifiers (for example ``UniProtKB:P01137`` and
``NCBIGene:7040`` both normalize to ``NCBIGene:7040``), one normalized id can have several raw
pre-images. The re-key fans out: the verdict is copied to every raw id that normalized to that
key. A normalized id absent from the map is kept as-is.

This script does NOT import or touch the pipeline. It lives in a subdirectory (not directly
under ``semmeddb/``) on purpose: ``get_transform_version`` hashes ``*.py`` and ``*.json`` in
the ingest directory non-recursively, so a script placed here does not change the transform
version.

Inputs:

- ``--results-parquet``   LLM_PMID_Checker ``results.parquet`` (normalized keys).
- ``--normalization-map`` ``normalization_map.json`` from the CATRAX run that fed the checker,
  shaped ``{"normalization_map": {"<raw curie>": ["<normalized curie>", ...]}}``.
- ``--transform-edges``   (optional) raw transform-stage ``*_edges.jsonl`` (``.gz`` accepted).
  When given, coverage is computed twice: for the ORIGINAL normalized-keyed verdicts (the
  baseline, expected to miss a large fraction of raw edges) and for the re-keyed ones, so the
  manifest shows the before/after improvement.
- ``--out-parquet``       re-keyed verdicts.
- ``--out-manifest``      JSON manifest with checksums, fan-out stats and coverage.

Memory note: the re-key itself streams (lazy scan plus ``sink_parquet``), but the optional
coverage check materializes the ~28.9M (edge, PMID) key rows plus one verdict key table at a
time. Prefer a machine with >=16 GB RAM, as with ``analysis/verify_pmid_filter.py``.

Run:

    uv run python src/translator_ingest/ingests/semmeddb/analysis/rekey_pmid_verdicts.py \
        --results-parquet data/pmid_checker/results.parquet \
        --normalization-map rekey_ingredients/normalization_map.json \
        --transform-edges data/.../transform_892b6acb/semmeddb_uncapped_edges.jsonl \
        --out-parquet data/pmid_checker/results_raw_keyed.parquet \
        --out-manifest data/pmid_checker/results_raw_keyed.manifest.json
"""

import argparse
import gzip
import hashlib
import json
import logging
from collections.abc import Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

import polars as pl

LOGGER = logging.getLogger("rekey_pmid_verdicts")

# Top-level key of the CATRAX normalization map file: {"normalization_map": {raw: [normalized]}}.
NORMALIZATION_MAP_KEY: str = "normalization_map"

# Verdict columns read from results.parquet.
VERDICT_COLUMNS: list[str] = ["subject_curie", "predicate", "object_curie", "PMID", "support"]

# Key columns of a verdict table, used for the coverage join.
VERDICT_KEY_COLUMNS: list[str] = ["subject_curie", "predicate", "object_curie", "PMID"]

# Column order of the re-keyed parquet. The two ``*_normalized`` columns are kept for
# provenance: they let a reader see exactly which normalized key a raw row came from.
OUTPUT_COLUMNS: list[str] = [
    "subject_curie",
    "predicate",
    "object_curie",
    "PMID",
    "support",
    "subject_curie_normalized",
    "object_curie_normalized",
]

# Key columns of the exploded (edge, publication) table streamed from the transform edges.
EDGE_KEY_SCHEMA: dict[str, pl.DataType] = {
    "subject": pl.String(),
    "predicate": pl.String(),
    "object": pl.String(),
    "PMID": pl.String(),
}

# Join keys for coverage: the edge triple plus a prefix-stripped PMID.
TRIPLE_KEYS: list[str] = ["subject", "predicate", "object"]
JOIN_KEYS: list[str] = [*TRIPLE_KEYS, "pmid_key"]

# Renames that bring a verdict table onto the edge table's column names.
VERDICT_TO_EDGE_NAMES: dict[str, str] = {"subject_curie": "subject", "object_curie": "object"}

# Publication lists are sometimes serialized KGX-TSV style as a pipe-delimited string.
PUBLICATION_SEPARATOR: str = "|"

MANIFEST_NOTES: list[str] = [
    "Verdicts come from the LLM PMID-checker run whose keys are Babel / Node Normalizer release 2025sep1.",
    "Raw ids are kg2.10.3 SemMedDB transform-stage identifiers; they are frozen and do not change between "
    "Babel releases.",
    "Fan-out: normalization conflates identifiers, so a verdict is copied to every raw pre-image of its "
    "normalized key. One input row can therefore produce several output rows.",
    "A normalized id with no entry in the normalization map is carried through unchanged.",
    "coverage_before is the original normalized-keyed verdict table joined against the raw transform edges "
    "(the drift baseline); coverage_after is the re-keyed table against the same edges.",
]


def open_text(path: Path) -> AbstractContextManager[TextIO]:
    """Open a possibly gzipped text file for reading."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("rt", encoding="utf-8")


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file, read in a streaming fashion.

    Inputs run to several GB, so the file is never held in memory.
    """
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def pmid_key_expr(column: str) -> pl.Expr:
    """Return an expression normalizing a PMID column to a bare-id join key.

    Strips a leading ``PMID:`` (any case) and surrounding whitespace, so ``"PMID:123"``,
    ``"123"`` and the integer ``123`` all collapse to ``"123"``. Without this, a verdict table
    that stores bare numbers would silently score 0% coverage against ``PMID:``-prefixed edges.
    """
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.replace(r"(?i)^pmid:", "")
        .alias("pmid_key")
    )


def load_inverse_normalization_map(map_file: Path) -> pl.DataFrame:
    """Load the normalization map and invert it into a ``normalized_curie -> raw_curie`` table.

    The file maps each raw id to a list of normalized ids (almost always exactly one). The
    returned frame has one row per raw/normalized pair, so a normalized id that several raw ids
    collapse onto appears once per pre-image. Rows are sorted for reproducibility.
    """
    with map_file.open("rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    mapping: dict[str, list[str]] = payload[NORMALIZATION_MAP_KEY]
    frame = pl.DataFrame(
        {"raw_curie": list(mapping.keys()), "normalized_curie": list(mapping.values())},
        schema={"raw_curie": pl.String(), "normalized_curie": pl.List(pl.String())},
    )
    return (
        frame.explode("normalized_curie")
        .drop_nulls("normalized_curie")
        .select("normalized_curie", "raw_curie")
        .unique()
        .sort(["normalized_curie", "raw_curie"])
    )


def rekey_verdicts(verdicts: pl.LazyFrame, inverse_map: pl.DataFrame) -> pl.LazyFrame:
    """Rewrite verdict subject/object ids from normalized back to raw ids.

    Each side is left-joined against the inverse map, so a normalized id with several raw
    pre-images fans out into one row per pre-image; a normalized id missing from the map keeps
    its original value. The normalized keys are retained as two extra provenance columns and
    exact duplicate rows are dropped.
    """
    subject_map = inverse_map.lazy().rename(
        {"normalized_curie": "subject_curie_normalized", "raw_curie": "subject_curie_raw"}
    )
    object_map = inverse_map.lazy().rename(
        {"normalized_curie": "object_curie_normalized", "raw_curie": "object_curie_raw"}
    )
    return (
        verdicts.rename({"subject_curie": "subject_curie_normalized", "object_curie": "object_curie_normalized"})
        .join(subject_map, on="subject_curie_normalized", how="left")
        .join(object_map, on="object_curie_normalized", how="left")
        .with_columns(
            pl.col("subject_curie_raw").fill_null(pl.col("subject_curie_normalized")).alias("subject_curie"),
            pl.col("object_curie_raw").fill_null(pl.col("object_curie_normalized")).alias("object_curie"),
        )
        .select(OUTPUT_COLUMNS)
        .unique()
    )


def iter_publication_rows(record: dict[str, Any]) -> Iterator[tuple[str, str, str, str]]:
    """Yield ``(subject, predicate, object, PMID)`` for each publication of one KGX edge.

    ``publications`` is a JSON list in our JSONL output but may be a pipe-delimited string in
    KGX TSV-derived files, so both are accepted. Edges missing subject/predicate/object, or
    carrying no publication, yield nothing.

    >>> edge = {"subject": "CHEBI:1", "predicate": "biolink:affects", "object": "NCBIGene:2",
    ...         "publications": ["PMID:10", "PMID:20"]}
    >>> list(iter_publication_rows(edge))
    [('CHEBI:1', 'biolink:affects', 'NCBIGene:2', 'PMID:10'), ('CHEBI:1', 'biolink:affects', 'NCBIGene:2', 'PMID:20')]
    >>> list(iter_publication_rows({"subject": "A", "predicate": "p", "object": "B",
    ...                             "publications": "PMID:1|PMID:2"}))
    [('A', 'p', 'B', 'PMID:1'), ('A', 'p', 'B', 'PMID:2')]
    >>> list(iter_publication_rows({"subject": "A", "predicate": "p", "object": "B"}))
    []
    """
    subject = record.get("subject")
    predicate = record.get("predicate")
    obj = record.get("object")
    if not (subject and predicate and obj):
        return
    publications = record.get("publications") or []
    if isinstance(publications, str):
        publications = [pmid for pmid in publications.split(PUBLICATION_SEPARATOR) if pmid]
    for pmid in publications:
        if pmid:
            yield subject, predicate, obj, pmid


def iter_edge_publication_keys(edges_file: Path, chunk_size: int = 100_000) -> Iterator[pl.DataFrame]:
    """Stream a transform-stage edges JSONL into (edge, publication) key frames.

    The real file is ~1.7M lines / ~11 GB, so it is read line by line and emitted in batches of
    at most ``chunk_size`` publication rows. All publications of one edge stay in the same
    batch, so a batch can slightly exceed ``chunk_size``. Yields nothing for an empty file.
    """
    columns: dict[str, list[str]] = {name: [] for name in EDGE_KEY_SCHEMA}
    rows_in_chunk = 0
    with open_text(edges_file) as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            record: dict[str, Any] = json.loads(stripped)
            for subject, predicate, obj, pmid in iter_publication_rows(record):
                columns["subject"].append(subject)
                columns["predicate"].append(predicate)
                columns["object"].append(obj)
                columns["PMID"].append(pmid)
                rows_in_chunk += 1
            if rows_in_chunk >= chunk_size:
                yield pl.DataFrame(columns, schema=EDGE_KEY_SCHEMA)
                columns = {name: [] for name in EDGE_KEY_SCHEMA}
                rows_in_chunk = 0
    if rows_in_chunk:
        yield pl.DataFrame(columns, schema=EDGE_KEY_SCHEMA)


def compute_coverage(edge_keys: pl.DataFrame, verdict_keys: pl.DataFrame) -> dict[str, float | int]:
    """Measure how much of the raw edge set the verdict table can address.

    ``edge_keys`` holds ``subject, predicate, object, PMID`` rows (one per publication of each
    edge); ``verdict_keys`` holds ``subject_curie, predicate, object_curie, PMID`` rows. Both
    sides are compared on a prefix-stripped PMID. Semi-joins are used rather than Python sets so
    this scales to the real ~29M row tables.

    ``edges_total`` and ``edges_with_verdict`` count distinct ``(subject, predicate, object)``
    triples; a triple counts as covered when any of its publications has a verdict.
    """
    edges = edge_keys.with_columns(pmid_key_expr("PMID")).select(JOIN_KEYS)
    verdicts = (
        verdict_keys.rename(VERDICT_TO_EDGE_NAMES).with_columns(pmid_key_expr("PMID")).select(JOIN_KEYS)
    )

    pairs_total = edges.height
    pairs_with_verdict = edges.join(verdicts, on=JOIN_KEYS, how="semi").height

    edge_triples = edges.select(TRIPLE_KEYS).unique()
    verdict_triples = verdicts.select(TRIPLE_KEYS).unique()
    edges_total = edge_triples.height
    edges_with_verdict = edge_triples.join(verdict_triples, on=TRIPLE_KEYS, how="semi").height

    return {
        "pmid_pairs_total": pairs_total,
        "pmid_pairs_with_verdict": pairs_with_verdict,
        "pmid_coverage": round(pairs_with_verdict / pairs_total, 6) if pairs_total else 0.0,
        "edges_total": edges_total,
        "edges_with_verdict": edges_with_verdict,
        "edge_coverage": round(edges_with_verdict / edges_total, 6) if edges_total else 0.0,
    }


def _as_lazy(frame: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
    """Return ``frame`` as a LazyFrame, accepting either polars frame type."""
    return frame.lazy() if isinstance(frame, pl.DataFrame) else frame


def rekey_stats(before: pl.LazyFrame | pl.DataFrame, after: pl.LazyFrame | pl.DataFrame) -> dict[str, int]:
    """Compare the verdict table before and after re-keying.

    ``fan_out_rows_added`` is the net row delta: conflation adds rows, the duplicate drop
    removes them, so it can be negative if the input contained rows that collapse onto one
    another. ``subjects_renamed`` / ``objects_renamed`` count output rows whose raw id differs
    from the normalized id it came from.
    """
    input_rows = int(_as_lazy(before).select(pl.len()).collect().item())
    summary = (
        _as_lazy(after)
        .select(
            pl.len().alias("output_rows"),
            (pl.col("subject_curie") != pl.col("subject_curie_normalized")).sum().alias("subjects_renamed"),
            (pl.col("object_curie") != pl.col("object_curie_normalized")).sum().alias("objects_renamed"),
        )
        .collect()
        .row(0, named=True)
    )
    output_rows = int(summary["output_rows"])
    return {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "fan_out_rows_added": output_rows - input_rows,
        "subjects_renamed": int(summary["subjects_renamed"]),
        "objects_renamed": int(summary["objects_renamed"]),
    }


def load_edge_keys(edges_file: Path, chunk_size: int = 100_000) -> pl.DataFrame:
    """Stream the transform edges into a single (edge, publication) key table, logging progress."""
    frames: list[pl.DataFrame] = []
    rows_read = 0
    for chunk in iter_edge_publication_keys(edges_file, chunk_size=chunk_size):
        frames.append(chunk)
        rows_read += chunk.height
        LOGGER.info("streamed %s edge-publication rows from %s", f"{rows_read:,}", edges_file.name)
    if not frames:
        return pl.DataFrame(schema=EDGE_KEY_SCHEMA)
    return pl.concat(frames, rechunk=True)


def build_manifest(
    results_parquet: Path,
    normalization_map: Path,
    out_parquet: Path,
    transform_edges: Path | None,
    stats: dict[str, int],
    coverage_before: dict[str, float | int] | None,
    coverage_after: dict[str, float | int] | None,
) -> dict[str, Any]:
    """Assemble the manifest describing this re-key run."""
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "results_parquet": {"path": str(results_parquet), "sha256": sha256_file(results_parquet)},
        "normalization_map": {"path": str(normalization_map), "sha256": sha256_file(normalization_map)},
        "out_parquet": {"path": str(out_parquet), "sha256": sha256_file(out_parquet)},
        "transform_edges": str(transform_edges) if transform_edges is not None else None,
        "rekey": stats,
        "coverage_before": coverage_before,
        "coverage_after": coverage_after,
        "notes": MANIFEST_NOTES,
    }


def format_summary(manifest: dict[str, Any]) -> str:
    """Render the manifest's key numbers as a human-readable console summary."""
    stats = manifest["rekey"]
    lines = [
        "=== SemMedDB PMID-checker verdict re-key ===",
        f"rows   {stats['input_rows']:,} -> {stats['output_rows']:,}  (net fan-out {stats['fan_out_rows_added']:+,})",
        f"renamed subjects {stats['subjects_renamed']:,}   renamed objects {stats['objects_renamed']:,}",
    ]
    for label in ("coverage_before", "coverage_after"):
        coverage = manifest[label]
        if coverage is None:
            continue
        lines += [
            "",
            f"-- {label} (vs raw transform edges) --",
            f"  PMID pairs {coverage['pmid_pairs_with_verdict']:,} / {coverage['pmid_pairs_total']:,}"
            f"  = {coverage['pmid_coverage']:.2%}",
            f"  edges      {coverage['edges_with_verdict']:,} / {coverage['edges_total']:,}"
            f"  = {coverage['edge_coverage']:.2%}",
        ]
    return "\n".join(lines)


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Re-key the verdicts, write the parquet and the manifest, and return the manifest."""
    LOGGER.info("loading normalization map %s", args.normalization_map)
    inverse_map = load_inverse_normalization_map(args.normalization_map)
    LOGGER.info("inverse map holds %s normalized/raw pairs", f"{inverse_map.height:,}")

    verdicts = pl.scan_parquet(args.results_parquet).select(VERDICT_COLUMNS)
    args.out_parquet.parent.mkdir(parents=True, exist_ok=True)
    LOGGER.info("re-keying %s -> %s", args.results_parquet, args.out_parquet)
    rekey_verdicts(verdicts, inverse_map).sink_parquet(args.out_parquet)

    stats = rekey_stats(verdicts, pl.scan_parquet(args.out_parquet))
    LOGGER.info("re-key rows %s -> %s", f"{stats['input_rows']:,}", f"{stats['output_rows']:,}")

    coverage_before: dict[str, float | int] | None = None
    coverage_after: dict[str, float | int] | None = None
    if args.transform_edges is not None:
        edge_keys = load_edge_keys(args.transform_edges, chunk_size=args.chunk_size)
        LOGGER.info("computing coverage of the original normalized-keyed verdicts")
        coverage_before = compute_coverage(
            edge_keys, pl.read_parquet(args.results_parquet, columns=VERDICT_KEY_COLUMNS)
        )
        LOGGER.info("computing coverage of the re-keyed verdicts")
        coverage_after = compute_coverage(
            edge_keys, pl.read_parquet(args.out_parquet, columns=VERDICT_KEY_COLUMNS)
        )

    manifest = build_manifest(
        results_parquet=args.results_parquet,
        normalization_map=args.normalization_map,
        out_parquet=args.out_parquet,
        transform_edges=args.transform_edges,
        stats=stats,
        coverage_before=coverage_before,
        coverage_after=coverage_after,
    )
    args.out_manifest.parent.mkdir(parents=True, exist_ok=True)
    args.out_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    LOGGER.info("manifest written to %s", args.out_manifest)
    return manifest


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-parquet", type=Path, required=True, help="LLM_PMID_Checker results.parquet")
    parser.add_argument(
        "--normalization-map", type=Path, required=True,
        help="normalization_map.json of the CATRAX run that fed the checker",
    )
    parser.add_argument(
        "--transform-edges", type=Path, default=None,
        help="raw transform-stage edges JSONL (.gz ok); enables the before/after coverage check",
    )
    parser.add_argument("--out-parquet", type=Path, required=True, help="re-keyed verdict parquet to write")
    parser.add_argument("--out-manifest", type=Path, required=True, help="JSON manifest to write")
    parser.add_argument(
        "--chunk-size", type=int, default=100_000,
        help="publication rows per streamed edge batch (default: 100000)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point: re-key the verdicts, then print the summary."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    manifest = run(parse_args(argv))
    print(format_summary(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
