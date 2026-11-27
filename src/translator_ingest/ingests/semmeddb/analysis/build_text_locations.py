r"""Build subject/object text positions for every (KG2 SemMedDB record, PMID) supporting sentence.

The UI highlights subject and object in the supporting sentence of a TextMiningStudyResult, as
for TMKP (``subject_location_in_text`` / ``object_location_in_text``: ``[start, end]``, 0-based,
end exclusive). KG2 kept each PMID's sentence, both scores and the publication date
(``publications_info``, KG2.10.3 converter lines 79-85), but not the mention offsets; those live in
the SemMedDB MySQL tables.

Inputs: ``--semmeddb-dir`` holds the ``export_semmeddb_predications.sh`` export of PREDICATION
joined with PREDICATION_AUX and SENTENCE; ``semmeddb_export`` reads it and proves it complete
(statuses, no partial files, canonical names, rows summing to ``--expected-rows``, each chunk's ids
in its range, no truncated chunk). ``--expected-rows`` is the source table's row count,
``mysql ... -e "SELECT COUNT(*) FROM semmeddb.PREDICATION"`` on the same database during the export,
kept with its provenance as ``predication_count.txt`` next to the chunks. ``--mrcui`` is UMLS
``MRCUI.RRF`` and ``--kg2-edges`` the kg2.10.3 SemMedDB edges JSONL.

The artifact covers a superset of the records the transform emits with a location. A record is
used when ``semmeddb.record_filter_reason`` passes, it has a subject, object and predicate (else the
transform skips it as an invalid edge), and it has exactly one kg2_id, of the converter's form.
The transform's other checks are not applied: the reader's ``in_exact`` predicate filter in
``semmeddb.yaml``, the PMID verdict filter and the invalid-node check, so the artifact can hold
spans for records or PMIDs the transform drops. Skipped records are counted per reason. If those
rules change and the artifact is not rebuilt, newly admitted records get no locations, and the
transform's text-location coverage guard only fails once they are more than
``1 - semmeddb.MIN_TEXT_LOCATION_COVERAGE`` of the emitted study results.

Matching follows KG2.10.3 (RTXteam/RTX-KG2, tag KG2.10.3; see ``kg2_semmeddb_converter`` for the
rules and their upstream lines): each non-negated SemMedDB row yields the KG2 relations the
converter made from it, and a row matches a record only when one of them equals the record's
kg2_id (subject, predicate, object). Negated (``NEG_``) rows are dropped and counted: KG2 gave them
the same id as their positive twin, kept the first edge per id and then dropped negated edges, so
a kept record's evidence is never a negated row. xref relations get no spans.

A matching row also needs the same PMID, the exact same sentence (a 64-bit hash computed by the same
polars expression on both sides) and the same ``subject score`` / ``object score`` strings as the
record's ``publications_info[PMID]``, and its spans must check out: ``sentence[start:end]`` equals
SUBJECT_TEXT (and OBJECT_TEXT), with offsets ``X_START_INDEX - SENT_START_INDEX``. If rows with
different spans remain in one group, the smallest (subject_start, object_start) is kept and counted.
A key whose records did not all get a span is dropped, since the transform puts it on all of them.

Only the raw group KG2 kept may supply spans. KG2 grouped SemMedDB rows by their raw SUBJECT_CUI,
PREDICATE and OBJECT_CUI strings, and several positive groups can make the same kg2_id (``C1`` and
``C1|931``, a retired CUI and its MRCUI target, gene lists sharing a gene). ``process/merge_graphs.py``
(KG2.10.3 lines 96-99) kept the first edge per id, so a record's publications come from one group:
the one whose PMIDs cover all of them. Our export left-joins where KG2 natural-joined (KG2 also
joined CITATIONS), so our groups can only hold more PMIDs, and covering, not equality, is the test.
A group counts only PMIDs with a non-negated row, and all rows of a raw group yield the same
relations. The kept group must also hold the evidence KG2 stored: a row with the record's sentence
and scores at every (record, PMID) pair that has a sentence. Only rows of such kept groups are
candidates, chosen before the text check, so a kept group whose rows all fail the text check gives
its pair no span instead of letting another group's row stand in. A record with several kept groups
uses them all; a pair is dropped when one of them has rows for it but none passes the text check,
or when they pick different spans. A record with no kept group gets no spans.
All of these are counted.

KG2 reverses a few SemMedDB predicates (``part_of`` becomes ``biolink:has_part``); for those the
SemMedDB subject span goes to the object columns and vice versa. Each predicate's orientation is
derived from the kg2_ids of used records whose ends literally equal the record's subject and
object; skipped records do not vote. A predicate seen both ways, or a matched predicate with no
orientation, stops the build.

The output is keyed by ``kg2_id`` and ``text_locations.evidence_fingerprint(PMID, publications_info[PMID])``
and globally sorted by that key; its columns are the loader's contract, imported from ``text_locations``.
Records sharing a kg2_id and evidence collapse into one row; two different span sets under one key
stop the build.

Work is split into buckets by ``PMID mod --buckets``, so matching runs one bucket at a time.
Intermediates go to ``--work-dir``, which must not exist yet, and are deleted after a successful
build unless ``--keep-work`` (``work_dir.WorkFiles``: only the directories and files the build
created, each while it still is the one created, so the work dir stays if it holds anything else).
A failed build leaves the work dir behind, so a rerun must use a new one or delete it first.
Before anything is written, ``output_guards`` refuses outputs that alias an input, a code file or
each other or lie inside ``--semmeddb-dir`` or ``--work-dir``, and locks each output
(``<output>.lock``) so a second build of the same outputs stops; the work dir is then created
atomically, so a second build on it stops.
It lives in ``analysis/`` on purpose: ``get_transform_version`` does not hash this directory.

Run:

    uv run python src/translator_ingest/ingests/semmeddb/analysis/build_text_locations.py \
        --semmeddb-dir data/semmeddb_raw/semmedVER43_2023 --expected-rows 124749251 \
        --mrcui data/semmeddb_raw/semmedVER43_2023/MRCUI.RRF \
        --kg2-edges data/semmeddb/semmeddb-2023-kg2.10.3/source_data/kg2.10.3-semmeddb-edges.jsonl.gz \
        --work-dir data/semmeddb_raw/work \
        --out-parquet data/semmeddb_raw/semmeddb_text_locations_raw_keyed.parquet \
        --out-manifest data/semmeddb_raw/semmeddb_text_locations_raw_keyed.manifest.json
"""

import argparse
import json
import logging
from collections import Counter
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from translator_ingest.ingests.semmeddb.analysis.kg2_records import (
    PAIR_SCHEMA,
    RECORD_SCHEMA,
    RELATION_COLUMNS,
    Kg2Tally,
    iter_kg2_batches,
)
from translator_ingest.ingests.semmeddb.analysis.kg2_semmeddb_converter import (
    CURIE_SEPARATOR,
    kg2_relations,
    load_cui_remap,
    negated,
)
from translator_ingest.ingests.semmeddb.analysis.kg2_orientation import (
    derive_orientation,
    orientation_frame,
    orientation_table,
)
from translator_ingest.ingests.semmeddb.analysis.output_guards import (
    ResolvedOutputs,
    deferred_signals,
    output_locks,
    refuse_unsafe_outputs,
    staged_outputs,
)
from translator_ingest.ingests.semmeddb.analysis.rekey_pmid_verdicts import sha256_file
from translator_ingest.ingests.semmeddb.analysis.semmeddb_export import (
    CHUNK_SUFFIX,
    ESCAPE_CHARACTER,
    CheckedExport,
    check_listing_unchanged,
    export_listing,
    PREDICATION_ID_RANGE_SIZE,
    check_export,
    check_predication_ids,
    read_semmeddb_chunk,
    unescape_mysql,
)
from translator_ingest.ingests.semmeddb.analysis.text_location_report import (
    REPO_ROOT,
    SourceHashes,
    code_provenance,
    coverage_table,
    format_summary,
    hash_files,
    matching_summary,
)
from translator_ingest.ingests.semmeddb.analysis.work_dir import (
    BUCKET_PREFIX,
    OUTPUT_SUBDIR,
    PAIRS_SUBDIR,
    PART_PREFIX,
    PUBLICATIONS_SUBDIR,
    RECORDS_SUBDIR,
    SEMMEDDB_SUBDIR,
    bucket_directory,
    WorkFiles,
)
from translator_ingest.ingests.semmeddb.text_locations import (
    TEXT_LOCATION_ARTIFACT_SCHEMA,
    TEXT_LOCATION_FINGERPRINT_COLUMN,
    TEXT_LOCATION_KG2_ID_COLUMN,
    TEXT_SPAN_COLUMNS,
)

LOGGER = logging.getLogger("build_text_locations")

PMID_PREFIX = "PMID:"
HASH_SEED = 0
DEFAULT_BUCKETS = 16
DEFAULT_BATCH_PAIRS = 2_000_000

# One (record, PMID) pair; a record holds each PMID once.
PAIR_KEY: list[str] = ["record_idx", "pmid"]
MATCH_KEY: list[str] = ["pmid", "sentence_hash", "semmed_predicate", "kg2_subject", "kg2_object"]
SENTENCE_KEY_COLUMNS: list[str] = ["pmid", "sentence_hash"]
# A record and one raw SemMedDB group (raw SUBJECT_CUI, PREDICATE, OBJECT_CUI) that can supply it.
GROUP_KEY: list[str] = ["record_idx", "raw_group"]
RAW_GROUP_COLUMNS: list[str] = ["SUBJECT_CUI", "PREDICATE", "OBJECT_CUI"]
RAW_GROUP_SEPARATOR = "\x1f"
OUTPUT_KEY: list[str] = [TEXT_LOCATION_KG2_ID_COLUMN, TEXT_LOCATION_FINGERPRINT_COLUMN]
# Earliest span first; the ends only break ties between spans with the same starts.
SPAN_ORDER: list[str] = ["subject_start", "object_start", "subject_end", "object_end"]
SEMMED_SPAN_COLUMNS: list[str] = [f"semmed_{column}" for column in TEXT_SPAN_COLUMNS]
# Coverage is also reported per value of these columns.
BREAKDOWNS: list[str] = ["semmed_predicate", "predicate", "id_prefixes"]

PUBLICATION_PART_SCHEMA: dict[str, pl.DataType] = {"record_idx": pl.UInt32(), "pmid": pl.Int64()}
PAIR_PART_SCHEMA: dict[str, pl.DataType] = {
    **{name: dtype for name, dtype in PAIR_SCHEMA.items() if name != "sentence"},
    "pmid": pl.Int64(),
    "sentence_hash": pl.UInt64(),
}
# One (SemMedDB row, KG2 relation it yields) as stored in the bucketed work files.
SEMMED_PART_SCHEMA: dict[str, pl.DataType] = {
    "predication_id": pl.Int64(),
    "pmid": pl.Int64(),
    "sentence_hash": pl.UInt64(),
    "raw_group": pl.UInt64(),
    "semmed_predicate": pl.String(),
    "semmed_subject_score": pl.String(),
    "semmed_object_score": pl.String(),
    **{name: pl.Int32() for name in SEMMED_SPAN_COLUMNS},
    "subject_text_ok": pl.Boolean(),
    "object_text_ok": pl.Boolean(),
    "kg2_subject": pl.String(),
    "kg2_object": pl.String(),
    "remapped": pl.Boolean(),
}
MANIFEST_NOTES: list[str] = [
    "Spans: 0-based, end exclusive, code points of publications_info[PMID]['sentence'], record-oriented.",
    "Key: kg2_id and text_locations.evidence_fingerprint(PMID, publications_info[PMID]); sorted by that key.",
    "Records must pass semmeddb.record_filter_reason, have a subject, object and predicate, and have one "
    "well-formed kg2_id; only those records vote on orientation.",
    "A row matches when a KG2.10.3 converter relation of it equals the kg2_id and PMID, sentence and "
    "scores equal the record's evidence; rows whose span text differs from the sentence are not used.",
    "Only kept raw groups supply spans: their PMIDs cover the record's publications (merge_graphs.py 96-99) and "
    "they hold its stored sentence and scores at every pair with a sentence. No fallback: a pair is dropped "
    "when a kept group only has rows failing the text check or kept groups pick different spans.",
    "Several spans left in one group for a (record, PMID): the smallest (subject_start, object_start) is kept.",
    "pairs_located_only_via_mrcui: located pairs whose every usable row needed the MRCUI remap.",
]


def empty_breakdowns() -> dict[str, Counter[str]]:
    """Return one empty counter per ``BREAKDOWNS`` column."""
    return {column: Counter() for column in BREAKDOWNS}


@dataclass
class MatchTally:
    """Counts gathered while matching the buckets, summed over all buckets.

    ``pairs_by`` and ``matched_by`` map each ``BREAKDOWNS`` column to pair counts per value.
    """

    counts: Counter[str] = field(default_factory=Counter)
    pairs_by: dict[str, Counter[str]] = field(default_factory=empty_breakdowns)
    matched_by: dict[str, Counter[str]] = field(default_factory=empty_breakdowns)


def span_matches(sentence: str, start: str, end: str, text: str) -> pl.Expr:
    """Return whether ``sentence[start:end] == text`` with ``0 <= start < end <= len(sentence)``.

    Offsets count Unicode code points, as Python ``str`` slicing does. Nulls give ``False``.

    >>> frame = pl.DataFrame({"s": ["a héllo b", "abc", "abc", "abc"], "start": [2, -1, 1, 2],
    ...                       "end": [7, 1, 9, 2], "text": ["héllo", "a", "bc", ""]})
    >>> frame.select(span_matches("s", "start", "end", "text")).to_series().to_list()
    [True, False, False, False]
    """
    start_expr = pl.col(start)
    end_expr = pl.col(end)
    sliced = pl.col(sentence).str.slice(start_expr, (end_expr - start_expr).clip(lower_bound=0))
    return (
        (start_expr >= 0)
        & (end_expr > start_expr)
        & (end_expr <= pl.col(sentence).str.len_chars())
        & (sliced == pl.col(text))
    ).fill_null(False)


def sentence_hash(column: str) -> pl.Expr:
    """Return the 64-bit hash of an exact sentence text, null for a null sentence.

    Both sides of the sentence join use this one expression, so equal texts get equal hashes.
    """
    return pl.when(pl.col(column).is_not_null()).then(pl.col(column).hash(HASH_SEED)).alias("sentence_hash")


def bucket_of(n_buckets: int) -> pl.Expr:
    """Return the work bucket of a row: its numeric PMID modulo ``n_buckets``."""
    return (pl.col("pmid") % n_buckets).alias("bucket")


def write_bucketed(frame: pl.DataFrame, root: Path, n_buckets: int, stem: str, work_files: WorkFiles) -> None:
    """Write each bucket of ``frame`` (by its ``bucket`` column) to ``root/bucket_NNN/stem.parquet``."""
    for bucket in range(n_buckets):
        part = frame.filter(pl.col("bucket") == bucket).drop("bucket")
        if part.height:
            work_files.write_parquet(part, bucket_directory(root, bucket) / f"{stem}.parquet")


def read_parts(work_files: WorkFiles, directory: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Read the parts the build wrote in ``directory`` (``WorkFiles.parts``), or an empty frame of ``schema``."""
    files = work_files.parts(directory)
    if not files:
        return pl.DataFrame(schema=schema)
    return pl.read_parquet(files, glob=False)


def read_bucket(work_files: WorkFiles, root: Path, bucket: int, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Read the parts the build wrote for one bucket under ``root``, or an empty frame of ``schema``."""
    return read_parts(work_files, bucket_directory(root, bucket), schema)


def with_pmid(frame: pl.DataFrame, n_buckets: int) -> pl.DataFrame:
    """Add the numeric PMID of the ``PMID`` key and its bucket; a key not ``PMID:<digits>`` fails."""
    pmid = pl.col("PMID").str.strip_prefix(PMID_PREFIX).cast(pl.Int64).alias("pmid")
    return frame.with_columns(pmid).with_columns(bucket_of(n_buckets))


def split_kg2_edges(edges_file: Path, work_files: WorkFiles, n_buckets: int, batch_pairs: int) -> Kg2Tally:
    """Stream the KG2 edges into record parts and bucketed publication and pair parts, and tally them.

    Pair sentences are replaced by their hash.
    """
    tally = Kg2Tally()
    work_dir = work_files.root
    work_files.directory(work_dir / RECORDS_SUBDIR)
    for batch_number, batch in enumerate(iter_kg2_batches(edges_file, batch_pairs, tally)):
        stem = f"{PART_PREFIX}{batch_number:05d}"
        records, publications, pairs = batch.frames()
        work_files.write_parquet(records, work_dir / RECORDS_SUBDIR / f"{stem}.parquet")
        publication_parts = with_pmid(publications, n_buckets).drop("PMID")
        write_bucketed(publication_parts, work_dir / PUBLICATIONS_SUBDIR, n_buckets, stem, work_files)
        hashed_pairs = with_pmid(pairs, n_buckets).with_columns(sentence_hash("sentence")).drop("sentence")
        write_bucketed(hashed_pairs, work_dir / PAIRS_SUBDIR, n_buckets, stem, work_files)
        LOGGER.info(
            "KG2: %s records, %s pairs with a sentence",
            f"{tally.counts['records']:,}",
            f"{tally.counts['pairs_with_sentence']:,}",
        )
    return tally


def relative_offset(column: str) -> pl.Expr:
    """Return a SemMedDB document offset as an offset into its sentence."""
    return pl.col(column).cast(pl.Int64) - pl.col("SENT_START_INDEX").cast(pl.Int64)


def semmeddb_lean_frame(raw: pl.DataFrame) -> pl.DataFrame:
    """Reduce parsed export rows to what matching needs, before the KG2 relations are made.

    Unescapes the sentence and the subject/object texts, turns document offsets into sentence
    offsets, checks both spans against their texts and hashes the sentence. The predicate stays raw
    for ``kg2_relations``. Non-numeric PMIDs or offsets fail the strict casts.
    """
    return raw.with_columns(
        unescape_mysql("SENTENCE").alias("sentence"),
        unescape_mysql("SUBJECT_TEXT").alias("subject_text"),
        unescape_mysql("OBJECT_TEXT").alias("object_text"),
        relative_offset("SUBJECT_START_INDEX").alias("semmed_subject_start"),
        relative_offset("SUBJECT_END_INDEX").alias("semmed_subject_end"),
        relative_offset("OBJECT_START_INDEX").alias("semmed_object_start"),
        relative_offset("OBJECT_END_INDEX").alias("semmed_object_end"),
    ).select(
        pl.col("PREDICATION_ID").cast(pl.Int64).alias("predication_id"),
        pl.col("PMID").cast(pl.Int64).alias("pmid"),
        sentence_hash("sentence"),
        pl.concat_str(RAW_GROUP_COLUMNS, separator=RAW_GROUP_SEPARATOR).hash(HASH_SEED).alias("raw_group"),
        pl.col("PREDICATE").alias("predicate"),
        pl.col("SUBJECT_SCORE").alias("semmed_subject_score"),
        pl.col("OBJECT_SCORE").alias("semmed_object_score"),
        *[pl.col(column).cast(pl.Int32) for column in SEMMED_SPAN_COLUMNS],
        *[
            span_matches("sentence", f"semmed_{side}_start", f"semmed_{side}_end", f"{side}_text").alias(
                f"{side}_text_ok"
            )
            for side in ("subject", "object")
        ],
        pl.col("SUBJECT_CUI").alias("subject_cui"),
        pl.col("OBJECT_CUI").alias("object_cui"),
    )


def semmeddb_chunk_counts(raw: pl.DataFrame, lean: pl.DataFrame, relations: pl.DataFrame) -> Counter[str]:
    """Count rows, nulls, dropped negations, escapes, text-check failures and KG2 relations of a chunk.

    ``rows_without_relation`` counts non-negated rows that make no KG2 relation.
    """
    raw_counts = raw.select(
        pl.len().alias("rows"),
        pl.col("SENTENCE").is_null().sum().alias("rows_null_sentence"),
        pl.col("SUBJECT_START_INDEX").is_null().sum().alias("rows_null_aux"),
        pl.col("SENTENCE").str.contains(ESCAPE_CHARACTER, literal=True).sum().alias("rows_escaped_sentence"),
        negated("PREDICATE").sum().alias("rows_negated_dropped"),
    ).row(0, named=True)
    lean_counts = lean.select(
        (~pl.col("subject_text_ok")).sum().alias("rows_subject_text_fail"),
        (~pl.col("object_text_ok")).sum().alias("rows_object_text_fail"),
    ).row(0, named=True)
    relation_counts = {
        "relation_rows": relations.height,
        "rows_without_relation": lean.height
        - int(raw_counts["rows_negated_dropped"])
        - relations["predication_id"].n_unique(),
    }
    return Counter({name: int(value) for name, value in (raw_counts | lean_counts | relation_counts).items()})


def split_semmeddb_chunks(
    chunks: list[Path], work_files: WorkFiles, n_buckets: int, cui_remap: pl.DataFrame, hashes: SourceHashes
) -> tuple[Counter[str], list[dict[str, Any]]]:
    """Turn every export chunk into bucketed (row, KG2 relation) parquet parts.

    Returns the summed counts and one provenance entry (name, rows, id range and the sha256 taken
    before the build read the chunk) per chunk.
    """
    counts: Counter[str] = Counter()
    provenance: list[dict[str, Any]] = []
    for chunk in chunks:
        raw = read_semmeddb_chunk(chunk)
        lean = semmeddb_lean_frame(raw)
        id_range = check_predication_ids(chunk, lean["predication_id"])
        relations = kg2_relations(lean, cui_remap)
        counts.update(semmeddb_chunk_counts(raw, lean, relations))
        stem = chunk.name.removesuffix(CHUNK_SUFFIX)
        semmed_root = work_files.root / SEMMEDDB_SUBDIR
        write_bucketed(relations.with_columns(bucket_of(n_buckets)), semmed_root, n_buckets, stem, work_files)
        provenance.append({"name": chunk.name, "sha256": hashes.inputs[chunk], "rows": raw.height, **id_range})
        LOGGER.info("SemMedDB: %s done, %s rows so far", chunk.name, f"{counts['rows']:,}")
    return counts, provenance


def orient_spans(valid: pl.DataFrame, orientation: pl.DataFrame) -> pl.DataFrame:
    """Add the record-oriented ``TEXT_SPAN_COLUMNS``, swapping SemMedDB sides for reversed predicates.

    A matched predicate with no known orientation raises.
    """
    oriented = valid.join(orientation, on="semmed_predicate", how="left")
    unknown = oriented.filter(pl.col("flipped").is_null())["semmed_predicate"].unique().sort().to_list()
    if unknown:
        raise ValueError(f"matched SemMedDB predicates with no orientation in KG2: {unknown}")
    flipped = pl.col("flipped")
    return oriented.with_columns(
        pl.when(flipped)
        .then(pl.col(f"semmed_{other}_{end}"))
        .otherwise(pl.col(f"semmed_{side}_{end}"))
        .alias(f"{side}_{end}")
        for side, other in (("subject", "object"), ("object", "subject"))
        for end in ("start", "end")
    )


def same_scores() -> pl.Expr:
    """Return whether a candidate row carries the subject and object scores the record stored."""
    return (pl.col("subject_score") == pl.col("semmed_subject_score")) & (
        pl.col("object_score") == pl.col("semmed_object_score")
    )


def group_counts(groups: pl.DataFrame, records: pl.DataFrame, prefix: str) -> dict[str, int]:
    """Count the records with one, several or no (record, raw group) row in ``groups``."""
    per_record = records.join(groups.group_by("record_idx").len(), on="record_idx", how="left")["len"]
    counts = {
        f"records_one_{prefix}_group": (per_record == 1).sum(),
        f"records_several_{prefix}_groups": (per_record > 1).sum(),
        f"records_no_{prefix}_group": per_record.null_count(),
    }
    return {name: int(value) for name, value in counts.items()}


def bucket_group_counts(
    work_files: WorkFiles, bucket: int, relations: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Count per (record, raw group) in one bucket: covered publications and evidenced pairs.

    A publication is covered when the group has a row for it that yields the record's relation; a
    pair is evidenced when the group has such a row with the record's stored sentence and scores.
    """
    work_dir = work_files.root
    semmed = read_bucket(work_files, work_dir / SEMMEDDB_SUBDIR, bucket, SEMMED_PART_SCHEMA)
    publications = read_bucket(work_files, work_dir / PUBLICATIONS_SUBDIR, bucket, PUBLICATION_PART_SCHEMA)
    groups = semmed.select("pmid", *RELATION_COLUMNS, "raw_group").unique()
    covered = publications.join(relations, on="record_idx").join(groups, on=["pmid", *RELATION_COLUMNS])
    pairs = read_bucket(work_files, work_dir / PAIRS_SUBDIR, bucket, PAIR_PART_SCHEMA)
    evidenced = pairs.join(semmed, on=MATCH_KEY).filter(same_scores()).select(*PAIR_KEY, "raw_group").unique()
    return (
        covered.group_by(GROUP_KEY).agg(pl.len().alias("covered")),
        evidenced.group_by(GROUP_KEY).agg(pl.len().alias("evidenced")),
    )


def kept_groups(work_files: WorkFiles, n_buckets: int) -> tuple[pl.DataFrame, dict[str, int]]:
    """Return the (record, raw group) pairs of the groups KG2 kept, with counts per record.

    A covering group has a row (yielding the record's relation) at every publication of the record.
    A kept group is a covering group that also has a row with the record's stored sentence and
    scores at every (record, PMID) pair with a sentence. Both sets span buckets, so each bucket
    counts per (record, group) and the counts are summed.
    """
    records = read_parts(work_files, work_files.root / RECORDS_SUBDIR, RECORD_SCHEMA)
    relations = records.select("record_idx", *RELATION_COLUMNS)
    covered_parts, evidenced_parts = zip(
        *(bucket_group_counts(work_files, bucket, relations) for bucket in range(n_buckets)), strict=True
    )
    covered = pl.concat(covered_parts).group_by(GROUP_KEY).agg(pl.col("covered").sum())
    evidenced = pl.concat(evidenced_parts).group_by(GROUP_KEY).agg(pl.col("evidenced").sum())
    covering = covered.join(records, on="record_idx").filter(pl.col("covered") == pl.col("publication_count"))
    kept = (
        covering.join(evidenced, on=GROUP_KEY, how="left")
        .filter(pl.col("evidenced").fill_null(0) == pl.col("pair_count"))
        .select(GROUP_KEY)
    )
    return kept, group_counts(covering, records, "covering") | group_counts(kept, records, "kept")


def pick_spans(oriented: pl.DataFrame, tally: MatchTally) -> pl.DataFrame:
    """Pick one span per (record, PMID) from its kept groups' rows.

    Within one raw group the smallest (subject_start, object_start) wins, the ends breaking ties.
    When a record's kept groups pick different spans for a PMID, that pair is dropped. Counts the
    pairs with several spans in one group and the pairs dropped.
    """
    group_key = [*PAIR_KEY, "raw_group"]
    spans = oriented.select(*group_key, *TEXT_SPAN_COLUMNS).unique()
    several = spans.group_by(group_key).len().filter(pl.col("len") > 1).select(PAIR_KEY).unique().height
    picks = (
        spans.sort([*group_key, *SPAN_ORDER])
        .group_by(group_key, maintain_order=True)
        .first()
        .select(*PAIR_KEY, *TEXT_SPAN_COLUMNS)
        .unique()
    )
    disagreeing = picks.group_by(PAIR_KEY).len().filter(pl.col("len") > 1).select(PAIR_KEY)
    tally.counts.update({"pairs_several_spans": several, "pairs_groups_disagree_dropped": disagreeing.height})
    return picks.join(disagreeing, on=PAIR_KEY, how="anti")


def resolve_output_rows(
    chosen: pl.DataFrame, pairs: pl.DataFrame, tally: MatchTally
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Key the chosen spans by (kg2_id, evidence_fingerprint), merging records that share a key.

    The transform attaches a key's row to every record with that key, so a key is dropped when any
    of its (record, PMID) pairs got no pick, for example because those records' kept groups differ.
    Records sharing a key that all got a pick must agree; two different span sets raise. Returns
    the output rows and the (record, PMID) pairs they locate.
    """
    keys = pairs.select(*PAIR_KEY, *OUTPUT_KEY).unique(PAIR_KEY)
    unresolved = keys.join(chosen, on=PAIR_KEY, how="anti").select(OUTPUT_KEY).unique()
    picked = chosen.join(keys, on=PAIR_KEY, how="inner")
    keyed = picked.join(unresolved, on=OUTPUT_KEY, how="anti")
    output = keyed.select(*OUTPUT_KEY, *TEXT_SPAN_COLUMNS).unique()
    conflicts = output.filter(output.select(OUTPUT_KEY).is_duplicated())
    if conflicts.height:
        raise ValueError(f"(kg2_id, evidence_fingerprint) keys with different spans: {conflicts.head(4).rows()}")
    tally.counts.update(
        {
            "pairs_dropped_unresolved_key": picked.height - keyed.height,
            "pairs_located": keyed.height,
            "pairs_sharing_evidence_merged": keyed.height - output.height,
            "output_rows": output.height,
        }
    )
    return output.select(list(TEXT_LOCATION_ARTIFACT_SCHEMA)), keyed.select(PAIR_KEY)


def count_pairs(frame: pl.DataFrame) -> int:
    """Count distinct (record, PMID) pairs in a frame."""
    return frame.select(PAIR_KEY).unique().height


def count_by(frame: pl.DataFrame, column: str) -> Counter[str]:
    """Count rows per value of a String column."""
    return Counter(dict(frame.group_by(column).len().iter_rows()))


def with_id_prefixes(frame: pl.DataFrame) -> pl.DataFrame:
    """Add ``id_prefixes``, e.g. ``UMLS->NCBIGene``, from the kg2_id subject and object."""
    return frame.with_columns(
        pl.concat_str(
            pl.col("kg2_subject").str.splitn(CURIE_SEPARATOR, 2).struct.field("field_0"),
            pl.col("kg2_object").str.splitn(CURIE_SEPARATOR, 2).struct.field("field_0"),
            separator="->",
        ).alias("id_prefixes")
    )


def tally_breakdowns(pairs: pl.DataFrame, valid: pl.DataFrame, tally: MatchTally) -> None:
    """Add the pairs and the matched pairs per value of every ``BREAKDOWNS`` column."""
    pairs = with_id_prefixes(pairs)
    valid = with_id_prefixes(valid)
    for column in BREAKDOWNS:
        tally.pairs_by[column].update(count_by(pairs.select(*PAIR_KEY, column).unique(), column))
        tally.matched_by[column].update(count_by(valid.select(*PAIR_KEY, column).unique(), column))


def drop_pairs_with_a_failing_group(kept: pl.DataFrame, valid: pl.DataFrame, tally: MatchTally) -> pl.DataFrame:
    """Drop the valid rows of pairs where some kept group has rows but none passes the text check.

    Returns the remaining rows and counts the pairs that had a valid row but were dropped.
    """
    group_pair_key = [*PAIR_KEY, "raw_group"]
    failing = (
        kept.select(group_pair_key)
        .unique()
        .join(valid.select(group_pair_key).unique(), on=group_pair_key, how="anti")
        .select(PAIR_KEY)
        .unique()
    )
    tally.counts["pairs_kept_group_text_fail_dropped"] += count_pairs(valid.join(failing, on=PAIR_KEY, how="semi"))
    return valid.join(failing, on=PAIR_KEY, how="anti")


def match_bucket(
    pairs: pl.DataFrame, semmed: pl.DataFrame, kept_group: pl.DataFrame, orientation: pl.DataFrame, tally: MatchTally
) -> pl.DataFrame:
    """Match one bucket's KG2 pairs to the rows of their kept raw groups and return output rows.

    Rows are restricted to the kept groups before the text check, and a pair is dropped when any of
    its kept groups has rows for it but none passes the text check, so no group stands in for another.
    """
    candidates = pairs.join(semmed, on=MATCH_KEY, how="inner")
    scored = candidates.filter(same_scores())
    kept = scored.join(kept_group, on=GROUP_KEY, how="semi")
    valid = kept.filter(pl.col("subject_text_ok") & pl.col("object_text_ok"))
    usable = drop_pairs_with_a_failing_group(kept, valid, tally)
    chosen = pick_spans(orient_spans(usable, orientation), tally)
    output, located = resolve_output_rows(chosen, pairs, tally)
    matched = usable.join(located, on=PAIR_KEY, how="semi")
    tally.counts.update(
        {
            "pairs_with_sentence": count_pairs(pairs),
            "pairs_without_kept_group": count_pairs(pairs.join(kept_group, on="record_idx", how="anti")),
            "pairs_sentence_found": count_pairs(
                pairs.join(semmed.select(SENTENCE_KEY_COLUMNS).unique(), on=SENTENCE_KEY_COLUMNS, how="semi")
            ),
            "pairs_relation_found": count_pairs(candidates),
            "pairs_scores_matched": count_pairs(scored),
            "pairs_scores_matched_kept_group": count_pairs(kept),
            "pairs_valid_kept_group": count_pairs(valid),
            "pairs_matched": chosen.height,
            "pairs_located_without_remap": count_pairs(matched.filter(~pl.col("remapped"))),
            "candidate_rows": candidates.height,
            "scored_rows": scored.height,
            "kept_rows_subject_text_fail": kept.filter(~pl.col("subject_text_ok")).height,
            "kept_rows_object_text_fail": kept.filter(~pl.col("object_text_ok")).height,
        }
    )
    tally_breakdowns(pairs, matched, tally)
    return output


def match_buckets(
    work_files: WorkFiles, n_buckets: int, kept_group: pl.DataFrame, orientation: pl.DataFrame
) -> tuple[list[Path], MatchTally]:
    """Match every bucket, writing one output part per bucket, and return the parts and tally."""
    tally = MatchTally()
    work_dir = work_files.root
    output_root = work_files.directory(work_dir / OUTPUT_SUBDIR)
    parts: list[Path] = []
    for bucket in range(n_buckets):
        output = match_bucket(
            read_bucket(work_files, work_dir / PAIRS_SUBDIR, bucket, PAIR_PART_SCHEMA),
            read_bucket(work_files, work_dir / SEMMEDDB_SUBDIR, bucket, SEMMED_PART_SCHEMA),
            kept_group,
            orientation,
            tally,
        )
        part = output_root / f"{BUCKET_PREFIX}{bucket:03d}.parquet"
        work_files.write_parquet(output, part)
        parts.append(part)
        LOGGER.info(
            "bucket %s/%s: %s output rows, %s pairs matched of %s so far",
            bucket + 1,
            n_buckets,
            f"{output.height:,}",
            f"{tally.counts['pairs_matched']:,}",
            f"{tally.counts['pairs_with_sentence']:,}",
        )
    return parts, tally


def write_output(parts: list[Path], staged_parquet: Path, expected_rows: int) -> dict[str, Any]:
    """Sort the bucket parts by ``OUTPUT_KEY`` into the staged parquet, check it and describe it.

    The check covers the schema, the row count and that every row's key is strictly greater than
    the previous one, which proves both the global order and the uniqueness the loader relies on.
    The hash and size are of the staged file, which is later moved into place unchanged.
    """
    pl.scan_parquet(parts, glob=False).sort(OUTPUT_KEY).sink_parquet(staged_parquet)
    written = pl.scan_parquet(staged_parquet, glob=False)
    schema = dict(written.collect_schema())
    if schema != TEXT_LOCATION_ARTIFACT_SCHEMA:
        raise ValueError(f"output schema {schema} differs from {TEXT_LOCATION_ARTIFACT_SCHEMA}")
    kg2_id, fingerprint = (pl.col(column) for column in OUTPUT_KEY)
    after_previous = (kg2_id > kg2_id.shift()) | ((kg2_id == kg2_id.shift()) & (fingerprint > fingerprint.shift()))
    rows, ascending = written.select(pl.len(), after_previous.slice(1).all()).collect().row(0)
    if rows != expected_rows or not ascending:
        raise ValueError(f"output has {rows:,} rows (expected {expected_rows:,}), strictly ascending: {ascending}")
    LOGGER.info("assembled %s rows into the staged %s", f"{rows:,}", staged_parquet.name)
    return {
        "sha256": sha256_file(staged_parquet),
        "rows": rows,
        "size_bytes": staged_parquet.stat().st_size,
        "columns": {name: str(dtype) for name, dtype in schema.items()},
        "sorted_by": OUTPUT_KEY,
    }


def input_files(args: argparse.Namespace, export_files: list[Path]) -> list[Path]:
    """Return every file the build reads: ``--kg2-edges``, ``--mrcui`` and the export dir's listed files."""
    return [args.kg2_edges, args.mrcui, *export_files]


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Check the paths and the export, lock the outputs, claim the work dir and build; return the manifest.

    No output can lie inside the work dir when its claim succeeds: such outputs were refused, and
    an output's lock, created next to it, would have made the work dir exist already. Both outputs
    are written to staged files and moved into place, parquet first, only once both are complete,
    so a build that fails leaves the previous parquet and manifest as they were. The outputs are
    resolved once, at preflight, before any guard rule, and only those paths are checked, locked,
    staged and published. The code is hashed first, then the export dir is listed once and every
    input file hashed, before the build reads any input; before it publishes, the export dir is
    listed and both are hashed again, which stops the build if a file changed, appeared or went.
    From the end of the build on, SIGINT, SIGTERM and SIGHUP wait (``deferred_signals``) until the
    outputs are published, the work dir is cleaned up and the locks are released.
    """
    code = code_provenance(__name__)
    outputs = (args.out_parquet, args.out_manifest)
    directories = {"--semmeddb-dir": args.semmeddb_dir, "--work-dir": args.work_dir}
    export_files = export_listing(args.semmeddb_dir)
    inputs = input_files(args, export_files)
    targets = ResolvedOutputs.at_preflight(outputs)
    refuse_unsafe_outputs(targets, inputs, directories, [REPO_ROOT / entry["path"] for entry in code])
    hashes = SourceHashes(__name__, code, hash_files(inputs))
    export = check_export(args.semmeddb_dir, args.expected_rows, export_files)
    with ExitStack() as held_signals, output_locks(targets):
        with staged_outputs(targets) as (staged_parquet, staged_manifest):
            work_files = WorkFiles.claim(args.work_dir)
            manifest = build(args, export, hashes, staged_parquet, work_files)
            staged_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            work_files.check_unchanged()
            check_listing_unchanged(args.semmeddb_dir, export_files)
            hashes.check_unchanged()
            LOGGER.info("re-hashed %s inputs and the code: unchanged", f"{len(hashes.inputs):,}")
            # The re-hash takes minutes, so the export dir is listed again once it is done.
            check_listing_unchanged(args.semmeddb_dir, export_files)
            held_signals.enter_context(deferred_signals())
        LOGGER.info("published %s and %s", args.out_parquet, args.out_manifest)
        if not args.keep_work:
            work_files.remove()
    return manifest


def build(
    args: argparse.Namespace,
    export: CheckedExport,
    hashes: SourceHashes,
    staged_parquet: Path,
    work_files: WorkFiles,
) -> dict[str, Any]:
    """Build the text-location parquet into ``staged_parquet`` from the checked export; return its manifest.

    Every intermediate goes under the claimed work dir through ``work_files``. The manifest's hashes
    of the inputs and code are ``hashes``, taken before the build read anything.
    """
    cui_remap, mrcui_stats = load_cui_remap(args.mrcui)
    LOGGER.info("MRCUI remap: %s retired CUIs", f"{mrcui_stats['remap_retired_cuis']:,}")

    # SemMedDB first: its id-range check can still fail the build, and it is the cheaper stage.
    semmed_counts, chunk_provenance = split_semmeddb_chunks(export.chunks, work_files, args.buckets, cui_remap, hashes)
    kg2 = split_kg2_edges(args.kg2_edges, work_files, args.buckets, args.batch_pairs)
    flipped = derive_orientation(kg2.orientation)
    LOGGER.info("reversed SemMedDB predicates: %s", sorted(p for p, is_flipped in flipped.items() if is_flipped))
    kept, kept_group = kept_groups(work_files, args.buckets)
    LOGGER.info("raw groups per kept record: %s", kept_group)
    parts, match = match_buckets(work_files, args.buckets, kept, orientation_frame(flipped))
    work_files.check_unchanged()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "polars_version": pl.__version__,
        "code": hashes.code,
        "parameters": {"buckets": args.buckets, "hash_seed": HASH_SEED},
        "inputs": {
            "kg2_edges": {"path": str(args.kg2_edges), "sha256": hashes.inputs[args.kg2_edges]},
            "mrcui": {"path": str(args.mrcui), "sha256": hashes.inputs[args.mrcui]},
            "semmeddb_dir": str(args.semmeddb_dir),
            "semmeddb_export": {**export.summary, "predication_id_range_size": PREDICATION_ID_RANGE_SIZE},
            "semmeddb_chunks": chunk_provenance,
            "semmeddb_files": [
                {"path": path.relative_to(args.semmeddb_dir).as_posix(), "sha256": digest}
                for path, digest in sorted(hashes.inputs.items())
                if path.is_relative_to(args.semmeddb_dir)
            ],
        },
        "mrcui": mrcui_stats,
        "kg2": {**dict(sorted(kg2.counts.items())), "records_skipped": dict(sorted(kg2.skipped.items()))},
        "semmeddb": dict(sorted(semmed_counts.items())),
        "orientation": orientation_table(kg2.orientation, flipped),
        "kept_group": kept_group,
        "matching": matching_summary(match.counts, kg2.counts["pairs_with_sentence"]),
        "coverage_by": {
            column: coverage_table(match.pairs_by[column], match.matched_by[column]) for column in BREAKDOWNS
        },
        "output": {"path": str(args.out_parquet), **write_output(parts, staged_parquet, match.counts["output_rows"])},
        "notes": MANIFEST_NOTES,
    }


def positive_int(text: str) -> int:
    """Parse a command-line count that must be at least 1, so argparse stops on anything else.

    >>> positive_int("3")
    3
    >>> positive_int("0")
    Traceback (most recent call last):
    ...
    argparse.ArgumentTypeError: must be at least 1, got 0
    """
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--semmeddb-dir", type=Path, required=True, help="dir with chunk_*.tsv.zst, .rows, .status")
    parser.add_argument("--expected-rows", type=int, required=True, help="COUNT(*) of the exported source table")
    parser.add_argument("--mrcui", type=Path, required=True, help="UMLS MRCUI.RRF")
    parser.add_argument("--kg2-edges", type=Path, required=True, help="kg2.10.3-semmeddb-edges.jsonl(.gz)")
    parser.add_argument("--work-dir", type=Path, required=True, help="intermediate dir; must not exist yet")
    parser.add_argument("--out-parquet", type=Path, required=True, help="text-location parquet to write")
    parser.add_argument("--out-manifest", type=Path, required=True, help="JSON manifest to write")
    parser.add_argument(
        "--buckets", type=positive_int, default=DEFAULT_BUCKETS, help="PMID buckets (default: %(default)s)"
    )
    parser.add_argument(
        "--batch-pairs",
        type=positive_int,
        default=DEFAULT_BATCH_PAIRS,
        help="KG2 pair rows per batch (default: %(default)s)",
    )
    parser.add_argument("--keep-work", action="store_true", help="keep --work-dir for inspection")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point: build the parquet and manifest, then print the summary."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    manifest = run(parse_args(argv))
    print(format_summary(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
