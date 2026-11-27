"""Turn KG2 SemMedDB edge records into the text-location build's record, publication and pair rows.

A record is used only when the transform could give it locations: ``semmeddb.record_filter_reason``
passes, it has a subject, object and predicate, and it has exactly one kg2_id that parses
(``kg2_semmeddb_converter.parse_kg2_id``). Only used records vote on orientation. Skipped records
are counted per reason.

It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from translator_ingest.ingests.semmeddb.analysis.kg2_orientation import kg2_orientation
from translator_ingest.ingests.semmeddb.analysis.kg2_semmeddb_converter import Kg2Ends, parse_kg2_id
from translator_ingest.ingests.semmeddb.analysis.rekey_pmid_verdicts import open_text
from translator_ingest.ingests.semmeddb.semmeddb import PREDICATE_REMAP, record_filter_reason, record_kg2_ids
from translator_ingest.ingests.semmeddb.text_locations import (
    EVIDENCE_FIELDS,
    TEXT_LOCATION_FINGERPRINT_COLUMN,
    TEXT_LOCATION_KG2_ID_COLUMN,
    evidence_fingerprint,
)

# The publications_info keys of the evidence, in the fingerprint's order.
SENTENCE_KEY, SUBJECT_SCORE_KEY, OBJECT_SCORE_KEY = EVIDENCE_FIELDS
KG2_ID_COUNT_SKIP_REASON = "kg2_id_count_not_one_skipped"
# The transform skips a record missing any of these as an invalid edge.
EDGE_KEYS: tuple[str, ...] = ("subject", "predicate", "object")
INVALID_EDGE_SKIP_REASON = "invalid_edge_skipped"
# The KG2 counts of the manifest, reported even when no record is kept.
KG2_COUNTS: tuple[str, ...] = (
    "records",
    "records_kept",
    "kg2_ids_malformed",
    "publications",
    "pairs",
    "pairs_with_sentence",
    "pairs_without_sentence",
)
MALFORMED_KG2_ID_SKIP_REASON = "malformed_kg2_id_skipped"

RELATION_COLUMNS: list[str] = ["semmed_predicate", "kg2_subject", "kg2_object"]

# One KG2 (record, PMID) row as streamed from the JSONL, before hashing.
PAIR_SCHEMA: dict[str, pl.DataType] = {
    "record_idx": pl.UInt32(),
    TEXT_LOCATION_KG2_ID_COLUMN: pl.String(),
    "predicate": pl.String(),
    "PMID": pl.String(),
    "sentence": pl.String(),
    "subject_score": pl.String(),
    "object_score": pl.String(),
    TEXT_LOCATION_FINGERPRINT_COLUMN: pl.UInt64(),
    "semmed_predicate": pl.String(),
    "kg2_subject": pl.String(),
    "kg2_object": pl.String(),
}
# One kept record: its kg2_id relation and how many distinct publications it has.
RECORD_SCHEMA: dict[str, pl.DataType] = {
    "record_idx": pl.UInt32(),
    **dict.fromkeys(RELATION_COLUMNS, pl.String()),
    "publication_count": pl.UInt32(),
    "pair_count": pl.UInt32(),
}
# One publication of a kept record, as streamed and as stored in the bucketed work files.
PUBLICATION_SCHEMA: dict[str, pl.DataType] = {"record_idx": pl.UInt32(), "PMID": pl.String()}


@dataclass
class Kg2Tally:
    """Counts gathered while streaming the KG2 edges file; ``KG2_COUNTS`` are always reported, if only as 0."""

    counts: Counter[str] = field(default_factory=lambda: Counter(dict.fromkeys(KG2_COUNTS, 0)))
    orientation: Counter[tuple[str, str]] = field(default_factory=Counter)
    skipped: Counter[str] = field(default_factory=Counter)


def iter_kg2_records(edges_file: Path) -> Iterator[dict[str, Any]]:
    """Stream the KG2 edges JSONL (``.gz`` accepted), one record per non-blank line."""
    with open_text(edges_file) as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


# A kept record's RECORD_SCHEMA row, PUBLICATION_SCHEMA rows and PAIR_SCHEMA rows.
KeptRecord = tuple[tuple[Any, ...], list[tuple[Any, ...]], list[tuple[Any, ...]]]


def vote_orientation(ends: Kg2Ends, record: dict[str, Any], tally: Kg2Tally) -> None:
    """Tally how a used record's kg2_id ends relate to its subject and object, for its SemMedDB predicate."""
    kg2_subject, semmed_predicate, kg2_object = ends
    orientation = kg2_orientation(kg2_subject, kg2_object, record["subject"], record["object"])
    tally.orientation[(semmed_predicate, orientation)] += 1


def kept_ends(record: dict[str, Any], tally: Kg2Tally) -> Kg2Ends | str:
    """Return the ends of the record's one kg2_id, or the reason the build skips the record.

    The checks run in the transform's order: its filters (``record_filter_reason``), then a missing
    or empty subject, object or predicate (its invalid edges), then not exactly one kg2_id (a missing
    or null ``kg2_ids`` has none; such a record gets no locations), then a kg2_id that does not parse.
    ``kg2_ids`` is read as the transform reads it (``semmeddb.record_kg2_ids``: anything but a list
    is none) and parsed only for a record that passed the first two checks; its kg2_ids that do not
    parse are counted as ``kg2_ids_malformed``.
    """
    reason = record_filter_reason(record) or (
        None if all(record.get(key) for key in EDGE_KEYS) else INVALID_EDGE_SKIP_REASON
    )
    if reason is not None:
        return reason
    kg2_ids = [parse_kg2_id(kg2_id) for kg2_id in record_kg2_ids(record)]
    tally.counts["kg2_ids_malformed"] += kg2_ids.count(None)
    if len(kg2_ids) != 1:
        return KG2_ID_COUNT_SKIP_REASON
    (ends,) = kg2_ids
    return MALFORMED_KG2_ID_SKIP_REASON if ends is None else ends


def kept_record(record_idx: int, record: dict[str, Any], tally: Kg2Tally) -> KeptRecord | None:
    """Return the rows of a record the transform keeps, or None for one it does not.

    A skipped record is counted under its ``kept_ends`` reason and does not vote on orientation,
    so a record the transform rejects cannot sway it. Pairs are the PMIDs with a sentence;
    publications are all of the record's distinct PMIDs, which its kept raw group must cover.
    """
    ends = kept_ends(record, tally)
    if isinstance(ends, str):
        tally.skipped[ends] += 1
        return None
    vote_orientation(ends, record, tally)
    (kg2_id,) = record_kg2_ids(record)
    kg2_subject, semmed_predicate, kg2_object = ends
    relation = (semmed_predicate, kg2_subject, kg2_object)
    predicate: str = PREDICATE_REMAP.get(record["predicate"], record["predicate"])
    publications = sorted(set(record.get("publications") or []))
    publications_info: dict[str, dict[str, str]] = record.get("publications_info") or {}
    pairs = [
        (
            record_idx,
            kg2_id,
            predicate,
            pmid,
            info[SENTENCE_KEY],
            info.get(SUBJECT_SCORE_KEY),
            info.get(OBJECT_SCORE_KEY),
            evidence_fingerprint(pmid, info),
            *relation,
        )
        for pmid, info in publications_info.items()
        if info.get(SENTENCE_KEY)
    ]
    tally.counts.update(
        {
            "records_kept": 1,
            "publications": len(publications),
            "pairs": len(publications_info),
            "pairs_with_sentence": len(pairs),
            "pairs_without_sentence": len(publications_info) - len(pairs),
        }
    )
    return (
        (record_idx, *relation, len(publications), len(pairs)),
        [(record_idx, pmid) for pmid in publications],
        pairs,
    )


def empty_columns(schema: dict[str, pl.DataType]) -> dict[str, list[Any]]:
    """Return one empty list per column of ``schema``."""
    return {name: [] for name in schema}


@dataclass
class Kg2Batch:
    """Column lists of one batch of the three KG2 tables: kept records, publications and pairs."""

    records: dict[str, list[Any]] = field(default_factory=lambda: empty_columns(RECORD_SCHEMA))
    publications: dict[str, list[Any]] = field(default_factory=lambda: empty_columns(PUBLICATION_SCHEMA))
    pairs: dict[str, list[Any]] = field(default_factory=lambda: empty_columns(PAIR_SCHEMA))

    def add(self, kept: KeptRecord) -> None:
        """Append one kept record's rows."""
        record_row, publication_rows, pair_rows = kept
        tables = ((self.records, [record_row]), (self.publications, publication_rows), (self.pairs, pair_rows))
        for columns, rows in tables:
            for row in rows:
                for values, value in zip(columns.values(), row, strict=True):
                    values.append(value)

    def frames(self) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Return the records, publications and pairs frames."""
        return (
            pl.DataFrame(self.records, schema=RECORD_SCHEMA),
            pl.DataFrame(self.publications, schema=PUBLICATION_SCHEMA),
            pl.DataFrame(self.pairs, schema=PAIR_SCHEMA),
        )


def iter_kg2_batches(edges_file: Path, batch_pairs: int, tally: Kg2Tally) -> Iterator[Kg2Batch]:
    """Stream the KG2 edges into batches of about ``batch_pairs`` pairs; no record spans two."""
    batch = Kg2Batch()
    for record_idx, record in enumerate(iter_kg2_records(edges_file)):
        tally.counts["records"] += 1
        kept = kept_record(record_idx, record, tally)
        if kept is not None:
            batch.add(kept)
        if len(batch.pairs["record_idx"]) >= batch_pairs:
            yield batch
            batch = Kg2Batch()
    if batch.records["record_idx"]:
        yield batch
