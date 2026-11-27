"""Text-location artifact of the SemMedDB ingest: its contract, its key and its loader.

``semmeddb_text_locations_raw_keyed.parquet`` holds the subject and object spans of the evidence
sentence of each KG2 record, built by ``analysis/build_text_locations.py`` from the raw SemMedDB
export. Each span is ``[start, end]``: 0-based, end-exclusive character offsets into the sentence,
oriented to the record's subject and object, the convention TMKP uses. Rows are keyed by the
record's raw kg2.10.3 ``kg2_id`` and by ``evidence_fingerprint`` of a PMID and the evidence KG2
stored for it. The build and the transform both compute the key with the function in this module,
so they cannot drift apart.
"""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import polars as pl

# Entity locations per (KG2 record, evidence) in that evidence's sentence, downloaded into
# source_data/ (see download.yaml). Keyed by the record's raw kg2.10.3 kg2_id and the
# evidence_fingerprint of one of its PMIDs, unique on that pair, and sorted by it.
TEXT_LOCATION_ARTIFACT_FILENAME = "semmeddb_text_locations_raw_keyed.parquet"
TEXT_LOCATION_KG2_ID_COLUMN = "kg2_id"
TEXT_LOCATION_FINGERPRINT_COLUMN = "evidence_fingerprint"
TEXT_SPAN_COLUMNS = ["subject_start", "subject_end", "object_start", "object_end"]
TEXT_LOCATION_ARTIFACT_SCHEMA: dict[str, pl.DataType] = {
    TEXT_LOCATION_KG2_ID_COLUMN: pl.String(),
    TEXT_LOCATION_FINGERPRINT_COLUMN: pl.UInt64(),
    **{column: pl.Int32() for column in TEXT_SPAN_COLUMNS},
}

# The publications_info fields KG2 copied from the one SemMedDB predication it kept for a PMID.
# Together with the PMID they identify that predication's evidence, so the text-location artifact
# is keyed by a fingerprint of them and a span only attaches to the exact sentence it was found in.
EVIDENCE_FIELDS: tuple[str, ...] = ("sentence", "subject score", "object score")
EVIDENCE_FINGERPRINT_BYTES = 8


def evidence_fingerprint(pmid: str, publication_info: Mapping[str, object]) -> int:
    """Return a 64-bit fingerprint of the evidence KG2 stored for one PMID of one record.

    The PMID and the ``EVIDENCE_FIELDS`` values are encoded as an ASCII-escaped JSON array, so no
    field value can run into the next one and even a lone surrogate encodes. That is hashed with
    BLAKE2b and read as an unsigned big-endian integer (it fits polars ``UInt64``). A missing or
    null field counts as the empty string, and any other value as its ``str()``, so odd input never
    stops the transform. For ASCII text, as in all of kg2.10.3, the ASCII escaping changes nothing.
    ``analysis/build_text_locations.py`` keys the text-location artifact with this same function,
    so the transform and the build always agree.

    >>> sentence = "Aspirin treats diabetes in adults."
    >>> info = {"sentence": sentence, "subject score": "1000", "object score": "790"}
    >>> evidence_fingerprint("PMID:24096582", info)
    1145651608178317754
    >>> rescored = {**info, "object score": "1000"}
    >>> evidence_fingerprint("PMID:24096582", rescored) == evidence_fingerprint("PMID:24096582", info)
    False
    >>> unscored = {"sentence": sentence, "subject score": None}
    >>> evidence_fingerprint("PMID:1", unscored) == evidence_fingerprint("PMID:1", {"sentence": sentence})
    True
    >>> shifted = {"sentence": "x\x1f1000", "subject score": "1000"}
    >>> evidence_fingerprint("PMID:1", shifted) == evidence_fingerprint(
    ...     "PMID:1", {"sentence": "x", "subject score": "1000\x1f1000"}
    ... )
    False
    >>> evidence_fingerprint("PMID:1", {"sentence": "bad " + chr(0xD800)}) > 0
    True
    """
    values = (publication_info.get(field) for field in EVIDENCE_FIELDS)
    fields = [pmid, *("" if value is None else str(value) for value in values)]
    payload = json.dumps(fields, ensure_ascii=True, separators=(",", ":"))
    digest = hashlib.blake2b(payload.encode("utf-8"), digest_size=EVIDENCE_FINGERPRINT_BYTES).digest()
    return int.from_bytes(digest, "big")


class TextSpans(NamedTuple):
    """Subject and object spans of one record's evidence in its sentence: 0-based, end exclusive."""

    subject_start: int
    subject_end: int
    object_start: int
    object_end: int


@dataclass(frozen=True)
class TextLocationIndex:
    """Text locations of every (kg2_id, evidence) in the artifact, kept columnar.

    ``spans`` has one row per (kg2_id, evidence fingerprint) with the fingerprint and its four
    span columns, in the artifact's order, so the rows of one kg2_id are contiguous. ``row_ranges``
    maps a kg2_id to its ``(start, end)`` row range in ``spans``. Only that dict holds python
    objects, one entry per kg2_id; a kg2_id's rows become python objects only when ``spans_for``
    is called for it.
    """

    spans: pl.DataFrame
    row_ranges: dict[str, tuple[int, int]]

    def __len__(self) -> int:
        """Return the number of kg2_ids with at least one text location."""
        return len(self.row_ranges)

    def spans_for(self, kg2_id: str) -> dict[int, TextSpans]:
        """Return the spans of each evidence of the kg2_id, keyed by evidence fingerprint.

        The dict is empty for a kg2_id with no row in the artifact.

        >>> kg2_id = "CHEBI:1---SEMMEDDB:treats---None---None---None---MONDO:1---SEMMEDDB:"
        >>> rows = [(17, 0, 7, 15, 23)]
        >>> columns = [TEXT_LOCATION_FINGERPRINT_COLUMN, *TEXT_SPAN_COLUMNS]
        >>> spans = pl.DataFrame(rows, schema=columns, orient="row")
        >>> index = TextLocationIndex(spans=spans, row_ranges={kg2_id: (0, 1)})
        >>> index.spans_for(kg2_id)
        {17: TextSpans(subject_start=0, subject_end=7, object_start=15, object_end=23)}
        >>> index.spans_for("CHEBI:2---SEMMEDDB:treats---None---None---None---MONDO:1---SEMMEDDB:")
        {}
        """
        row_range = self.row_ranges.get(kg2_id)
        if row_range is None:
            return {}
        start, end = row_range
        rows = self.spans.slice(start, end - start).iter_rows()
        return {fingerprint: TextSpans(*spans) for fingerprint, *spans in rows}


def load_text_locations(artifact_file: Path) -> TextLocationIndex:
    """Read the text-location artifact into a ``TextLocationIndex``.

    The artifact has tens of millions of rows, so no python object is made per row: the rows stay
    polars columns, and python only walks one row per kg2_id to map it to its row range. The build
    writes the rows sorted by (kg2_id, evidence_fingerprint), so the loader checks that order
    instead of sorting, which would about double the memory the load needs. Fails fast with
    ValueError when the artifact breaks its contract: a missing column or another dtype, a null
    value, more than one row for a (kg2_id, evidence_fingerprint), or rows out of order.
    """
    # glob=False everywhere: polars treats a path as a glob pattern by default, so a directory name
    # with brackets (e.g. ``in[1]``) would silently read a sibling (``in1``) instead.
    schema = pl.scan_parquet(artifact_file, glob=False).collect_schema()
    found_schema = {column: schema.get(column) for column in TEXT_LOCATION_ARTIFACT_SCHEMA}
    if found_schema != TEXT_LOCATION_ARTIFACT_SCHEMA:
        raise ValueError(
            f"Text-location artifact {artifact_file} has columns {found_schema}, "
            f"expected {TEXT_LOCATION_ARTIFACT_SCHEMA}."
        )

    frame = pl.read_parquet(artifact_file, columns=list(TEXT_LOCATION_ARTIFACT_SCHEMA), glob=False)
    null_counts = dict(zip(frame.columns, frame.null_count().row(0), strict=True))
    if any(null_counts.values()):
        raise ValueError(f"Text-location artifact {artifact_file} has null values: {null_counts}.")

    # Compare every row with the one before it. The first row has no predecessor, so its
    # comparisons are null, and any() skips nulls.
    kg2_id = pl.col(TEXT_LOCATION_KG2_ID_COLUMN)
    fingerprint = pl.col(TEXT_LOCATION_FINGERPRINT_COLUMN)
    same_kg2_id = kg2_id == kg2_id.shift()
    repeats_previous_row, before_previous_row = frame.select(
        repeats=(same_kg2_id & (fingerprint == fingerprint.shift())).any(),
        out_of_order=((kg2_id < kg2_id.shift()) | (same_kg2_id & (fingerprint < fingerprint.shift()))).any(),
    ).row(0)
    if repeats_previous_row:
        raise ValueError(
            f"Text-location artifact {artifact_file} has more than one row for a "
            f"({TEXT_LOCATION_KG2_ID_COLUMN}, {TEXT_LOCATION_FINGERPRINT_COLUMN})."
        )
    if before_previous_row:
        raise ValueError(
            f"Text-location artifact {artifact_file} is not sorted by "
            f"({TEXT_LOCATION_KG2_ID_COLUMN}, {TEXT_LOCATION_FINGERPRINT_COLUMN})."
        )

    kg2_id_starts = (
        frame.with_row_index("row_start")
        .filter(kg2_id.ne_missing(kg2_id.shift()))
        .with_columns(row_end=pl.col("row_start").shift(-1, fill_value=frame.height))
        .select(TEXT_LOCATION_KG2_ID_COLUMN, "row_start", "row_end")
    )
    row_ranges = {
        record_kg2_id: (row_start, row_end) for record_kg2_id, row_start, row_end in kg2_id_starts.iter_rows()
    }
    return TextLocationIndex(spans=frame.drop(TEXT_LOCATION_KG2_ID_COLUMN), row_ranges=row_ranges)


def spans_in_bounds(spans: TextSpans, sentence_length: int) -> bool:
    """Return True when both spans are non-empty and lie inside a sentence of this length.

    >>> spans_in_bounds(TextSpans(0, 7, 15, 23), 23)
    True
    >>> spans_in_bounds(TextSpans(0, 7, 15, 24), 23)
    False
    >>> spans_in_bounds(TextSpans(7, 7, 15, 23), 23)
    False
    >>> spans_in_bounds(TextSpans(-1, 7, 15, 23), 23)
    False
    """
    return (
        0 <= spans.subject_start < spans.subject_end <= sentence_length
        and 0 <= spans.object_start < spans.object_end <= sentence_length
    )
