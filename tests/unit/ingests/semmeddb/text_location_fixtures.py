"""Fixtures for the SemMedDB text-location build tests.

The fixtures are tiny but real files written to ``tmp_path``: ``mysql -B`` style ``.tsv.zst``
chunks with ``.rows``, ``.status`` and ``header.tsv``, a gzipped KG2 edges JSONL and an MRCUI.RRF.
Each PMID below exercises one rule:

- 1  direct UMLS match (plus an unrelated predication in the same sentence)
- 2  ``part_of`` reversed by KG2 into ``biolink:has_part``: spans swap sides
- 3  gene-only id fields make an NCBIGene -> NCBIGene relation
- 4  retired CUI remapped through MRCUI (``SY``, current ``Y``)
- 5  span text does not equal the sentence slice: the row is not used
- 6  the subject occurs twice with equal scores: the earliest span wins
- 9  MRCUI ``RB`` row, which KG2 did not use: no match
- 10 escaped tab and backslash in the export
- 11 empty sentence in KG2: not a pair
- 12 KG2 canonicalized the record subject: matched through the kg2_id
- 13 SemMedDB row with NULL PREDICATION_AUX fields
- 14 ``SEMMEDDB:xref`` record: no predication makes it
- 15 the subject occurs twice with different scores: the record's scores pick the later one
- 16 one kg2_id, two records with different evidence: two rows (defensive only: in kg2.10.3 the
     records that share a kg2_id are exact copies)
- 18 a CUI-led field with genes makes no gene relation; a gene-only field does
- 19 several genes on both sides: KG2 made no relation
- 20 one kg2_id, two records with the same evidence: one row
- 21 only a ``NEG_`` predication: KG2 dropped negated edges, so it is no evidence and gets no row
- 22 canonicalized record on a reversed predicate; 23 its literal twin that sets the orientation
- 24 the record's scores match no row: no match
- 25 a ``NEG_`` and a positive predication of the same triple, NEG_ first: the positive span wins
- 26, 27 two raw groups make one kg2_id; only the later-span group covers both publications, so it
  alone supplies the spans
- 28 two raw groups both cover the record's publications and pick different spans: dropped
- 29 a publication no raw group has a row for: no covering group, no spans
- 30, 31 two raw groups cover the record, but only one has the record's scores at PMID 31, so it is
  the kept group; its PMID 30 row fails the text check, and the other group does not stand in
- 32 two kept groups, one of whose rows fails the text check: the pair gets no span, although the
  other kept group's row passes
- 30 again: a second record with the same kg2_id and PMID 30 evidence, whose PMID 31 scores only
  the other group has; it gets a PMID 30 span, the first record none, so their shared key is dropped

KG2 kept the raw group whose PMIDs cover all of a record's publications, padding PMIDs included,
so every positive fixture group also gets a row at each padding PMID.

Four more records fail the transform's record filters or have two kg2_ids, so they are skipped.
"""

import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import zstandard

from translator_ingest.ingests.semmeddb.analysis.kg2_semmeddb_converter import (
    KG2_ID_QUALIFIERS,
    KG2_ID_SEPARATOR,
    NO_QUALIFIERS,
)
from translator_ingest.ingests.semmeddb.analysis.output_guards import LOCK_SUFFIX, STAGED_SUFFIX
from translator_ingest.ingests.semmeddb.analysis.semmeddb_export import PREDICATION_ID_RANGE_SIZE, SEMMEDDB_COLUMNS
from translator_ingest.ingests.semmeddb.semmeddb import MAX_SKIPPED_PUBLICATION_COUNT
from translator_ingest.ingests.semmeddb.text_locations import evidence_fingerprint


def mysql_escape(text: str) -> str:
    """Escape a field the way ``mysql -B`` does (backslash first, so it is not doubled twice)."""
    return text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\x00", "\\0")


def rows_payload(rows: list[list[str]]) -> bytes:
    """Return export rows as ``mysql -B`` writes them: tab-separated, every row ending in a newline."""
    return "".join("\t".join(row) + "\n" for row in rows).encode("utf-8")


def write_chunk(directory: Path, name: str, rows: list[list[str]], status: str = "0") -> None:
    """Write ``name.tsv.zst``, ``name.rows`` and ``name.status`` the way the export script does.

    The chunk is one zstd frame with a checksum, as the ``zstd`` command writes it.
    """
    compressed = zstandard.ZstdCompressor(write_checksum=True).compress(rows_payload(rows))
    (directory / f"{name}.tsv.zst").write_bytes(compressed)
    (directory / f"{name}.rows").write_text(f"{len(rows)}\n")
    (directory / f"{name}.status").write_text(f"{status}\n")


KEPT_PUBLICATION_COUNT = MAX_SKIPPED_PUBLICATION_COUNT + 1
# Padding PMIDs start here, far above every fixture PMID, so they never collide with evidence.
PADDING_PMID_BASE = 900_000
# Every positive fixture group has a row at each of these; a record padded beyond them has no
# covering group.
PADDING_PMIDS = [PADDING_PMID_BASE + number for number in range(MAX_SKIPPED_PUBLICATION_COUNT)]
SENT_START = 100
TOP_SCORE = "1000"
TOP_SCORES = (TOP_SCORE, TOP_SCORE)
TREATS = "biolink:treats_or_applied_or_studied_to_treat"
AFFECTS = "biolink:affects"
CAUSES = "biolink:causes"
INTERACTS = "biolink:interacts_with"
COEXISTS = "biolink:coexists_with"
LOCATED_IN = "biolink:located_in"

S1 = "Aspirin treats headache in adults."
S2 = "The nucleus is part of the cell."
S3 = "BRCA2 interacts with RAD51 in cells."
S4 = "Lupus causes fever."
S5 = "Metformin lowers glucose."
S6 = "Insulin and insulin analogs lower glucose."
S9 = "Measles causes rash."
S10 = "Zinc\tdeficiency causes anemia \\ fatigue."
S12 = "Ibuprofen treats pain."
S13 = "Vitamin C prevents scurvy."
S15 = "Tau binds tubulin, and Tau aggregates."
S16_TITLE = "Aspirin treats hypertension."
S16_ABSTRACT = "Low-dose aspirin treats hypertension."
S18 = "p38 MAPK and ERK coexist with JNK."
S19 = "SMAD2 and SMAD3 bind SMAD4 and SMAD7."
S20 = "Aspirin treats migraine."
S21 = "Aspirin does not treat gout."
S22 = "Albumin is abundant in plasma."
S23 = "Glucose is found in blood."
S24 = "Caffeine affects alertness."
S25 = "Aspirin did not help, but aspirin treats stroke."
S26 = "Nicotine and nicotine patches raise blood pressure."
S27 = "Nicotine raises blood pressure."
S28 = "Caffeine and caffeine drinks reduce fatigue."
S29 = "Lisinopril treats hypertension."
S30 = "Ethanol and ethanol vapors damage liver."
S31 = "Ethanol damages liver."
S32 = "Tramadol relieves pain quickly."


# (kg2_id, evidence_fingerprint, subject_start, subject_end, object_start, object_end)
OutputRow = tuple[str, int, int, int, int, int]
Spans = tuple[int, int, int, int]
# (subject, predicate, object, PMID, sentence, subject location, object location) of one study result
StudyLocation = tuple[str, str, str, str, str, list[int] | None, list[int] | None]

MRCUI_LINES = [
    "C0000002|2000AC|SY|||C0007404|Y|",
    "C0000005|2000AC|RB|||C0000006|Y|",
    "C0000007|2000AC|SY|||C0000008|N|",
    "C0000011|2000AC|SY||||Y|",
]

OUTPUT_COLUMNS_CONTRACT: dict[str, pl.DataType] = {
    "kg2_id": pl.String(),
    "evidence_fingerprint": pl.UInt64(),
    "subject_start": pl.Int32(),
    "subject_end": pl.Int32(),
    "object_start": pl.Int32(),
    "object_end": pl.Int32(),
}


def span(sentence: str, text: str, start: int | None = None) -> tuple[int, int]:
    """Return the span of ``text`` at ``start``, or of its first occurrence in ``sentence``."""
    begin = sentence.index(text) if start is None else start
    return begin, begin + len(text)


@dataclass(frozen=True)
class Predication:
    """One SemMedDB predication of the fixture; ``subject``/``obj`` are ``(id field, text)``.

    Spans are the first occurrence of each text unless ``subject_at`` gives the subject's start.
    """

    pmid: int
    predicate: str
    sentence: str
    subject: tuple[str, str]
    obj: tuple[str, str]
    subject_at: int | None = None
    scores: tuple[str, str] = TOP_SCORES

    def row(self, row_id: int) -> list[str]:
        """Return the escaped export row, in ``SEMMEDDB_COLUMNS`` order."""
        subject_start, subject_end = span(self.sentence, self.subject[1], self.subject_at)
        object_start, object_end = span(self.sentence, self.obj[1])
        fields = {
            "PREDICATION_ID": str(row_id),
            "SENTENCE_ID": str(row_id),
            "PMID": str(self.pmid),
            "PREDICATE": self.predicate,
            "SUBJECT_CUI": self.subject[0],
            "SUBJECT_NAME": self.subject[1],
            "SUBJECT_SEMTYPE": "phsu",
            "OBJECT_CUI": self.obj[0],
            "OBJECT_NAME": self.obj[1],
            "OBJECT_SEMTYPE": "dsyn",
            "SENT_TYPE": "ab",
            "SENT_NUMBER": "1",
            "SENT_START_INDEX": str(SENT_START),
            "SENT_END_INDEX": str(SENT_START + len(self.sentence)),
            "SUBJECT_TEXT": self.subject[1],
            "SUBJECT_START_INDEX": str(SENT_START + subject_start),
            "SUBJECT_END_INDEX": str(SENT_START + subject_end),
            "SUBJECT_SCORE": self.scores[0],
            "INDICATOR_TYPE": "VERB",
            "PREDICATE_START_INDEX": "0",
            "PREDICATE_END_INDEX": "0",
            "OBJECT_TEXT": self.obj[1],
            "OBJECT_START_INDEX": str(SENT_START + object_start),
            "OBJECT_END_INDEX": str(SENT_START + object_end),
            "OBJECT_SCORE": self.scores[1],
            "SENTENCE": self.sentence,
        }
        return [mysql_escape(fields[column]) for column in SEMMEDDB_COLUMNS]


def null_aux_row(row_id: int) -> list[str]:
    """Return an export row whose PREDICATION_AUX columns are NULL (no AUX row in MySQL)."""
    row = Predication(13, "PREVENTS", S13, ("C0042878", "Vitamin C"), ("C0036474", "scurvy")).row(row_id)
    aux_columns = SEMMEDDB_COLUMNS[SEMMEDDB_COLUMNS.index("SUBJECT_TEXT") : SEMMEDDB_COLUMNS.index("SENTENCE")]
    return ["NULL" if column in aux_columns else value for column, value in zip(SEMMEDDB_COLUMNS, row, strict=True)]


CHUNK_A: list[Predication] = [
    Predication(1, "TREATS", S1, ("C0004057", "Aspirin"), ("C0018681", "headache")),
    Predication(1, "PROCESS_OF", S1, ("C0018681", "headache"), ("C0001675", "adults")),
    Predication(1, "ISA", S1, ("C0004057", "Aspirin"), ("C0018681", "headache")),
    Predication(2, "PART_OF", S2, ("C0028630", "nucleus"), ("C0007634", "cell")),
    Predication(3, "INTERACTS_WITH", S3, ("675", "BRCA2"), ("5888", "RAD51")),
    Predication(4, "CAUSES", S4, ("C0000002", "Lupus"), ("C0015967", "fever")),
    Predication(5, "INHIBITS", S5, ("C0025598", "Metformin"), ("C0017725", "glucose"), subject_at=1),
    Predication(6, "INHIBITS", S6, ("C0021641", "insulin"), ("C0017725", "glucose")),
    Predication(6, "INHIBITS", S6, ("C0021641", "Insulin"), ("C0017725", "glucose")),
    Predication(9, "CAUSES", S9, ("C0000005", "Measles"), ("C0015230", "rash")),
    Predication(10, "CAUSES", S10, ("C0235950", "Zinc\tdeficiency"), ("C0015672", "fatigue")),
    Predication(12, "TREATS", S12, ("C0020740", "Ibuprofen"), ("C0030193", "pain")),
]
CHUNK_B: list[Predication] = [
    Predication(15, "INTERACTS_WITH", S15, ("C0248337", "Tau"), ("C0041365", "tubulin"), scores=("694", TOP_SCORE)),
    Predication(15, "INTERACTS_WITH", S15, ("C0248337", "Tau"), ("C0041365", "tubulin"), subject_at=23),
    Predication(16, "TREATS", S16_TITLE, ("C0004057", "Aspirin"), ("C0020538", "hypertension")),
    Predication(16, "TREATS", S16_ABSTRACT, ("C0004057", "aspirin"), ("C0020538", "hypertension")),
    Predication(18, "COEXISTS_WITH", S18, ("C0164786|5594", "p38 MAPK"), ("C0164852", "JNK")),
    Predication(18, "COEXISTS_WITH", S18, ("2048|5594", "ERK"), ("C0164852", "JNK")),
    Predication(19, "INTERACTS_WITH", S19, ("4087|4088", "SMAD2"), ("4089|4092", "SMAD4")),
    Predication(20, "TREATS", S20, ("C0004057", "Aspirin"), ("C0149931", "migraine")),
    Predication(20, "TREATS", S20, ("C0004057|9999", "Aspirin"), ("C0149931", "migraine")),
    Predication(21, "NEG_TREATS", S21, ("C0004057", "Aspirin"), ("C0018099", "gout")),
    Predication(22, "LOCATION_OF", S22, ("C0032105", "plasma"), ("C0001924", "Albumin")),
    Predication(23, "LOCATION_OF", S23, ("C0005767", "blood"), ("C0017725", "Glucose")),
    Predication(24, "AFFECTS", S24, ("C0006644", "Caffeine"), ("C0002022", "alertness")),
    Predication(25, "NEG_TREATS", S25, ("C0004057", "Aspirin"), ("C0038454", "stroke")),
    Predication(25, "TREATS", S25, ("C0004057", "aspirin"), ("C0038454", "stroke")),
    Predication(26, "AFFECTS", S26, ("C0028040", "Nicotine"), ("C0005823", "blood pressure")),
    Predication(26, "AFFECTS", S26, ("C0028040|4000", "nicotine"), ("C0005823", "blood pressure")),
    Predication(27, "AFFECTS", S27, ("C0028040|4000", "Nicotine"), ("C0005823", "blood pressure")),
    Predication(28, "AFFECTS", S28, ("C0006644", "Caffeine"), ("C0015672", "fatigue")),
    Predication(28, "AFFECTS", S28, ("C0006644|5000", "caffeine"), ("C0015672", "fatigue")),
    Predication(29, "TREATS", S29, ("C0065374", "Lisinopril"), ("C0020538", "hypertension")),
    Predication(30, "AFFECTS", S30, ("C0001962", "Ethanol"), ("C0023884", "liver"), subject_at=1),
    Predication(30, "AFFECTS", S30, ("C0001962|6000", "ethanol"), ("C0023884", "liver")),
    Predication(31, "AFFECTS", S31, ("C0001962", "Ethanol"), ("C0023884", "liver")),
    Predication(31, "AFFECTS", S31, ("C0001962|6000", "Ethanol"), ("C0023884", "liver"), scores=("500", TOP_SCORE)),
    Predication(32, "TREATS", S32, ("C0040616", "Tramadol"), ("C0030193", "pain")),
    Predication(32, "TREATS", S32, ("C0040616|7000", "Tramadol"), ("C0030193", "pain"), subject_at=1),
]


def padding_predications(predications: list[Predication]) -> list[Predication]:
    """Give every positive raw group of ``predications`` one row at each of ``PADDING_PMIDS``."""
    groups: dict[tuple[str, str, str], Predication] = {}
    for predication in predications:
        if not predication.predicate.startswith("NEG_"):
            groups.setdefault((predication.subject[0], predication.predicate, predication.obj[0]), predication)
    return [
        Predication(pmid, first.predicate, f"{first.subject[1]} and {first.obj[1]}.", first.subject, first.obj)
        for first in groups.values()
        for pmid in PADDING_PMIDS
    ]


def chunk_rows(predications: list[Predication], first_id: int) -> list[list[str]]:
    """Return the export rows of ``predications``, numbered from ``first_id``."""
    return [predication.row(first_id + offset) for offset, predication in enumerate(predications)]


def chunk_first_id(number: int) -> int:
    """Return the first PREDICATION_ID chunk ``number`` may hold."""
    return number * PREDICATION_ID_RANGE_SIZE


CHUNK_A_ROWS = chunk_rows(CHUNK_A, chunk_first_id(1))
PADDING = padding_predications(CHUNK_A + CHUNK_B)
CHUNK_B_ROWS = [
    *chunk_rows(CHUNK_B, chunk_first_id(2)),
    null_aux_row(chunk_first_id(2) + len(CHUNK_B)),
    *chunk_rows(PADDING, chunk_first_id(2) + len(CHUNK_B) + 1),
]
TOTAL_ROWS = len(CHUNK_A_ROWS) + len(CHUNK_B_ROWS)


def kg2_id_of(subject: str, predicate: str, obj: str) -> str:
    """Return a kg2_id the way KG2.10.3 writes it, with the predicate's qualifier parts."""
    qualifiers = KG2_ID_QUALIFIERS.get(predicate, NO_QUALIFIERS)
    return KG2_ID_SEPARATOR.join([subject, f"SEMMEDDB:{predicate}", *qualifiers, obj, "SEMMEDDB:"])


def evidence(sentence: str, scores: tuple[str, str] = TOP_SCORES) -> dict[str, str]:
    """Return one ``publications_info`` entry."""
    return {"sentence": sentence, "subject score": scores[0], "object score": scores[1], "publication date": "2020"}


def kg2_record(
    edge: tuple[str, str, str],
    kg2_id: tuple[str, str, str],
    publications_info: dict[str, dict[str, str]],
    *,
    publication_count: int = KEPT_PUBLICATION_COUNT,
    domain_range_exclusion: bool = False,
) -> dict[str, Any]:
    """Build one KG2 edge record; ``edge`` is the record triple, ``kg2_id`` the SemMedDB one.

    ``publications`` lists the evidence PMIDs, padded with PMIDs that have no evidence up to
    ``publication_count``, so by default the record passes the transform's publication filter.
    """
    padding = [f"PMID:{PADDING_PMID_BASE + number}" for number in range(publication_count - len(publications_info))]
    return {
        "kg2_ids": [kg2_id_of(*kg2_id)],
        "subject": edge[0],
        "predicate": edge[1],
        "object": edge[2],
        "domain_range_exclusion": domain_range_exclusion,
        "publications": [*publications_info, *padding],
        "publications_info": publications_info,
    }


def literal(edge: tuple[str, str, str], semmed_predicate: str, pmid: str, sentence: str) -> dict[str, Any]:
    """Build a record whose kg2_id ends literally equal its subject and object."""
    subject, _, obj = edge
    return kg2_record(edge, (subject, semmed_predicate, obj), {pmid: evidence(sentence)})


R16_KG2_ID = ("UMLS:C0004057", "treats", "UMLS:C0020538")
R20_KG2_ID = ("UMLS:C0004057", "treats", "UMLS:C0149931")
R22_KG2_ID = ("UMLS:C0032105", "location_of", "UMLS:C0001924")
R26_KG2_ID = ("UMLS:C0028040", "affects", "UMLS:C0005823")
R28_KG2_ID = ("UMLS:C0006644", "affects", "UMLS:C0015672")
R29_KG2_ID = ("UMLS:C0065374", "treats", "UMLS:C0020538")
R30_KG2_ID = ("UMLS:C0001962", "affects", "UMLS:C0023884")
R32_KG2_ID = ("UMLS:C0040616", "treats", "UMLS:C0030193")
# The scores only the C0001962|6000 group has at PMID 31.
R30_SECOND_RECORD_SCORES = ("500", TOP_SCORE)

KG2_RECORDS: list[dict[str, Any]] = [
    literal(("UMLS:C0004057", TREATS, "UMLS:C0018681"), "treats", "PMID:1", S1),
    kg2_record(
        ("UMLS:C0007634", "biolink:has_part", "UMLS:C0028630"),
        ("UMLS:C0028630", "part_of", "UMLS:C0007634"),
        {"PMID:2": evidence(S2)},
    ),
    literal(("NCBIGene:675", INTERACTS, "NCBIGene:5888"), "interacts_with", "PMID:3", S3),
    literal(("UMLS:C0007404", CAUSES, "UMLS:C0015967"), "causes", "PMID:4", S4),
    literal(("UMLS:C0025598", AFFECTS, "UMLS:C0017725"), "inhibits", "PMID:5", S5),
    literal(("UMLS:C0021641", AFFECTS, "UMLS:C0017725"), "inhibits", "PMID:6", S6),
    literal(("UMLS:C0000006", CAUSES, "UMLS:C0015230"), "causes", "PMID:9", S9),
    literal(("UMLS:C0235950", CAUSES, "UMLS:C0015672"), "causes", "PMID:10", S10),
    literal(("UMLS:C0011849", CAUSES, "UMLS:C0015672"), "causes", "PMID:11", ""),
    kg2_record(
        ("CHEBI:5855", TREATS, "UMLS:C0030193"),
        ("UMLS:C0020740", "treats", "UMLS:C0030193"),
        {"PMID:12": evidence(S12)},
    ),
    literal(("UMLS:C0042878", "biolink:related_to", "NCBIGene:9999"), "xref", "PMID:14", S13),
    literal(("UMLS:C0248337", INTERACTS, "UMLS:C0041365"), "interacts_with", "PMID:15", S15),
    # Defensive: in kg2.10.3 the records that share a kg2_id are exact copies.
    kg2_record(("UMLS:C0004057", TREATS, "UMLS:C0020538"), R16_KG2_ID, {"PMID:16": evidence(S16_TITLE)}),
    kg2_record(("UMLS:C0004057", TREATS, "UMLS:C0020538"), R16_KG2_ID, {"PMID:16": evidence(S16_ABSTRACT)}),
    literal(("NCBIGene:5594", COEXISTS, "UMLS:C0164852"), "coexists_with", "PMID:18", S18),
    literal(("UMLS:C0164786", COEXISTS, "UMLS:C0164852"), "coexists_with", "PMID:18", S18),
    literal(("NCBIGene:4087", INTERACTS, "NCBIGene:4089"), "interacts_with", "PMID:19", S19),
    kg2_record(("UMLS:C0004057", TREATS, "UMLS:C0149931"), R20_KG2_ID, {"PMID:20": evidence(S20)}),
    kg2_record(("UMLS:C0004057", TREATS, "UMLS:C0149931"), R20_KG2_ID, {"PMID:20": evidence(S20)}),
    literal(("UMLS:C0004057", TREATS, "UMLS:C0018099"), "treats", "PMID:21", S21),
    kg2_record(("CHEBI:9999", LOCATED_IN, "UBERON:0001969"), R22_KG2_ID, {"PMID:22": evidence(S22)}),
    kg2_record(
        ("UMLS:C0017725", LOCATED_IN, "UMLS:C0005767"),
        ("UMLS:C0005767", "location_of", "UMLS:C0017725"),
        {"PMID:23": evidence(S23)},
    ),
    kg2_record(
        ("UMLS:C0006644", AFFECTS, "UMLS:C0002022"),
        ("UMLS:C0006644", "affects", "UMLS:C0002022"),
        {"PMID:24": evidence(S24, (TOP_SCORE, "888"))},
    ),
    literal(("UMLS:C0004057", TREATS, "UMLS:C0038454"), "treats", "PMID:25", S25),
    kg2_record(
        ("UMLS:C0028040", AFFECTS, "UMLS:C0005823"),
        R26_KG2_ID,
        {"PMID:26": evidence(S26), "PMID:27": evidence(S27)},
    ),
    literal(("UMLS:C0006644", AFFECTS, "UMLS:C0015672"), "affects", "PMID:28", S28),
    kg2_record(
        ("UMLS:C0065374", TREATS, "UMLS:C0020538"),
        R29_KG2_ID,
        {"PMID:29": evidence(S29)},
        publication_count=KEPT_PUBLICATION_COUNT + 1,
    ),
    kg2_record(
        ("UMLS:C0001962", AFFECTS, "UMLS:C0023884"),
        R30_KG2_ID,
        {"PMID:30": evidence(S30), "PMID:31": evidence(S31)},
    ),
    literal(("UMLS:C0040616", TREATS, "UMLS:C0030193"), "treats", "PMID:32", S32),
    kg2_record(
        ("UMLS:C0001962", AFFECTS, "UMLS:C0023884"),
        R30_KG2_ID,
        {"PMID:30": evidence(S30), "PMID:31": evidence(S31, R30_SECOND_RECORD_SCORES)},
    ),
]

# Each would add an output row if it were not skipped.
SKIPPED_RECORDS: list[dict[str, Any]] = [
    kg2_record(
        ("UMLS:C9000001", TREATS, "UMLS:C0018681"),
        ("UMLS:C0004057", "treats", "UMLS:C0018681"),
        {"PMID:1": evidence(S1)},
        domain_range_exclusion=True,
    ),
    kg2_record(
        ("NCBIGene:9000675", INTERACTS, "NCBIGene:5888"),
        ("NCBIGene:675", "interacts_with", "NCBIGene:5888"),
        {"PMID:3": evidence(S3)},
        publication_count=MAX_SKIPPED_PUBLICATION_COUNT,
    ),
    literal(("UMLS:C0004057", "biolink:related_to", "UMLS:C0018681"), "isa", "PMID:1", S1),
    {
        **literal(("UMLS:C0004057", TREATS, "UMLS:C0018681"), "treats", "PMID:1", S1),
        "kg2_ids": [kg2_id_of("UMLS:C0004057", "treats", "UMLS:C0018681")] * 2,
    },
]


def expected(
    kg2_id: tuple[str, str, str], pmid: str, sentence: str, subject: tuple[str, int | None], obj: str
) -> OutputRow:
    """Return one expected output row; ``subject`` is ``(text, start or None for the first)``."""
    key = (kg2_id_of(*kg2_id), evidence_fingerprint(pmid, evidence(sentence)))
    return (*key, *span(sentence, *subject), *span(sentence, obj))


EXPECTED_ROWS: set[OutputRow] = {
    expected(("UMLS:C0004057", "treats", "UMLS:C0018681"), "PMID:1", S1, ("Aspirin", None), "headache"),
    expected(("UMLS:C0028630", "part_of", "UMLS:C0007634"), "PMID:2", S2, ("cell", None), "nucleus"),
    expected(("NCBIGene:675", "interacts_with", "NCBIGene:5888"), "PMID:3", S3, ("BRCA2", None), "RAD51"),
    expected(("UMLS:C0007404", "causes", "UMLS:C0015967"), "PMID:4", S4, ("Lupus", None), "fever"),
    expected(("UMLS:C0021641", "inhibits", "UMLS:C0017725"), "PMID:6", S6, ("Insulin", None), "glucose"),
    expected(("UMLS:C0235950", "causes", "UMLS:C0015672"), "PMID:10", S10, ("Zinc\tdeficiency", None), "fatigue"),
    expected(("UMLS:C0020740", "treats", "UMLS:C0030193"), "PMID:12", S12, ("Ibuprofen", None), "pain"),
    expected(("UMLS:C0248337", "interacts_with", "UMLS:C0041365"), "PMID:15", S15, ("Tau", 23), "tubulin"),
    expected(R16_KG2_ID, "PMID:16", S16_TITLE, ("Aspirin", None), "hypertension"),
    expected(R16_KG2_ID, "PMID:16", S16_ABSTRACT, ("aspirin", None), "hypertension"),
    expected(("NCBIGene:5594", "coexists_with", "UMLS:C0164852"), "PMID:18", S18, ("ERK", None), "JNK"),
    expected(("UMLS:C0164786", "coexists_with", "UMLS:C0164852"), "PMID:18", S18, ("p38 MAPK", None), "JNK"),
    expected(R20_KG2_ID, "PMID:20", S20, ("Aspirin", None), "migraine"),
    expected(R22_KG2_ID, "PMID:22", S22, ("Albumin", None), "plasma"),
    expected(("UMLS:C0005767", "location_of", "UMLS:C0017725"), "PMID:23", S23, ("Glucose", None), "blood"),
    expected(("UMLS:C0004057", "treats", "UMLS:C0038454"), "PMID:25", S25, ("aspirin", None), "stroke"),
    expected(R26_KG2_ID, "PMID:26", S26, ("nicotine", None), "blood pressure"),
    expected(R26_KG2_ID, "PMID:27", S27, ("Nicotine", None), "blood pressure"),
    expected(R30_KG2_ID, "PMID:31", S31, ("Ethanol", None), "liver"),
    (
        kg2_id_of(*R30_KG2_ID),
        evidence_fingerprint("PMID:31", evidence(S31, R30_SECOND_RECORD_SCORES)),
        *span(S31, "Ethanol"),
        *span(S31, "liver"),
    ),
}


def write_kg2_edges(path: Path, records: list[dict[str, Any]]) -> Path:
    """Write KG2 edge records as gzipped JSONL."""
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.writelines(json.dumps(record) + "\n" for record in records)
    return path


def write_inputs(tmp_path: Path, chunks: dict[str, list[list[str]]], records: list[dict[str, Any]]) -> dict[str, Path]:
    """Write all inputs under ``tmp_path`` (created if needed) and return the paths ``main`` needs."""
    semmeddb_dir = tmp_path / "semmeddb"
    semmeddb_dir.mkdir(parents=True)
    (semmeddb_dir / "header.tsv").write_text("\t".join(SEMMEDDB_COLUMNS) + "\n")
    for name, rows in chunks.items():
        write_chunk(semmeddb_dir, name, rows)
    mrcui = tmp_path / "MRCUI.RRF"
    mrcui.write_text("".join(line + "\n" for line in MRCUI_LINES))
    return {
        "semmeddb_dir": semmeddb_dir,
        "mrcui": mrcui,
        "kg2_edges": write_kg2_edges(tmp_path / "edges.jsonl.gz", records),
        "work_dir": tmp_path / "work",
        "out_parquet": tmp_path / "out" / "text_locations.parquet",
        "out_manifest": tmp_path / "out" / "text_locations.manifest.json",
    }


def symlink(link: Path, target: Path) -> Path:
    """Create the symlink ``link`` to ``target``, which need not exist, and return ``link``."""
    link.symlink_to(target)
    return link


def hardlink(link: Path, target: Path) -> Path:
    """Create ``link`` as a hard link to the existing file ``target`` and return ``link``."""
    link.hardlink_to(target)
    return link


def argv_for(paths: dict[str, Path], expected_rows: int, *extra: str) -> list[str]:
    """Turn the input paths into a command line, with two buckets so both code paths split."""
    argv = [item for name, path in paths.items() for item in (f"--{name.replace('_', '-')}", str(path))]
    return [*argv, "--expected-rows", str(expected_rows), "--buckets", "2", *extra]


def output_rows(inputs: dict[str, Path]) -> list[OutputRow]:
    """Return the built artifact's rows in file order."""
    return pl.read_parquet(inputs["out_parquet"]).rows()


def input_bytes(inputs: dict[str, Path]) -> dict[Path, bytes]:
    """Return the bytes of every input file: KG2 edges, MRCUI and each file of the export dir."""
    files = [inputs["kg2_edges"], inputs["mrcui"], *sorted(inputs["semmeddb_dir"].iterdir())]
    return {path: path.read_bytes() for path in files}


def leftover_build_files(inputs: dict[str, Path]) -> list[Path]:
    """Return every output lock and staged output left anywhere under the test's directory."""
    root = inputs["semmeddb_dir"].parent
    return sorted([*root.rglob(f"*{LOCK_SUFFIX}"), *root.rglob(f"*{STAGED_SUFFIX}")])


def full_inputs(directory: Path) -> dict[str, Path]:
    """Write the full fixture set (two chunks with rows, one empty chunk, all KG2 records) into ``directory``."""
    chunks = {"chunk_001": CHUNK_A_ROWS, "chunk_002": CHUNK_B_ROWS, "chunk_003": []}
    return write_inputs(directory, chunks, [*KG2_RECORDS, *SKIPPED_RECORDS])
