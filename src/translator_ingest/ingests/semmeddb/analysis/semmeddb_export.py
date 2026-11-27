r"""Read and check the SemMedDB export that ``export_semmeddb_predications.sh`` wrote.

The export is ``mysql -B -N`` output split into ``chunk_NNN.tsv.zst`` files: no header, no quoting,
``\t \n \\ \0`` escaped inside fields, NULL written as ``NULL``. Each chunk has a ``.rows`` line
count and a ``.status`` exit status, and ``header.tsv`` names the columns. ``check_export`` proves
the export complete before anything is read: canonical chunk names (exactly three digits, as the
export's ``printf "%03d"`` writes them, so two files can never claim one chunk number), every status
0 with its chunk and ``.rows``, no ``*.part`` file left, and the ``.rows`` summing to the source
table's COUNT(*). ``check_predication_ids`` then proves each chunk holds only its own unique ids,
and ``read_semmeddb_chunk`` that each chunk is complete.

It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

import fnmatch
import re
from pathlib import Path
from typing import Any, NamedTuple

import polars as pl
import zstandard

# Column order of the mysql -B -N export; header.tsv next to the chunks must hold the same names.
SEMMEDDB_COLUMNS: list[str] = [
    "PREDICATION_ID",
    "SENTENCE_ID",
    "PMID",
    "PREDICATE",
    "SUBJECT_CUI",
    "SUBJECT_NAME",
    "SUBJECT_SEMTYPE",
    "OBJECT_CUI",
    "OBJECT_NAME",
    "OBJECT_SEMTYPE",
    "SENT_TYPE",
    "SENT_NUMBER",
    "SENT_START_INDEX",
    "SENT_END_INDEX",
    "SUBJECT_TEXT",
    "SUBJECT_START_INDEX",
    "SUBJECT_END_INDEX",
    "SUBJECT_SCORE",
    "INDICATOR_TYPE",
    "PREDICATE_START_INDEX",
    "PREDICATE_END_INDEX",
    "OBJECT_TEXT",
    "OBJECT_START_INDEX",
    "OBJECT_END_INDEX",
    "OBJECT_SCORE",
    "SENTENCE",
]
HEADER_FILENAME = "header.tsv"
CHUNK_GLOB = "chunk_*.tsv.zst"
CHUNK_PREFIX = "chunk_"
# The export names chunks with printf "%03d", so a canonical stem has exactly three ASCII digits
# (\d would also accept other scripts' digits, which int() reads as the same number).
CHUNK_STEM_PATTERN = re.compile(r"chunk_[0-9]{3}")
# chunk_NNN holds PREDICATION_IDs NNN * size to (NNN + 1) * size - 1: export_semmeddb_predications.sh
# cuts the table with RANGE_SIZE=5000000, and a test keeps the two values equal.
PREDICATION_ID_RANGE_SIZE = 5_000_000
CHUNK_SUFFIX = ".tsv.zst"
ROW_COUNT_SUFFIX = ".rows"
STATUS_GLOB = "chunk_*.status"
ROW_COUNT_GLOB = "chunk_*.rows"
STATUS_SUFFIX = ".status"
PARTIAL_FILE_GLOB = "*.part"
SUCCESS_STATUS = "0"
TSV_SEPARATOR = "\t"
# mysql -B ends every row, the last one included, with this byte, and escapes tabs inside fields, so
# every raw tab separates two fields and a complete row has exactly this many.
ROW_TERMINATOR = b"\n"
FIELD_SEPARATOR = b"\t"
SEPARATORS_PER_ROW = len(SEMMEDDB_COLUMNS) - 1
# Compressed bytes fed to the zstd decompressor per step.
DECOMPRESS_READ_SIZE = 16 * 1024 * 1024

# mysql -B writes SQL NULL as this literal and escapes exactly these four sequences in fields.
MYSQL_NULL = "NULL"
MYSQL_ESCAPES: dict[str, str] = {"\\\\": "\\", "\\t": "\t", "\\n": "\n", "\\0": "\x00"}
ESCAPE_CHARACTER = "\\"


def unescape_mysql(column: str) -> pl.Expr:
    r"""Decode the ``mysql -B`` escapes of a String column, scanning left to right.

    Chained ``str.replace`` calls would be wrong: ``\\t`` is an escaped backslash followed by a
    plain ``t``, not a backslash followed by a tab. ``replace_many`` with ``leftmost=True``
    consumes one escape pair at a time from the left, which is how mysql wrote them.

    >>> frame = pl.DataFrame({"text": [r"a\tb", r"c\\td", r"e\\\tf", r"g\0h\ni", None]})
    >>> frame.select(unescape_mysql("text"))["text"].to_list()
    ['a\tb', 'c\\td', 'e\\\tf', 'g\x00h\ni', None]
    """
    return pl.col(column).str.replace_many(list(MYSQL_ESCAPES), list(MYSQL_ESCAPES.values()), leftmost=True)


def chunk_row_count(chunk: Path) -> int:
    """Return the line count the export recorded for a chunk in its ``.rows`` file."""
    rows_file = chunk.with_name(chunk.name.removesuffix(CHUNK_SUFFIX) + ROW_COUNT_SUFFIX)
    return int(rows_file.read_text(encoding="utf-8").strip())


class CheckedExport(NamedTuple):
    """The chunks of an export ``check_export`` proved complete, and the summary the manifest records."""

    chunks: list[Path]
    summary: dict[str, Any]


def export_listing(semmeddb_dir: Path) -> list[Path]:
    """Return every file under the export dir, sorted.

    The build lists the dir once, with this, then hashes exactly these files and gives the same list
    to ``check_export``, so no file can join between the hashing and the checks.
    """
    return sorted(path for path in semmeddb_dir.rglob("*") if path.is_file())


def check_listing_unchanged(semmeddb_dir: Path, listing: list[Path]) -> None:
    """Raise ValueError if a file was added to or removed from the export dir since ``listing`` was taken."""
    now, before = set(export_listing(semmeddb_dir)), set(listing)
    added, removed = sorted(now - before), sorted(before - now)
    if added or removed:
        raise ValueError(
            f"files of {semmeddb_dir} were added or removed while the build ran: added {added}, removed {removed}"
        )


def listed(semmeddb_dir: Path, listing: list[Path], pattern: str) -> list[Path]:
    """Return the files of ``listing`` directly in ``semmeddb_dir`` whose name matches ``pattern``, sorted."""
    return sorted(path for path in listing if path.parent == semmeddb_dir and fnmatch.fnmatchcase(path.name, pattern))


def header_columns(headers: list[Path]) -> list[str] | None:
    """Return the column names of the export's ``header.tsv`` (a list of 0 or 1 path), None without one."""
    return [
        column for header in headers for column in header.read_text(encoding="utf-8").rstrip("\n").split(TSV_SEPARATOR)
    ] or None


def check_export(semmeddb_dir: Path, expected_rows: int, listing: list[Path]) -> CheckedExport:
    """Return the export chunks after proving the export complete, else raise with every problem.

    Only the files of ``listing`` (``export_listing``) directly in ``semmeddb_dir`` count; the dir is
    not listed again. The header must name ``SEMMEDDB_COLUMNS``, every chunk file must have a
    canonical ``chunk_NNN`` name, every ``chunk_*.status`` must hold 0 and have its ``.tsv.zst`` and
    ``.rows`` (and every chunk a status), no ``*.part`` file may be left, and the ``.rows`` files must
    sum to ``expected_rows``, the source table's COUNT(*).
    """
    header = header_columns(listed(semmeddb_dir, listing, HEADER_FILENAME))
    chunks = listed(semmeddb_dir, listing, CHUNK_GLOB)
    stems = {chunk.name.removesuffix(CHUNK_SUFFIX) for chunk in chunks}
    statuses = {path.name.removesuffix(STATUS_SUFFIX): path for path in listed(semmeddb_dir, listing, STATUS_GLOB)}
    failed = sorted(
        stem for stem, path in statuses.items() if path.read_text(encoding="utf-8").strip() != SUCCESS_STATUS
    )
    row_stems = {path.name.removesuffix(ROW_COUNT_SUFFIX) for path in listed(semmeddb_dir, listing, ROW_COUNT_GLOB)}
    without_rows = sorted(stems - row_stems)
    partial_files = sorted(path.name for path in listed(semmeddb_dir, listing, PARTIAL_FILE_GLOB))
    non_canonical = sorted(stem for stem in stems | set(statuses) | row_stems if not CHUNK_STEM_PATTERN.fullmatch(stem))
    rows = sum(chunk_row_count(chunk) for chunk in chunks if chunk.name.removesuffix(CHUNK_SUFFIX) not in without_rows)
    checks = [
        (header is None, f"no {HEADER_FILENAME}"),
        (
            header is not None and header != SEMMEDDB_COLUMNS,
            f"{HEADER_FILENAME} columns {header} differ from {SEMMEDDB_COLUMNS}",
        ),
        (bool(non_canonical), f"chunk names not of the form chunk_NNN: {non_canonical}"),
        (not statuses, f"no {STATUS_GLOB} file"),
        (bool(failed), f"status not {SUCCESS_STATUS}: {failed}"),
        (stems != set(statuses), f"status without chunk or chunk without status: {sorted(stems ^ set(statuses))}"),
        (bool(without_rows), f"chunk without {ROW_COUNT_SUFFIX}: {without_rows}"),
        (bool(partial_files), f"partial files left: {partial_files}"),
        (rows != expected_rows, f"{ROW_COUNT_SUFFIX} files sum to {rows:,}, expected {expected_rows:,}"),
    ]
    problems = [message for failing, message in checks if failing]
    if problems:
        raise ValueError(f"SemMedDB export in {semmeddb_dir} is incomplete: {'; '.join(problems)}")
    return CheckedExport(
        chunks,
        {
            "chunks": len(chunks),
            "statuses_zero": len(statuses),
            "partial_files": 0,
            "rows": rows,
            "expected_rows": expected_rows,
        },
    )


def check_predication_ids(chunk: Path, predication_ids: pl.Series) -> dict[str, int | None]:
    """Fail unless a chunk's PREDICATION_IDs are non-null, unique and inside its name's range.

    ``chunk_NNN`` holds ids ``NNN * PREDICATION_ID_RANGE_SIZE`` up to the next chunk's first id, so
    an id outside that range or a repeated id means the chunk does not hold what its name says.
    Returns the chunk's smallest and largest id (None for an empty chunk).
    """
    number = int(chunk.name.removesuffix(CHUNK_SUFFIX).removeprefix(CHUNK_PREFIX))
    first, last = number * PREDICATION_ID_RANGE_SIZE, (number + 1) * PREDICATION_ID_RANGE_SIZE - 1
    nulls = predication_ids.null_count()
    outside = predication_ids.filter((predication_ids < first) | (predication_ids > last)).len()
    duplicates = predication_ids.len() - nulls - predication_ids.drop_nulls().n_unique()
    if nulls or outside or duplicates:
        raise ValueError(
            f"{chunk.name}: {outside:,} PREDICATION_IDs outside {first:,}-{last:,}, {duplicates:,} duplicate, "
            f"{nulls:,} null"
        )
    smallest, largest = predication_ids.to_frame().select(pl.all().min(), pl.all().max().name.suffix("_max")).row(0)
    return {"min_predication_id": smallest, "max_predication_id": largest}


def decompress_chunk(chunk: Path, read_size: int = DECOMPRESS_READ_SIZE) -> bytes:
    """Decompress every zstd frame of a chunk, failing unless the last frame reached its end.

    ``ZstdDecompressor.stream_reader`` returns the data of a frame cut inside its checksum trailer
    without an error, so each frame goes through a ``decompressobj``, whose ``eof`` turns True only
    after the frame's last byte, checksum included. Bytes after a finished frame start the next one.
    An empty file has no frame and fails too. The file is read ``read_size`` bytes at a time.
    """
    decompressor = zstandard.ZstdDecompressor()
    frame = decompressor.decompressobj()
    frame_ended = False
    pieces: list[bytes] = []
    with chunk.open("rb") as handle:
        while data := handle.read(read_size):
            while data:
                pieces.append(frame.decompress(data))
                frame_ended = frame.eof
                data = frame.unused_data if frame_ended else b""
                if frame_ended:
                    frame = decompressor.decompressobj()
    if not frame_ended:
        raise ValueError(f"{chunk.name}: the zstd data stops before the end of its frame, the chunk is truncated")
    return b"".join(pieces)


def read_semmeddb_chunk(chunk: Path) -> pl.DataFrame:
    """Decompress one export chunk and parse it into all-String columns, NULL as null.

    The export has no quoting, so quoting is off. Empty strings stay empty strings. A truncated
    chunk stops here: ``decompress_chunk`` requires the end of the last zstd frame (a cut inside
    the frame), the data must end with the newline that ends every ``mysql -B`` row (a complete
    frame of a cut-off stream), and the row count must equal the chunk's ``.rows`` file (a cut
    between rows). Every row must also have exactly ``SEPARATORS_PER_ROW`` raw tabs; polars would
    pad a row with a missing field, and a missing middle field would shift the columns after it.
    """
    payload = decompress_chunk(chunk)
    if payload and not payload.endswith(ROW_TERMINATOR):
        raise ValueError(f"{chunk.name}: decompressed data does not end with a newline, the chunk is truncated")
    malformed = [
        number
        for number, line in enumerate(payload.split(ROW_TERMINATOR)[:-1], start=1)
        if line.count(FIELD_SEPARATOR) != SEPARATORS_PER_ROW
    ]
    if malformed:
        raise ValueError(
            f"{chunk.name}: {len(malformed):,} lines without exactly {SEPARATORS_PER_ROW} tab separators, "
            f"the first is line {malformed[0]:,}"
        )
    if payload:
        frame = pl.read_csv(
            payload,
            separator=TSV_SEPARATOR,
            has_header=False,
            new_columns=SEMMEDDB_COLUMNS,
            quote_char=None,
            infer_schema=False,
            null_values=MYSQL_NULL,
            empty_string_is_null=False,
        )
    else:
        frame = pl.DataFrame(schema=dict.fromkeys(SEMMEDDB_COLUMNS, pl.String()))
    expected = chunk_row_count(chunk)
    if frame.height != expected:
        raise ValueError(f"{chunk.name}: parsed {frame.height:,} rows, {ROW_COUNT_SUFFIX} file says {expected:,}")
    return frame
