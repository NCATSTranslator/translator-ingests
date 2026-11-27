"""Tests for reading and checking the SemMedDB export (``analysis/semmeddb_export.py``).

The chunks are tiny real ``mysql -B`` style ``.tsv.zst`` files written to ``tmp_path``. The checks
that need a whole export run through the build, in ``test_build_text_locations.py``.
"""

import doctest
import re
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
import zstandard

from tests.unit.ingests.semmeddb.text_location_fixtures import mysql_escape, rows_payload, write_chunk
from translator_ingest.ingests.semmeddb.analysis import build_text_locations, semmeddb_export
from translator_ingest.ingests.semmeddb.analysis.semmeddb_export import (
    DECOMPRESS_READ_SIZE,
    PREDICATION_ID_RANGE_SIZE,
    SEMMEDDB_COLUMNS,
    check_export,
    check_listing_unchanged,
    decompress_chunk,
    export_listing,
    read_semmeddb_chunk,
    unescape_mysql,
)

EXPORT_SCRIPT = Path(build_text_locations.__file__).with_name("export_semmeddb_predications.sh")
SENTENCE = "Aspirin treats headache in adults."
ROW_COUNT = 5
# Where the truncation test cuts: inside the last row, which is longer than this.
CUT_BEFORE_END = 40
# A zstd frame written with a checksum ends with these 4 bytes of XXH64.
CHECKSUM_BYTES = 4
# SELECT items of the export script whose column is exported under another name.
SELECT_EXPORT_NAMES: dict[str, str] = {"s.TYPE": "SENT_TYPE", "s.NUMBER": "SENT_NUMBER"}
# Read sizes for the frame tests: byte by byte, an odd size that cuts frames anywhere, the default.
READ_SIZES = [1, 7, DECOMPRESS_READ_SIZE]


def plain_rows(count: int) -> list[list[str]]:
    """Return ``count`` export rows whose fields are just their row number, with a real sentence."""
    return [[str(number)] * (len(SEMMEDDB_COLUMNS) - 1) + [SENTENCE] for number in range(count)]


def framed(payload: bytes) -> bytes:
    """Compress ``payload`` into one complete zstd frame with a checksum, as the export writes it."""
    return zstandard.ZstdCompressor(write_checksum=True).compress(payload)


def cut_after_last_block(payload: bytes) -> bytes:
    """Compress ``payload`` into a frame that stops after its last data block, before the frame end."""
    compressor = zstandard.ZstdCompressor(write_checksum=True).compressobj()
    return compressor.compress(payload) + compressor.flush(zstandard.COMPRESSOBJ_FLUSH_BLOCK)


def write_raw_chunk(directory: Path, compressed: bytes, rows: int) -> Path:
    """Write ``chunk_001.tsv.zst`` with exactly these bytes and a ``.rows`` file, and return the chunk."""
    (directory / "chunk_001.rows").write_text(f"{rows}\n")
    chunk = directory / "chunk_001.tsv.zst"
    chunk.write_bytes(compressed)
    return chunk


def test_module_doctests_pass() -> None:
    """The doctests of the export module pass."""
    results = doctest.testmod(semmeddb_export)
    assert results.failed == 0
    assert results.attempted > 0


@pytest.mark.parametrize(
    "escaped, plain",
    [
        ("plain", "plain"),
        ("a\\tb", "a\tb"),
        ("a\\nb", "a\nb"),
        ("a\\0b", "a\x00b"),
        ("a\\\\b", "a\\b"),
        ("a\\\\tb", "a\\tb"),
        ("a\\\\\\tb", "a\\\tb"),
        ("\\\\\\\\", "\\\\"),
    ],
)
def test_unescape_mysql_decodes_left_to_right(escaped: str, plain: str) -> None:
    """mysql -B escapes decode left to right, so an escaped backslash before t stays a backslash."""
    assert pl.DataFrame({"text": [escaped]}).select(unescape_mysql("text")).item() == plain
    assert mysql_escape(plain) == escaped


def test_read_semmeddb_chunk_keeps_null_and_empty_apart(tmp_path: Path) -> None:
    """The literal NULL reads as null, an empty field stays an empty string."""
    row = plain_rows(1)[0]
    row[SEMMEDDB_COLUMNS.index("SUBJECT_TEXT")] = "NULL"
    row[SEMMEDDB_COLUMNS.index("OBJECT_SEMTYPE")] = ""
    write_chunk(tmp_path, "chunk_001", [row])
    frame = read_semmeddb_chunk(tmp_path / "chunk_001.tsv.zst")
    assert frame["SUBJECT_TEXT"].item() is None
    assert frame["OBJECT_SEMTYPE"].item() == ""
    assert frame["SENTENCE"].item() == SENTENCE


def test_read_semmeddb_chunk_rejects_a_chunk_cut_inside_a_row(tmp_path: Path) -> None:
    """A complete frame whose data stops inside a row (the export stream cut off) stops here."""
    chunk = write_raw_chunk(tmp_path, framed(rows_payload(plain_rows(ROW_COUNT))[:-CUT_BEFORE_END]), ROW_COUNT)
    with pytest.raises(ValueError, match="does not end with a newline"):
        read_semmeddb_chunk(chunk)


@pytest.mark.parametrize("read_size", READ_SIZES)
@pytest.mark.parametrize(
    "compress",
    [
        pytest.param(lambda payload: framed(payload)[:-CHECKSUM_BYTES], id="cut_inside_the_checksum"),
        pytest.param(lambda payload: framed(payload)[:-1], id="cut_at_the_last_byte"),
        pytest.param(cut_after_last_block, id="cut_after_the_last_block"),
        pytest.param(lambda payload: framed(payload) + framed(payload)[:-CHECKSUM_BYTES], id="second_frame_cut"),
        pytest.param(lambda payload: b"", id="empty_file"),
    ],
)
def test_decompress_chunk_rejects_a_frame_cut_before_its_end(
    compress: Callable[[bytes], bytes], read_size: int, tmp_path: Path
) -> None:
    """A frame cut before its end, inside the checksum say, can decode to whole rows; it stops here."""
    chunk = write_raw_chunk(tmp_path, compress(rows_payload(plain_rows(ROW_COUNT))), ROW_COUNT)
    with pytest.raises(ValueError, match="stops before the end of its frame"):
        decompress_chunk(chunk, read_size)


@pytest.mark.parametrize("read_size", READ_SIZES)
def test_decompress_chunk_reads_every_frame(read_size: int, tmp_path: Path) -> None:
    """A chunk of several complete frames, empty ones included, decompresses to their concatenation."""
    payloads = [rows_payload(plain_rows(ROW_COUNT)[:2]), b"", rows_payload(plain_rows(ROW_COUNT)[2:])]
    chunk = write_raw_chunk(tmp_path, b"".join(framed(payload) for payload in payloads), ROW_COUNT)
    assert decompress_chunk(chunk, read_size) == b"".join(payloads)
    assert read_semmeddb_chunk(chunk)["PREDICATION_ID"].to_list() == [str(number) for number in range(ROW_COUNT)]


@pytest.mark.parametrize("missing_field", ["SENTENCE", "SUBJECT_CUI"])
def test_read_semmeddb_chunk_rejects_a_row_with_a_missing_field(missing_field: str, tmp_path: Path) -> None:
    """A row one tab short (a missing last or middle field) stops here; polars would pad or shift it."""
    rows = plain_rows(ROW_COUNT)
    del rows[1][SEMMEDDB_COLUMNS.index(missing_field)]
    write_chunk(tmp_path, "chunk_001", rows)
    with pytest.raises(ValueError, match="1 lines without exactly 25 tab separators, the first is line 2"):
        read_semmeddb_chunk(tmp_path / "chunk_001.tsv.zst")


def test_read_semmeddb_chunk_rejects_row_count_mismatch(tmp_path: Path) -> None:
    """A chunk whose parsed rows differ from its .rows file (a cut between rows) stops here."""
    write_chunk(tmp_path, "chunk_001", plain_rows(ROW_COUNT))
    (tmp_path / "chunk_001.rows").write_text(f"{ROW_COUNT + 1}\n")
    with pytest.raises(ValueError, match="rows"):
        read_semmeddb_chunk(tmp_path / "chunk_001.tsv.zst")


def test_export_script_matches_the_build() -> None:
    """The export script cuts chunks by the build's range size, selects the build's columns and documents them.

    Each SELECT item is a PREDICATION (p), SENTENCE (s) or PREDICATION_AUX (a) column; two SENTENCE
    columns get their export names, the rest keep theirs.
    """
    script = EXPORT_SCRIPT.read_text(encoding="utf-8")
    range_size = re.search(r"^RANGE_SIZE=(\d+)$", script, re.MULTILINE)
    header = re.search(r"printf '(.*)\\n' > header\.tsv", script)
    select = re.search(r"^SELECT (.*?)^FROM ", script, re.MULTILINE | re.DOTALL)
    assert range_size is not None and int(range_size.group(1)) == PREDICATION_ID_RANGE_SIZE
    assert header is not None and header.group(1).split("\\t") == SEMMEDDB_COLUMNS
    assert select is not None
    items = [item.strip() for item in select.group(1).split(",")]
    assert all(re.fullmatch(r"[psa]\.[A-Z_]+", item) for item in items), items
    assert [SELECT_EXPORT_NAMES.get(item, item.split(".")[1]) for item in items] == SEMMEDDB_COLUMNS


def test_check_export_uses_only_the_listing_it_is_given(tmp_path: Path) -> None:
    """A file that appears after the listing is not read: a failing status left out of it does not count."""
    (tmp_path / "header.tsv").write_text("\t".join(SEMMEDDB_COLUMNS) + "\n")
    write_chunk(tmp_path, "chunk_001", plain_rows(ROW_COUNT))
    listing = export_listing(tmp_path)
    write_chunk(tmp_path, "chunk_002", [], status="1")
    assert check_export(tmp_path, ROW_COUNT, listing).chunks == [tmp_path / "chunk_001.tsv.zst"]
    with pytest.raises(ValueError, match="status not 0"):
        check_export(tmp_path, ROW_COUNT, export_listing(tmp_path))
    with pytest.raises(ValueError, match="added or removed while the build ran: added .*chunk_002"):
        check_listing_unchanged(tmp_path, listing)
