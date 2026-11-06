"""
CSV → Markdown table converter (stdlib only).

Reads one or more CSV files (or stdin) and emits GitHub-flavored Markdown
tables. Columns whose data cells are all numeric are right-aligned by default;
everything else is left-aligned. Handy for turning the EDA CSVs under
``eda/csv/`` into tables for issues, the RIG, or the CHANGELOG.

Usage::

    uv run python csv_to_md.py channel_summary.csv
    uv run python csv_to_md.py csv/*.csv          # each table under an H3 heading
    cat data.csv | uv run python csv_to_md.py     # from stdin
    uv run python csv_to_md.py --left data.csv    # disable numeric auto-align
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Iterable, Sequence


def _is_number(value: str) -> bool:
    """
    Whether a cell should be treated as numeric (for right-alignment).

    Accepts ints, floats, and a leading sign; empty cells are not numeric.

    >>> [_is_number(v) for v in ("0", "-3", "4.17", "1e3", "", "n/a", "9606.ENSP")]
    [True, True, True, True, False, False, False]
    """
    text = value.strip()
    if not text:
        return False
    try:
        float(text)
        return True
    except ValueError:
        return False


def _escape(cell: object) -> str:
    """
    Render a cell for Markdown: stringify, collapse newlines, escape pipes.

    >>> _escape("a|b")
    'a\\\\|b'
    >>> _escape(None)
    ''
    >>> _escape("line1\\nline2")
    'line1 line2'
    """
    if cell is None:
        return ""
    return str(cell).replace("\n", " ").replace("|", "\\|")


def _alignments(rows: Sequence[Sequence[object]], auto: bool = True) -> list[str]:
    """
    Per-column alignment: ``"right"`` when every data cell (rows after the
    header) is numeric and ``auto`` is on, else ``"left"``.

    >>> _alignments([["name", "n"], ["a", "1"], ["b", "2"]])
    ['left', 'right']
    >>> _alignments([["name", "n"], ["a", "1"]], auto=False)
    ['left', 'left']
    """
    width = max((len(r) for r in rows), default=0)
    if not auto or len(rows) < 2:
        return ["left"] * width
    aligns: list[str] = []
    for col in range(width):
        data = [r[col] for r in rows[1:] if col < len(r) and str(r[col]).strip() != ""]
        numeric = bool(data) and all(_is_number(str(v)) for v in data)
        aligns.append("right" if numeric else "left")
    return aligns


def csv_to_md(rows: Sequence[Sequence[object]], auto_align: bool = True) -> str:
    """
    Convert rows (first row is the header) into a Markdown table string.

    >>> print(csv_to_md([["name", "n"], ["alpha", "1"], ["beta", "20"]]))
    | name | n |
    | --- | ---: |
    | alpha | 1 |
    | beta | 20 |
    >>> csv_to_md([])
    ''
    """
    rows = [list(r) for r in rows]
    if not rows:
        return ""
    width = max(len(r) for r in rows)
    aligns = _alignments(rows, auto_align)

    def fmt(row: Sequence[object]) -> str:
        cells = [_escape(row[i]) if i < len(row) else "" for i in range(width)]
        return "| " + " | ".join(cells) + " |"

    sep = "| " + " | ".join("---:" if a == "right" else "---" for a in aligns) + " |"
    lines = [fmt(rows[0]), sep] + [fmt(r) for r in rows[1:]]
    return "\n".join(lines)


def read_csv(source: Iterable[str]) -> list[list[str]]:
    """Parse CSV text lines into a list of rows."""
    return [row for row in csv.reader(source)]


def main(argv: list[str]) -> int:
    auto_align = True
    paths: list[str] = []
    for arg in argv:
        if arg in ("--left", "--no-auto-align"):
            auto_align = False
        elif arg in ("-h", "--help"):
            print(__doc__)
            return 0
        else:
            paths.append(arg)

    if not paths:  # stdin
        print(csv_to_md(read_csv(sys.stdin), auto_align))
        return 0

    for i, path in enumerate(paths):
        rows = read_csv(Path(path).read_text().splitlines())
        if len(paths) > 1:
            if i:
                print()
            print(f"### {Path(path).stem}\n")
        print(csv_to_md(rows, auto_align))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
