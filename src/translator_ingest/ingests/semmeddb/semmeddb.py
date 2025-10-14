from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional, Set, Tuple, Dict, Any, TextIO, cast


PREDICATES_TO_KEEP: Set[str] = {
    # list provided by user; keep exact biolink curies
    "biolink:treats_or_applied_or_studied_to_treat",
    "biolink:affects",
    "biolink:preventative_for_condition",
    "biolink:coexists_with",
    "biolink:causes",
    "biolink:related_to",
    "biolink:interacts_with",
    "biolink:located_in",
    "biolink:predisposes_to_condition",
    "biolink:physically_interacts_with",
    "biolink:disrupts",
}


def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    """stream json objects line by line using a generator to limit memory usage"""
    with path.open("r") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                # skip malformed lines rather than failing the entire run
                continue


def _open_output(path: Path) -> TextIO:
    """open output file ensuring parent directory exists"""
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("w")


def _get_progress() -> Callable[[Iterable[Any], Optional[int]], Iterable[Any]]:
    """optional progress bar with graceful fallback if tqdm is missing"""
    try:
        from tqdm import tqdm  # type: ignore

        def bar(iterable: Iterable[Any], total: Optional[int] = None) -> Iterable[Any]:
            # cast because tqdm returns an untyped iterable which mypy treats as Any
            return cast(Iterable[Any], tqdm(iterable, total=total, unit="lines"))

        return bar
    except Exception:

        def identity(iterable: Iterable[Any], total: Optional[int] = None) -> Iterable[Any]:  # noqa: ARG001
            return iterable

        return identity


def filter_semmed_edges(
    input_path: Path = Path("data/semmeddb_kg2_kgx/kg2.10.3-semmeddb-edges.jsonl"),
    output_path: Path = Path("data/semmeddb/semmeddb_edges.jsonl"),
    predicates_to_keep: Set[str] = PREDICATES_TO_KEEP,
) -> Tuple[int, int]:
    """
    stream filter a large semmeddb edges jsonl by predicate.

    returns (total_read, total_written)
    """
    progress = _get_progress()

    total_read = 0
    total_written = 0

    with _open_output(output_path) as out:
        for obj in progress(_iter_jsonl(input_path), None):
            total_read += 1
            # predicate field name in kgx is typically 'predicate'
            predicate = obj.get("predicate") or obj.get("edge_label")
            if predicate in predicates_to_keep:
                out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                total_written += 1

    return total_read, total_written


def main() -> None:
    input_env = os.environ.get("SEM_MED_IN", "data/semmeddb_kg2_kgx/kg2.10.3-semmeddb-edges.jsonl")
    output_env = os.environ.get("SEM_MED_OUT", "data/semmeddb/semmeddb_edges.jsonl")
    total_read, total_written = filter_semmed_edges(Path(input_env), Path(output_env))
    # simple terminal summary for quick sanity check
    print(f"read={total_read} written={total_written} kept={total_written}")


if __name__ == "__main__":
    main()
