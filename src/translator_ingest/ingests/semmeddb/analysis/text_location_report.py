"""Turn the text-location build's counts into the manifest's summaries and the console summary.

It also names what built an artifact: ``code_provenance`` hashes the build's source files and
``SourceHashes`` holds them with the hash of every input file, taken before the build reads them, to
check again before the build publishes.
It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

import inspect
import sys
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from translator_ingest.ingests.semmeddb.analysis.rekey_pmid_verdicts import sha256_file

# The repo modules the build runs besides its own, whose sha256 the manifest records.
CODE_MODULES: tuple[str, ...] = (
    "translator_ingest.ingests.semmeddb.analysis.kg2_orientation",
    "translator_ingest.ingests.semmeddb.analysis.kg2_records",
    "translator_ingest.ingests.semmeddb.analysis.kg2_semmeddb_converter",
    "translator_ingest.ingests.semmeddb.analysis.output_guards",
    "translator_ingest.ingests.semmeddb.analysis.rekey_pmid_verdicts",
    "translator_ingest.ingests.semmeddb.analysis.semmeddb_export",
    "translator_ingest.ingests.semmeddb.analysis.text_location_report",
    "translator_ingest.ingests.semmeddb.analysis.work_dir",
    "translator_ingest.ingests.semmeddb.semmeddb",
    "translator_ingest.ingests.semmeddb.text_locations",
)
# The repo root is the nearest directory above this file that holds REPO_ROOT_FILE.
REPO_ROOT_FILE = "pyproject.toml"
REPO_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / REPO_ROOT_FILE).is_file())


def code_provenance(build_module: str) -> list[dict[str, str]]:
    """Return the path (relative to the repo root) and sha256 of the build module and ``CODE_MODULES``.

    Each module's file is its ``__file__`` (``inspect.getfile``), the file this process loaded it
    from. ``build_module`` is the build's ``__name__``, which is ``__main__`` when it runs as a script.
    Every module is already imported, so an unknown name fails.
    """
    files = sorted({Path(inspect.getfile(sys.modules[name])).resolve() for name in (build_module, *CODE_MODULES)})
    return [{"path": file.relative_to(REPO_ROOT).as_posix(), "sha256": sha256_file(file)} for file in files]


def hash_files(paths: Iterable[Path]) -> dict[Path, str]:
    """Return the sha256 of each file, keyed by its path (a path given twice is hashed once)."""
    return {path: sha256_file(path) for path in dict.fromkeys(paths)}


@dataclass(frozen=True)
class SourceHashes:
    """The sha256 of the build's code and of every input file, taken before the build reads any input.

    The code is hashed first, before the inputs, whose hashing takes a while, so a checkout during
    it cannot change the code the manifest names.
    """

    build_module: str
    code: list[dict[str, str]]
    inputs: dict[Path, str]

    def check_unchanged(self) -> None:
        """Raise ValueError if an input or code file no longer has the hash taken before the build.

        The manifest records the hashes taken before the build read anything, so an input or source
        file replaced while it ran would make the manifest describe files the build did not read.
        """
        changed = [str(path) for path, digest in hash_files(self.inputs).items() if digest != self.inputs[path]]
        if code_provenance(self.build_module) != self.code:
            changed.append("the build's code (see the manifest's code section)")
        if changed:
            raise ValueError(f"inputs changed while the build ran: {changed}")


def ratio(numerator: int, denominator: int) -> float:
    """Return ``numerator / denominator`` rounded to six places, 0.0 for an empty denominator.

    >>> ratio(1, 3)
    0.333333
    >>> ratio(0, 0)
    0.0
    """
    return round(numerator / denominator, 6) if denominator else 0.0


def coverage_table(pairs: Counter[str], matched: Counter[str]) -> dict[str, dict[str, float | int]]:
    """Combine total and matched counts per key into ``{key: {pairs, matched, coverage}}``.

    >>> coverage_table(Counter({"a": 4, "b": 1}), Counter({"a": 3}))
    {'a': {'pairs': 4, 'matched': 3, 'coverage': 0.75}, 'b': {'pairs': 1, 'matched': 0, 'coverage': 0.0}}
    """
    return {
        key: {"pairs": pairs[key], "matched": matched[key], "coverage": ratio(matched[key], pairs[key])}
        for key in sorted(pairs)
    }


def matching_summary(counts: Counter[str], pairs_with_sentence: int) -> dict[str, float | int]:
    """Derive the headline matching numbers from the summed bucket counts."""
    summary: dict[str, float | int] = dict(sorted(counts.items()))
    if counts["pairs_with_sentence"] != pairs_with_sentence:
        raise ValueError(
            f"buckets hold {counts['pairs_with_sentence']:,} pairs, KG2 streaming counted {pairs_with_sentence:,}"
        )
    summary["pairs_located_only_via_mrcui"] = counts["pairs_located"] - counts["pairs_located_without_remap"]
    summary["pairs_lost_to_scores"] = counts["pairs_relation_found"] - counts["pairs_scores_matched"]
    summary["pairs_lost_to_kept_group_rule"] = (
        counts["pairs_scores_matched"] - counts["pairs_scores_matched_kept_group"]
    )
    summary["pairs_lost_to_text_check"] = counts["pairs_scores_matched_kept_group"] - counts["pairs_valid_kept_group"]
    summary["coverage"] = ratio(counts["pairs_located"], counts["pairs_with_sentence"])
    return summary


def format_summary(manifest: dict[str, Any]) -> str:
    """Render the manifest's headline numbers as a console summary."""
    matching = manifest["matching"]
    output = manifest["output"]
    return "\n".join(
        [
            "=== SemMedDB text locations ===",
            f"KG2 records {manifest['kg2']['records']:,}, kept {manifest['kg2']['records_kept']:,},"
            f" pairs with a sentence {matching['pairs_with_sentence']:,}",
            f"located {matching['pairs_located']:,} = {matching['coverage']:.2%}"
            f" (only via MRCUI {matching['pairs_located_only_via_mrcui']:,}),"
            f" dropped for a key another record could not locate {matching['pairs_dropped_unresolved_key']:,}",
            f"several spans {matching['pairs_several_spans']:,}, lost to scores {matching['pairs_lost_to_scores']:,},"
            f" lost to text check {matching['pairs_lost_to_text_check']:,},"
            f" lost to the kept-group rule {matching['pairs_lost_to_kept_group_rule']:,},"
            f" dropped as disagreeing groups {matching['pairs_groups_disagree_dropped']:,}"
            f" or for a kept group failing the text check {matching['pairs_kept_group_text_fail_dropped']:,}",
            f"output {output['rows']:,} rows, {output['size_bytes']:,} bytes -> {output['path']}",
        ]
    )
