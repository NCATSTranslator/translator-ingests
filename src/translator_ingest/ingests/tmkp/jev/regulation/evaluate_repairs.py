"""
Score Jev's repair choices against blind labels for one family's samples:

- repair: pairs Jev acted on or dropped, labeled with the candidates (and, for gene_gene, TMKP's current concept)
  shown in shuffled order. chem_gene's labels predate the human-only rule; repair_labels_human_overrides.json
  re-adjudicates its non-human rows under the final rule (any organism maps to its human counterpart, or none).
- symbol (chem_gene only): pairs flagged by the HGNC symbol hint, with TMKP's current concept among the options.

A pair's outcome follows build_repairs.py: symbol-hint pairs keep or repair at P >= 0.7, other pairs repair at
P >= 0.9, anything else is dropped.

    python evaluate_repairs.py validation/chem_gene
    python evaluate_repairs.py validation/gene_gene
"""

import json
import sys
from pathlib import Path

from build_repairs import HINT_ACCEPT, REPAIR_ACCEPT, read_jsonl
from rules import norm


def outcome(choice: dict) -> str | None:
    """The concept a pair ends up with (its current one when kept), or None when its sentences are dropped."""
    threshold = HINT_ACCEPT if choice.get("symbol_hint") else REPAIR_ACCEPT
    if choice["probability"] < threshold or choice["choice"] == "none":
        return None
    return choice["curie"] if choice["choice"] == "keep" else choice["chosen"]["curie"]


def truth(label: dict, letter_to_curie: dict, override: str | None = None) -> set[str]:
    """The labeled correct concept(s); empty for none."""
    letters = override if override is not None else (
        "" if label["answer"] == "NONE"
        else label["answer"] + "".join(x.strip() for x in (label.get("also_correct") or "").split(",")))
    return {letter_to_curie[x] for x in letters}


def report(name: str, rows: list[tuple[str | None, set[str], int]]) -> None:
    """rows: (outcome, true concepts, sentence count)."""
    acted = [(o, t, c) for o, t, c in rows if o is not None]
    correct = [(o, t, c) for o, t, c in acted if o in t]
    missed = sum(1 for o, t, _ in rows if o is None and t)
    print(f"{name}: acted on {len(acted)} of {len(rows)} pairs; correct {len(correct)} "
          f"({len(correct) / len(acted):.0%}), {sum(c for *_, c in correct) / sum(c for *_, c in acted):.1%} by "
          f"sentences; dropped although a right answer was offered: {missed}")


def load_by_n(path: Path) -> dict[int, dict]:
    """A JSON object keyed by sample row number."""
    with open(path) as f:
        return {int(n): x for n, x in json.load(f).items()}


def main(validation: str) -> None:
    """Print accuracy of the acted-on choices for the family's samples."""
    v = Path(validation)
    choices = {(r["role"], r["key"], r["curie"]): r for r in read_jsonl(v / "answers_repair_choices.jsonl")}

    key, labels = load_by_n(v / "repair_key.json"), load_by_n(v / "repair_labels.json")
    overrides_path = v / "repair_labels_human_overrides.json"
    overrides = {}
    if overrides_path.exists():
        with open(overrides_path) as f:
            overrides = {int(n): x for n, x in json.load(f)["overrides"].items()}
    rows = []
    for n, k in key.items():
        choice = choices.get((k["role"], k["key"], k["curie"]))
        rows.append((outcome(choice) if choice else None, truth(labels[n], k["letter_to_curie"], overrides.get(n)),
                     k["count"]))
    report("repair sample", rows)

    if not (v / "symbol_key.json").exists():
        return
    key, labels = load_by_n(v / "symbol_key.json"), load_by_n(v / "symbol_labels.json")
    mentions = {r["n"]: r["mention"] for r in read_jsonl(v / "symbol_sample.jsonl")}
    by_concept = {(c["curie"], c["key"]): c for c in choices.values() if c.get("symbol_hint")}
    rows = [(outcome(by_concept[(k["current"], norm(mentions[n]))]), truth(labels[n], k["letter_to_curie"]),
             k["count"]) for n, k in key.items()]
    report("symbol-hint sample", rows)


if __name__ == "__main__":
    main(*sys.argv[1:])
