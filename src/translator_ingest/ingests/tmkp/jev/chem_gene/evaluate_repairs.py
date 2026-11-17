"""
Score Jev's repair choices against blind labels on two samples:

- repair: 100 pairs judged wrong (P(wrong) >= 0.9); labeled with the candidates shown in shuffled order. The labels
  used a "non-human genes get none" rule; repair_labels_human_overrides.json re-adjudicates those rows under the
  final rule (any organism maps to its human counterpart, or none).
- symbol: 70 pairs flagged by the HGNC symbol hint; TMKP's current concept was among the shuffled options.

A pair's outcome follows build_repairs.py: symbol-hint pairs keep or repair at P >= 0.7, other pairs repair at
P >= 0.9, anything else is dropped.

    python evaluate_repairs.py validation
"""

import json
import sys
from pathlib import Path

from build_repairs import HINT_ACCEPT, REPAIR_ACCEPT
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


def main(validation: str) -> None:
    """Print accuracy of the acted-on choices for both samples."""
    v = Path(validation)
    with open(v / "answers_repair_choices.jsonl") as f:
        choices = {(r["role"], r["key"], r["curie"]): r for r in map(json.loads, f)}

    def load_json(name: str) -> dict[int, dict]:
        with open(v / name) as f:
            return {int(n): x for n, x in json.load(f).items()}

    key, labels = load_json("repair_key.json"), load_json("repair_labels.json")
    with open(v / "repair_labels_human_overrides.json") as f:
        overrides = {int(n): x for n, x in json.load(f)["overrides"].items()}
    rows = []
    for n, k in key.items():
        choice = choices.get((k["role"], k["key"], k["curie"]))
        rows.append((outcome(choice) if choice else None, truth(labels[n], k["letter_to_curie"], overrides.get(n)),
                     k["count"]))
    report("repair sample", rows)

    key, labels = load_json("symbol_key.json"), load_json("symbol_labels.json")
    with open(v / "symbol_sample.jsonl") as f:
        mentions = {r["n"]: r["mention"] for r in map(json.loads, f)}
    by_concept = {(c["curie"], c["key"]): c for c in choices.values() if c.get("symbol_hint")}
    rows = [(outcome(by_concept[(k["current"], norm(mentions[n]))]), truth(labels[n], k["letter_to_curie"]),
             k["count"]) for n, k in key.items()]
    report("symbol-hint sample", rows)


if __name__ == "__main__":
    main(*sys.argv[1:])
