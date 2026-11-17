"""
Score the final sentence decision (build_repairs.decide) on the held-out sample: how well "not kept as mapped"
(dropped or moved to a repaired edge) finds the sentences that don't support their edge as mapped.

    python evaluate_filter.py validation

A sentence is unsupported when its labeled mapping is WRONG or its labeled relation isn't the edge's direction
(INC for increased, DEC for decreased). Labels were made blind to Jev by a separate Claude instance from a written
rubric; a protein family or complex mapped to one member counts as WRONG.
"""

import json
import sys
from collections import Counter
from pathlib import Path

from build_repairs import decide, read_jsonl


def load_answers(v: Path) -> dict:
    """The subset of Jev's answers the held-out sample needs (relation answers come per row)."""
    def load(name: str) -> list[dict]:
        return read_jsonl(v / name)

    return {
        "pair_wrong": {(r["role"], r["key"], r["curie"]): r["probabilities"]["wrong"]
                       for r in load("answers_pair_scores.jsonl")},
        "choices": {(r["role"], r["key"], r["curie"]): r for r in load("answers_repair_choices.jsonl")},
        "cpt1": {(r["edge_id"], r["sentence_id"]): r for r in load("answers_cpt1_choices.jsonl")},
    }


def main(validation: str) -> None:
    """Print precision and recall of not keeping a sentence on its edge, and the decisions by label."""
    v = Path(validation)
    answers = load_answers(v)
    with open(v / "filter_heldout_relation.jsonl") as f:
        relation = {r["n"]: r["relation"]["probabilities"] for r in map(json.loads, f)}
    with open(v / "filter_heldout_labels.json") as f:
        labels = {int(n): x for n, x in json.load(f).items()}
    with open(v / "filter_heldout_sample.jsonl") as f:
        sample = {r["n"]: r for r in map(json.loads, f)}
    bad, removed, table = set(), set(), Counter()
    for n, s in sample.items():
        status, reason, _, _ = decide(s, relation[n], answers)
        want = "INC" if s["object_direction"] == "increased" else "DEC"
        is_bad = labels[n]["mapping"] == "WRONG" or labels[n]["relation"] != want
        bad |= {n} if is_bad else set()
        removed |= {n} if status != "keep" else set()
        table[("unsupported" if is_bad else "supported", status)] += 1
    hits = len(removed & bad)
    kept = set(sample) - removed
    print(f"{len(bad)} of {len(sample)} held-out sentences are unsupported as mapped")
    print(f"not kept: {len(removed)}, precision {hits / len(removed):.2f}, recall {hits / len(bad):.2f}; "
          f"supported among kept {len(kept - bad) / len(kept):.0%}")
    for (truth, status), count in sorted(table.items()):
        print(f"  {truth:12} -> {status:7} {count}")


if __name__ == "__main__":
    main(*sys.argv[1:])
