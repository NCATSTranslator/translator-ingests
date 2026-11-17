"""
Score the final sentence decision (build_repairs.decide) on a family's labeled filter sample: how well "not kept as
mapped" (dropped or moved to a repaired edge) finds the sentences that don't support their edge as mapped.

    python evaluate_filter.py validation/chem_gene
    python evaluate_filter.py validation/gene_gene

A sentence is unsupported when its labeled mapping is WRONG or its labeled relation isn't the edge's direction
(INC for increased, DEC for decreased). Labels were made blind to Jev by separate Claude instances from a written
rubric; a protein family or complex mapped to one member counts as WRONG.
"""

import json
import sys
from collections import Counter
from pathlib import Path

from build_repairs import decide, read_jsonl


def load_answers(v: Path) -> dict:
    """The subset of Jev's answers the sample needs (relation answers come per sentence)."""
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
    relation = {(r["edge_id"], r["sentence_id"]): r["relation"]["probabilities"]
                for r in read_jsonl(v / "filter_relation.jsonl")}
    with open(v / "filter_labels.json") as f:
        labels = {int(n): x for n, x in json.load(f).items()}
    sample = {r["n"]: r for r in read_jsonl(v / "filter_sample.jsonl")}
    bad, removed, table = set(), set(), Counter()
    for n, s in sample.items():
        status, _, _, _ = decide(s, relation[(s["edge_id"], s["sentence_id"])], answers)
        want = "INC" if s["object_direction"] == "increased" else "DEC"
        if labels[n]["mapping"] == "WRONG" or labels[n]["relation"] != want:
            bad.add(n)
        if status != "keep":
            removed.add(n)
        table[("unsupported" if n in bad else "supported", status)] += 1
    hits = len(removed & bad)
    kept = set(sample) - removed
    print(f"{len(bad)} of {len(sample)} sentences are unsupported as mapped")
    print(f"not kept: {len(removed)}, precision {hits / len(removed):.2f}, recall {hits / len(bad):.2f}; "
          f"supported among kept {len(kept - bad) / len(kept):.0%} ({len(kept - bad)} of {len(kept)})")
    for (truth, status), count in sorted(table.items()):
        print(f"  {truth:12} -> {status:7} {count}")


if __name__ == "__main__":
    main(*sys.argv[1:])
