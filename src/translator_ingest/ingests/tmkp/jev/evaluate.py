"""
Compare Jev's answers on a validation sample with the hand labels: precision and recall for unsupported sentences
at several thresholds, and a confusion table.

    python evaluate.py validation/sample_b.jsonl validation/labels_b.json validation/jev_v3_b.jsonl

Labels: T treats, S studied (patients or live animals), N tried with no effect, L in vitro only, C causes, O other.
A sentence is unsupported (bad) when labeled L, C or O.
"""

import argparse
import json
from collections import Counter
from pathlib import Path

from prompt import BAD_THRESHOLD, p_bad

BAD_LABELS = set("LCO")
LABEL_OF_CHOICE = {"treats": "T", "studied": "S", "no_effect": "N", "cell_line": "L", "causes": "C", "other": "O"}


def precision_recall(flagged: set, bad: set) -> tuple[float, float]:
    """
    Precision and recall of flagged sentences against the truly bad ones.

    >>> precision_recall({1, 2, 3, 4}, {1, 2, 3, 5})
    (0.75, 0.75)
    """
    hits = len(flagged & bad)
    return hits / len(flagged), hits / len(bad)


def main() -> None:
    """Print precision/recall at several thresholds and a label-by-answer table."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sample", type=Path, help="sample from sample_sentences.py")
    parser.add_argument("labels", type=Path, help="hand labels, {n: label}")
    parser.add_argument("scores", type=Path, help="answers from score_sentences.py run on the sample")
    args = parser.parse_args()

    with open(args.sample) as f:
        key_of = {r["n"]: (r["edge_id"], r["sentence_id"]) for r in map(json.loads, f)}
    with open(args.labels) as f:
        labels = {key_of[int(n)]: label for n, label in json.load(f).items()}
    with open(args.scores) as f:
        answers = {(r["edge_id"], r["sentence_id"]): r for r in map(json.loads, f)}
    bad = {k for k, label in labels.items() if label in BAD_LABELS}
    print(f"{len(bad)} of {len(labels)} sentences labeled unsupported")
    for threshold in sorted({0.3, 0.5, BAD_THRESHOLD, 0.8}):
        flagged = {k for k in labels if p_bad(answers[k]["probabilities"]) >= threshold}
        precision, recall = precision_recall(flagged, bad)
        print(f"  p_bad >= {threshold}: removes {len(flagged)}, precision {precision:.2f}, recall {recall:.2f}")
    table = Counter((labels[k], LABEL_OF_CHOICE[answers[k]["choice"]]) for k in labels)
    print("rows: hand label; columns: Jev's top answer")
    print("     " + "".join(f"{c:>5}" for c in "TSNLCO"))
    for label in "TSNLCO":
        print(f"  {label}  " + "".join(f"{table[(label, c)]:>5}" for c in "TSNLCO"))


if __name__ == "__main__":
    main()
