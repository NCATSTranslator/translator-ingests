"""
Step 3: roll sentence answers up to edges and write the list of edge ids the ingest drops.

An edge is kept if any of its sentences has p_bad below BAD_THRESHOLD, and dropped otherwise. Also writes one line
per edge with its lowest p_bad, so a different threshold can be tried without asking Jev again.

    python build_drop_list.py treats_sentences.jsonl scores.jsonl treats_edges.jsonl \
        tmkp_treats_jev_v3_dropped_edges.txt.gz
"""

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path

from prompt import BAD_THRESHOLD, p_bad


def evidence_bucket(evidence_count: int) -> str:
    """
    Group an edge's evidence count for reporting.

    >>> [evidence_bucket(n) for n in (1, 3, 7, 12)]
    ['1', '2-4', '5-9', '10+']
    """
    if evidence_count == 1:
        return "1"
    if evidence_count <= 4:
        return "2-4"
    return "5-9" if evidence_count <= 9 else "10+"


def main() -> None:
    """Write per-edge results and the drop list, and print a summary."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sentences", type=Path, help="sentences from extract_sentences.py")
    parser.add_argument("scores", type=Path, help="answers from score_sentences.py")
    parser.add_argument("edges_out", type=Path, help="JSON lines, one per edge, with min_p_bad and keep")
    parser.add_argument("drop_list", type=Path, help="gzipped edge ids to drop, one per line")
    parser.add_argument("--threshold", type=float, default=BAD_THRESHOLD)
    args = parser.parse_args()

    scores, choices = {}, Counter()
    with open(args.scores) as f:
        for r in map(json.loads, f):
            scores[(r["edge_id"], r["sentence_id"])] = p_bad(r["probabilities"])
            choices[r["choice"]] += 1
    edges: dict[str, dict] = {}
    with open(args.sentences) as f:
        for s in map(json.loads, f):
            key = (s["edge_id"], s["sentence_id"])
            if key not in scores:
                raise SystemExit(f"sentence {key} has no score; finish score_sentences.py first")
            e = edges.setdefault(s["edge_id"], {"edge_id": s["edge_id"], "subject": s["subject"],
                                                "object": s["object"], "evidence_count": s["evidence_count"],
                                                "n_sentences": 0, "min_p_bad": 1.0})
            e["n_sentences"] += 1
            e["min_p_bad"] = min(e["min_p_bad"], scores[key])

    kept, total = Counter(), Counter()
    with open(args.edges_out, "w") as out:
        for e in edges.values():
            e["keep"] = e["min_p_bad"] < args.threshold
            bucket = evidence_bucket(e["evidence_count"])
            total[bucket] += 1
            kept[bucket] += e["keep"]
            out.write(json.dumps(e) + "\n")
    dropped = sorted(e["edge_id"] for e in edges.values() if not e["keep"])
    with gzip.open(args.drop_list, "wt") as f:
        f.write("".join(f"{edge_id}\n" for edge_id in dropped))

    print("sentence answers:", dict(choices.most_common()))
    print(f"edges: {len(edges)}, dropped {len(dropped)} ({len(dropped) / len(edges):.1%}) at p_bad >= {args.threshold}")
    for bucket in ("1", "2-4", "5-9", "10+"):
        print(f"  evidence {bucket:>4}: kept {kept[bucket]} of {total[bucket]} ({kept[bucket] / total[bucket]:.1%})")


if __name__ == "__main__":
    main()
