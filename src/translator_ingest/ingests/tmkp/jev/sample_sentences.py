"""
Draw a validation sample: 100 treats edges from each evidence stratum (1, 2-4, 5+), one random supporting sentence
from each, shuffled and numbered. Edges in an earlier sample can be excluded so the next sample is held out.

The validation samples in validation/ were drawn with:

    python sample_sentences.py edges.tsv.gz nodes.tsv.gz validation/sample_a.jsonl --seed 20261028
    python sample_sentences.py edges.tsv.gz nodes.tsv.gz validation/sample_b.jsonl --seed 777 \
        --exclude validation/sample_a.jsonl
"""

import argparse
import gzip
import json
import random
from pathlib import Path

from extract_sentences import ATTRIBUTES, EDGE_ID, OBJECT, PREDICATE, SUBJECT, TREATS, load_node_names, mention

PER_STRATUM = 100


def stratum(evidence_count: int) -> str:
    """
    The evidence stratum an edge is sampled from.

    >>> [stratum(n) for n in (1, 4, 5)]
    ['1', '2-4', '5+']
    """
    return "1" if evidence_count == 1 else ("2-4" if evidence_count <= 4 else "5+")


def main() -> None:
    """Reservoir-sample PER_STRATUM edges per stratum in one pass over edges.tsv.gz."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("edges", type=Path, help="TMKP edges.tsv.gz")
    parser.add_argument("nodes", type=Path, help="TMKP nodes.tsv.gz")
    parser.add_argument("out", type=Path, help="output JSON lines")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--exclude", type=Path, help="earlier sample whose edges are left out")
    args = parser.parse_args()

    names = load_node_names(args.nodes)
    exclude = set()
    if args.exclude:
        with open(args.exclude) as f:
            exclude = {json.loads(line)["edge_id"] for line in f}
    random.seed(args.seed)
    samples: dict[str, list] = {"1": [], "2-4": [], "5+": []}
    seen = {k: 0 for k in samples}
    with gzip.open(args.edges, "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if p[PREDICATE] != TREATS or p[EDGE_ID] in exclude:
                continue
            attributes = json.loads(p[ATTRIBUTES])
            evidence = int(next(a["value"] for a in attributes if a["attribute_type_id"] == "biolink:evidence_count"))
            k = stratum(evidence)
            seen[k] += 1
            if len(samples[k]) < PER_STRATUM:
                slot = len(samples[k])
                samples[k].append(None)
            else:
                slot = random.randrange(seen[k])
                if slot >= PER_STRATUM:
                    continue
            results = [a for a in attributes if a["attribute_type_id"] == "biolink:has_supporting_study_result"]
            result = random.choice(results)
            nested = {n["attribute_type_id"].removeprefix("biolink:"): n["value"] for n in result["attributes"]}
            text = nested.get("supporting_text", "")
            samples[k][slot] = {
                "edge_id": p[EDGE_ID],
                "stratum": k,
                "evidence_count": evidence,
                "n_sentences": len(results),
                "subject": p[SUBJECT],
                "subject_name": names.get(p[SUBJECT]),
                "object": p[OBJECT],
                "object_name": names.get(p[OBJECT]),
                "subject_mention": mention(text, nested.get("subject_location_in_text")),
                "object_mention": mention(text, nested.get("object_location_in_text")),
                "sentence_id": result["value"],
                "document": nested.get("supporting_document"),
                "section": nested.get("supporting_text_located_in"),
                "extraction_confidence": nested.get("extraction_confidence_score"),
                "text": text,
            }
    rows = [row for k in samples for row in samples[k]]
    random.shuffle(rows)
    with open(args.out, "w") as out:
        for i, row in enumerate(rows):
            out.write(json.dumps({"n": i, **row}) + "\n")
    print(f"edges seen per stratum {seen}; wrote {len(rows)} sentences to {args.out}")


if __name__ == "__main__":
    main()
