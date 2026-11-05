"""
Step 1: write one JSON line per supporting sentence of every TMKP treats edge.

Each line holds the edge id (column 14 of edges.tsv.gz, the `id` the ingest reads), the sentence id, the edge's
evidence count, the two node ids and names, the sentence, and the two mentions cut from it by their character spans.

    python extract_sentences.py edges.tsv.gz nodes.tsv.gz treats_sentences.jsonl
"""

import argparse
import gzip
import json
from pathlib import Path
from typing import Any, Iterator

TREATS = "biolink:treats_or_applied_or_studied_to_treat"
SUBJECT, PREDICATE, OBJECT, EDGE_ID, ATTRIBUTES = 0, 1, 2, 13, 18


def mention(text: str, location: str | None) -> str | None:
    """
    Cut a mention out of a sentence by its "start|end" character span.

    >>> mention("Doxycycline therapy for leptospirosis", "24|37")
    'leptospirosis'
    """
    if not location:
        return None
    start, end = map(int, str(location).split("|")[:2])
    return text[start:end]


def load_node_names(nodes_path: Path) -> dict[str, str]:
    """Map node id to name from nodes.tsv.gz (id, name, category; no header)."""
    with gzip.open(nodes_path, "rt") as f:
        return {p[0]: p[1] for p in (line.rstrip("\n").split("\t") for line in f)}


def treats_sentences(edges_path: Path, names: dict[str, str]) -> Iterator[dict[str, Any]]:
    """Yield one record per supporting sentence of each treats edge in edges.tsv.gz (no header)."""
    with gzip.open(edges_path, "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if p[PREDICATE] != TREATS:
                continue
            attributes = json.loads(p[ATTRIBUTES])
            evidence = int(next(a["value"] for a in attributes if a["attribute_type_id"] == "biolink:evidence_count"))
            for result in attributes:
                if result["attribute_type_id"] != "biolink:has_supporting_study_result":
                    continue
                nested = {n["attribute_type_id"].removeprefix("biolink:"): n["value"] for n in result["attributes"]}
                text = nested.get("supporting_text", "")
                yield {
                    "edge_id": p[EDGE_ID],
                    "sentence_id": result["value"],
                    "evidence_count": evidence,
                    "subject": p[SUBJECT],
                    "subject_name": names.get(p[SUBJECT]),
                    "object": p[OBJECT],
                    "object_name": names.get(p[OBJECT]),
                    "subject_mention": mention(text, nested.get("subject_location_in_text")),
                    "object_mention": mention(text, nested.get("object_location_in_text")),
                    "text": text,
                }


def main() -> None:
    """Write treats sentences from the TMKP KGX files."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("edges", type=Path, help="TMKP edges.tsv.gz")
    parser.add_argument("nodes", type=Path, help="TMKP nodes.tsv.gz")
    parser.add_argument("out", type=Path, help="output JSON lines")
    args = parser.parse_args()
    names = load_node_names(args.nodes)
    count = 0
    with open(args.out, "w") as out:
        for sentence in treats_sentences(args.edges, names):
            out.write(json.dumps(sentence) + "\n")
            count += 1
    print(f"wrote {count} sentences to {args.out}")


if __name__ == "__main__":
    main()
