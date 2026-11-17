"""
Build the table of distinct (mention, mapped concept) pairs in TMKP chemical -> protein sentences, for judging each
mapping once instead of once per sentence. One line per pair: role (subject = chemical, object = protein), the
normalized mention key, the concept id and name, the most common way the mention is written, how many sentences use
the pair, and up to EXAMPLES example sentences (chosen at random, reproducibly) with the mention's span.

    python build_pairs.py edges.tsv.gz nodes.tsv.gz pairs.jsonl
"""
import gzip
import json
import random
import sys
from collections import Counter

from rules import norm

RELATION = "biolink:ChemicalToGeneAssociation"
EXAMPLES = 3


def main(edges_path: str, nodes_path: str, out_path: str) -> None:
    """One pass over the edges, reservoir-sampling examples per pair."""
    with gzip.open(nodes_path, "rt") as f:
        names = {p[0]: p[1] for p in (line.rstrip("\n").split("\t") for line in f)}
    rng = random.Random(20261111)
    pairs: dict[tuple, dict] = {}
    with gzip.open(edges_path, "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if p[14] != RELATION or p[1] == "biolink:treats_or_applied_or_studied_to_treat":
                continue
            for result in json.loads(p[18]):
                if result["attribute_type_id"] != "biolink:has_supporting_study_result":
                    continue
                nested = {n["attribute_type_id"].removeprefix("biolink:"): n["value"] for n in result["attributes"]}
                text = nested.get("supporting_text", "")
                for role, curie, loc in (("subject", p[0], nested.get("subject_location_in_text")),
                                         ("object", p[2], nested.get("object_location_in_text"))):
                    if not loc:
                        continue
                    start, end = map(int, str(loc).split("|")[:2])
                    mention = text[start:end]
                    key = (role, norm(mention), curie)
                    pair = pairs.setdefault(key, {"count": 0, "forms": Counter(), "examples": []})
                    pair["count"] += 1
                    pair["forms"][mention] += 1
                    example = {"text": text, "span": [start, end], "edge_id": p[13]}
                    if len(pair["examples"]) < EXAMPLES:
                        pair["examples"].append(example)
                    else:
                        slot = rng.randrange(pair["count"])
                        if slot < EXAMPLES:
                            pair["examples"][slot] = example
    with open(out_path, "w") as out:
        for (role, key, curie), pair in sorted(pairs.items(), key=lambda kv: -kv[1]["count"]):
            out.write(json.dumps({"role": role, "key": key, "curie": curie, "name": names.get(curie),
                                  "mention": pair["forms"].most_common(1)[0][0], "count": pair["count"],
                                  "examples": pair["examples"]}) + "\n")
    print(f"{len(pairs)} pairs written to {out_path}")


if __name__ == "__main__":
    main(*sys.argv[1:])
