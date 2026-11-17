"""
Write one JSON line per supporting sentence of every TMKP edge of one regulation family (families.py), with the
edge's direction qualifier and both mention spans.

    python extract_sentences.py chem_gene edges.tsv.gz nodes.tsv.gz chem_gene_sentences.jsonl
"""
import gzip
import json
import sys

from families import in_family



def main(family: str, edges_path: str, nodes_path: str, out_path: str) -> None:
    """One pass over edges.tsv.gz."""
    with gzip.open(nodes_path, "rt") as f:
        names = {p[0]: p[1] for p in (line.rstrip("\n").split("\t") for line in f)}
    count = 0
    with gzip.open(edges_path, "rt") as f, open(out_path, "w") as out:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if not in_family(family, p[1], p[14]):
                continue
            for result in json.loads(p[18]):
                if result["attribute_type_id"] != "biolink:has_supporting_study_result":
                    continue
                nested = {n["attribute_type_id"].removeprefix("biolink:"): n["value"] for n in result["attributes"]}
                text = nested.get("supporting_text", "")

                def span(loc):
                    if not loc:
                        return None
                    a, b = map(int, str(loc).split("|")[:2])
                    return [a, b]

                s_span, o_span = span(nested.get("subject_location_in_text")), span(nested.get("object_location_in_text"))
                out.write(json.dumps({
                    "edge_id": p[13], "sentence_id": result["value"], "subject": p[0], "subject_name": names.get(p[0]),
                    "object": p[2], "object_name": names.get(p[2]), "object_direction": p[9],
                    "subject_span": s_span, "object_span": o_span,
                    "subject_mention": text[s_span[0]:s_span[1]] if s_span else None,
                    "object_mention": text[o_span[0]:o_span[1]] if o_span else None, "text": text}) + "\n")
                count += 1
    print(f"wrote {count} sentences")


if __name__ == "__main__":
    main(*sys.argv[1:])
