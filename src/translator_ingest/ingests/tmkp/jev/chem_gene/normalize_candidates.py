"""
Merge repair candidates that are the same concept under different ids (UniProtKB:P05112, NCBIGene:3565 and
PR:000001391 are all IL-4), using NodeNorm with gene/protein conflation, and drop candidates that are the concept
being replaced. Keeps one representative per concept: a reviewed UniProt protein, else a PR entry, else a chemical
id, else whatever NodeNorm prefers. UniProt isoform ids (with a "-") are dropped when the concept has another id.

Only human UniProt proteins (those of HGNC genes) are kept: everything maps to human, as in TMKP's design.

    python normalize_candidates.py candidates.jsonl out.jsonl hgnc_complete_set.txt
"""
import csv
import json
import sys
import urllib.request

HUMAN: set[str] = set()
NODENORM = "https://nodenormalization-sri.renci.org/get_normalized_nodes"
PREFERENCE = ["UniProtKB", "PR", "CHEBI", "DRUGBANK", "PUBCHEM.COMPOUND", "NCBIGene"]


def normalize(curies: list[str]) -> dict[str, dict | None]:
    """NodeNorm results for curies, in batches; failures raise."""
    out = {}
    for i in range(0, len(curies), 1000):
        body = json.dumps({"curies": curies[i:i + 1000], "conflate": True, "drug_chemical_conflate": False}).encode()
        req = urllib.request.Request(NODENORM, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=300) as resp:
            out.update(json.load(resp))
    return out


def rank(curie: str) -> tuple[int, int]:
    """Sort key for choosing a concept's representative id."""
    prefix = curie.split(":")[0]
    return (PREFERENCE.index(prefix) if prefix in PREFERENCE else len(PREFERENCE), "-" in curie)


def main(in_path: str, out_path: str, hgnc_path: str) -> None:
    """Write candidates grouped by normalized concept."""
    with open(hgnc_path) as f:
        HUMAN.update(f"UniProtKB:{u}" for row in csv.DictReader(f, delimiter="\t")
                     for u in row["uniprot_ids"].split("|") if u)
    with open(in_path) as f:
        rows = [json.loads(line) for line in f]
    curies = sorted({c["curie"] for r in rows for c in r["candidates"]} | {r["curie"] for r in rows})
    norm = normalize(curies)

    def concept(curie: str) -> tuple[str, str | None, list[str]]:
        n = norm.get(curie)
        if not n:
            return curie, None, []
        return n["id"]["identifier"], n["id"].get("label"), n.get("type", [])[:1]

    with open(out_path, "w") as out:
        for r in rows:
            replaced = concept(r["curie"])[0]
            groups: dict[str, list] = {}
            for c in r["candidates"]:
                cid, label, types = concept(c["curie"])
                if cid == replaced:
                    continue
                groups.setdefault(cid, []).append({**c, "concept": cid, "concept_label": label, "concept_type": types})
            merged = []
            for cid, members in groups.items():
                best = min(members, key=lambda c: rank(c["curie"]))
                merged.append({"curie": best["curie"], "concept": cid, "label": best["label"],
                               "concept_label": best["concept_label"],
                               "type": (best["concept_type"] or [best["type"]])[0].removeprefix("biolink:"),
                               "sources": sorted({m["source"] for m in members})})
            # Everything maps to human (TMKP's design): keep only UniProt proteins of human (HGNC) genes
            merged = [c for c in merged if not c["curie"].startswith("UniProtKB:") or c["curie"].split("-")[0] in HUMAN]
            out.write(json.dumps({**r, "candidates": merged}) + "\n")
    print(f"normalized candidates for {len(rows)} pairs")


if __name__ == "__main__":
    main(*sys.argv[1:])
