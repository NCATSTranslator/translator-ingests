# /// script
# requires-python = ">=3.12"
# dependencies = ["typesafe-sdk==0.7.2"]
# ///
"""
Per-sentence isoform choice for "CPT1" protein mentions (CPT1A liver, CPT1B muscle and heart, CPT1C brain, or
none), from each sentence's context. build_repairs.py accepts a choice at P >= 0.7; it overrides the pair-level
verdict for these sentences.

    TYPESAFE_API_KEY="$(security find-generic-password -s typesafe -w)" \\
        uv run choose_cpt1.py chem_gene_sentences.jsonl chem_gene_cpt1_choices.jsonl
"""

import json
import sys
from concurrent.futures import ThreadPoolExecutor

from typesafe_sdk import TypeSafeClient, TypeSafeError

import jev_client
from prompts import CPT1_CRITERIA, CPT1_INSTRUCTIONS, CPT1_UNIPROT
from rules import norm

KEYS = {"cpt1", "cpt1a", "cpt1b", "cpt1c"}


def ask(client: TypeSafeClient, s: dict) -> dict:
    """One Choice about one sentence."""
    a, b = s["object_span"]
    text = f'{s["text"][:a]}«{s["text"][a:b]}»{s["text"][b:]}'
    key = {"edge_id": s["edge_id"], "sentence_id": s["sentence_id"], "object": s["object"]}
    try:
        answer = jev_client.choose(client, {"text": text, "mention": s["object_mention"]}, CPT1_INSTRUCTIONS,
                                   CPT1_CRITERIA)
    except TypeSafeError as e:
        return {**key, "error": f"{type(e).__name__}: {e}"}
    return {**key, "mention": s["object_mention"], "choice": answer["choice"],
            "curie": CPT1_UNIPROT.get(answer["choice"]), "probability": answer["probabilities"][answer["choice"]],
            "probabilities": answer["probabilities"]}


def main(sentences_path: str, out_path: str) -> None:
    """Ask about every sentence whose protein mention is CPT1 (any isoform spelling)."""
    with open(sentences_path) as f:
        rows = [s for s in map(json.loads, f) if s["object_span"] and norm(s["object_mention"]) in KEYS]
    client = jev_client.connect()
    with ThreadPoolExecutor(max_workers=16) as pool, open(out_path, "w") as out:
        results = list(pool.map(lambda s: ask(client, s), rows))
        for r in results:
            out.write(json.dumps(r) + "\n")
    print(f"asked about {len(rows)} sentences; {sum('error' in r for r in results)} errors")


if __name__ == "__main__":
    main(*sys.argv[1:])
