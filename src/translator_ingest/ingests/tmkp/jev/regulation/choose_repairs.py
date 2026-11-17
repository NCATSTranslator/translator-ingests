# /// script
# requires-python = ">=3.12"
# dependencies = ["typesafe-sdk==0.7.2"]
# ///
"""
For each wrongly mapped (mention, concept) pair, ask Jev which candidate the mention actually means, or none, from
the pair's example sentences. Pairs flagged by the HGNC symbol hint also get a "keep the current mapping" option.
"none" is listed first: Jev leans toward the first option, and leaving a pair dropped is the cheaper error.
build_repairs.py accepts a choice at P >= 0.9 (P >= 0.7 for symbol-hint pairs).

    TYPESAFE_API_KEY="$(security find-generic-password -s typesafe -w)" \\
        uv run choose_repairs.py chem_gene_repair_candidates_norm.jsonl chem_gene_pairs.jsonl chem_gene_repair_choices.jsonl

A failed call is recorded in the output with an "error" field; a rerun asks every pair again (about $0.15).
"""

import json
import sys
from concurrent.futures import ThreadPoolExecutor

from typesafe_sdk import TypeSafeClient, TypeSafeError

import jev_client
from prompts import HINT_INSTRUCTIONS, REPAIR_INSTRUCTIONS, REPAIR_NONE, marked


def ask(client: TypeSafeClient, row: dict, pair: dict) -> dict:
    """One Choice among none, (for symbol-hint pairs) keeping the current mapping, and the candidates."""
    options = {f"c{i + 1}": c for i, c in enumerate(row["candidates"])}
    criteria = {"none": REPAIR_NONE}
    if row.get("symbol_hint"):
        criteria["keep"] = {"what": f'The current mapping is right: {row["name"] or row["curie"]} ({row["curie"]})'}
    criteria |= {k: {"what": f'{c["concept_label"] or c["label"]} ({c["type"]}; {c["curie"]})'}
                 for k, c in options.items()}
    examples = [marked(e) for e in pair["examples"]]
    if row.get("symbol_hint"):
        state = {"mention": row["mention"], "current_concept": row["name"] or row["curie"], "examples": examples}
        instructions = HINT_INSTRUCTIONS
    else:
        state = {"mention": row["mention"], "wrong_concept": row["name"] or row["curie"], "examples": examples}
        instructions = REPAIR_INSTRUCTIONS
    key = {"role": row["role"], "key": row["key"], "curie": row["curie"], "symbol_hint": bool(row.get("symbol_hint"))}
    try:
        answer = jev_client.choose(client, state, instructions, criteria)
    except TypeSafeError as e:
        return {**key, "error": f"{type(e).__name__}: {e}"}
    return {**key, "mention": row["mention"], "count": row["count"], "choice": answer["choice"],
            "chosen": options.get(answer["choice"]), "probability": answer["probabilities"][answer["choice"]],
            "probabilities": answer["probabilities"], "candidates": row["candidates"],
            "input_tokens": answer["input_tokens"]}


def main(candidates_path: str, pairs_path: str, out_path: str) -> None:
    """Ask about every pair that has a candidate or a symbol hint."""
    with open(candidates_path) as f:
        rows = [r for r in map(json.loads, f) if r["candidates"] or r.get("symbol_hint")]
    wanted = {(r["role"], r["key"], r["curie"]) for r in rows}
    with open(pairs_path) as f:
        pairs = {k: p for p in map(json.loads, f) if (k := (p["role"], p["key"], p["curie"])) in wanted}
    client = jev_client.connect()
    with ThreadPoolExecutor(max_workers=16) as pool, open(out_path, "w") as out:
        results = list(pool.map(lambda r: ask(client, r, pairs[(r["role"], r["key"], r["curie"])]), rows))
        for result in results:
            out.write(json.dumps(result) + "\n")
    print(f"asked about {len(rows)} pairs; {sum('error' in r for r in results)} errors recorded in the output")


if __name__ == "__main__":
    main(*sys.argv[1:])
