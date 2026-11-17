# /// script
# requires-python = ">=3.12"
# dependencies = ["typesafe-sdk==0.7.2"]
# ///
"""
Ask Jev the two per-item questions for TMKP chemical -> protein sentences, resumably:

- pairs: the mapping question, once per distinct (mention, concept) pair (input: build_pairs.py output)
- relation: the relation question, once per sentence (input: extract_sentences.py output)

Items already in the output are skipped. Failed calls go to the errors file (rewritten each run) and are retried on
the next run. A chunk in which most calls fail stops the run.

    TYPESAFE_API_KEY="$(security find-generic-password -s typesafe -w)" \\
        uv run score.py pairs chem_gene_pairs.jsonl chem_gene_pair_scores.jsonl pair_errors.jsonl
    TYPESAFE_API_KEY="$(security find-generic-password -s typesafe -w)" \\
        uv run score.py relation chem_gene_sentences.jsonl chem_gene_relation_scores.jsonl relation_errors.jsonl
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path

from typesafe_sdk import TypeSafeClient, TypeSafeError

import jev_client
from prompts import (PAIR_CRITERIA, PAIR_INSTRUCTIONS, PAIR_KIND, RELATION_CRITERIA, RELATION_INSTRUCTIONS,
                     marked)

CHUNK = 4000
KEYS = {"pairs": ("role", "key", "curie"), "relation": ("edge_id", "sentence_id")}


def ask_pair(client: TypeSafeClient, pair: dict) -> dict:
    """The mapping question for one (mention, concept) pair, shown with its example sentences."""
    state = {"mention": pair["mention"], "concept": pair["name"] or pair["curie"],
             "examples": [marked(e) for e in pair["examples"]]}
    answer = jev_client.choose(client, state, PAIR_INSTRUCTIONS.format(kind=PAIR_KIND[pair["role"]]), PAIR_CRITERIA)
    return {"role": pair["role"], "key": pair["key"], "curie": pair["curie"], "count": pair["count"],
            "choice": answer["choice"], "probabilities": answer["probabilities"], "model": answer["model"],
            "input_tokens": answer["input_tokens"]}


def ask_relation(client: TypeSafeClient, s: dict) -> dict:
    """The relation question for one sentence."""
    state = {"text": s["text"], "regulator": f'{s["subject_mention"]} [{s["subject_name"]}]',
             "target": f'{s["object_mention"]} [{s["object_name"]}]'}
    answer = jev_client.choose(client, state, RELATION_INSTRUCTIONS, RELATION_CRITERIA)
    return {"edge_id": s["edge_id"], "sentence_id": s["sentence_id"],
            "relation": {"choice": answer["choice"], "probabilities": answer["probabilities"]},
            "model": answer["model"], "input_tokens": answer["input_tokens"]}


def main() -> None:
    """Score every item of the input not yet in the output."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=sorted(KEYS))
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("errors", type=Path)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--limit", type=int, help="score at most this many items (for a trial run)")
    args = parser.parse_args()
    fields = KEYS[args.mode]
    client = jev_client.connect()
    done = set()
    if args.output.exists():
        with open(args.output) as f:
            done = {tuple(r[k] for k in fields) for r in map(json.loads, f)}
    print(f"already scored: {len(done)}", flush=True)
    question = ask_pair if args.mode == "pairs" else ask_relation

    def ask(item: dict) -> dict:
        try:
            return question(client, item)
        except TypeSafeError as e:
            return {**{k: item[k] for k in fields}, "error": f"{type(e).__name__}: {e}"}

    with open(args.input) as source:
        todo = (item for item in map(json.loads, source) if tuple(item[k] for k in fields) not in done)
        if args.limit:
            todo = islice(todo, args.limit)
        start, n_ok, n_err, tokens = time.time(), 0, 0, 0
        with (ThreadPoolExecutor(max_workers=args.concurrency) as pool, open(args.output, "a") as out,
              open(args.errors, "w") as err):
            while chunk := list(islice(todo, CHUNK)):
                chunk_err = 0
                for result in pool.map(ask, chunk):
                    if "error" in result:
                        err.write(json.dumps(result) + "\n")
                        chunk_err += 1
                    else:
                        out.write(json.dumps(result) + "\n")
                        tokens += result["input_tokens"]
                n_ok += len(chunk) - chunk_err
                n_err += chunk_err
                out.flush()
                err.flush()
                print(f"scored {n_ok} errors {n_err} rate {(n_ok + n_err) / (time.time() - start):.0f}/s "
                      f"input_tokens {tokens} cost ${tokens / 1e6 * 0.042:.2f}", flush=True)
                if chunk_err > len(chunk) / 2:
                    raise SystemExit(f"stopping: {chunk_err}/{len(chunk)} calls failed in the last chunk")


if __name__ == "__main__":
    main()
