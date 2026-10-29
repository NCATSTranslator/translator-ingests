# /// script
# requires-python = ">=3.12"
# dependencies = ["typesafe-sdk==0.7.2"]
# ///
"""
Step 2: ask Jev about every sentence and write its answer (choice and per-option probabilities) as JSON lines.

Needs a TypeSafe API key in TYPESAFE_API_KEY. Run with uv so the SDK is installed in a throwaway environment:

    uv run score_sentences.py treats_sentences.jsonl scores.jsonl errors.jsonl --concurrency 32

Resumable: sentences already in the scores file are skipped. A failed call goes to the errors file (rewritten each
run) and is not scored, so rerunning retries it. A chunk in which most calls fail stops the run, since that means
the service or the key is broken.
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path
from typing import Any

from typesafe_sdk import Choice, RetryPolicy, TypeSafeClient, TypeSafeError

from prompt import CRITERIA, INSTRUCTIONS, MODEL, sentence_state

CHUNK = 4000
PRICE_PER_MILLION_INPUT_TOKENS = 0.042  # USD; output tokens are free


def ask(client: TypeSafeClient, sentence: dict[str, Any]) -> dict[str, Any]:
    """One Choice question about one sentence. Service failures are recorded, not raised, so a rerun retries them."""
    key = {"edge_id": sentence["edge_id"], "sentence_id": sentence["sentence_id"]}
    try:
        response = client.system_one(
            state=sentence_state(sentence), questions={"q": Choice(instructions=INSTRUCTIONS, criteria=CRITERIA)}
        )
    except TypeSafeError as e:
        return {**key, "error": f"{type(e).__name__}: {e}"}
    answer = response.answers["q"]
    return {
        **key,
        "choice": answer.choice,
        "probabilities": dict(answer.probabilities),
        "model": response.model,
        "input_tokens": response.usage.input_tokens,
    }


def main() -> None:
    """Score every sentence not yet in the scores file."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sentences", type=Path, help="sentences from extract_sentences.py")
    parser.add_argument("scores", type=Path, help="JSON lines to append answers to")
    parser.add_argument("errors", type=Path, help="JSON lines of failed calls (rewritten each run)")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--limit", type=int, help="score at most this many sentences (for a trial run)")
    args = parser.parse_args()

    # max_retries=0: a failure is recorded and retried on the next run rather than hidden by the SDK
    client = TypeSafeClient(
        api_key=os.environ["TYPESAFE_API_KEY"], model=MODEL, timeout=30, retry=RetryPolicy(max_retries=0)
    )
    done = set()
    if args.scores.exists():
        with open(args.scores) as f:
            done = {(r["edge_id"], r["sentence_id"]) for r in map(json.loads, f)}
    print(f"already scored: {len(done)}", flush=True)

    with open(args.sentences) as source:
        todo = (s for s in map(json.loads, source) if (s["edge_id"], s["sentence_id"]) not in done)
        if args.limit:
            todo = islice(todo, args.limit)
        start, n_ok, n_err, tokens = time.time(), 0, 0, 0
        with (
            ThreadPoolExecutor(max_workers=args.concurrency) as pool,
            open(args.scores, "a") as out,
            open(args.errors, "w") as err,
        ):
            while chunk := list(islice(todo, CHUNK)):
                chunk_err = 0
                for result in pool.map(lambda s: ask(client, s), chunk):
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
                rate = (n_ok + n_err) / (time.time() - start)
                print(
                    f"scored {n_ok} errors {n_err} rate {rate:.0f}/s input_tokens {tokens} "
                    f"cost ${tokens / 1e6 * PRICE_PER_MILLION_INPUT_TOKENS:.2f}",
                    flush=True,
                )
                if chunk_err > len(chunk) / 2:
                    raise SystemExit(f"stopping: {chunk_err}/{len(chunk)} calls failed in the last chunk; see {args.errors}")


if __name__ == "__main__":
    main()
