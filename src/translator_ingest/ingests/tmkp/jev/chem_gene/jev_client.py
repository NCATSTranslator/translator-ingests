"""
Thin wrapper over the TypeSafe SDK for the chemical -> protein scripts: one connected client, and one- or
multi-question calls that return plain dicts. Needs TYPESAFE_API_KEY (fill it from a secret store for the one
command; see ../README.md).
"""

import os
from typing import Any

from typesafe_sdk import Choice, RetryPolicy, TypeSafeClient

MODEL = "jev-1.13.0"


def connect() -> TypeSafeClient:
    """A client for MODEL. No SDK retries: a failed call is recorded and retried by rerunning the script."""
    return TypeSafeClient(api_key=os.environ["TYPESAFE_API_KEY"], model=MODEL, timeout=30,
                          retry=RetryPolicy(max_retries=0))


def ask(client: TypeSafeClient, state: dict, questions: dict[str, tuple[str, dict]]) -> dict[str, Any]:
    """
    Several Choice questions ({id: (instructions, criteria)}) about one state, in one request.
    Returns {"answers": {id: {"choice", "probabilities"}}, "model", "input_tokens"}.
    """
    response = client.system_one(state=state, questions={
        qid: Choice(instructions=instructions, criteria=criteria) for qid, (instructions, criteria) in questions.items()})
    return {"answers": {qid: {"choice": a.choice, "probabilities": dict(a.probabilities)}
                        for qid, a in response.answers.items()},
            "model": response.model, "input_tokens": response.usage.input_tokens}


def choose(client: TypeSafeClient, state: dict, instructions: str, criteria: dict) -> dict[str, Any]:
    """One Choice question. Returns {"choice", "probabilities", "model", "input_tokens"}."""
    result = ask(client, state, {"q": (instructions, criteria)})
    return {**result["answers"]["q"], "model": result["model"], "input_tokens": result["input_tokens"]}
