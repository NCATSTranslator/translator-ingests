"""
The question put to Jev for every supporting sentence of a TMKP treats edge, and how its answer is read.

This is prompt version 3, the one used to build tmkp_treats_jev_v3_dropped_edges.txt.gz. Jev answers a Choice
question with a probability for each option. A sentence is bad when the probabilities of the BAD_CHOICES add up to at
least BAD_THRESHOLD; an edge is dropped when all of its sentences are bad.
"""

from typing import Any

MODEL = "jev-1.13.0"
BAD_CHOICES = ("other", "cell_line", "causes")
BAD_THRESHOLD = 0.7

INSTRUCTIONS = (
    "In `text`, is `drug` used, given or tested to treat or prevent `condition` itself? `drug` and `condition` give "
    "the words used in `text` and, in brackets, the concept they were mapped to. Judge only what `text` says. A "
    "drug applied to cells or animals as a tool to study a mechanism is not being tested as a treatment."
)

# Option order matters: Jev leans toward earlier options, so the options that drop a sentence come first.
CRITERIA = {
    "other": {
        "what": "`text` does not say `drug` is used or tested against `condition` itself. `condition` is only "
        "background: the patients have it, but `drug` treats something else (a complication, a "
        "symptom, a co-infection, side effects, transplant conditioning). Or `drug` is a lab reagent, "
        "solvent, culture ingredient, inducer, carrier or excipient, or the two are only listed together.",
        "examples": [
            "tranexamic acid to reduce bleeding during AML treatment",
            "prednisone as immunosuppression in patients who also had HEV infection",
            "medium supplemented with penicillin/streptomycin",
        ],
    },
    "cell_line": {
        "what": "The only evidence is in vitro: `drug` was applied to cells, cell lines, primary cultures "
        "or tissue samples of `condition` (killing them, slowing growth, changing expression), with "
        "no patients and no live animals in `text`.",
        "examples": [
            "quercetin reduced proliferation of three myeloma cell lines",
            "IPF fibroblasts treated with EGCG",
            "active against Ebola virus in cultured cells",
        ],
    },
    "causes": {
        "what": "`drug` causes, induces or worsens `condition`, or `condition` is an adverse effect, "
        "contraindication or reason for caution with `drug`.",
        "examples": ["zalcitabine-induced hepatic steatosis", "use with caution in kidney disease"],
    },
    "no_effect": {
        "what": "`drug` was tried against `condition` but showed no or minimal benefit.",
        "examples": ["provoked no effect on seizure frequency", "recalcitrant to topical mupirocin"],
    },
    "studied": {
        "what": "`drug` is tested or proposed as a treatment or prevention of `condition` itself in patients "
        "or live animals: a trial, case, cohort, animal model or xenograft, or a stated proposal to "
        "use it for `condition`.",
        "examples": [
            "a phase II trial in PDAC",
            "reduced renal fibrosis in rats",
            "suppressed tumor growth in a mouse xenograft model",
        ],
    },
    "treats": {
        "what": "`text` states `drug` is used for, approved for, or effective at treating or preventing "
        "`condition` itself in patients.",
        "examples": ["doxycycline therapy for leptospirosis", "azathioprine is used to treat CAR"],
    },
}


def sentence_state(sentence: dict[str, Any]) -> dict[str, str]:
    """
    The state Jev judges for one sentence: the text, and each mention followed by the concept it was mapped to.

    >>> sentence_state({"text": "Doxycycline therapy for leptospirosis", "subject_mention": "Doxycycline",
    ...                 "subject_name": "Doxycycline", "object_mention": "leptospirosis",
    ...                 "object_name": "leptospirosis"})["drug"]
    'Doxycycline [Doxycycline]'
    """
    return {
        "text": sentence["text"],
        "drug": f'{sentence["subject_mention"]} [{sentence["subject_name"]}]',
        "condition": f'{sentence["object_mention"]} [{sentence["object_name"]}]',
    }


def p_bad(probabilities: dict[str, float]) -> float:
    """
    Probability that a sentence does not support its treats edge.

    >>> p_bad({"other": 0.5, "cell_line": 0.1, "causes": 0.1, "no_effect": 0.0, "studied": 0.3, "treats": 0.0})
    0.7
    """
    return round(sum(probabilities[c] for c in BAD_CHOICES), 6)
