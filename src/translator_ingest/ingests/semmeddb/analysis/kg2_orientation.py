"""How KG2 oriented each SemMedDB predicate: same as SemMedDB, or subject and object reversed.

KG2 reverses a few SemMedDB predicates (``part_of`` becomes ``biolink:has_part``). A kg2_id keeps
SemMedDB's subject and object, so comparing its ends with the record's subject and object shows the
orientation, but only when both are literal: a record whose ids KG2 canonicalized says nothing.
``derive_orientation`` turns those votes into one orientation per predicate and stops on a conflict.

It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

from collections import Counter

import polars as pl

# How a kg2_id's ends relate to its record's subject and object.
SAME = "same"
FLIPPED = "flipped"
SELF_LOOP = "self_loop"
UNDETERMINED = "undetermined"
ORIENTATIONS: list[str] = [SAME, FLIPPED, SELF_LOOP, UNDETERMINED]


def kg2_orientation(kg2_subject: str, kg2_object: str, record_subject: str, record_object: str) -> str:
    """Classify how a kg2_id's ends relate to its record's subject and object.

    Only literal equality counts. KG2 canonicalized some record ids, and such a record says
    nothing about orientation, so it is ``undetermined``.

    >>> kg2_orientation("A", "B", "A", "B")
    'same'
    >>> kg2_orientation("A", "B", "B", "A")
    'flipped'
    >>> kg2_orientation("A", "A", "A", "A")
    'self_loop'
    >>> kg2_orientation("UMLS:C1", "B", "MONDO:1", "B")
    'undetermined'
    """
    if kg2_subject == kg2_object or record_subject == record_object:
        return SELF_LOOP
    if (kg2_subject, kg2_object) == (record_subject, record_object):
        return SAME
    if (kg2_subject, kg2_object) == (record_object, record_subject):
        return FLIPPED
    return UNDETERMINED


def derive_orientation(orientation_counts: Counter[tuple[str, str]]) -> dict[str, bool]:
    """Decide per SemMedDB predicate whether KG2 reversed subject and object (``True``).

    Only ``same`` and ``flipped`` kg2_ids vote. A predicate with votes both ways raises,
    because the span swap assumes one orientation per predicate. A predicate with no vote is
    left out; matching raises later if such a predicate produces a match.

    >>> derive_orientation(Counter({("part_of", FLIPPED): 3, ("treats", SAME): 2,
    ...                             ("treats", UNDETERMINED): 9, ("xref", SELF_LOOP): 1}))
    {'part_of': True, 'treats': False}
    >>> derive_orientation(Counter({("isa", SAME): 1, ("isa", FLIPPED): 1}))
    Traceback (most recent call last):
    ...
    ValueError: SemMedDB predicates seen both same and flipped in KG2: ['isa']
    """
    predicates = sorted({predicate for predicate, _ in orientation_counts})
    conflicting = [
        predicate
        for predicate in predicates
        if orientation_counts[(predicate, SAME)] and orientation_counts[(predicate, FLIPPED)]
    ]
    if conflicting:
        raise ValueError(f"SemMedDB predicates seen both same and flipped in KG2: {conflicting}")
    return {
        predicate: orientation_counts[(predicate, FLIPPED)] > 0
        for predicate in predicates
        if orientation_counts[(predicate, SAME)] or orientation_counts[(predicate, FLIPPED)]
    }


def orientation_frame(flipped: dict[str, bool]) -> pl.DataFrame:
    """Turn the per-predicate orientation into a joinable ``semmed_predicate, flipped`` frame."""
    return pl.DataFrame(
        {"semmed_predicate": list(flipped), "flipped": list(flipped.values())},
        schema={"semmed_predicate": pl.String(), "flipped": pl.Boolean()},
    )


def orientation_table(
    orientation_counts: Counter[tuple[str, str]], flipped: dict[str, bool]
) -> dict[str, dict[str, int | str | None]]:
    """Report the orientation votes and the derived orientation of every SemMedDB predicate."""
    predicates = sorted({predicate for predicate, _ in orientation_counts})
    table: dict[str, dict[str, int | str | None]] = {}
    for predicate in predicates:
        row: dict[str, int | str | None] = {kind: orientation_counts[(predicate, kind)] for kind in ORIENTATIONS}
        row["derived"] = None if predicate not in flipped else (FLIPPED if flipped[predicate] else SAME)
        table[predicate] = row
    return table
