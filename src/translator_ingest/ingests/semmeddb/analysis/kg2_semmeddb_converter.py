"""Rules by which KG2.10.3 turned SemMedDB rows into the edges whose ids are the records' kg2_ids.

KG2.10.3 (RTXteam/RTX-KG2, tag KG2.10.3) groups SemMedDB predications by the raw
``SUBJECT_CUI, PREDICATE, OBJECT_CUI`` strings (``extract/semmeddb_mysql_to_tuplelist_jsonl.py``
lines 50-54), and ``convert/semmeddb_tuplelist_json_to_kg_jsonl.py`` turns each group into edges.
This module reproduces the parts of that pipeline that decide which kg2_id a SemMedDB row can be
the evidence of, cited by upstream file and line:

- converter ``get_remapped_cuis`` (38-55): MRCUI rows with REL ``SY``, current ``Y`` and a
  non-empty new CUI;
- converter ``get_cui_if_exists`` (117-127): a field's first id is a CUI when
  ``.upper().startswith('C')``, and is then remapped; the other ids of such a field only make
  ``xref`` edges (130-138), which carry no predication of their own and get no relation here;
- converter ``get_rels_to_make_for_row`` (141-190), every branch: each side is its CUI or all of
  its gene ids, the relations are the cross product, and a row with several genes on both sides
  makes none;
- converter ``EDGE_LABELS_EXCLUDE_FOR_LOOPS`` (28, applied at 300-302): no self-loop for those
  predicates; ``make_rel`` lowercases the predicate into the kg2_id (89-90);
- negated rows are never evidence. The converter strips ``NEG_`` (27, 281-285), but
  ``kg2_util.make_edge`` builds the id (726) before ``make_rel`` sets ``negated`` (99), so a NEG_
  group and its positive twin share an id. ``process/merge_graphs.py`` keeps the first edge per id
  (96-99), and ``process/run-simplify.sh`` (28) runs ``filter_kg_and_remap_predicates.py
  --dropNegated``, which drops negated edges (207). A kept record's evidence therefore always comes
  from a positive group, so NEG_ rows are dropped here instead of stripped.

It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

import re
from pathlib import Path

import polars as pl

# MRCUI.RRF rows end with a separator, which reads as one extra empty column.
MRCUI_COLUMNS: list[str] = ["CUI1", "VER", "REL", "RELA", "MAPREASON", "CUI2", "MAPIN", "TRAILING"]
MRCUI_SEPARATOR = "|"
MRCUI_SYNONYM_RELATION = "SY"
MRCUI_IS_CURRENT = "Y"

# A kg2_id is subject---SEMMEDDB:predicate---q1---q2---q3---object---SEMMEDDB: (seven parts). The
# shapes below are the ones the 21,355,695 kg2_ids of kg2.10.3-semmeddb-edges.jsonl.gz have: every
# end is UMLS:C and 7 digits or NCBIGene: and digits, the predicate is one of
# KG2_SEMMEDDB_PREDICATES (make_rel lowercased it), and the three qualifier parts are
# KG2_ID_QUALIFIERS for the predicate.
KG2_ID_SEPARATOR = "---"
KG2_ID_PART_COUNT = 7
SEMMEDDB_PREDICATE_PREFIX = "SEMMEDDB:"
KG2_ID_SOURCE = "SEMMEDDB:"
KG2_ID_END_PATTERN = re.compile(r"UMLS:C[0-9]{7}|NCBIGene:[0-9]+")
# The SemMedDB predicates a KG2.10.3 edge can carry: the lowercase SEMMEDDB:* keys of
# maps/predicate-remap.yaml at tag KG2.10.3 (lines 4317-4548; the last entry, SEMMEDDB:xref, has
# its operation: keep on 4547) whose operation is keep or invert.
# process/filter_kg_and_remap_predicates.py drops an edge whose predicate is delete (lines 233-234:
# 1532, measurement_of, method_of, nom, prep, verb) and stops the KG2 build on a source predicate
# the yaml does not list (warning_source_predicate_curies_not_in_config, lines 108-121, exit(1)
# at 121, called at 327). These are exactly the 31 predicates of the kg2_ids of
# kg2.10.3-semmeddb-edges.jsonl.gz.
KG2_SEMMEDDB_PREDICATES: frozenset[str] = frozenset(
    {
        "administered_to",
        "affects",
        "associated_with",
        "augments",
        "causes",
        "coexists_with",
        "compared_with",
        "complicates",
        "converts_to",
        "diagnoses",
        "disrupts",
        "higher_than",
        "inhibits",
        "interacts_with",
        "isa",
        "location_of",
        "lower_than",
        "manifestation_of",
        "measures",
        "occurs_in",
        "part_of",
        "precedes",
        "predisposes",
        "prevents",
        "process_of",
        "produces",
        "same_as",
        "stimulates",
        "treats",
        "uses",
        "xref",
    }
)
# The qualifier parts (qualified predicate, object aspect, object direction) of each SemMedDB
# predicate KG2.10.3 qualifies: maps/predicate-remap.yaml at tag KG2.10.3, entries
# SEMMEDDB:augments (4344-4350), SEMMEDDB:inhibits (4406-4412) and SEMMEDDB:stimulates
# (4520-4526). Every other predicate has NO_QUALIFIERS. These are exactly the three qualified
# combinations in the kg2_ids of kg2.10.3-semmeddb-edges.jsonl.gz (705,796, 1,032,294 and
# 1,175,331 ids).
KG2_ID_QUALIFIERS: dict[str, tuple[str, str, str]] = {
    "augments": ("biolink:causes", "activity_or_abundance", "increased"),
    "inhibits": ("biolink:causes", "activity", "decreased"),
    "stimulates": ("biolink:causes", "activity", "increased"),
}
NO_QUALIFIERS: tuple[str, str, str] = ("None", "None", "None")
CURIE_SEPARATOR = ":"
UMLS_PREFIX = "UMLS:"
NCBIGENE_PREFIX = "NCBIGene:"
# A SemMedDB id field holds an optional CUI followed by Entrez gene ids, e.g. "C0796614|931".
SEMMEDDB_ID_SEPARATOR = "|"
CUI_INITIAL = "C"
NEGATION_PATTERN = r"^NEG_"
LOOP_EXCLUDED_PREDICATES: list[str] = ["same_as", "higher_than", "lower_than", "different_from", "compared_with"]


# The (subject, SemMedDB predicate, object) a KG2 SemMedDB edge id encodes.
Kg2Ends = tuple[str, str, str]


def parse_kg2_id(kg2_id: object) -> Kg2Ends | None:
    """Split a KG2 SemMedDB edge id into ``(subject, SemMedDB predicate, object)``, None if malformed.

    The id must have a shape KG2 writes: seven parts, both ends matching ``KG2_ID_END_PATTERN``,
    ``SEMMEDDB:`` and one of ``KG2_SEMMEDDB_PREDICATES`` (``make_rel`` lowercased them, so the
    predicate is not lowercased here), exactly the predicate's ``KG2_ID_QUALIFIERS``
    (``NO_QUALIFIERS`` for most), and ``SEMMEDDB:`` as the last part.
    Any other id, or one that is not a string (a JSON null, say), was not made by the converter, so
    no SemMedDB row can match it and it must not vote on orientation.

    >>> parse_kg2_id(
    ...     "NCBIGene:1---SEMMEDDB:administered_to---None---None---None---UMLS:C0007634---SEMMEDDB:"
    ... )
    ('NCBIGene:1', 'administered_to', 'UMLS:C0007634')
    >>> print(parse_kg2_id("A---B"), parse_kg2_id(None))
    None None
    >>> print(parse_kg2_id("UMLS:C0000001---SEMMEDDB:PART_OF---None---None---None---UMLS:C0000002---SEMMEDDB:"))
    None
    >>> print(parse_kg2_id("UMLS:C0000001---SEMMEDDB:inhibits---None---None---None---UMLS:C0000002---SEMMEDDB:"))
    None
    >>> print(parse_kg2_id(
    ...     "UMLS:C0000001---SEMMEDDB:unknown_predicate---None---None---None---UMLS:C0000002---SEMMEDDB:"
    ... ))
    None
    """
    if not isinstance(kg2_id, str):
        return None
    parts = kg2_id.split(KG2_ID_SEPARATOR)
    if len(parts) != KG2_ID_PART_COUNT:
        return None
    subject, predicate_part, *qualifiers, obj, source = parts
    predicate = predicate_part.removeprefix(SEMMEDDB_PREDICATE_PREFIX)
    well_formed = (
        predicate_part.startswith(SEMMEDDB_PREDICATE_PREFIX)
        and predicate in KG2_SEMMEDDB_PREDICATES
        and tuple(qualifiers) == KG2_ID_QUALIFIERS.get(predicate, NO_QUALIFIERS)
        and all(KG2_ID_END_PATTERN.fullmatch(end) for end in (subject, obj))
        and source == KG2_ID_SOURCE
    )
    return (subject, predicate, obj) if well_formed else None


def negated(column: str) -> pl.Expr:
    """Return whether a raw SemMedDB PREDICATE is a negation (``NEG_`` prefix, as ``NEG_REGEX``).

    >>> pl.DataFrame({"p": ["TREATS", "NEG_TREATS", "NEG_lower_than", "neg_isa"]}).select(
    ...     negated("p")
    ... ).to_series().to_list()
    [False, True, True, False]
    """
    return pl.col(column).str.contains(NEGATION_PATTERN)


def load_cui_remap(mrcui_file: Path) -> tuple[pl.DataFrame, dict[str, int]]:
    """Load the retired-CUI remap as ``get_remapped_cuis`` built it (KG2.10.3 lines 38-55).

    Rows with REL ``SY``, current ``Y`` and a non-empty new CUI; a later row for the same retired
    CUI wins, as in the upstream dict. Returns ``retired_cui -> current_cui`` and counts.
    """
    mrcui = pl.read_csv(
        mrcui_file,
        glob=False,
        separator=MRCUI_SEPARATOR,
        has_header=False,
        new_columns=MRCUI_COLUMNS,
        quote_char=None,
        infer_schema=False,
        empty_string_is_null=False,
    )
    remap = (
        mrcui.filter(
            (pl.col("REL") == MRCUI_SYNONYM_RELATION) & (pl.col("MAPIN") == MRCUI_IS_CURRENT) & (pl.col("CUI2") != "")
        )
        .select(pl.col("CUI1").alias("retired_cui"), pl.col("CUI2").alias("current_cui"))
        .unique(subset="retired_cui", keep="last", maintain_order=True)
        .sort("retired_cui")
    )
    stats = {
        "mrcui_rows": mrcui.height,
        "remap_retired_cuis": remap.height,
        "remap_targets_that_are_retired_too": remap.filter(
            pl.col("current_cui").is_in(remap["retired_cui"].implode())
        ).height,
    }
    return remap, stats


def side_curies(frame: pl.DataFrame, side: str, cui_remap: pl.DataFrame) -> pl.DataFrame:
    """Add ``{side}_curies``, the KG2 ids one side of a SemMedDB row stands for, and ``{side}_remapped``.

    As ``get_cui_if_exists`` (KG2.10.3 lines 117-127) and ``get_rels_to_make_for_row`` use them:
    a field whose first id starts with ``C`` (any case) stands for that CUI, remapped, and its gene
    ids only make xrefs; any other field stands for all of its ids as NCBIGene ids.
    """
    ids = pl.col(f"{side}_cui").str.split(SEMMEDDB_ID_SEPARATOR)
    is_cui = pl.col("_first").str.to_uppercase().str.starts_with(CUI_INITIAL)
    remap = cui_remap.select(pl.col("retired_cui").alias("_first"), pl.col("current_cui").alias("_current"))
    return (
        frame.with_columns(ids.alias("_ids"), ids.list.first().alias("_first"))
        .join(remap, on="_first", how="left", maintain_order="left")
        .with_columns(
            pl.when(is_cui)
            .then(pl.concat_list(pl.lit(UMLS_PREFIX) + pl.coalesce("_current", "_first")))
            .otherwise(pl.col("_ids").list.eval(pl.lit(NCBIGENE_PREFIX) + pl.element()))
            .alias(f"{side}_curies"),
            (is_cui & pl.col("_current").is_not_null()).alias(f"{side}_remapped"),
        )
        .drop("_ids", "_first", "_current")
    )


def kg2_relations(frame: pl.DataFrame, cui_remap: pl.DataFrame) -> pl.DataFrame:
    """Explode SemMedDB rows into the KG2 relations (non-xref) they can be the kept evidence of.

    Drops negated rows, lowercases ``predicate`` into ``semmed_predicate``, then mirrors every
    branch of ``get_rels_to_make_for_row`` as the cross product of the two sides from
    ``side_curies``, none when both sides hold several genes, and drops the self-loops that
    ``EDGE_LABELS_EXCLUDE_FOR_LOOPS`` excludes. Replaces ``subject_cui``/``object_cui`` with
    ``kg2_subject``/``kg2_object`` and adds ``remapped``.

    >>> remap = pl.DataFrame({"retired_cui": ["C0000002"], "current_cui": ["C0007404"]})
    >>> rows = pl.DataFrame({
    ...     "row": [1, 2, 3, 4, 5, 6, 7, 8],
    ...     "subject_cui": ["C1", "C0000002|5", "C1", "2048|5594", "6", "6|9", "C1", "C1"],
    ...     "object_cui": ["C2", "C2", "7|8", "C3", "7|8", "7|8", "C1", "C2"],
    ...     "predicate": ["TREATS", "TREATS", "AFFECTS", "AFFECTS", "INTERACTS_WITH", "INTERACTS_WITH",
    ...                   "SAME_AS", "NEG_TREATS"],
    ... })
    >>> for row in kg2_relations(rows, remap).iter_rows():
    ...     print(row)
    (1, 'treats', 'UMLS:C1', 'UMLS:C2', False)
    (2, 'treats', 'UMLS:C0007404', 'UMLS:C2', True)
    (3, 'affects', 'UMLS:C1', 'NCBIGene:7', False)
    (3, 'affects', 'UMLS:C1', 'NCBIGene:8', False)
    (4, 'affects', 'NCBIGene:2048', 'UMLS:C3', False)
    (4, 'affects', 'NCBIGene:5594', 'UMLS:C3', False)
    (5, 'interacts_with', 'NCBIGene:6', 'NCBIGene:7', False)
    (5, 'interacts_with', 'NCBIGene:6', 'NCBIGene:8', False)
    """
    positive = (
        frame.filter(negated("predicate").not_())
        .with_columns(pl.col("predicate").str.to_lowercase())
        .rename({"predicate": "semmed_predicate"})
    )
    sides = side_curies(side_curies(positive, "subject", cui_remap), "object", cui_remap)
    both_sides_several = (pl.col("subject_curies").list.len() > 1) & (pl.col("object_curies").list.len() > 1)
    excluded_loop = (pl.col("kg2_subject") == pl.col("kg2_object")) & pl.col("semmed_predicate").is_in(
        LOOP_EXCLUDED_PREDICATES
    )
    return (
        sides.filter(both_sides_several.not_())
        .explode("subject_curies")
        .explode("object_curies")
        .rename({"subject_curies": "kg2_subject", "object_curies": "kg2_object"})
        .filter(excluded_loop.not_())
        .with_columns((pl.col("subject_remapped") | pl.col("object_remapped")).alias("remapped"))
        .drop("subject_cui", "object_cui", "subject_remapped", "object_remapped")
    )
