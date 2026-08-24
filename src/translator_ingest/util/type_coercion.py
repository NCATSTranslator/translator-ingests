"""Coerce raw NDJSON record values onto the types the Biolink Model expects.

KGX validation runs the ingest output against the pinned ``biolink-model``
(4.4.2), whose Association slots are typed (``p_value``/``adjusted_p_value`` are
``float``, qualifier and enum slots are constrained strings). Source NDJSON
records, however, carry statistics as free-form strings - often in scientific
notation (``"1.234e-08"``, a Tablassert output convention) and under a zoo of
column spellings ("p value", "P.Value", "padj", "adj.P.Val", "FDR", ...).

This module ports the column-name classifiers and value coercion tables proven
in Tablassert (``tablassert.coerce``) into the ingest repo, reworked from
polars LazyFrames onto plain record dicts:

* ``pvalue_target`` / ``effect_size_target`` / ``effect_type_target`` /
  ``study_size_target`` / ``coerced_target`` - separator-insensitive
  column-name classification onto canonical Biolink slot names.
* ``parse_optional_float`` / ``parse_optional_int`` - numeric coercion that
  accepts scientific notation and silently drops non-numeric artifacts
  (leaked headers, tissue labels).
* ``map_effect_type_value`` - raw metric labels ("Spearman Correlation",
  "OR", "log2FC") onto canonical ``EffectTypeEnum`` tokens.
* ``significance_qualifier`` - p-value banding onto
  ``StatisticalSignificanceQualifierEnum`` values.
* ``coerce_record_types`` - one-call coercion of an edge record onto its
  typed slots.

Slots that postdate the pinned model - ``effect_size`` / ``effect_type``
(Biolink PR #1774) and ``statistical_significance_qualifier`` (PR #1766) - are
declared locally in ``CUSTOM_SLOT_FIELDS`` and attached to the chosen
Association class by ``custom_association_class``. The extension is driven by
a "slot missing from the base class" check, so it self-retires once the pinned
biolink-model release declares the slots natively: upgrade the pin and the
overrides disappear without any code change.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Any, Literal, NamedTuple, Optional

from pydantic import create_model


from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

# A handful of records carry non-numeric artifacts in the stat columns (e.g. a
# leaked header "Adjusted P-value" or tissue labels like "Liver: Lactate"). Only
# values matching a real number are converted; everything else is dropped.
# Scientific notation ("1.234e-08", "5E-3") is accepted by design: Tablassert
# emits p-values as controlled scientific-notation strings.
_NUMERIC_RE = re.compile(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def parse_optional_float(value: Any) -> float | None:
    """Return float(value) only when value is a real number; otherwise None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or not _NUMERIC_RE.fullmatch(text):
        return None
    return float(text)


def parse_optional_int(value: Any) -> int | None:
    """Return int(value) only when value is a real integer; otherwise None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text or not _NUMERIC_RE.fullmatch(text):
        return None
    parsed = float(text)
    if not parsed.is_integer():
        return None
    return int(parsed)


# --- Column-name classification fragments ------------------------------------
# Ported from tablassert.coerce: real-world column labels separate tokens with
# spaces, underscores, hyphens, or dots ("p value", "p_value", "p-value",
# "p.value"); treat any run of these as an optional token separator shared by
# every pattern below.
_SEP: str = r"[\s_.\-]*"
# "value" spelled val / value, optionally plural (vals / values).
_VALUE: str = r"val(?:ue)?s?"
# "adjusted" spelled adj / adjusted.
_ADJUSTED: str = r"adj(?:usted)?"
# Optional trailing numeric qualifier for multi-phenotype outputs ("pvalue1",
# "p_value_2", "padj_1"): a separator-or-nothing then digits.
_NUMQUAL: str = r"(?:[\s_.\-]*\d+)?"

# A P value token: leading "p" then a "value" word across an optional separator,
# with an optional trailing numeric qualifier ("p value", "p-value", "pvalue",
# "p vals", "pvalue1", "p_value_2").
PVALUE_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b p {_SEP} {_VALUE} {_NUMQUAL} \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# A Q value token (Storey's q value); same shape as the P value token.
QVALUE_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b q {_SEP} {_VALUE} {_NUMQUAL} \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# A P value already marked adjusted, with an optional trailing numeric qualifier
# ("padj", "p.adj", "p adjusted", "padj_1").
PADJ_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b p {_SEP} {_ADJUSTED} {_NUMQUAL} \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# A bare "P" standing as its own token (common GWAS convention: "P", "p SMR",
# "gwas p", plus underscore/dot/hyphen-glued forms like "raw_p", "snp_p",
# "p_nominal"). "Its own token" means not glued to a letter or digit, so gene/
# protein and chemistry names ("p53", "p16", "pH", "protein", "phosphate") and
# words merely ending in p ("top value", "group value") stay excluded.
BARE_P_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    r"""
    (?<![A-Za-z0-9]) p (?![A-Za-z0-9])
    """,
    re.IGNORECASE | re.VERBOSE,
)
# Adjustment methods that by themselves imply an adjusted P value, unlike the
# generic words "adjusted"/"corrected" which also modify hazard/odds ratios.
STANDALONE_ADJUSTED_PATTERN: re.Pattern[str] = re.compile(
    r"""
    \b
    (?:
        fdr                       # Benjamini-Hochberg false discovery rate
        | bonferroni              # Bonferroni correction
        | holm                    # Holm-Bonferroni correction
        | false\ discovery\ rate  # FDR spelled out (literal spaces)
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# Generic adjustment words that only imply an adjusted P value when a P/Q value
# token is also present (see pvalue_target).
CONTEXTUAL_ADJUSTED_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b
    (?:
        {_ADJUSTED}    # adj / adjusted
        | corrected    # corrected
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# Stem of "significant"/"significance": a deliberate substring match (no word
# boundary) so every inflection is caught; these columns are categorical flags.
SIGNIFICANCE_FLAG_PATTERN: re.Pattern[str] = re.compile(
    r"""
    significan
    """,
    re.IGNORECASE | re.VERBOSE,
)


def pvalue_target(name: str) -> str | None:
    """Map a column name to its canonical Biolink-compliant target name.

    Returns ``"p_value"``, ``"adjusted_p_value"``, or ``None`` if the name does
    not look like a p/q-value column.

    Tokens are delimiter-anchored so "Group value" / "top value" style
    substrings are not falsely matched. A bare ``"P"`` counts when it stands as
    its own token (delimited by whitespace, ``_``, ``.`` or ``-``), covering
    GWAS conventions like ``"P"``, ``"gwas p"`` and ``"raw_p"``/``"snp_p"`` while
    excluding ``"p53"``/``"pH"``/``"protein"``. ``"padj"``/``"p.adj"`` cover
    DESeq2 conventions. Value/adjusted tokens may carry a trailing numeric
    qualifier (``"pvalue1"``, ``"padj_1"``). ``"adj"``/``"adjusted"``/
    ``"corrected"`` only count alongside a p/q-value token, since they are
    generic words also used for adjusted hazard/odds ratios (unlike
    ``"fdr"``/``"bonferroni"``/``"holm"``). ``"significance"``/``"significant"``
    columns are categorical flags, not the numeric value, so they are excluded
    unless a p/q-value token is also present.
    """
    core_pvalue: bool = bool(PVALUE_TOKEN_PATTERN.search(name)) or bool(BARE_P_TOKEN_PATTERN.search(name))
    core_qvalue: bool = bool(QVALUE_TOKEN_PATTERN.search(name))
    core_padj: bool = bool(PADJ_TOKEN_PATTERN.search(name))
    has_core: bool = core_pvalue or core_qvalue or core_padj

    if SIGNIFICANCE_FLAG_PATTERN.search(name) and not has_core:
        return None

    is_adjusted: bool = (
        core_padj
        or core_qvalue
        or bool(STANDALONE_ADJUSTED_PATTERN.search(name))
        or (bool(CONTEXTUAL_ADJUSTED_PATTERN.search(name)) and has_core)
    )

    if not (has_core or is_adjusted):
        return None
    return "adjusted_p_value" if is_adjusted else "p_value"


# --- Effect-type name fragments ----------------------------------------------
# Labels naming WHICH statistic an effect size is expressed in. Whole-name matches
# cover bare labels ("metric", "effect type"); token matches cover qualified forms
# ("effect size type"). "_" is a word char for \b, so identifier-like names such as
# "metric_value" fall through.
EFFECT_TYPE_EXACT_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    ^
    (?:
        effect {_SEP} type
        | effect {_SEP} metric
        | statistic(?:al)? {_SEP} type
        | metric {_SEP} type
        | metric
    )
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)
EFFECT_TYPE_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b
    (?:
        effect {_SEP} type
        | effect {_SEP} metric
        | effect {_SEP} size {_SEP} type
        | statistic(?:al)? {_SEP} type
        | metric {_SEP} type
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# --- Effect-size fragments -----------------------------------------------------
# Numeric effect/statistic column labels. Whole-name matches cover bare statistic
# tokens ("ES", "OR", "HR", "beta", "rho", "r", plus the old ``relationship_strength``
# name); token matches require a multi-word statistic label, and word-boundary
# anchoring keeps identifier columns out ("correlation_id" fails the trailing \b
# because "_" is a word char).
EFFECT_SIZE_EXACT_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    ^
    (?:
        effect {_SEP} size
        | relationship {_SEP} strength
        | es
        | beta
        | beta {_SEP} coefficients?
        | log2 {_SEP} fc
        | log2 {_SEP} fold {_SEP} change
        | odds {_SEP} ratio
        | or
        | hazard {_SEP} ratio
        | hr
        | risk {_SEP} ratio
        | correlation
        | rho
        | r
    )
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)
EFFECT_SIZE_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b
    (?:
        effect {_SEP} size
        | relationship {_SEP} strength
        | beta {_SEP} coefficients?
        | log2 {_SEP} fc
        | log2 {_SEP} fold {_SEP} change
        | odds {_SEP} ratio
        | hazard {_SEP} ratio
        | risk {_SEP} ratio
        | correlation {_SEP} coefficients?
        | correlation
        | spearman(?:s)? {_SEP} rho
        | pearson(?:s)? {_SEP} r
        | kendall(?:s)? {_SEP} tau
        | rho
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def effect_type_target(name: str) -> str | None:
    """Map effect-type-like column names to the canonical ``effect_type`` slot.

    Returns ``"effect_type"`` when the name looks like an effect-type/metric
    label ("effect type", "effect metric", "statistic type", "metric type",
    bare "metric"), else ``None``.
    """
    if EFFECT_TYPE_EXACT_PATTERN.search(name):
        return "effect_type"
    if EFFECT_TYPE_TOKEN_PATTERN.search(name):
        return "effect_type"
    return None


def effect_size_target(name: str) -> str | None:
    """Map effect-size-like column names to the canonical ``effect_size`` slot.

    Returns ``"effect_size"`` when the name matches any of the effect-size
    patterns, else ``None``.

    Effect-type/metric labels are categorical, not the numeric size, so they
    are excluded here and belong to ``effect_type_target`` (mirroring the
    significance-flag exclusion in ``pvalue_target``). The old
    ``relationship_strength`` name is a candidate, so it is renamed forward to
    ``effect_size``.
    """
    if effect_type_target(name):
        return None
    if EFFECT_SIZE_EXACT_PATTERN.search(name):
        return "effect_size"
    if EFFECT_SIZE_TOKEN_PATTERN.search(name):
        return "effect_size"
    return None


# --- Study-size fragments ----------------------------------------------------
# Population units whose count denotes a study size. Bare "cohort"/"cohort_id"
# still do not match: every pattern requires a count/quantity word alongside.
_UNIT: str = r"samples?|participants?|subjects?|individuals?|patients?|cases?|cohorts?"
# Count nouns following a unit ("sample_count", "participants_n", "cases_size").
_COUNT_WORD: str = r"n|count|number|size"
# Quantity words preceding a unit ("number of samples", "total participants").
_QUANTITY: str = r"n|num|number|count|total"

# Whole-name matches for the canonical study-size labels ("n", "sample_size",
# "study size", "study_n", "n_total", "cohort_size", "supporting_study_size").
STUDY_SIZE_EXACT_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    ^
    (?:
        n
        | total {_SEP} n
        | n {_SEP} total
        | sample {_SEP} size
        | study {_SEP} size
        | study {_SEP} n
        | cohort {_SEP} size
        | supporting {_SEP} study {_SEP} size
    )
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)
# A population unit followed by a count noun ("sample_count", "participant_count",
# "cohort_count", "enrollment_count", "enrolled_count", "samples_n").
STUDY_SIZE_COUNT_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b
    (?:
        {_UNIT}
        | enrollment
        | enrolled
    )
    {_SEP}
    (?: {_COUNT_WORD} )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# A quantity word (optionally followed by "of") before a population unit
# ("number of samples", "num_samples", "n_samples", "total participants").
STUDY_SIZE_PREFIX_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b
    (?: {_QUANTITY} )
    {_SEP}
    (?: of {_SEP} )?
    (?: {_UNIT} )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# A population unit followed by a bare "n" ("samples_n", "participants_n").
STUDY_SIZE_SUFFIX_PATTERN: re.Pattern[str] = re.compile(
    rf"""
    \b
    (?: {_UNIT} )
    {_SEP}
    n
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)
# Whole-name singular labels that unambiguously denote a study size on their own
# ("participants", "enrollment", "enrolled").
STUDY_SIZE_SINGLETON_PATTERN: re.Pattern[str] = re.compile(
    r"""
    ^
    (?:
        participants
        | enrollment
        | enrolled
    )
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)


def study_size_target(name: str) -> str | None:
    """Map study-size-like column names to the canonical ``study_size`` slot.

    Bare ``"n"`` is allowed, but other matches need explicit sample/study-size
    context. Returns ``"study_size"`` on a match, else ``None``.
    """
    if STUDY_SIZE_EXACT_PATTERN.search(name):
        return "study_size"
    if STUDY_SIZE_COUNT_PATTERN.search(name):
        return "study_size"
    if STUDY_SIZE_PREFIX_PATTERN.search(name):
        return "study_size"
    if STUDY_SIZE_SUFFIX_PATTERN.search(name):
        return "study_size"
    if STUDY_SIZE_SINGLETON_PATTERN.search(name):
        return "study_size"
    return None


def coerced_target(name: str) -> str:
    """Map a column name to the canonical slot ``coerce_record_types`` routes it to.

    Classifier order mirrors the Tablassert clean-phase op order:
    ``coerce_pvalue_columns`` runs first, so a p/q-value alias is claimed before
    the study-size and effect classifiers ever see it.
    """
    return (
        pvalue_target(name)
        or effect_size_target(name)
        or effect_type_target(name)
        or study_size_target(name)
        or name
    )


# --- Effect-type value coercion ------------------------------------------------
# Permissible ``effect_type`` values from Biolink PR #1774 (EffectTypeEnum).
# Locally defined because the pinned biolink-model (4.4.2) predates the PR.
# Switch-over: replace this tuple with the installed model's
# ``EffectTypeEnum`` values once the pin includes PR #1774.
EFFECT_TYPE_VALUES: tuple[str, ...] = (
    "cohens_d",
    "correlation_coefficient",
    "eta_squared",
    "glasss_delta",
    "goodman_kruskal_gamma",
    "hazard_ratio",
    "hedges_g",
    "inverse_variance_weighted",
    "kendalls_tau",
    "log2_fold_change",
    "matthews_correlation_coefficient",
    "mr_egger",
    "odds_ratio",
    "omega_squared",
    "pearsons_r",
    "polychoric_correlation",
    "r2_linkage_disequilibrium",
    "regression_coefficient",
    "relative_risk",
    "root_mean_square_standardized_effect",
    "spearmans_rho",
    "standardized_mean_difference",
    "strictly_standardized_mean_difference",
    "wald_ratio",
    "weighted_median",
)

# Raw value aliases -> canonical EffectTypes values (Biolink PR #1774). Keys are
# matched case/separator-insensitively: both sides are lower-cased with every
# separator/apostrophe stripped ("Cohen's d" -> "cohensd").
_EFFECT_TYPE_ALIASES: tuple[tuple[str, str], ...] = (
    ("Cohen's d", "cohens_d"),
    ("odds ratio", "odds_ratio"),
    ("OR", "odds_ratio"),
    ("hazard ratio", "hazard_ratio"),
    ("HR", "hazard_ratio"),
    ("risk ratio", "relative_risk"),
    ("RR", "relative_risk"),
    ("relative risk", "relative_risk"),
    ("Spearman", "spearmans_rho"),
    ("spearman rho", "spearmans_rho"),
    ("spearman's rho", "spearmans_rho"),
    ("Pearson", "pearsons_r"),
    ("pearson r", "pearsons_r"),
    ("Kendall", "kendalls_tau"),
    ("log2FC", "log2_fold_change"),
    ("log2 fold change", "log2_fold_change"),
    ("logFC", "log2_fold_change"),  # limma/edgeR spelling; log-fold-change is base-2 by convention
    ("beta", "regression_coefficient"),
    ("regression coefficient", "regression_coefficient"),
    ("SMD", "standardized_mean_difference"),
    ("eta squared", "eta_squared"),
    ("eta2", "eta_squared"),
    ("omega squared", "omega_squared"),
    ("MCC", "matthews_correlation_coefficient"),
    ("matthews", "matthews_correlation_coefficient"),
    ("wald", "wald_ratio"),
    ("IVW", "inverse_variance_weighted"),
    ("MR-Egger", "mr_egger"),
    ("weighted median", "weighted_median"),
    ("Glass", "glasss_delta"),
    ("polychoric", "polychoric_correlation"),
    ("Goodman-Kruskal", "goodman_kruskal_gamma"),
    ("r2", "r2_linkage_disequilibrium"),
    ("LD r2", "r2_linkage_disequilibrium"),
    ("Hedges", "hedges_g"),
    ("SSMD", "strictly_standardized_mean_difference"),
    ("correlation coefficient", "correlation_coefficient"),
)


def _normalize_effect_type(value: str) -> str:
    """Lower-case and strip separators/apostrophes for case/separator-insensitive matching."""
    # \u2019 is the right single quote (curly apostrophe) so both apostrophe styles normalize alike.
    return re.sub(r"[\s_.\-'\u2019]+", "", value.lower())


@lru_cache(maxsize=1)
def _effect_type_vocab() -> dict[str, str]:
    """Normalized alias table covering every permissible value plus common spellings (cached)."""
    table: dict[str, str] = {_normalize_effect_type(value): value for value in EFFECT_TYPE_VALUES}
    for raw, canonical in _EFFECT_TYPE_ALIASES:
        table[_normalize_effect_type(raw)] = canonical
    return table


@lru_cache(maxsize=4096)
def map_effect_type_value(raw: Any) -> str | None:
    """Map one raw effect-type/metric label to a canonical value, or None when nothing matches.

    Exact and alias matching are case/separator-insensitive. Free-form method
    labels ("Spearman Correlation", "Pearson correlation analysis") are matched
    by the longest alias embedded in the normalized label, so a method sentence
    still resolves to its metric. The Biolink range of ``effect_type`` is the
    enum, so values matching nothing are dropped to ``None`` rather than
    carried through.
    """
    if raw is None:
        return None
    text: str = str(raw).strip()
    if not text:
        return None
    table = _effect_type_vocab()
    key: str = _normalize_effect_type(text)
    hit: str | None = table.get(key)
    if hit is not None:
        return hit
    # Longest embedded alias wins ("spearmancorrelation" contains "spearman";
    # "pearsoncorrelationanalysis" contains both "pearson" and "correlation
    # coefficient" - the longer, more specific alias takes precedence).
    embedded: list[tuple[int, str]] = [
        (len(_normalize_effect_type(alias)), canonical)
        for alias, canonical in _EFFECT_TYPE_ALIASES
        if _normalize_effect_type(alias) and _normalize_effect_type(alias) in key
    ]
    if embedded:
        return max(embedded)[1]
    return None


# --- Statistical significance qualifier (Biolink PR #1766) --------------------
# Permissible values from StatisticalSignificanceQualifierEnum. Locally defined
# because the pinned biolink-model (4.4.2) predates the PR. Switch-over: replace
# with the installed model's enum values once the pin includes PR #1766.
SIGNIFICANCE_QUALIFIER_VALUES: tuple[str, ...] = (
    "very_strongly_significant",
    "strongly_significant",
    "significant",
    "suggestive",
    "not_significant",
)


def significance_qualifier(p_value: float | None) -> str | None:
    """Band a p-value into one of the five ``statistical_significance_qualifier`` values.

    Boundaries are conventional (alpha = 0.05 with suggestive/near bands) and
    hardcoded because the enum definitions are canonical. A null p-value bands
    to ``None``: the qualifier may only be set when the numeric slot is
    populated (Biolink class rule).
    """
    if p_value is None:
        return None
    if p_value <= 0.001:
        return "very_strongly_significant"
    if p_value <= 0.01:
        return "strongly_significant"
    if p_value <= 0.05:
        return "significant"
    if p_value <= 0.10:
        return "suggestive"
    return "not_significant"


# --- Custom Biolink slot overrides ---------------------------------------------
# Association slots from the user's custom Biolink Model additions that the
# pinned biolink-model (4.4.2) does not yet declare: ``effect_size`` /
# ``effect_type`` (PR #1774) and ``statistical_significance_qualifier``
# (PR #1766). Declared here so the ingest can carry them before the pin
# catches up; ``custom_association_class`` attaches only the slots a given
# base class is missing, so the overrides self-retire on model upgrade.
CUSTOM_SLOT_FIELDS: dict[str, Any] = {
    "effect_size": (Optional[float], None),
    "effect_type": (Optional[Literal[*EFFECT_TYPE_VALUES]], None),
    "statistical_significance_qualifier": (Optional[Literal[*SIGNIFICANCE_QUALIFIER_VALUES]], None),
}


@lru_cache(maxsize=None)
def custom_association_class(base_cls: type) -> type:
    """Extend a Biolink Association class with the custom override slots it lacks.

    The pinned biolink-model forbids extra fields, so slots it does not declare
    cannot ride on an edge until the model (or this override) provides them.
    The generated subclass declares exactly the missing custom slots, keeps the
    parent's validation config (lax numeric coercion, closed enum literals),
    and serializes identically - the koza JSONL writer emits these fields like
    any native slot. When the pinned model gains a slot natively, it stops
    being "missing" and the override for it silently disappears.
    """
    missing = {name: spec for name, spec in CUSTOM_SLOT_FIELDS.items() if name not in base_cls.model_fields}
    if not missing:
        return base_cls
    return create_model(f"{base_cls.__name__}WithCustomSlots", __base__=base_cls, **missing)


# --- Record-level coercion ------------------------------------------------------
# Slots ``coerce_record_types`` populates with a typed value. ``study_size`` is
# deliberately absent: in the current Biolink Model it lives on the inlined
# Study node (PR #1770), not on Association, so study-size-like columns stay in
# the ingest's untyped attribute overlays.
_NUMERIC_SLOT_COERCERS: dict[str, Any] = {
    "p_value": parse_optional_float,
    "adjusted_p_value": parse_optional_float,
    "effect_size": parse_optional_float,
}

# Fallback source column for ``effect_type`` when no effect-type-like column is
# present: the assertion method names the statistical metric ("Spearman
# Correlation"), which is exactly what the companion slot disambiguates.
_EFFECT_TYPE_FALLBACK_COLUMN = "assertion method"


class CoercedSlots(NamedTuple):
    """Result of coercing one edge record onto typed Biolink slots."""

    slots: dict[str, Any]
    claimed_columns: frozenset[str]


def _similarity(column: str, target: str) -> float:
    """Similarity between a column name and its canonical spaced target name."""
    return SequenceMatcher(None, column.lower(), target.replace("_", " ")).ratio()


def coerce_record_types(record: dict[str, Any]) -> CoercedSlots:
    """Coerce the statistical columns of an edge record onto typed Biolink slots.

    Every column name is classified (:func:`coerced_target`); the best
    candidate per canonical slot is coerced to the slot's expected type. An
    exact canonical column always wins over aliases; remaining ties resolve by
    similarity to the canonical name (difflib standing in for the rapidfuzz
    ranking Tablassert uses) and then by record order. Only parseable values
    claim a slot, so a leaked header or tissue label never pollutes a numeric
    slot - the column simply stays unclaimed for the untyped overlays.

    Beside the column-driven slots, two derived slots are populated:

    * ``statistical_significance_qualifier`` - banded from the raw p-value when
      present, else from the adjusted p-value (class rule: only set when a
      numeric significance slot is populated).
    * ``effect_type`` - from an effect-type-like column, else derived from the
      assertion method label; only set when ``effect_size`` is populated
      (class rule: the type disambiguates the size, never travels alone).

    Returns the typed slot values and the set of source columns they consumed;
    the caller can keep unclaimed columns in its untyped overlays without
    duplicating what already landed on a typed slot.
    """
    candidates: dict[str, list[str]] = {}
    for column in record:
        target = coerced_target(column)
        if target in _NUMERIC_SLOT_COERCERS or target == "effect_type":
            candidates.setdefault(target, []).append(column)

    slots: dict[str, Any] = {}
    claimed: set[str] = set()

    for target, columns in candidates.items():
        chosen = max(columns, key=lambda c: (_similarity(c, target), -columns.index(c)))
        if target in _NUMERIC_SLOT_COERCERS:
            value = _NUMERIC_SLOT_COERCERS[target](record[chosen])
            if value is not None:
                slots[target] = value
                claimed.add(chosen)

    # effect_type: classifier columns first, assertion method as fallback.
    effect_type_columns = candidates.get("effect_type", [])
    if effect_type_columns:
        chosen = max(effect_type_columns, key=lambda c: (_similarity(c, "effect_type"), -effect_type_columns.index(c)))
        mapped = map_effect_type_value(record[chosen])
        if mapped is not None:
            slots["effect_type"] = mapped
            claimed.add(chosen)
    elif record.get(_EFFECT_TYPE_FALLBACK_COLUMN):
        mapped = map_effect_type_value(record[_EFFECT_TYPE_FALLBACK_COLUMN])
        if mapped is not None:
            slots["effect_type"] = mapped

    # Class rule (PR #1774): effect_type may only be populated when effect_size
    # is also populated - the type is meaningless without the value it names.
    if "effect_type" in slots and "effect_size" not in slots:
        slots.pop("effect_type", None)
        claimed.difference_update(effect_type_columns)

    # Class rule (PR #1766): the significance qualifier needs a numeric
    # significance slot; prefer the raw p-value, fall back to adjusted.
    band_source = slots.get("p_value", slots.get("adjusted_p_value"))
    qualifier = significance_qualifier(band_source)
    if qualifier is not None:
        slots["statistical_significance_qualifier"] = qualifier

    return CoercedSlots(slots, frozenset(claimed))


__all__ = [
    "CUSTOM_SLOT_FIELDS",
    "EFFECT_TYPE_VALUES",
    "SIGNIFICANCE_QUALIFIER_VALUES",
    "CoercedSlots",
    "coerce_record_types",
    "coerced_target",
    "custom_association_class",
    "effect_size_target",
    "effect_type_target",
    "map_effect_type_value",
    "parse_optional_float",
    "parse_optional_int",
    "pvalue_target",
    "significance_qualifier",
    "study_size_target",
]
